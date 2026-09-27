// Package server serves Redis clients and routes their commands through Raft on the leader.
package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/simar-s2/quorumdb/internal/kv"
	"github.com/simar-s2/quorumdb/internal/raft"
	"github.com/simar-s2/quorumdb/internal/resp"
	"github.com/simar-s2/quorumdb/internal/transport"
)

const (
	version     = "0.1.0"
	maxPipeline = 1024
)

type Config struct {
	ID        string
	RedisAddr string            // client listen address
	RaftAddr  string            // peer listen address
	Peers     map[string]string // member ID -> peer address, for all members
	DataDir   string
	// Raft holds timing and snapshot settings; ID, Peers, DataDir and Logger are filled in by New.
	Raft           raft.Config
	CommandTimeout time.Duration
	Logger         *slog.Logger

	// Unsafe modes, only used as negative controls to show the chaos test catches violations.
	UnsafeLocalReads bool // serve reads from local state on any node, skipping ReadIndex
	UnsafeEarlyAck   bool // acknowledge SET as soon as the leader appends it, before commit
}

type Server struct {
	cfg     Config
	logger  *slog.Logger
	store   *kv.Store
	raft    *raft.Raft
	trans   *transport.TCP
	ln      net.Listener
	started time.Time

	commands  atomic.Uint64
	forwarded atomic.Uint64

	mu     sync.Mutex
	closed bool
	conns  map[net.Conn]struct{}
	stop   chan struct{}
	wg     sync.WaitGroup
}

// New recovers Raft state, starts the peer listener and then the client listener.
func New(cfg Config) (*Server, error) {
	if cfg.CommandTimeout == 0 {
		cfg.CommandTimeout = 5 * time.Second
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	var ids []string
	others := map[string]string{}
	for id, addr := range cfg.Peers {
		ids = append(ids, id)
		if id != cfg.ID {
			others[id] = addr
		}
	}
	if _, ok := cfg.Peers[cfg.ID]; !ok {
		ids = append(ids, cfg.ID)
	}
	sort.Strings(ids)

	trans, err := transport.Listen(cfg.RaftAddr, others, cfg.Logger)
	if err != nil {
		return nil, fmt.Errorf("peer listener: %w", err)
	}
	store := kv.NewStore()
	rc := cfg.Raft
	rc.ID, rc.Peers, rc.DataDir, rc.Logger = cfg.ID, ids, cfg.DataDir, cfg.Logger
	r, err := raft.New(rc, trans, store)
	if err != nil {
		trans.Close()
		return nil, err
	}
	s := &Server{
		cfg:     cfg,
		logger:  cfg.Logger.With("node", cfg.ID),
		store:   store,
		raft:    r,
		trans:   trans,
		started: time.Now(),
		conns:   map[net.Conn]struct{}{},
		stop:    make(chan struct{}),
	}
	if err := trans.Serve(r, s); err != nil {
		trans.Close()
		return nil, err
	}
	r.Start()

	ln, err := net.Listen("tcp", cfg.RedisAddr)
	if err != nil {
		r.Shutdown()
		trans.Close()
		return nil, fmt.Errorf("client listener: %w", err)
	}
	s.ln = ln
	s.wg.Add(2)
	go s.acceptLoop()
	go s.expireLoop()
	s.logger.Info("quorumdb node started", "redis_addr", ln.Addr().String(), "raft_addr", trans.Addr().String(), "members", strings.Join(ids, ","))
	return s, nil
}

func (s *Server) Addr() net.Addr { return s.ln.Addr() }

// Raft exposes the consensus module (used by tests).
func (s *Server) Raft() *raft.Raft { return s.raft }

// Close stops the listeners and connections, then shuts down Raft, which flushes the log.
func (s *Server) Close() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	close(s.stop)
	s.ln.Close()
	for c := range s.conns {
		c.Close()
	}
	s.mu.Unlock()
	s.wg.Wait()
	s.trans.Close()
	s.raft.Shutdown()
}

func (s *Server) acceptLoop() {
	defer s.wg.Done()
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			select {
			case <-s.stop:
				return
			default:
			}
			s.logger.Warn("accept failed", "err", err)
			time.Sleep(10 * time.Millisecond)
			continue
		}
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			conn.Close()
			return
		}
		s.conns[conn] = struct{}{}
		s.wg.Add(1)
		s.mu.Unlock()
		go s.handleConn(conn)
	}
}

// handleConn serves one client, executing each burst of pipelined commands as one batch.
func (s *Server) handleConn(conn net.Conn) {
	defer func() {
		conn.Close()
		s.mu.Lock()
		delete(s.conns, conn)
		s.mu.Unlock()
		s.wg.Done()
	}()
	rd := resp.NewReader(conn)
	var out []byte
	batch := make([][][]byte, 0, 16)
	for {
		batch = batch[:0]
		cmd, err := rd.ReadCommand()
		if err != nil {
			if errors.Is(err, resp.ErrProtocol) {
				conn.Write(resp.AppendError(nil, "ERR Protocol error: "+strings.TrimPrefix(err.Error(), "protocol error: ")))
			}
			return
		}
		batch = append(batch, cmd)
		var readErr error
		for rd.Buffered() > 0 && len(batch) < maxPipeline {
			cmd, err := rd.ReadCommand()
			if err != nil {
				readErr = err
				break
			}
			batch = append(batch, cmd)
		}

		replies, quit := s.execBatch(batch)
		out = out[:0]
		for _, r := range replies {
			out = append(out, r...)
		}
		if errors.Is(readErr, resp.ErrProtocol) {
			out = resp.AppendError(out, "ERR Protocol error: "+strings.TrimPrefix(readErr.Error(), "protocol error: "))
		}
		if _, err := conn.Write(out); err != nil || quit || readErr != nil {
			return
		}
	}
}

// execBatch answers commands in order: local ones here, the rest routed to the leader together.
func (s *Server) execBatch(batch [][][]byte) (replies [][]byte, quit bool) {
	s.commands.Add(uint64(len(batch)))
	replies = make([][]byte, len(batch))
	var routed [][][]byte
	var pos []int
	readOnly := true
	for i, args := range batch {
		c, errReply := lookup(args)
		switch {
		case errReply != nil:
			replies[i] = errReply
		case c.class == classLocal:
			replies[i] = c.local(s, nil, args)
			if c.name == "quit" {
				return replies[:i+1], true
			}
		default:
			routed = append(routed, args)
			pos = append(pos, i)
			readOnly = readOnly && c.class == classRead
		}
	}
	if len(routed) > 0 {
		for k, r := range s.route(routed, readOnly) {
			replies[pos[k]] = r
		}
	}
	return replies, false
}

// route runs commands on the leader, directly or by forwarding, retrying only if provably not executed.
func (s *Server) route(cmds [][][]byte, readOnly bool) [][]byte {
	if readOnly && s.cfg.UnsafeLocalReads {
		now := time.Now().UnixMilli()
		out := make([][]byte, len(cmds))
		for k, args := range cmds {
			out[k] = commands[strings.ToLower(string(args[0]))].read(s, nil, args, now)
		}
		return out
	}
	ctx, cancel := context.WithTimeout(context.Background(), s.cfg.CommandTimeout)
	defer cancel()
	for {
		leader, isLeader := s.raft.Leader()
		switch {
		case isLeader:
			out, err := s.execLeader(ctx, cmds)
			if err == nil {
				return out
			}
			if !errors.Is(err, raft.ErrNotLeader) {
				return fill(len(cmds), errorReply(err))
			}
		case leader != "":
			out, err := s.trans.Forward(ctx, leader, cmds)
			if err == nil {
				s.forwarded.Add(uint64(len(cmds)))
				return out
			}
			retryable := readOnly || errors.Is(err, raft.ErrNotLeader) || errors.Is(err, transport.ErrNotSent)
			if !retryable {
				return fill(len(cmds), resp.AppendError(nil, "TRYAGAIN lost connection to the leader, outcome unknown"))
			}
		}
		select {
		case <-ctx.Done():
			if leader == "" {
				return fill(len(cmds), resp.AppendError(nil, "CLUSTERDOWN no leader elected"))
			}
			return fill(len(cmds), resp.AppendError(nil, "TIMEOUT could not reach the leader"))
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// HandleForward runs commands forwarded by a follower.
func (s *Server) HandleForward(cmds [][][]byte) ([][]byte, error) {
	for _, args := range cmds {
		if len(args) == 0 {
			return nil, errors.New("empty command")
		}
		if c, errReply := lookup(args); errReply != nil || c.class == classLocal {
			return nil, errors.New("forwarded command is not routable")
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), s.cfg.CommandTimeout)
	defer cancel()
	return s.execLeader(ctx, cmds)
}

// execLeader runs commands in order, batching consecutive writes and consecutive reads.
func (s *Server) execLeader(ctx context.Context, cmds [][][]byte) ([][]byte, error) {
	out := make([][]byte, len(cmds))
	for i := 0; i < len(cmds); {
		cls := commands[strings.ToLower(string(cmds[i][0]))].class
		j := i + 1
		for j < len(cmds) && commands[strings.ToLower(string(cmds[j][0]))].class == cls {
			j++
		}
		var err error
		if cls == classWrite {
			err = s.execWrites(ctx, cmds[i:j], out[i:j])
		} else {
			err = s.execReads(ctx, cmds[i:j], out[i:j])
		}
		if err != nil {
			if i == 0 && errors.Is(err, raft.ErrNotLeader) {
				return nil, err
			}
			reply := errorReply(err)
			for k := i; k < len(cmds); k++ {
				if out[k] == nil {
					out[k] = reply
				}
			}
			return out, nil
		}
		i = j
	}
	return out, nil
}

// execWrites proposes writes and waits for each to be committed by a majority and applied.
func (s *Server) execWrites(ctx context.Context, cmds [][][]byte, out [][]byte) error {
	now := time.Now().UnixMilli()
	data := make([][]byte, 0, len(cmds))
	pos := make([]int, 0, len(cmds))
	for k, args := range cmds {
		c, errReply := commands[strings.ToLower(string(args[0]))].write(args, now)
		if errReply != nil {
			out[k] = errReply
			continue
		}
		data = append(data, c.Encode())
		pos = append(pos, k)
	}
	if len(data) == 0 {
		return nil
	}
	futs, err := s.raft.Propose(data)
	if err != nil {
		return err
	}
	for x, f := range futs {
		if s.cfg.UnsafeEarlyAck && strings.EqualFold(string(cmds[pos[x]][0]), "set") && len(cmds[pos[x]]) == 3 {
			out[pos[x]] = resp.AppendSimple(nil, "OK")
			continue
		}
		res, err := f.Wait(ctx)
		if err != nil {
			out[pos[x]] = errorReply(err)
			continue
		}
		out[pos[x]] = encodeResult(nil, res)
	}
	return nil
}

// execReads confirms leadership with ReadIndex, waits for that index to apply, then reads locally.
func (s *Server) execReads(ctx context.Context, cmds [][][]byte, out [][]byte) error {
	idx, err := s.raft.ReadIndex(ctx)
	if err != nil {
		return err
	}
	if err := s.raft.WaitApplied(ctx, idx); err != nil {
		return err
	}
	now := time.Now().UnixMilli()
	for k, args := range cmds {
		out[k] = commands[strings.ToLower(string(args[0]))].read(s, nil, args, now)
	}
	return nil
}

func errorReply(err error) []byte {
	switch {
	case errors.Is(err, raft.ErrNotLeader):
		return resp.AppendError(nil, "TRYAGAIN leadership changed, retry the request")
	case errors.Is(err, raft.ErrLeadershipLost):
		return resp.AppendError(nil, "TRYAGAIN leadership changed before the write committed, outcome unknown")
	case errors.Is(err, context.DeadlineExceeded):
		return resp.AppendError(nil, "TIMEOUT no majority confirmed the request in time, outcome unknown")
	case errors.Is(err, raft.ErrShutdown):
		return resp.AppendError(nil, "ERR node is shutting down")
	}
	return resp.AppendError(nil, "ERR "+err.Error())
}

func fill(n int, reply []byte) [][]byte {
	out := make([][]byte, n)
	for i := range out {
		out[i] = reply
	}
	return out
}

// expireLoop has the leader propose a sweep so expired keys are freed on every replica.
func (s *Server) expireLoop() {
	defer s.wg.Done()
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-s.stop:
			return
		case <-t.C:
		}
		now := time.Now().UnixMilli()
		if _, leader := s.raft.Leader(); !leader || !s.store.HasExpired(now) {
			continue
		}
		c := &kv.Command{Op: kv.OpSweep, Now: now}
		if futs, err := s.raft.Propose([][]byte{c.Encode()}); err == nil {
			ctx, cancel := context.WithTimeout(context.Background(), s.cfg.CommandTimeout)
			futs[0].Wait(ctx)
			cancel()
		}
	}
}

func cmdInfo(s *Server, b []byte, args [][]byte) []byte {
	section := "all"
	if len(args) > 1 {
		section = strings.ToLower(string(args[1]))
	}
	st := s.raft.Status()
	var sb strings.Builder
	want := func(name string) bool {
		return section == "all" || section == "default" || section == "everything" || section == name
	}
	if want("server") {
		fmt.Fprintf(&sb, "# Server\r\nredis_version:7.0.0\r\nquorumdb_version:%s\r\nnode_id:%s\r\nredis_addr:%s\r\nraft_addr:%s\r\nuptime_in_seconds:%d\r\n\r\n",
			version, s.cfg.ID, s.ln.Addr(), s.trans.Addr(), int(time.Since(s.started).Seconds()))
	}
	if want("raft") || want("replication") {
		leader := st.LeaderID
		if leader == "" {
			leader = "none"
		}
		fmt.Fprintf(&sb, "# Raft\r\nrole:%s\r\nterm:%d\r\nleader_id:%s\r\ncommit_index:%d\r\nlast_applied:%d\r\nlast_log_index:%d\r\nlast_log_term:%d\r\nsnapshot_index:%d\r\nmembers:%s\r\n\r\n",
			st.State, st.Term, leader, st.CommitIndex, st.LastApplied, st.LastLogIndex, st.LastLogTerm, st.SnapshotIndex, strings.Join(s.memberIDs(), ","))
	}
	if want("stats") {
		fmt.Fprintf(&sb, "# Stats\r\ntotal_commands_processed:%d\r\nforwarded_commands:%d\r\n\r\n", s.commands.Load(), s.forwarded.Load())
	}
	if want("keyspace") {
		fmt.Fprintf(&sb, "# Keyspace\r\ndb0:keys=%d\r\n", s.store.Len())
	}
	return resp.AppendBulkString(b, strings.TrimRight(sb.String(), "\r\n")+"\r\n")
}

func (s *Server) memberIDs() []string {
	ids := make([]string, 0, len(s.cfg.Peers)+1)
	for id := range s.cfg.Peers {
		ids = append(ids, id)
	}
	if _, ok := s.cfg.Peers[s.cfg.ID]; !ok {
		ids = append(ids, s.cfg.ID)
	}
	sort.Strings(ids)
	return ids
}
