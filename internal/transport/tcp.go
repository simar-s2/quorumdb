// Package transport carries traffic between cluster members: Raft RPCs and
// client commands forwarded from followers to the leader. It uses Go's
// net/rpc with gob encoding over long-lived TCP connections; each connection
// multiplexes concurrent calls.
package transport

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/rpc"
	"sync"
	"time"

	"github.com/simar-s2/quorumdb/internal/raft"
)

const dialTimeout = time.Second

// ErrNotSent means the request never left this node (the dial failed), so
// retrying it cannot execute it twice.
var ErrNotSent = errors.New("transport: request not sent")

// RaftHandler receives Raft RPCs; *raft.Raft implements it.
type RaftHandler interface {
	HandleRequestVote(*raft.RequestVoteArgs) *raft.RequestVoteReply
	HandleAppendEntries(*raft.AppendEntriesArgs) *raft.AppendEntriesReply
	HandleInstallSnapshot(*raft.InstallSnapshotArgs) *raft.InstallSnapshotReply
}

// ForwardHandler executes client commands forwarded by a follower. It
// returns one encoded RESP reply per command, or raft.ErrNotLeader if it
// executed nothing because it is not the leader.
type ForwardHandler interface {
	HandleForward(cmds [][][]byte) ([][]byte, error)
}

type ForwardArgs struct{ Commands [][][]byte }
type ForwardReply struct{ Replies [][]byte }

// Raft RPCs and forwarded commands use separate connections, so a slow
// client request never delays heartbeats.
type channel uint8

const (
	chanRaft channel = iota
	chanForward
)

type poolKey struct {
	peer string
	ch   channel
}

type peerConn struct {
	mu     sync.Mutex // held while dialing
	client *rpc.Client
}

type TCP struct {
	addrs  map[string]string // peer ID -> address
	ln     net.Listener
	srv    *rpc.Server
	logger *slog.Logger

	mu      sync.Mutex
	closed  bool
	pools   map[poolKey]*peerConn
	inbound map[net.Conn]struct{}
}

// Listen opens the listener for peer traffic. peers maps member IDs to their
// addresses.
func Listen(addr string, peers map[string]string, logger *slog.Logger) (*TCP, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &TCP{
		addrs:   peers,
		ln:      ln,
		srv:     rpc.NewServer(),
		logger:  logger,
		pools:   make(map[poolKey]*peerConn),
		inbound: make(map[net.Conn]struct{}),
	}, nil
}

func (t *TCP) Addr() net.Addr { return t.ln.Addr() }

// Serve registers the handlers and starts accepting connections.
func (t *TCP) Serve(rh RaftHandler, fh ForwardHandler) error {
	if err := t.srv.RegisterName("Raft", &raftService{rh}); err != nil {
		return err
	}
	if err := t.srv.RegisterName("KV", &kvService{fh}); err != nil {
		return err
	}
	go t.acceptLoop()
	return nil
}

func (t *TCP) acceptLoop() {
	for {
		conn, err := t.ln.Accept()
		if err != nil {
			t.mu.Lock()
			closed := t.closed
			t.mu.Unlock()
			if closed {
				return
			}
			t.logger.Warn("transport: accept failed", "err", err)
			time.Sleep(10 * time.Millisecond)
			continue
		}
		t.mu.Lock()
		t.inbound[conn] = struct{}{}
		t.mu.Unlock()
		go func() {
			t.srv.ServeConn(conn)
			t.mu.Lock()
			delete(t.inbound, conn)
			t.mu.Unlock()
		}()
	}
}

// Close stops the listener and closes every connection.
func (t *TCP) Close() error {
	t.mu.Lock()
	t.closed = true
	for c := range t.inbound {
		c.Close()
	}
	pools := t.pools
	t.pools = map[poolKey]*peerConn{}
	t.mu.Unlock()
	for _, pc := range pools {
		pc.mu.Lock()
		if pc.client != nil {
			pc.client.Close()
		}
		pc.mu.Unlock()
	}
	return t.ln.Close()
}

func (t *TCP) client(ctx context.Context, peer string, ch channel) (*rpc.Client, error) {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil, errors.New("transport closed")
	}
	addr, ok := t.addrs[peer]
	if !ok {
		t.mu.Unlock()
		return nil, fmt.Errorf("unknown peer %q", peer)
	}
	key := poolKey{peer, ch}
	pc := t.pools[key]
	if pc == nil {
		pc = &peerConn{}
		t.pools[key] = pc
	}
	t.mu.Unlock()

	pc.mu.Lock()
	defer pc.mu.Unlock()
	if pc.client != nil {
		return pc.client, nil
	}
	dctx, cancel := context.WithTimeout(ctx, dialTimeout)
	defer cancel()
	var d net.Dialer
	conn, err := d.DialContext(dctx, "tcp", addr)
	if err != nil {
		return nil, err
	}
	pc.client = rpc.NewClient(conn)
	return pc.client, nil
}

// drop discards a connection after a failure so the next call redials. This
// is also how a silently dropped (blackholed) connection is recovered: the
// call times out, the connection is closed, and a new one is made.
func (t *TCP) drop(peer string, ch channel, c *rpc.Client) {
	t.mu.Lock()
	pc := t.pools[poolKey{peer, ch}]
	t.mu.Unlock()
	if pc != nil {
		pc.mu.Lock()
		if pc.client == c {
			pc.client = nil
		}
		pc.mu.Unlock()
	}
	c.Close()
}

func (t *TCP) call(ctx context.Context, peer string, ch channel, method string, args, reply any) error {
	c, err := t.client(ctx, peer, ch)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrNotSent, err)
	}
	call := c.Go(method, args, reply, make(chan *rpc.Call, 1))
	select {
	case <-call.Done:
		if call.Error != nil {
			var se rpc.ServerError
			if !errors.As(call.Error, &se) {
				t.drop(peer, ch, c)
			}
		}
		return call.Error
	case <-ctx.Done():
		t.drop(peer, ch, c)
		return ctx.Err()
	}
}

func (t *TCP) RequestVote(ctx context.Context, peer string, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	var reply raft.RequestVoteReply
	if err := t.call(ctx, peer, chanRaft, "Raft.RequestVote", args, &reply); err != nil {
		return nil, err
	}
	return &reply, nil
}

func (t *TCP) AppendEntries(ctx context.Context, peer string, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	var reply raft.AppendEntriesReply
	if err := t.call(ctx, peer, chanRaft, "Raft.AppendEntries", args, &reply); err != nil {
		return nil, err
	}
	return &reply, nil
}

func (t *TCP) InstallSnapshot(ctx context.Context, peer string, args *raft.InstallSnapshotArgs) (*raft.InstallSnapshotReply, error) {
	var reply raft.InstallSnapshotReply
	if err := t.call(ctx, peer, chanRaft, "Raft.InstallSnapshot", args, &reply); err != nil {
		return nil, err
	}
	return &reply, nil
}

// Forward sends client commands to the leader. A raft.ErrNotLeader or
// ErrNotSent error means nothing was executed; any other error leaves the
// outcome unknown.
func (t *TCP) Forward(ctx context.Context, leader string, cmds [][][]byte) ([][]byte, error) {
	var reply ForwardReply
	err := t.call(ctx, leader, chanForward, "KV.Forward", &ForwardArgs{Commands: cmds}, &reply)
	var se rpc.ServerError
	if errors.As(err, &se) && string(se) == raft.ErrNotLeader.Error() {
		return nil, raft.ErrNotLeader
	}
	if err != nil {
		return nil, err
	}
	return reply.Replies, nil
}

// Services exposed through net/rpc.

type raftService struct{ h RaftHandler }

func (s *raftService) RequestVote(args *raft.RequestVoteArgs, reply *raft.RequestVoteReply) error {
	*reply = *s.h.HandleRequestVote(args)
	return nil
}

func (s *raftService) AppendEntries(args *raft.AppendEntriesArgs, reply *raft.AppendEntriesReply) error {
	*reply = *s.h.HandleAppendEntries(args)
	return nil
}

func (s *raftService) InstallSnapshot(args *raft.InstallSnapshotArgs, reply *raft.InstallSnapshotReply) error {
	*reply = *s.h.HandleInstallSnapshot(args)
	return nil
}

type kvService struct{ h ForwardHandler }

func (s *kvService) Forward(args *ForwardArgs, reply *ForwardReply) error {
	replies, err := s.h.HandleForward(args.Commands)
	if err != nil {
		return err
	}
	reply.Replies = replies
	return nil
}
