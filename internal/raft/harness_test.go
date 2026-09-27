package raft

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"math/rand/v2"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// This file is the test harness: an in-memory network that can partition
// nodes, cut single links and drop or delay messages, plus a cluster helper
// that checks safety properties on every step.

var errUnreachable = errors.New("test network: unreachable")

type memNetwork struct {
	mu         sync.Mutex
	nodes      map[string]*Raft
	down       map[string]bool
	cut        map[[2]string]bool // one-way: from -> to
	unreliable bool
	rpcs       atomic.Int64
}

func newMemNetwork() *memNetwork {
	return &memNetwork{nodes: map[string]*Raft{}, down: map[string]bool{}, cut: map[[2]string]bool{}}
}

func (n *memNetwork) reachableLocked(from, to string) bool {
	return !n.down[from] && !n.down[to] && !n.cut[[2]string{from, to}]
}

type memTransport struct {
	net  *memNetwork
	from string
}

// deliver runs call on the target node if the network allows it, and drops
// the reply if the return path is broken.
func deliver[T any](t *memTransport, ctx context.Context, to string, call func(*Raft) T) (T, error) {
	var zero T
	n := t.net
	n.rpcs.Add(1)
	n.mu.Lock()
	node := n.nodes[to]
	ok := node != nil && n.reachableLocked(t.from, to)
	unreliable := n.unreliable
	n.mu.Unlock()

	if unreliable {
		time.Sleep(time.Duration(rand.IntN(5)) * time.Millisecond)
		if rand.IntN(10) == 0 {
			ok = false
		}
	}
	if !ok {
		select {
		case <-time.After(time.Duration(1+rand.IntN(10)) * time.Millisecond):
		case <-ctx.Done():
		}
		return zero, errUnreachable
	}
	ch := make(chan T, 1)
	go func() { ch <- call(node) }()
	var v T
	select {
	case v = <-ch:
	case <-ctx.Done():
		return zero, ctx.Err()
	}
	n.mu.Lock()
	back := n.nodes[t.from] != nil && n.reachableLocked(to, t.from)
	n.mu.Unlock()
	if !back || unreliable && rand.IntN(10) == 0 {
		return zero, errUnreachable
	}
	return v, nil
}

func (t *memTransport) RequestVote(ctx context.Context, to string, a *RequestVoteArgs) (*RequestVoteReply, error) {
	return deliver(t, ctx, to, func(r *Raft) *RequestVoteReply { return r.HandleRequestVote(a) })
}

func (t *memTransport) AppendEntries(ctx context.Context, to string, a *AppendEntriesArgs) (*AppendEntriesReply, error) {
	return deliver(t, ctx, to, func(r *Raft) *AppendEntriesReply { return r.HandleAppendEntries(a) })
}

func (t *memTransport) InstallSnapshot(ctx context.Context, to string, a *InstallSnapshotArgs) (*InstallSnapshotReply, error) {
	return deliver(t, ctx, to, func(r *Raft) *InstallSnapshotReply { return r.HandleInstallSnapshot(a) })
}

// testFSM records which value was applied at which index.
type testFSM struct {
	c    *cluster
	id   string
	mu   sync.Mutex
	last uint64
	vals map[uint64]string
}

type fsmSnapshot struct {
	Last uint64
	Vals map[uint64]string
}

func (f *testFSM) Apply(index uint64, data []byte) any {
	f.mu.Lock()
	if index <= f.last {
		f.c.fail("%s applied index %d after %d", f.id, index, f.last)
	}
	f.last = index
	f.vals[index] = string(data)
	f.mu.Unlock()
	f.c.recordApply(f.id, index, string(data))
	return index
}

func (f *testFSM) Snapshot() func() ([]byte, error) {
	f.mu.Lock()
	snap := fsmSnapshot{Last: f.last, Vals: maps.Clone(f.vals)}
	f.mu.Unlock()
	return func() ([]byte, error) {
		var buf bytes.Buffer
		err := gob.NewEncoder(&buf).Encode(snap)
		return buf.Bytes(), err
	}
}

func (f *testFSM) Restore(data []byte) error {
	var s fsmSnapshot
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&s); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if s.Vals == nil {
		s.Vals = map[uint64]string{}
	}
	f.last, f.vals = s.Last, s.Vals
	return nil
}

func (f *testFSM) get(index uint64) (string, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	v, ok := f.vals[index]
	return v, ok
}

type cluster struct {
	t     *testing.T
	ids   []string
	dirs  []string
	net   *memNetwork
	rafts []*Raft
	fsms  []*testFSM
	opts  []func(*Config)

	mu        sync.Mutex
	committed map[uint64]string   // first value seen applied at each index
	leaders   map[uint64][]string // term -> nodes that won it
	errs      []string
}

var quietLogger = slog.New(slog.NewTextHandler(io.Discard, nil))

func newCluster(t *testing.T, n int, opts ...func(*Config)) *cluster {
	t.Helper()
	c := &cluster{
		t:         t,
		net:       newMemNetwork(),
		rafts:     make([]*Raft, n),
		fsms:      make([]*testFSM, n),
		opts:      opts,
		committed: map[uint64]string{},
		leaders:   map[uint64][]string{},
	}
	base := t.TempDir()
	for i := 0; i < n; i++ {
		c.ids = append(c.ids, fmt.Sprintf("n%d", i+1))
		c.dirs = append(c.dirs, filepath.Join(base, fmt.Sprintf("n%d", i+1)))
	}
	for i := 0; i < n; i++ {
		c.start(i)
	}
	t.Cleanup(c.shutdown)
	return c
}

func (c *cluster) config(i int) Config {
	cfg := Config{
		ID:                c.ids[i],
		Peers:             c.ids,
		DataDir:           c.dirs[i],
		HeartbeatInterval: 20 * time.Millisecond,
		ElectionTimeout:   150 * time.Millisecond,
		SnapshotThreshold: 1 << 30,
		Logger:            quietLogger,
	}
	if testing.Verbose() && os.Getenv("RAFT_DEBUG") != "" {
		cfg.Logger = slog.Default()
	}
	for _, o := range c.opts {
		o(&cfg)
	}
	return cfg
}

func (c *cluster) start(i int) {
	c.t.Helper()
	fsm := &testFSM{c: c, id: c.ids[i], vals: map[uint64]string{}}
	r, err := New(c.config(i), &memTransport{net: c.net, from: c.ids[i]}, fsm)
	if err != nil {
		c.t.Fatalf("starting %s: %v", c.ids[i], err)
	}
	r.leaderHook = func(id string, term uint64) {
		c.mu.Lock()
		defer c.mu.Unlock()
		c.leaders[term] = append(c.leaders[term], id)
		if len(c.leaders[term]) > 1 {
			c.errs = append(c.errs, fmt.Sprintf("election safety violated: term %d has leaders %v", term, c.leaders[term]))
		}
	}
	c.net.mu.Lock()
	c.rafts[i], c.fsms[i] = r, fsm
	c.net.nodes[c.ids[i]] = r
	c.net.mu.Unlock()
	r.Start()
}

// crash stops node i as if the process died. Its disk state is kept.
func (c *cluster) crash(i int) {
	c.net.mu.Lock()
	delete(c.net.nodes, c.ids[i])
	r := c.rafts[i]
	c.rafts[i], c.fsms[i] = nil, nil
	c.net.mu.Unlock()
	if r != nil {
		r.Shutdown()
	}
}

// node and fsm return the current instance for slot i (nil while crashed).
// Workers use them because crash and start swap the slots concurrently.
func (c *cluster) node(i int) *Raft {
	c.net.mu.Lock()
	defer c.net.mu.Unlock()
	return c.rafts[i]
}

func (c *cluster) fsm(i int) *testFSM {
	c.net.mu.Lock()
	defer c.net.mu.Unlock()
	return c.fsms[i]
}

func (c *cluster) restart(i int) {
	c.crash(i)
	c.start(i)
}

func (c *cluster) shutdown() {
	for i := range c.rafts {
		c.crash(i)
	}
	c.checkErrors()
}

func (c *cluster) disconnect(i int) {
	c.net.mu.Lock()
	c.net.down[c.ids[i]] = true
	c.net.mu.Unlock()
}

func (c *cluster) connect(i int) {
	c.net.mu.Lock()
	delete(c.net.down, c.ids[i])
	c.net.mu.Unlock()
}

func (c *cluster) setUnreliable(v bool) {
	c.net.mu.Lock()
	c.net.unreliable = v
	c.net.mu.Unlock()
}

func (c *cluster) isUp(i int) bool {
	c.net.mu.Lock()
	defer c.net.mu.Unlock()
	return c.rafts[i] != nil && !c.net.down[c.ids[i]]
}

func (c *cluster) fail(format string, args ...any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.errs = append(c.errs, fmt.Sprintf(format, args...))
}

// recordApply checks State Machine Safety: no two nodes (or two lives of one
// node) ever apply different values at the same index.
func (c *cluster) recordApply(id string, index uint64, v string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if prev, ok := c.committed[index]; ok && prev != v {
		c.errs = append(c.errs, fmt.Sprintf("%s applied %q at index %d, but %q was applied there before", id, v, index, prev))
		return
	}
	c.committed[index] = v
}

func (c *cluster) checkErrors() {
	c.t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, e := range c.errs {
		c.t.Error(e)
	}
	c.errs = nil
}

// checkOneLeader waits for exactly one leader among connected nodes and
// returns its position.
func (c *cluster) checkOneLeader() int {
	c.t.Helper()
	for try := 0; try < 50; try++ {
		time.Sleep(50 * time.Millisecond)
		byTerm := map[uint64][]int{}
		var top uint64
		for i, r := range c.rafts {
			if !c.isUp(i) {
				continue
			}
			if s := r.Status(); s.State == Leader {
				byTerm[s.Term] = append(byTerm[s.Term], i)
				top = max(top, s.Term)
			}
		}
		for term, ls := range byTerm {
			if len(ls) > 1 {
				c.t.Fatalf("term %d has %d leaders", term, len(ls))
			}
		}
		if len(byTerm) > 0 {
			return byTerm[top][0]
		}
	}
	c.t.Fatal("expected one leader, got none")
	return -1
}

func (c *cluster) checkNoLeader() {
	c.t.Helper()
	for i, r := range c.rafts {
		if c.isUp(i) && r.Status().State == Leader {
			c.t.Fatalf("expected no leader among connected nodes, but %s is leader", c.ids[i])
		}
	}
}

func (c *cluster) checkTermsAgree() uint64 {
	c.t.Helper()
	var term uint64
	for i, r := range c.rafts {
		if !c.isUp(i) {
			continue
		}
		t := r.Status().Term
		if term == 0 {
			term = t
		} else if t != term {
			c.t.Fatalf("nodes disagree on term: %d vs %d", term, t)
		}
	}
	return term
}

// nCommitted returns how many running nodes have applied index, and the
// value, checking that they agree.
func (c *cluster) nCommitted(index uint64) (int, string) {
	count, val := 0, ""
	for i := range c.fsms {
		f := c.fsm(i)
		if f == nil {
			continue
		}
		v, ok := f.get(index)
		if !ok {
			continue
		}
		if count > 0 && v != val {
			c.t.Fatalf("index %d: %s has %q, others have %q", index, c.ids[i], v, val)
		}
		count, val = count+1, v
	}
	return count, val
}

func (c *cluster) propose(i int, cmd string) (*Future, error) {
	r := c.node(i)
	if r == nil {
		return nil, ErrShutdown
	}
	futs, err := r.Propose([][]byte{[]byte(cmd)})
	if err != nil {
		return nil, err
	}
	return futs[0], nil
}

// one submits cmd to whichever node is leader and waits until at least
// expected nodes have applied it. It retries across leader changes, like a
// client would, and returns the entry's index.
func (c *cluster) one(cmd string, expected int) uint64 {
	c.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	start := 0
	for time.Now().Before(deadline) {
		for k := range c.rafts {
			i := (start + k) % len(c.rafts)
			if !c.isUp(i) {
				continue
			}
			f, err := c.propose(i, cmd)
			if err != nil {
				continue
			}
			start = i
			until := time.Now().Add(2 * time.Second)
			for time.Now().Before(until) {
				if n, v := c.nCommitted(f.Index); n >= expected && v == cmd {
					return f.Index
				}
				time.Sleep(10 * time.Millisecond)
			}
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	c.t.Fatalf("one(%q) did not reach agreement on %d nodes", cmd, expected)
	return 0
}

// waitApplied waits until every running node has applied index.
func (c *cluster) waitApplied(index uint64) {
	c.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		done := true
		for i, r := range c.rafts {
			if c.isUp(i) && r.Status().LastApplied < index {
				done = false
			}
		}
		if done {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	c.t.Fatalf("not all nodes applied index %d", index)
}
