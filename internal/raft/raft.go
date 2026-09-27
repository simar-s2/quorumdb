package raft

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"slices"
	"sync"
	"sync/atomic"
	"time"
)

const (
	maxApplyBatch   = 1024
	snapshotTimeout = 30 * time.Second
)

var errStepDown = errors.New("raft: no longer leader for this term")

// Raft is one member of a cluster. All state below mu is guarded by mu; the
// long-running goroutines are:
//
//	ticker     election timeouts, and check-quorum while leader
//	applier    applies committed entries to the FSM and takes snapshots
//	persister  fsyncs the leader's own log so it can count itself
//	replicate  one per follower while leader: AppendEntries/InstallSnapshot
type Raft struct {
	cfg    Config
	id     string
	peers  []string // other members
	quorum int
	trans  Transport
	fsm    FSM
	store  *storage
	logger *slog.Logger

	mu sync.Mutex

	// Persistent state (Figure 2). Saved to disk before answering any RPC
	// that depends on it.
	currentTerm uint64
	votedFor    string
	log         *memLog

	// Volatile state.
	state             State
	leaderID          string
	commitIndex       uint64
	lastApplied       uint64
	electionDeadline  time.Time
	lastLeaderContact time.Time

	// Leader state, reinitialized after every election win.
	nextIndex    map[string]uint64
	matchIndex   map[string]uint64
	lastAck      map[string]time.Time // last successful RPC round trip, for check-quorum
	ackSeq       map[string]uint64    // highest heartbeat sequence acknowledged, for ReadIndex
	durableIndex uint64               // leader's own log is fsynced up to here
	noopIndex    uint64
	hbSeq        uint64
	replCh       map[string]chan struct{}
	leaderCancel context.CancelFunc

	futures      map[uint64]*Future
	readWaiters  []*readWaiter
	applyWaiters []*applyWaiter

	applyCond    *sync.Cond
	persistCh    chan struct{}
	snapMu       sync.Mutex // serializes snapshot installation and creation
	snapshotting atomic.Bool

	shutdown   bool
	shutdownCh chan struct{}
	wg         sync.WaitGroup

	// leaderHook, if set by tests, is called (under mu) on every election win.
	leaderHook func(id string, term uint64)
}

// Future is returned by Propose and resolves once the entry is applied.
type Future struct {
	Index uint64
	Term  uint64
	done  chan struct{}
	res   any
	err   error
}

// Wait blocks until the entry is applied and returns the FSM's result. An
// error other than ErrNotLeader means the outcome is unknown.
func (f *Future) Wait(ctx context.Context) (any, error) {
	select {
	case <-f.done:
		return f.res, f.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

type readWaiter struct {
	seq uint64
	ch  chan error
}

type applyWaiter struct {
	index uint64
	ch    chan struct{}
}

// New restores a node from cfg.DataDir. Call Start to begin participating.
func New(cfg Config, trans Transport, fsm FSM) (*Raft, error) {
	cfg.setDefaults()
	store, rec, err := openStorage(cfg.DataDir, !cfg.NoSync)
	if err != nil {
		return nil, err
	}
	r := &Raft{
		cfg:        cfg,
		id:         cfg.ID,
		trans:      trans,
		fsm:        fsm,
		store:      store,
		logger:     cfg.Logger.With("node", cfg.ID),
		futures:    make(map[uint64]*Future),
		persistCh:  make(chan struct{}, 1),
		shutdownCh: make(chan struct{}),
	}
	for _, p := range cfg.Peers {
		if p != cfg.ID {
			r.peers = append(r.peers, p)
		}
	}
	r.quorum = (len(r.peers)+1)/2 + 1
	r.applyCond = sync.NewCond(&r.mu)
	r.currentTerm, r.votedFor = rec.term, rec.votedFor
	r.log = newMemLog(rec.snap.Index, rec.snap.Term, rec.entries)
	if rec.snapData != nil {
		if err := fsm.Restore(rec.snapData); err != nil {
			store.close()
			return nil, fmt.Errorf("raft: restoring snapshot: %w", err)
		}
		r.commitIndex, r.lastApplied = rec.snap.Index, rec.snap.Index
	}
	r.resetElectionTimer()
	r.logger.Info("raft: recovered state", "term", r.currentTerm, "voted_for", r.votedFor,
		"snapshot_index", r.log.snapIndex(), "last_index", r.log.lastIndex())
	return r, nil
}

// Start launches the background goroutines.
func (r *Raft) Start() {
	r.wg.Add(3)
	go r.runTicker()
	go r.runApplier()
	go r.runPersister()
}

// Shutdown stops the node and closes its storage. Pending proposals fail
// with ErrShutdown.
func (r *Raft) Shutdown() {
	r.mu.Lock()
	if r.shutdown {
		r.mu.Unlock()
		return
	}
	r.shutdown = true
	close(r.shutdownCh)
	if r.leaderCancel != nil {
		r.leaderCancel()
		r.leaderCancel = nil
	}
	r.state = Follower
	r.failReads(ErrShutdown)
	for idx, f := range r.futures {
		f.err = ErrShutdown
		close(f.done)
		delete(r.futures, idx)
	}
	r.applyCond.Broadcast()
	r.mu.Unlock()
	r.wg.Wait()
	r.store.close()
}

func (r *Raft) isShutdown() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.shutdown
}

// storageFailed crashes the node. If the disk cannot be written, the node
// can no longer keep the promises it made to the cluster, so continuing
// would be unsafe.
func (r *Raft) storageFailed(err error) {
	r.logger.Error("raft: storage failure", "err", err)
	panic(fmt.Sprintf("raft %s: storage failure: %v", r.id, err))
}

func (r *Raft) persistMeta() {
	if r.shutdown {
		return
	}
	if err := r.store.saveMeta(r.currentTerm, r.votedFor); err != nil {
		r.storageFailed(err)
	}
}

func (r *Raft) resetElectionTimer() {
	d := r.cfg.ElectionTimeout + rand.N(r.cfg.ElectionTimeout)
	r.electionDeadline = time.Now().Add(d)
}

func (r *Raft) rpcTimeout() time.Duration { return r.cfg.ElectionTimeout }

// ---------------------------------------------------------------------------
// Elections

func (r *Raft) runTicker() {
	defer r.wg.Done()
	t := time.NewTicker(r.cfg.HeartbeatInterval / 5)
	defer t.Stop()
	for {
		select {
		case <-r.shutdownCh:
			return
		case now := <-t.C:
			r.tick(now)
		}
	}
}

func (r *Raft) tick(now time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.shutdown {
		return
	}
	if r.state == Leader {
		r.checkQuorum(now)
		return
	}
	if now.After(r.electionDeadline) {
		r.campaign(true)
	}
}

// checkQuorum makes a leader that has not heard from a majority for a whole
// election timeout step down (thesis 6.2). A leader cut off in a minority
// partition then stops accepting requests it could never commit.
func (r *Raft) checkQuorum(now time.Time) {
	active := 1
	for _, p := range r.peers {
		if now.Sub(r.lastAck[p]) < r.cfg.ElectionTimeout {
			active++
		}
	}
	if active < r.quorum {
		r.logger.Warn("raft: lost contact with a majority, stepping down", "term", r.currentTerm)
		r.becomeFollower(r.currentTerm, "", false)
		r.resetElectionTimer()
	}
}

// campaign starts a pre-vote (pre=true) or a real election.
//
// Pre-vote (thesis 9.6): before incrementing its term, a node asks whether a
// majority would vote for it. Nodes that still hear from a leader say no, so
// a node returning from a partition cannot force a healthy leader to step
// down by showing up with an inflated term.
func (r *Raft) campaign(pre bool) {
	r.resetElectionTimer()
	r.leaderID = ""
	if pre {
		r.state = PreCandidate
	} else {
		r.state = Candidate
		r.currentTerm++
		r.votedFor = r.id
		r.persistMeta()
		r.logger.Info("raft: starting election", "term", r.currentTerm)
	}
	if len(r.peers) == 0 {
		if pre {
			r.campaign(false)
		} else {
			r.becomeLeader()
		}
		return
	}
	state, term := r.state, r.currentTerm
	args := &RequestVoteArgs{
		Term:         r.currentTerm,
		CandidateID:  r.id,
		LastLogIndex: r.log.lastIndex(),
		LastLogTerm:  r.log.lastTerm(),
		PreVote:      pre,
	}
	if pre {
		args.Term++
	}
	granted := 1 // our own vote
	for _, p := range r.peers {
		go func(peer string) {
			ctx, cancel := context.WithTimeout(context.Background(), r.rpcTimeout())
			defer cancel()
			reply, err := r.trans.RequestVote(ctx, peer, args)
			if err != nil {
				return
			}
			r.mu.Lock()
			defer r.mu.Unlock()
			if r.shutdown {
				return
			}
			if reply.Term > r.currentTerm && !reply.VoteGranted {
				r.becomeFollower(reply.Term, "", true)
				return
			}
			if r.state != state || r.currentTerm != term || !reply.VoteGranted {
				return
			}
			granted++
			if granted == r.quorum {
				if pre {
					r.campaign(false)
				} else {
					r.becomeLeader()
				}
			}
		}(p)
	}
}

// HandleRequestVote implements the RequestVote receiver (Figure 2) plus the
// pre-vote variant.
func (r *Raft) HandleRequestVote(args *RequestVoteArgs) *RequestVoteReply {
	r.mu.Lock()
	defer r.mu.Unlock()
	reply := &RequestVoteReply{Term: r.currentTerm}
	if r.shutdown || args.Term < r.currentTerm {
		return reply
	}
	// Election restriction (5.4.1): only vote for a candidate whose log is
	// at least as up to date as ours, so every leader holds every committed
	// entry.
	upToDate := args.LastLogTerm > r.log.lastTerm() ||
		args.LastLogTerm == r.log.lastTerm() && args.LastLogIndex >= r.log.lastIndex()

	if args.PreVote {
		heardFromLeader := r.state == Leader ||
			r.leaderID != "" && time.Since(r.lastLeaderContact) < r.cfg.ElectionTimeout
		reply.VoteGranted = upToDate && !heardFromLeader
		return reply
	}

	dirty := false
	if args.Term > r.currentTerm {
		r.becomeFollower(args.Term, "", false)
		dirty = true
	}
	if (r.votedFor == "" || r.votedFor == args.CandidateID) && upToDate {
		r.votedFor = args.CandidateID
		dirty = true
		reply.VoteGranted = true
		r.resetElectionTimer()
	}
	if dirty {
		// Term and vote must be durable before the vote leaves this node,
		// otherwise a restart could let us vote twice in one term.
		r.persistMeta()
	}
	reply.Term = r.currentTerm
	return reply
}

// becomeFollower adopts term if it is newer (clearing the vote) and steps
// down from leadership. persist=false lets a caller batch the metadata write
// with a vote.
func (r *Raft) becomeFollower(term uint64, leader string, persist bool) {
	if term > r.currentTerm {
		r.currentTerm = term
		r.votedFor = ""
		if persist {
			r.persistMeta()
		}
	}
	if r.state == Leader {
		r.leaderCancel()
		r.leaderCancel = nil
		r.failReads(ErrNotLeader)
		r.logger.Info("raft: stepped down", "term", r.currentTerm)
	}
	if leader != "" && leader != r.leaderID {
		r.logger.Info("raft: following leader", "leader", leader, "term", r.currentTerm)
	}
	r.state = Follower
	r.leaderID = leader
}

func (r *Raft) becomeLeader() {
	r.state = Leader
	r.leaderID = r.id
	now := time.Now()
	last := r.log.lastIndex()
	r.nextIndex = make(map[string]uint64)
	r.matchIndex = make(map[string]uint64)
	r.lastAck = make(map[string]time.Time)
	r.ackSeq = make(map[string]uint64)
	r.replCh = make(map[string]chan struct{})
	for _, p := range r.peers {
		r.nextIndex[p] = last + 1
		r.lastAck[p] = now
		r.ackSeq[p] = r.hbSeq
		r.replCh[p] = make(chan struct{}, 1)
	}
	r.durableIndex = 0
	ctx, cancel := context.WithCancel(context.Background())
	r.leaderCancel = cancel
	r.logger.Info("raft: became leader", "term", r.currentTerm, "last_index", last)
	if r.leaderHook != nil {
		r.leaderHook(r.id, r.currentTerm)
	}

	for _, p := range r.peers {
		r.wg.Add(1)
		go r.replicate(ctx, p, r.currentTerm, r.replCh[p])
	}
	// A new leader does not know which earlier-term entries are committed
	// and may not commit them by counting replicas (Figure 8). Committing a
	// no-op from its own term settles that.
	noop := Entry{Index: last + 1, Term: r.currentTerm, Type: EntryNoop}
	r.noopIndex = noop.Index
	r.appendLocal([]Entry{noop})
}

// ---------------------------------------------------------------------------
// Log replication, leader side

// appendLocal appends entries to the leader's log and WAL buffer. The fsync
// happens in the persister, in parallel with sending the entries to
// followers (thesis 10.2.1).
func (r *Raft) appendLocal(ents []Entry) {
	r.log.append(ents...)
	if _, err := r.store.wal.append(ents); err != nil {
		r.storageFailed(err)
	}
	signal(r.persistCh)
	r.triggerReplication()
}

func signal(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

func (r *Raft) triggerReplication() {
	for _, ch := range r.replCh {
		signal(ch)
	}
}

// runPersister fsyncs the leader's log. Only then does the leader count
// itself toward a majority. One fsync covers everything written so far.
func (r *Raft) runPersister() {
	defer r.wg.Done()
	for {
		select {
		case <-r.shutdownCh:
			return
		case <-r.persistCh:
		}
		r.mu.Lock()
		if r.state != Leader {
			r.mu.Unlock()
			continue
		}
		term, last := r.currentTerm, r.log.lastIndex()
		seq := r.store.wal.writtenSeq()
		r.mu.Unlock()

		if err := r.store.wal.syncTo(seq); err != nil {
			if r.isShutdown() {
				return
			}
			r.storageFailed(err)
		}

		r.mu.Lock()
		if r.state == Leader && r.currentTerm == term && last > r.durableIndex {
			r.durableIndex = last
			r.advanceCommit()
		}
		r.mu.Unlock()
	}
}

// replicate drives one follower: it sends whatever the follower is missing,
// or an empty heartbeat when idle. It keeps a single RPC in flight; entries
// that arrive meanwhile go out together in the next batch.
func (r *Raft) replicate(ctx context.Context, peer string, term uint64, trigger <-chan struct{}) {
	defer r.wg.Done()
	timer := time.NewTimer(0)
	defer timer.Stop()
	var backoff time.Duration
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		case <-trigger:
		}
		for {
			more, err := r.replicateOnce(ctx, peer, term)
			if errors.Is(err, errStepDown) {
				return
			}
			if err != nil {
				backoff = min(max(2*backoff, 10*time.Millisecond), r.cfg.HeartbeatInterval)
				select {
				case <-ctx.Done():
					return
				case <-time.After(backoff):
				}
				continue
			}
			backoff = 0
			if !more {
				break
			}
		}
		timer.Reset(r.cfg.HeartbeatInterval)
	}
}

// replicateOnce sends one AppendEntries (or InstallSnapshot) to peer. It
// reports whether the peer still has entries to catch up on.
func (r *Raft) replicateOnce(ctx context.Context, peer string, term uint64) (bool, error) {
	r.mu.Lock()
	if r.shutdown || r.state != Leader || r.currentTerm != term {
		r.mu.Unlock()
		return false, errStepDown
	}
	next := r.nextIndex[peer]
	seq := r.hbSeq
	if next <= r.log.snapIndex() {
		r.mu.Unlock()
		return r.sendSnapshot(ctx, peer, term, seq)
	}
	prev := next - 1
	prevTerm, _ := r.log.term(prev)
	hi := min(r.log.lastIndex()+1, next+uint64(r.cfg.MaxAppendEntries))
	args := &AppendEntriesArgs{
		Term:         term,
		LeaderID:     r.id,
		PrevLogIndex: prev,
		PrevLogTerm:  prevTerm,
		Entries:      r.log.slice(next, hi),
		LeaderCommit: r.commitIndex,
	}
	r.mu.Unlock()

	rctx, cancel := context.WithTimeout(ctx, r.rpcTimeout())
	reply, err := r.trans.AppendEntries(rctx, peer, args)
	cancel()
	if err != nil {
		return false, err
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.checkReplyTerm(reply.Term, term) {
		return false, errStepDown
	}
	r.recordAck(peer, seq)
	if reply.Success {
		match := prev + uint64(len(args.Entries))
		if match > r.matchIndex[peer] {
			r.matchIndex[peer] = match
			r.advanceCommit()
		}
		r.nextIndex[peer] = max(r.nextIndex[peer], match+1)
	} else {
		r.nextIndex[peer] = r.backtrack(reply)
	}
	return r.nextIndex[peer] <= r.log.lastIndex(), nil
}

// backtrack picks the next index to try after a consistency-check failure,
// skipping a whole conflicting term per round trip (the optimization at the
// end of section 5.3).
func (r *Raft) backtrack(reply *AppendEntriesReply) uint64 {
	if reply.ConflictTerm != 0 {
		if idx := r.log.lastIndexOfTerm(reply.ConflictTerm); idx != 0 {
			return idx + 1
		}
	}
	return max(reply.ConflictIndex, 1)
}

func (r *Raft) sendSnapshot(ctx context.Context, peer string, term, seq uint64) (bool, error) {
	meta, data, err := r.store.loadSnapshot()
	if err != nil {
		return false, err
	}
	args := &InstallSnapshotArgs{
		Term:              term,
		LeaderID:          r.id,
		LastIncludedIndex: meta.Index,
		LastIncludedTerm:  meta.Term,
		Data:              data,
	}
	rctx, cancel := context.WithTimeout(ctx, snapshotTimeout)
	reply, err := r.trans.InstallSnapshot(rctx, peer, args)
	cancel()
	if err != nil {
		return false, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.checkReplyTerm(reply.Term, term) {
		return false, errStepDown
	}
	r.recordAck(peer, seq)
	r.logger.Info("raft: sent snapshot", "peer", peer, "index", meta.Index, "bytes", len(data))
	if meta.Index > r.matchIndex[peer] {
		r.matchIndex[peer] = meta.Index
		r.advanceCommit()
	}
	r.nextIndex[peer] = max(r.nextIndex[peer], meta.Index+1)
	return r.nextIndex[peer] <= r.log.lastIndex(), nil
}

// checkReplyTerm steps down if the reply carries a newer term and reports
// whether we are still leader of term.
func (r *Raft) checkReplyTerm(replyTerm, term uint64) bool {
	if r.shutdown {
		return false
	}
	if replyTerm > r.currentTerm {
		r.becomeFollower(replyTerm, "", true)
		r.resetElectionTimer()
		return false
	}
	return r.state == Leader && r.currentTerm == term
}

func (r *Raft) recordAck(peer string, seq uint64) {
	r.lastAck[peer] = time.Now()
	if seq > r.ackSeq[peer] {
		r.ackSeq[peer] = seq
		r.resolveReads()
	}
}

// advanceCommit sets commitIndex to the highest index stored on a majority,
// but only if that entry is from the current term (5.4.2). Earlier entries
// are then committed indirectly.
func (r *Raft) advanceCommit() {
	if r.state != Leader {
		return
	}
	matches := make([]uint64, 0, len(r.peers)+1)
	matches = append(matches, r.durableIndex)
	for _, p := range r.peers {
		matches = append(matches, r.matchIndex[p])
	}
	slices.Sort(matches)
	n := matches[len(matches)-r.quorum]
	if n <= r.commitIndex {
		return
	}
	if t, ok := r.log.term(n); !ok || t != r.currentTerm {
		return
	}
	r.commitIndex = n
	r.applyCond.Broadcast()
}

// ---------------------------------------------------------------------------
// Log replication, follower side

// HandleAppendEntries implements the AppendEntries receiver (Figure 2).
func (r *Raft) HandleAppendEntries(args *AppendEntriesArgs) *AppendEntriesReply {
	r.mu.Lock()
	reply := &AppendEntriesReply{Term: r.currentTerm}
	if r.shutdown || args.Term < r.currentTerm {
		r.mu.Unlock()
		return reply
	}
	r.becomeFollower(args.Term, args.LeaderID, true)
	reply.Term = r.currentTerm
	r.lastLeaderContact = time.Now()
	r.resetElectionTimer()

	prev, prevTerm, ents := args.PrevLogIndex, args.PrevLogTerm, args.Entries
	if snap := r.log.snapIndex(); prev < snap {
		// Everything up to our snapshot is committed, so it matches the
		// leader's log. Skip the part of the request it covers.
		skip := snap - prev
		if skip >= uint64(len(ents)) {
			ents = nil
		} else {
			ents = ents[skip:]
		}
		prev, prevTerm = snap, r.log.snapTerm()
	}

	// Consistency check: our log must contain the entry before the new ones.
	if prev > r.log.lastIndex() {
		reply.ConflictIndex = r.log.lastIndex() + 1
		r.mu.Unlock()
		return reply
	}
	if t, _ := r.log.term(prev); t != prevTerm {
		reply.ConflictTerm = t
		reply.ConflictIndex = r.log.firstIndexOfTerm(prev)
		r.mu.Unlock()
		return reply
	}

	// Skip entries we already have. At the first missing or conflicting
	// entry, drop our suffix and take the leader's. Never truncate when
	// nothing conflicts: a delayed, shorter request must not erase entries.
	for i := range ents {
		if t, ok := r.log.term(ents[i].Index); ok && t == ents[i].Term {
			continue
		}
		r.log.truncateFrom(ents[i].Index)
		r.log.append(ents[i:]...)
		if _, err := r.store.wal.append(ents[i:]); err != nil {
			r.storageFailed(err)
		}
		break
	}

	lastNew := args.PrevLogIndex + uint64(len(args.Entries))
	if c := min(args.LeaderCommit, lastNew); c > r.commitIndex {
		r.commitIndex = c
		r.applyCond.Broadcast()
	}
	seq := r.store.wal.writtenSeq()
	r.mu.Unlock()

	// Acknowledge only once the entries are on disk: the leader will count
	// this reply toward a majority.
	if err := r.store.wal.syncTo(seq); err != nil {
		if r.isShutdown() {
			return &AppendEntriesReply{Term: reply.Term}
		}
		r.storageFailed(err)
	}
	reply.Success = true
	return reply
}

// HandleInstallSnapshot implements the InstallSnapshot receiver (Figure 13).
func (r *Raft) HandleInstallSnapshot(args *InstallSnapshotArgs) *InstallSnapshotReply {
	r.snapMu.Lock()
	defer r.snapMu.Unlock()
	r.mu.Lock()
	defer r.mu.Unlock()
	reply := &InstallSnapshotReply{Term: r.currentTerm}
	if r.shutdown || args.Term < r.currentTerm {
		return reply
	}
	r.becomeFollower(args.Term, args.LeaderID, true)
	reply.Term = r.currentTerm
	r.lastLeaderContact = time.Now()
	r.resetElectionTimer()

	idx, term := args.LastIncludedIndex, args.LastIncludedTerm
	if idx <= r.commitIndex {
		return reply // we already have everything it covers
	}

	// If our log has the snapshot's last entry, keep what follows it.
	// Otherwise the whole log is discarded.
	t, ok := r.log.term(idx)
	retain := ok && t == term
	var walStart uint64
	if retain {
		walStart = r.store.wal.pickStart(idx)
	} else {
		var err error
		if walStart, err = r.store.wal.reset(idx); err != nil {
			r.storageFailed(err)
		}
		r.log = newMemLog(idx, term, nil)
	}
	if err := r.store.saveSnapshot(snapshotMeta{Index: idx, Term: term, WALStart: walStart}, args.Data); err != nil {
		r.storageFailed(err)
	}
	if retain {
		r.log.compact(idx, term)
	}
	if err := r.store.wal.removeBefore(walStart); err != nil {
		r.logger.Warn("raft: removing old WAL segments", "err", err)
	}
	r.commitIndex = idx
	r.applyCond.Broadcast() // the applier restores the FSM from the snapshot
	r.logger.Info("raft: installed snapshot", "index", idx, "term", term, "from", args.LeaderID)
	return reply
}

// ---------------------------------------------------------------------------
// Applying entries and snapshots

func (r *Raft) runApplier() {
	defer r.wg.Done()
	for {
		r.mu.Lock()
		for !r.shutdown && r.lastApplied >= r.commitIndex && r.log.snapIndex() <= r.lastApplied {
			r.applyCond.Wait()
		}
		if r.shutdown {
			r.mu.Unlock()
			return
		}
		if r.log.snapIndex() > r.lastApplied {
			r.mu.Unlock()
			r.restoreSnapshot()
			continue
		}
		lo := r.lastApplied + 1
		hi := min(r.commitIndex, lo+maxApplyBatch-1)
		ents := r.log.slice(lo, hi+1)
		r.mu.Unlock()

		results := make([]any, len(ents))
		for i := range ents {
			if ents[i].Type == EntryCommand {
				results[i] = r.fsm.Apply(ents[i].Index, ents[i].Data)
			}
		}

		r.mu.Lock()
		r.lastApplied = hi
		for i := range ents {
			r.resolveFuture(&ents[i], results[i])
		}
		r.notifyApplied()
		snap := r.lastApplied-r.log.snapIndex() >= r.cfg.SnapshotThreshold && !r.snapshotting.Load()
		r.mu.Unlock()

		if snap {
			r.takeSnapshot(hi)
		}
	}
}

func (r *Raft) resolveFuture(e *Entry, res any) {
	f, ok := r.futures[e.Index]
	if !ok {
		return
	}
	delete(r.futures, e.Index)
	if f.Term == e.Term {
		f.res = res
	} else {
		// Our entry was overwritten by another leader's.
		f.err = ErrLeadershipLost
	}
	close(f.done)
}

func (r *Raft) restoreSnapshot() {
	meta, data, err := r.store.loadSnapshot()
	if err == nil {
		err = r.fsm.Restore(data)
	}
	if err != nil {
		if r.isShutdown() {
			return
		}
		r.storageFailed(err)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.lastApplied = max(r.lastApplied, meta.Index)
	r.commitIndex = max(r.commitIndex, meta.Index)
	for idx, f := range r.futures {
		if idx <= meta.Index {
			f.err = ErrLeadershipLost
			close(f.done)
			delete(r.futures, idx)
		}
	}
	r.notifyApplied()
}

// takeSnapshot captures the FSM (on the applier goroutine, so the state is
// exactly as of index) and writes it to disk in the background.
func (r *Raft) takeSnapshot(index uint64) {
	data, err := r.fsm.Snapshot()
	if err != nil {
		r.logger.Error("raft: FSM snapshot failed", "err", err)
		return
	}
	r.snapshotting.Store(true)
	r.wg.Add(1)
	go r.persistSnapshot(index, data)
}

func (r *Raft) persistSnapshot(index uint64, data []byte) {
	defer r.wg.Done()
	defer r.snapshotting.Store(false)
	r.snapMu.Lock()
	defer r.snapMu.Unlock()

	r.mu.Lock()
	if r.shutdown || index <= r.log.snapIndex() {
		r.mu.Unlock()
		return
	}
	term, _ := r.log.term(index)
	walStart := r.store.wal.pickStart(index)
	r.mu.Unlock()

	if err := r.store.saveSnapshot(snapshotMeta{Index: index, Term: term, WALStart: walStart}, data); err != nil {
		r.storageFailed(err)
	}
	r.mu.Lock()
	r.log.compact(index, term)
	r.mu.Unlock()
	if err := r.store.wal.removeBefore(walStart); err != nil {
		r.logger.Warn("raft: removing old WAL segments", "err", err)
	}
	// Start a new segment so the next snapshot can release this one.
	if _, err := r.store.wal.rotate(); err != nil {
		r.storageFailed(err)
	}
	r.logger.Debug("raft: snapshot saved", "index", index, "term", term, "bytes", len(data))
}

func (r *Raft) notifyApplied() {
	keep := r.applyWaiters[:0]
	for _, w := range r.applyWaiters {
		if w.index <= r.lastApplied {
			close(w.ch)
		} else {
			keep = append(keep, w)
		}
	}
	r.applyWaiters = keep
}

// ---------------------------------------------------------------------------
// Client-facing API

// Propose appends commands to the leader's log in the given order and returns
// one future per command. It fails with ErrNotLeader on other nodes.
func (r *Raft) Propose(data [][]byte) ([]*Future, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.shutdown {
		return nil, ErrShutdown
	}
	if r.state != Leader {
		return nil, ErrNotLeader
	}
	ents := make([]Entry, len(data))
	futs := make([]*Future, len(data))
	next := r.log.lastIndex() + 1
	for i, d := range data {
		ents[i] = Entry{Index: next + uint64(i), Term: r.currentTerm, Type: EntryCommand, Data: d}
		futs[i] = &Future{Index: ents[i].Index, Term: r.currentTerm, done: make(chan struct{})}
		r.futures[ents[i].Index] = futs[i]
	}
	r.appendLocal(ents)
	return futs, nil
}

// ReadIndex returns an index such that, once it has been applied, reading
// the FSM is linearizable (thesis 6.4):
//
//  1. Take readIndex = commitIndex, but no lower than this term's no-op, so
//     it covers every entry committed by earlier leaders.
//  2. Confirm we are still leader: a majority must acknowledge a heartbeat
//     sent after the request arrived. A deposed leader cannot get that.
//
// The caller then waits for lastApplied >= readIndex (WaitApplied). Reads
// share heartbeat rounds, so heavy read traffic costs few extra RPCs.
func (r *Raft) ReadIndex(ctx context.Context) (uint64, error) {
	r.mu.Lock()
	if r.shutdown {
		r.mu.Unlock()
		return 0, ErrShutdown
	}
	if r.state != Leader {
		r.mu.Unlock()
		return 0, ErrNotLeader
	}
	idx := max(r.commitIndex, r.noopIndex)
	if len(r.peers) == 0 {
		r.mu.Unlock()
		return idx, nil
	}
	r.hbSeq++
	w := &readWaiter{seq: r.hbSeq, ch: make(chan error, 1)}
	r.readWaiters = append(r.readWaiters, w)
	r.triggerReplication()
	r.mu.Unlock()

	select {
	case err := <-w.ch:
		return idx, err
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}

// resolveReads releases read requests whose heartbeat a majority (counting
// ourselves) has acknowledged.
func (r *Raft) resolveReads() {
	if len(r.readWaiters) == 0 {
		return
	}
	acks := make([]uint64, 0, len(r.peers)+1)
	acks = append(acks, r.hbSeq)
	for _, p := range r.peers {
		acks = append(acks, r.ackSeq[p])
	}
	slices.Sort(acks)
	confirmed := acks[len(acks)-r.quorum]
	i := 0
	for ; i < len(r.readWaiters) && r.readWaiters[i].seq <= confirmed; i++ {
		r.readWaiters[i].ch <- nil
	}
	r.readWaiters = r.readWaiters[i:]
}

func (r *Raft) failReads(err error) {
	for _, w := range r.readWaiters {
		w.ch <- err
	}
	r.readWaiters = nil
}

// WaitApplied blocks until the entry at index has been applied to the FSM.
func (r *Raft) WaitApplied(ctx context.Context, index uint64) error {
	r.mu.Lock()
	if r.lastApplied >= index {
		r.mu.Unlock()
		return nil
	}
	if r.shutdown {
		r.mu.Unlock()
		return ErrShutdown
	}
	w := &applyWaiter{index: index, ch: make(chan struct{})}
	r.applyWaiters = append(r.applyWaiters, w)
	r.mu.Unlock()
	select {
	case <-w.ch:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-r.shutdownCh:
		return ErrShutdown
	}
}

// Leader returns the ID of the leader this node knows about (possibly
// itself) and whether this node is currently the leader.
func (r *Raft) Leader() (string, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.leaderID, r.state == Leader
}

func (r *Raft) Status() Status {
	r.mu.Lock()
	defer r.mu.Unlock()
	return Status{
		ID:            r.id,
		State:         r.state,
		Term:          r.currentTerm,
		LeaderID:      r.leaderID,
		CommitIndex:   r.commitIndex,
		LastApplied:   r.lastApplied,
		LastLogIndex:  r.log.lastIndex(),
		LastLogTerm:   r.log.lastTerm(),
		SnapshotIndex: r.log.snapIndex(),
		SnapshotTerm:  r.log.snapTerm(),
	}
}
