package raft

import (
	"context"
	"fmt"
	"math/rand/v2"
	"sync"
	"testing"
	"time"
)

// --- Leader election ---

func TestInitialElection(t *testing.T) {
	c := newCluster(t, 3)
	c.checkOneLeader()
	term1 := c.checkTermsAgree()
	if term1 < 1 {
		t.Fatalf("term is %d after election", term1)
	}
	// With no failures the leader should keep its job.
	time.Sleep(600 * time.Millisecond)
	if term2 := c.checkTermsAgree(); term2 != term1 {
		t.Fatalf("term changed from %d to %d without failures", term1, term2)
	}
	c.checkOneLeader()
}

func TestReElection(t *testing.T) {
	c := newCluster(t, 3)
	l1 := c.checkOneLeader()

	c.disconnect(l1)
	l2 := c.checkOneLeader()
	if l2 == l1 {
		t.Fatal("disconnected leader is still the only leader")
	}

	// The old leader rejoins; it must not disturb the new one.
	c.connect(l1)
	if l := c.checkOneLeader(); l != l2 {
		t.Fatalf("leader changed from %d to %d when old leader rejoined", l2, l)
	}

	// Without a majority nobody can be elected.
	c.disconnect(l2)
	c.disconnect((l2 + 1) % 3)
	time.Sleep(700 * time.Millisecond)
	c.checkNoLeader()

	c.connect((l2 + 1) % 3)
	c.checkOneLeader()
	c.connect(l2)
	c.checkOneLeader()
}

// TestSplitVote makes every node a candidate in the same term; a later term must break the tie.
func TestSplitVote(t *testing.T) {
	c := newCluster(t, 3)
	c.checkOneLeader()
	term := c.checkTermsAgree()

	for i := range c.rafts {
		c.disconnect(i)
	}
	for _, r := range c.rafts {
		r.mu.Lock()
		r.campaign(false) // real election, skipping pre-vote
		r.mu.Unlock()
	}
	for i, r := range c.rafts {
		s := r.Status()
		if s.State != Candidate || s.Term != term+1 {
			t.Fatalf("node %d: state %v term %d, want candidate in term %d", i, s.State, s.Term, term+1)
		}
		r.mu.Lock()
		voted := r.votedFor
		r.mu.Unlock()
		if voted != c.ids[i] {
			t.Fatalf("node %d voted for %q, want itself", i, voted)
		}
	}
	for i := range c.rafts {
		c.connect(i)
	}

	l := c.checkOneLeader()
	won := c.rafts[l].Status().Term
	if won <= term+1 {
		t.Fatalf("leader elected in term %d, but term %d was a split vote", won, term+1)
	}
	c.mu.Lock()
	split := c.leaders[term+1]
	c.mu.Unlock()
	if len(split) != 0 {
		t.Fatalf("term %d should have no leader, got %v", term+1, split)
	}
}

// TestVoteRules checks one vote per term, the up-to-date rule, and votes persisted across restarts.
func TestVoteRules(t *testing.T) {
	dir := t.TempDir()
	open := func() *Raft {
		r, err := New(Config{ID: "a", Peers: []string{"a", "b", "c"}, DataDir: dir, Logger: quietLogger}, &memTransport{net: newMemNetwork(), from: "a"}, &testFSM{c: &cluster{t: t, committed: map[uint64]string{}}, vals: map[uint64]string{}})
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	vote := func(r *Raft, term uint64, cand string, lastIdx, lastTerm uint64) bool {
		return r.HandleRequestVote(&RequestVoteArgs{Term: term, CandidateID: cand, LastLogIndex: lastIdx, LastLogTerm: lastTerm}).VoteGranted
	}

	r := open()
	if !vote(r, 1, "b", 0, 0) {
		t.Fatal("first vote in term 1 should be granted")
	}
	if vote(r, 1, "c", 0, 0) {
		t.Fatal("second candidate in the same term got a vote")
	}
	if !vote(r, 1, "b", 0, 0) {
		t.Fatal("repeated request from the same candidate should be granted")
	}

	// Give node a two entries in term 2.
	ae := r.HandleAppendEntries(&AppendEntriesArgs{Term: 2, LeaderID: "b", Entries: []Entry{
		{Index: 1, Term: 2, Type: EntryCommand, Data: []byte("x")},
		{Index: 2, Term: 2, Type: EntryCommand, Data: []byte("y")},
	}})
	if !ae.Success {
		t.Fatal("AppendEntries failed")
	}
	if vote(r, 3, "c", 5, 1) {
		t.Fatal("vote granted to a candidate with an older last term")
	}
	if vote(r, 3, "c", 1, 2) {
		t.Fatal("vote granted to a candidate with a shorter log in the same term")
	}
	if got := r.Status().Term; got != 3 {
		t.Fatalf("term = %d, want 3 after seeing a term-3 request", got)
	}

	r.Shutdown()
	r = open()
	if s := r.Status(); s.Term != 3 || s.LastLogIndex != 2 {
		t.Fatalf("after restart: term %d last index %d, want 3 and 2", s.Term, s.LastLogIndex)
	}
	if !vote(r, 3, "b", 2, 2) {
		t.Fatal("up-to-date candidate should get the vote")
	}
	r.Shutdown()
	r = open()
	defer r.Shutdown()
	if vote(r, 3, "c", 9, 9) {
		t.Fatal("vote from before the restart was forgotten: voted twice in term 3")
	}
}

// TestPreVoteNoDisruption checks an isolated follower cannot inflate its term and depose the leader.
func TestPreVoteNoDisruption(t *testing.T) {
	c := newCluster(t, 3)
	l := c.checkOneLeader()
	term := c.rafts[l].Status().Term
	f := (l + 1) % 3

	c.disconnect(f)
	time.Sleep(1500 * time.Millisecond)
	if got := c.rafts[f].Status().Term; got != term {
		t.Fatalf("isolated follower's term grew from %d to %d", term, got)
	}
	c.connect(f)
	time.Sleep(500 * time.Millisecond)
	if nl := c.checkOneLeader(); nl != l {
		t.Fatalf("leader changed from %d to %d after follower rejoined", l, nl)
	}
	if got := c.rafts[l].Status().Term; got != term {
		t.Fatalf("leader's term changed from %d to %d", term, got)
	}
}

// --- Log replication ---

func TestBasicReplication(t *testing.T) {
	c := newCluster(t, 3)
	for i := 0; i < 20; i++ {
		c.one(fmt.Sprintf("cmd-%d", i), 3)
	}
	c.checkErrors()
}

func TestConcurrentProposals(t *testing.T) {
	c := newCluster(t, 3)
	l := c.checkOneLeader()
	var wg sync.WaitGroup
	var mu sync.Mutex
	indexes := map[uint64]bool{}
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				f, err := c.propose(l, fmt.Sprintf("g%d-%d", g, i))
				if err != nil {
					t.Error(err)
					return
				}
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				_, err = f.Wait(ctx)
				cancel()
				if err != nil {
					t.Error(err)
					return
				}
				mu.Lock()
				if indexes[f.Index] {
					t.Errorf("index %d used twice", f.Index)
				}
				indexes[f.Index] = true
				mu.Unlock()
			}
		}(g)
	}
	wg.Wait()
	if len(indexes) != 400 {
		t.Fatalf("committed %d entries, want 400", len(indexes))
	}
}

func TestNoCommitWithoutMajority(t *testing.T) {
	c := newCluster(t, 3)
	c.one("a", 3)
	l := c.checkOneLeader()
	c.disconnect((l + 1) % 3)
	c.one("b", 2) // one follower down is fine

	c.disconnect((l + 2) % 3)
	f, err := c.propose(l, "c")
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(400 * time.Millisecond)
	if n, _ := c.nCommitted(f.Index); n > 0 {
		t.Fatalf("entry applied on %d nodes without a majority", n)
	}
	c.connect((l + 1) % 3)
	c.connect((l + 2) % 3)
	c.one("d", 3)
	c.checkErrors()
}

// TestLeaderLogConflict checks that a partitioned leader's uncommitted entries get overwritten.
func TestLeaderLogConflict(t *testing.T) {
	c := newCluster(t, 3)
	c.one("base", 3)
	l1 := c.checkOneLeader()

	c.disconnect(l1)
	var stale []*Future
	for i := 0; i < 5; i++ {
		f, err := c.propose(l1, fmt.Sprintf("stale-%d", i))
		if err != nil {
			t.Fatal(err)
		}
		stale = append(stale, f)
	}
	l2 := c.checkOneLeader()
	for i := 0; i < 5; i++ {
		c.one(fmt.Sprintf("fresh-%d", i), 2)
	}

	c.connect(l1)
	idx := c.one("after-heal", 3)
	c.waitApplied(idx)
	for _, f := range stale {
		if _, v := c.nCommitted(f.Index); len(v) >= 5 && v[:5] == "stale" {
			t.Fatalf("stale entry %q committed at %d", v, f.Index)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_, err := f.Wait(ctx)
		cancel()
		if err != ErrLeadershipLost {
			t.Fatalf("stale proposal at %d resolved with %v, want ErrLeadershipLost", f.Index, err)
		}
	}
	_ = l2
	c.checkErrors()
}

// TestBackup stresses log backtracking with long divergent tails from repeated partitions.
func TestBackup(t *testing.T) {
	c := newCluster(t, 5)
	c.one("start", 5)

	l := c.checkOneLeader()
	c.disconnect((l + 2) % 5)
	c.disconnect((l + 3) % 5)
	c.disconnect((l + 4) % 5)
	for i := 0; i < 50; i++ {
		c.propose(l, fmt.Sprintf("lost-a-%d", i))
	}
	time.Sleep(300 * time.Millisecond)
	c.disconnect(l)
	c.disconnect((l + 1) % 5)

	c.connect((l + 2) % 5)
	c.connect((l + 3) % 5)
	c.connect((l + 4) % 5)
	for i := 0; i < 50; i++ {
		c.one(fmt.Sprintf("kept-a-%d", i), 3)
	}

	l2 := c.checkOneLeader()
	other := (l + 2) % 5
	if l2 == other {
		other = (l2 + 1) % 5
		if other == l || other == (l+1)%5 {
			other = (l + 3) % 5
		}
	}
	c.disconnect(other)
	for i := 0; i < 50; i++ {
		c.propose(l2, fmt.Sprintf("lost-b-%d", i))
	}
	time.Sleep(300 * time.Millisecond)

	for i := range c.rafts {
		c.disconnect(i)
	}
	c.connect(l)
	c.connect((l + 1) % 5)
	c.connect(other)
	for i := 0; i < 50; i++ {
		c.one(fmt.Sprintf("kept-b-%d", i), 3)
	}

	for i := range c.rafts {
		c.connect(i)
	}
	c.one("end", 5)
	c.checkErrors()
}

// TestFollowerAppendRules drives the AppendEntries receiver directly.
func TestFollowerAppendRules(t *testing.T) {
	dir := t.TempDir()
	open := func() *Raft {
		r, err := New(Config{ID: "f", Peers: []string{"f", "l"}, DataDir: dir, Logger: quietLogger}, &memTransport{net: newMemNetwork(), from: "f"}, &testFSM{c: &cluster{t: t, committed: map[uint64]string{}}, vals: map[uint64]string{}})
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	ent := func(i, term uint64) Entry {
		return Entry{Index: i, Term: term, Type: EntryCommand, Data: []byte(fmt.Sprintf("%d@%d", i, term))}
	}
	terms := func(r *Raft) string {
		r.mu.Lock()
		defer r.mu.Unlock()
		s := ""
		for i := r.log.snapIndex() + 1; i <= r.log.lastIndex(); i++ {
			tm, _ := r.log.term(i)
			s += fmt.Sprint(tm)
		}
		return s
	}

	r := open()
	ok := r.HandleAppendEntries(&AppendEntriesArgs{Term: 1, LeaderID: "l", Entries: []Entry{ent(1, 1), ent(2, 1), ent(3, 1), ent(4, 1), ent(5, 1)}})
	if !ok.Success || terms(r) != "11111" {
		t.Fatalf("initial append: success=%v log=%s", ok.Success, terms(r))
	}
	// A new leader in term 2 overwrites index 4 onwards.
	ok = r.HandleAppendEntries(&AppendEntriesArgs{Term: 2, LeaderID: "l", PrevLogIndex: 3, PrevLogTerm: 1, Entries: []Entry{ent(4, 2)}})
	if !ok.Success || terms(r) != "1112" {
		t.Fatalf("conflict: success=%v log=%s, want 1112", ok.Success, terms(r))
	}
	// A delayed, shorter request must not truncate anything.
	ok = r.HandleAppendEntries(&AppendEntriesArgs{Term: 2, LeaderID: "l", PrevLogIndex: 2, PrevLogTerm: 1, Entries: []Entry{ent(3, 1)}})
	if !ok.Success || terms(r) != "1112" {
		t.Fatalf("stale request: success=%v log=%s, want 1112", ok.Success, terms(r))
	}
	// Mismatch at prevLogIndex reports the conflicting term and its first index.
	rep := r.HandleAppendEntries(&AppendEntriesArgs{Term: 2, LeaderID: "l", PrevLogIndex: 3, PrevLogTerm: 2})
	if rep.Success || rep.ConflictTerm != 1 || rep.ConflictIndex != 1 {
		t.Fatalf("mismatch reply %+v, want conflict term 1 index 1", rep)
	}
	// Too short: ConflictIndex is our last index + 1.
	rep = r.HandleAppendEntries(&AppendEntriesArgs{Term: 2, LeaderID: "l", PrevLogIndex: 10, PrevLogTerm: 2})
	if rep.Success || rep.ConflictTerm != 0 || rep.ConflictIndex != 5 {
		t.Fatalf("short log reply %+v, want conflict index 5", rep)
	}
	// Stale term is rejected outright.
	rep = r.HandleAppendEntries(&AppendEntriesArgs{Term: 1, LeaderID: "x", PrevLogIndex: 4, PrevLogTerm: 2, Entries: []Entry{ent(5, 1)}})
	if rep.Success || rep.Term != 2 {
		t.Fatalf("stale leader reply %+v", rep)
	}

	// The WAL replays the overwrite correctly after a restart.
	r.Shutdown()
	r = open()
	defer r.Shutdown()
	if got := terms(r); got != "1112" {
		t.Fatalf("after restart log = %s, want 1112", got)
	}
	r.mu.Lock()
	data := string(r.log.slice(4, 5)[0].Data)
	r.mu.Unlock()
	if data != "4@2" {
		t.Fatalf("entry 4 = %q after restart, want 4@2", data)
	}
}

// --- Persistence and restarts ---

func TestRestartAll(t *testing.T) {
	c := newCluster(t, 3)
	var last uint64
	for i := 0; i < 10; i++ {
		last = c.one(fmt.Sprintf("before-%d", i), 3)
	}
	term := c.checkTermsAgree()
	for i := range c.rafts {
		c.crash(i)
	}
	for i := range c.rafts {
		c.start(i)
	}
	c.checkOneLeader()
	if got := c.checkTermsAgree(); got <= term {
		t.Fatalf("term %d after restart, want > %d", got, term)
	}
	c.waitApplied(last)
	for i := uint64(1); i <= last; i++ {
		c.nCommitted(i) // checks agreement
	}
	c.one("after", 3)
	c.checkErrors()
}

func TestRestartLeaderRepeatedly(t *testing.T) {
	c := newCluster(t, 3)
	for round := 0; round < 5; round++ {
		c.one(fmt.Sprintf("r%d-a", round), 3)
		l := c.checkOneLeader()
		c.crash(l)
		c.one(fmt.Sprintf("r%d-b", round), 2)
		c.start(l)
		c.one(fmt.Sprintf("r%d-c", round), 3)
	}
	c.checkErrors()
}

// TestCrashDuringWrites crashes random nodes while clients keep writing.
func TestCrashDuringWrites(t *testing.T) {
	c := newCluster(t, 3)
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for g := 0; g < 3; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				for n := range c.rafts {
					if c.isUp(n) {
						c.propose(n, fmt.Sprintf("w%d-%d", g, i))
					}
				}
				time.Sleep(5 * time.Millisecond)
			}
		}(g)
	}
	for k := 0; k < 8; k++ {
		victim := rand.IntN(3)
		c.crash(victim)
		time.Sleep(time.Duration(100+rand.IntN(300)) * time.Millisecond)
		c.start(victim)
		time.Sleep(time.Duration(100+rand.IntN(300)) * time.Millisecond)
	}
	close(stop)
	wg.Wait()
	c.one("final", 3)
	c.checkErrors()
}

// --- Snapshots and compaction ---

func withSnapshots(threshold uint64) func(*Config) {
	return func(c *Config) { c.SnapshotThreshold = threshold }
}

func TestSnapshotCompactsLog(t *testing.T) {
	c := newCluster(t, 3, withSnapshots(20))
	var last uint64
	for i := 0; i < 120; i++ {
		last = c.one(fmt.Sprintf("v%d", i), 3)
	}
	c.waitApplied(last)
	time.Sleep(200 * time.Millisecond) // snapshots are written in the background
	for i, r := range c.rafts {
		s := r.Status()
		if s.SnapshotIndex == 0 {
			t.Fatalf("node %d never took a snapshot", i)
		}
		if n := s.LastLogIndex - s.SnapshotIndex; n > 60 {
			t.Fatalf("node %d keeps %d entries in memory, compaction is not working", i, n)
		}
		if segs := r.store.wal.segmentCount(); segs > 4 {
			t.Fatalf("node %d has %d WAL segments, old ones are not deleted", i, segs)
		}
	}
	c.checkErrors()
}

// TestInstallSnapshot makes a follower lag behind a compacted leader so only InstallSnapshot helps.
func TestInstallSnapshot(t *testing.T) {
	c := newCluster(t, 3, withSnapshots(20))
	c.one("start", 3)
	l := c.checkOneLeader()
	lag := (l + 1) % 3

	c.crash(lag)
	var last uint64
	for i := 0; i < 100; i++ {
		last = c.one(fmt.Sprintf("while-down-%d", i), 2)
	}
	time.Sleep(200 * time.Millisecond)
	if s := c.rafts[l].Status(); s.SnapshotIndex < 50 {
		t.Fatalf("leader snapshot index %d, expected compaction past the lagging node", s.SnapshotIndex)
	}

	c.start(lag)
	c.waitApplied(last)
	for i := uint64(1); i <= last; i++ {
		c.nCommitted(i)
	}
	if v, ok := c.fsms[lag].get(last); !ok || v != "while-down-99" {
		t.Fatalf("lagging node has %q at %d", v, last)
	}
	// It keeps working afterwards, including after a restart that reloads the installed snapshot.
	c.one("after", 3)
	c.restart(lag)
	idx := c.one("after-restart", 3)
	c.waitApplied(idx)
	c.checkErrors()
}

func TestRestartFromSnapshot(t *testing.T) {
	c := newCluster(t, 3, withSnapshots(25))
	var last uint64
	for i := 0; i < 90; i++ {
		last = c.one(fmt.Sprintf("v%d", i), 3)
	}
	c.waitApplied(last)
	time.Sleep(200 * time.Millisecond)
	for i := range c.rafts {
		c.crash(i)
	}
	for i := range c.rafts {
		c.start(i)
		if s := c.rafts[i].Status(); s.SnapshotIndex == 0 || s.LastApplied < s.SnapshotIndex {
			t.Fatalf("node %d restarted without its snapshot: %+v", i, s)
		}
	}
	c.waitApplied(last)
	if v, _ := c.fsms[0].get(last); v != "v89" {
		t.Fatalf("value at %d after restart = %q", last, v)
	}
	c.one("more", 3)
	c.checkErrors()
}

// --- Linearizable reads ---

// TestReadIndexStaleLeader checks that an isolated leader cannot confirm a read.
func TestReadIndexStaleLeader(t *testing.T) {
	c := newCluster(t, 3)
	c.one("x", 3)
	l := c.checkOneLeader()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	idx, err := c.rafts[l].ReadIndex(ctx)
	cancel()
	if err != nil {
		t.Fatalf("ReadIndex on healthy leader: %v", err)
	}
	if idx < 2 {
		t.Fatalf("read index %d does not cover committed entries", idx)
	}

	c.disconnect(l)
	ctx, cancel = context.WithTimeout(context.Background(), 100*time.Millisecond)
	_, err = c.rafts[l].ReadIndex(ctx)
	cancel()
	if err == nil {
		t.Fatal("isolated leader confirmed a read")
	}

	nl := c.checkOneLeader()
	c.one("y", 2)
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := c.rafts[nl].ReadIndex(ctx); err != nil {
		t.Fatalf("ReadIndex on new leader: %v", err)
	}
	if _, err := c.rafts[(nl+1)%3].ReadIndex(ctx); err != ErrNotLeader && (nl+1)%3 != l {
		t.Fatalf("follower ReadIndex returned %v, want ErrNotLeader", err)
	}
	c.connect(l)
}

// --- Everything at once ---

// TestChurn mixes an unreliable network, partitions, crashes and snapshots, then checks agreement.
func TestChurn(t *testing.T) {
	if testing.Short() {
		t.Skip("slow")
	}
	c := newCluster(t, 5, withSnapshots(40))
	c.setUnreliable(true)
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				for n := range c.rafts {
					if c.isUp(n) {
						c.propose(n, fmt.Sprintf("c%d-%d", g, i))
					}
				}
				time.Sleep(3 * time.Millisecond)
			}
		}(g)
	}
	for k := 0; k < 20; k++ {
		n := rand.IntN(5)
		switch rand.IntN(3) {
		case 0:
			c.crash(n)
			time.Sleep(time.Duration(rand.IntN(200)) * time.Millisecond)
			c.start(n)
		case 1:
			c.disconnect(n)
			time.Sleep(time.Duration(rand.IntN(300)) * time.Millisecond)
			c.connect(n)
		default:
			time.Sleep(time.Duration(rand.IntN(200)) * time.Millisecond)
		}
	}
	close(stop)
	wg.Wait()
	c.setUnreliable(false)
	idx := c.one("final", 5)
	c.waitApplied(idx)
	c.checkErrors()
}
