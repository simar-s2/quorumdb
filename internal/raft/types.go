// Package raft implements the Raft consensus algorithm as described in
// "In Search of an Understandable Consensus Algorithm" (Ongaro & Ousterhout)
// and Diego Ongaro's PhD thesis: leader election with pre-vote, log
// replication, durable persistence, snapshots with log compaction, and
// ReadIndex linearizable reads.
package raft

import (
	"context"
	"errors"
	"log/slog"
	"time"
)

// State is the role a node currently plays.
type State uint8

const (
	Follower State = iota
	PreCandidate
	Candidate
	Leader
)

func (s State) String() string {
	switch s {
	case Follower:
		return "follower"
	case PreCandidate:
		return "pre-candidate"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	}
	return "unknown"
}

type EntryType uint8

const (
	// EntryCommand carries state machine data.
	EntryCommand EntryType = iota + 1
	// EntryNoop is appended by every new leader so it can commit entries
	// from earlier terms and serve ReadIndex reads (thesis section 6.4).
	EntryNoop
)

// Entry is one slot of the replicated log. Data is never modified after the
// entry is created, so slices of it can be shared freely.
type Entry struct {
	Index uint64
	Term  uint64
	Type  EntryType
	Data  []byte
}

// RequestVoteArgs is used for both real votes and pre-votes. A pre-vote asks
// "would you vote for me in Term?" without anyone changing their term.
type RequestVoteArgs struct {
	Term         uint64
	CandidateID  string
	LastLogIndex uint64
	LastLogTerm  uint64
	PreVote      bool
}

type RequestVoteReply struct {
	Term        uint64
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         uint64
	LeaderID     string
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []Entry
	LeaderCommit uint64
}

// AppendEntriesReply includes conflict hints so the leader can skip back a
// whole term at a time instead of one entry per round trip.
type AppendEntriesReply struct {
	Term    uint64
	Success bool
	// When the follower's log is too short, ConflictTerm is 0 and
	// ConflictIndex is the follower's last index + 1. Otherwise
	// ConflictTerm is the term of the follower's entry at PrevLogIndex and
	// ConflictIndex is the first index the follower has for that term.
	ConflictIndex uint64
	ConflictTerm  uint64
}

// InstallSnapshotArgs sends a whole snapshot in one message. The paper's
// chunked transfer is not implemented; snapshots of this store are small.
type InstallSnapshotArgs struct {
	Term              uint64
	LeaderID          string
	LastIncludedIndex uint64
	LastIncludedTerm  uint64
	Data              []byte
}

type InstallSnapshotReply struct {
	Term uint64
}

// Transport sends RPCs to other members, identified by ID.
type Transport interface {
	RequestVote(ctx context.Context, target string, args *RequestVoteArgs) (*RequestVoteReply, error)
	AppendEntries(ctx context.Context, target string, args *AppendEntriesArgs) (*AppendEntriesReply, error)
	InstallSnapshot(ctx context.Context, target string, args *InstallSnapshotArgs) (*InstallSnapshotReply, error)
}

// FSM is the replicated state machine. Apply is called for committed command
// entries, in log order, from a single goroutine. Snapshot is called from the
// same goroutine, so it always captures the state as of the last Apply.
type FSM interface {
	Apply(index uint64, data []byte) any
	Snapshot() ([]byte, error)
	Restore(data []byte) error
}

var (
	ErrNotLeader = errors.New("raft: not the leader")
	ErrShutdown  = errors.New("raft: node is shut down")
	// ErrLeadershipLost means the entry was appended by this node as leader
	// but its fate is unknown: it may or may not commit under a new leader.
	ErrLeadershipLost = errors.New("raft: leadership lost, outcome unknown")
)

// Config holds a node's settings. Zero values get the defaults below.
type Config struct {
	ID      string
	Peers   []string // IDs of all members, including ID
	DataDir string

	HeartbeatInterval time.Duration // default 50ms
	// ElectionTimeout is the minimum; each timeout is drawn uniformly from
	// [ElectionTimeout, 2*ElectionTimeout). Default 300ms.
	ElectionTimeout time.Duration
	// SnapshotThreshold is how many applied entries may accumulate in the
	// log before a snapshot is taken. Default 10000.
	SnapshotThreshold uint64
	// MaxAppendEntries caps the entries sent in one AppendEntries RPC.
	MaxAppendEntries int
	// NoSync skips fsync. Data then survives a process crash (it is in the
	// OS page cache) but not a power failure. Only for experiments.
	NoSync bool
	Logger *slog.Logger
}

func (c *Config) setDefaults() {
	if c.HeartbeatInterval == 0 {
		c.HeartbeatInterval = 50 * time.Millisecond
	}
	if c.ElectionTimeout == 0 {
		c.ElectionTimeout = 300 * time.Millisecond
	}
	if c.SnapshotThreshold == 0 {
		c.SnapshotThreshold = 10000
	}
	if c.MaxAppendEntries == 0 {
		c.MaxAppendEntries = 1024
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
}

// Status is a point-in-time view of a node, used by INFO and tests.
type Status struct {
	ID            string
	State         State
	Term          uint64
	LeaderID      string
	CommitIndex   uint64
	LastApplied   uint64
	LastLogIndex  uint64
	LastLogTerm   uint64
	SnapshotIndex uint64
	SnapshotTerm  uint64
}
