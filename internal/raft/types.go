// Package raft implements Raft from the paper and thesis: election, replication, snapshots and ReadIndex.
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
	// EntryNoop is appended by each new leader to commit earlier-term entries and enable ReadIndex.
	EntryNoop
)

// Entry is one log slot; Data is never modified once created.
type Entry struct {
	Index uint64
	Term  uint64
	Type  EntryType
	Data  []byte
}

// RequestVoteArgs is used for real votes and for pre-votes, which change no one's term.
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

// AppendEntriesReply carries conflict hints so the leader can skip back a whole term at once.
type AppendEntriesReply struct {
	Term    uint64
	Success bool
	// Conflict hints: our term at PrevLogIndex and its first index, or ConflictTerm 0 and our last index + 1.
	ConflictIndex uint64
	ConflictTerm  uint64
}

// InstallSnapshotArgs sends the whole snapshot in one message (no chunking).
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

// FSM is the replicated state machine; Apply and Snapshot run on the single applier goroutine.
type FSM interface {
	Apply(index uint64, data []byte) any
	Snapshot() (serialize func() ([]byte, error))
	Restore(data []byte) error
}

var (
	ErrNotLeader = errors.New("raft: not the leader")
	ErrShutdown  = errors.New("raft: node is shut down")
	// ErrLeadershipLost means an entry this node proposed may or may not commit.
	ErrLeadershipLost = errors.New("raft: leadership lost, outcome unknown")
)

// Config holds a node's settings. Zero values get the defaults below.
type Config struct {
	ID      string
	Peers   []string // IDs of all members, including ID
	DataDir string

	HeartbeatInterval time.Duration // default 50ms
	// ElectionTimeout is the minimum; each timeout is random in [T, 2T). Default 300ms.
	ElectionTimeout time.Duration
	// SnapshotThreshold is how many applied entries trigger a snapshot. Default 10000.
	SnapshotThreshold uint64
	// MaxAppendEntries caps the entries sent in one AppendEntries RPC.
	MaxAppendEntries int
	// NoSync skips fsync (survives process crashes, not power loss); only for experiments.
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
