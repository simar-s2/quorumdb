package transport

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/simar-s2/quorumdb/internal/raft"
)

type fakeHandler struct {
	leader bool
	block  chan struct{}
}

func (h *fakeHandler) HandleRequestVote(a *raft.RequestVoteArgs) *raft.RequestVoteReply {
	return &raft.RequestVoteReply{Term: a.Term, VoteGranted: a.CandidateID == "a"}
}

func (h *fakeHandler) HandleAppendEntries(a *raft.AppendEntriesArgs) *raft.AppendEntriesReply {
	if h.block != nil {
		<-h.block
	}
	return &raft.AppendEntriesReply{Term: a.Term, Success: len(a.Entries) == 2 && string(a.Entries[1].Data) == "y"}
}

func (h *fakeHandler) HandleInstallSnapshot(a *raft.InstallSnapshotArgs) *raft.InstallSnapshotReply {
	return &raft.InstallSnapshotReply{Term: uint64(len(a.Data))}
}

func (h *fakeHandler) HandleForward(cmds [][][]byte) ([][]byte, error) {
	if !h.leader {
		return nil, raft.ErrNotLeader
	}
	out := make([][]byte, len(cmds))
	for i, c := range cmds {
		out[i] = append([]byte("+"), c[0]...)
	}
	return out, nil
}

var discard = slog.New(slog.NewTextHandler(io.Discard, nil))

func pair(t *testing.T, h *fakeHandler) *TCP {
	t.Helper()
	server, err := Listen("127.0.0.1:0", nil, discard)
	if err != nil {
		t.Fatal(err)
	}
	if err := server.Serve(h, h); err != nil {
		t.Fatal(err)
	}
	client, err := Listen("127.0.0.1:0", map[string]string{"b": server.Addr().String(), "dead": "127.0.0.1:1"}, discard)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { client.Close(); server.Close() })
	return client
}

func TestRaftRPCs(t *testing.T) {
	c := pair(t, &fakeHandler{})
	ctx := context.Background()
	v, err := c.RequestVote(ctx, "b", &raft.RequestVoteArgs{Term: 4, CandidateID: "a"})
	if err != nil || !v.VoteGranted || v.Term != 4 {
		t.Fatalf("RequestVote = %+v, %v", v, err)
	}
	ae, err := c.AppendEntries(ctx, "b", &raft.AppendEntriesArgs{Term: 4, Entries: []raft.Entry{{Index: 1, Data: []byte("x")}, {Index: 2, Data: []byte("y")}}})
	if err != nil || !ae.Success {
		t.Fatalf("AppendEntries = %+v, %v", ae, err)
	}
	is, err := c.InstallSnapshot(ctx, "b", &raft.InstallSnapshotArgs{Data: make([]byte, 1<<20)})
	if err != nil || is.Term != 1<<20 {
		t.Fatalf("InstallSnapshot = %+v, %v", is, err)
	}
}

func TestForward(t *testing.T) {
	h := &fakeHandler{}
	c := pair(t, h)
	ctx := context.Background()
	if _, err := c.Forward(ctx, "b", [][][]byte{{[]byte("GET")}}); !errors.Is(err, raft.ErrNotLeader) {
		t.Fatalf("Forward to non-leader: %v, want ErrNotLeader", err)
	}
	h.leader = true
	out, err := c.Forward(ctx, "b", [][][]byte{{[]byte("A")}, {[]byte("B")}})
	if err != nil || len(out) != 2 || string(out[1]) != "+B" {
		t.Fatalf("Forward = %q, %v", out, err)
	}
}

func TestDialFailureIsNotSent(t *testing.T) {
	c := pair(t, &fakeHandler{})
	_, err := c.Forward(context.Background(), "dead", [][][]byte{{[]byte("SET")}})
	if !errors.Is(err, ErrNotSent) {
		t.Fatalf("got %v, want ErrNotSent", err)
	}
}

// A hung call is abandoned at its deadline and the connection replaced, so later calls work.
func TestTimeoutRecyclesConnection(t *testing.T) {
	h := &fakeHandler{block: make(chan struct{})}
	c := pair(t, h)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := c.AppendEntries(ctx, "b", &raft.AppendEntriesArgs{}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v, want deadline exceeded", err)
	}
	close(h.block)
	if _, err := c.RequestVote(context.Background(), "b", &raft.RequestVoteArgs{CandidateID: "a"}); err != nil {
		t.Fatalf("call after timeout: %v", err)
	}
}

func TestUnknownPeer(t *testing.T) {
	c := pair(t, &fakeHandler{})
	if _, err := c.RequestVote(context.Background(), "nobody", &raft.RequestVoteArgs{}); err == nil {
		t.Fatal("expected error for unknown peer")
	}
	var _ net.Addr = c.Addr()
}
