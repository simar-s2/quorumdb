package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/simar-s2/quorumdb/internal/raft"
)

type fakeRaftNode struct{ status raft.Status }

func (f *fakeRaftNode) Status() raft.Status { return f.status }

func getHealth(t *testing.T, handler http.Handler, path string) (int, healthResponse) {
	t.Helper()
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, path, nil))
	var response healthResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("%s: bad JSON %q: %v", path, recorder.Body.String(), err)
	}
	return recorder.Code, response
}

func TestHealthEndpoints(t *testing.T) {
	node := &fakeRaftNode{status: raft.Status{ID: "quorumdb-1", State: raft.Follower, Term: 7, LeaderID: "quorumdb-0", CommitIndex: 42, LastApplied: 41}}
	handler := newHealthHandler(node)

	code, response := getHealth(t, handler, "/readyz")
	if code != http.StatusOK || !response.Ready {
		t.Errorf("/readyz with a known leader: code %d, ready %v; want 200, true", code, response.Ready)
	}
	want := healthResponse{NodeID: "quorumdb-1", Role: "follower", Term: 7, LeaderID: "quorumdb-0", CommitIndex: 42, LastApplied: 41, Ready: true}
	if response != want {
		t.Errorf("/readyz body = %+v, want %+v", response, want)
	}

	// During an election there is no leader: not ready, but still alive.
	node.status = raft.Status{ID: "quorumdb-1", State: raft.Candidate, Term: 8}
	if code, response := getHealth(t, handler, "/readyz"); code != http.StatusServiceUnavailable || response.Ready {
		t.Errorf("/readyz without a leader: code %d, ready %v; want 503, false", code, response.Ready)
	}
	if code, response := getHealth(t, handler, "/healthz"); code != http.StatusOK || response.Role != "candidate" {
		t.Errorf("/healthz without a leader: code %d, role %q; want 200, candidate", code, response.Role)
	}
}
