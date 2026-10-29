package main

import (
	"encoding/json"
	"net/http"

	"github.com/simar-s2/quorumdb/internal/raft"
)

// raftStatusSource is the part of the Raft node the health endpoints need; tests pass a fake.
type raftStatusSource interface {
	Status() raft.Status
}

// healthResponse is the JSON body of both endpoints.
type healthResponse struct {
	NodeID      string `json:"node_id"`
	Role        string `json:"role"`
	Term        uint64 `json:"term"`
	LeaderID    string `json:"leader_id"`
	CommitIndex uint64 `json:"commit_index"`
	LastApplied uint64 `json:"last_applied"`
	Ready       bool   `json:"ready"`
}

// newHealthHandler serves the endpoints that Kubernetes probes:
//
//	/healthz  liveness: 200 whenever the process answers. If it stops answering, Kubernetes restarts it.
//	/readyz   readiness: 200 once this node knows the current leader, so it can serve or forward
//	          requests; 503 while there is no leader (during an election, or when cut off from a majority).
func newHealthHandler(node raftStatusSource) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		writeHealthResponse(w, http.StatusOK, newHealthResponse(node.Status()))
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		response := newHealthResponse(node.Status())
		statusCode := http.StatusOK
		if !response.Ready {
			statusCode = http.StatusServiceUnavailable
		}
		writeHealthResponse(w, statusCode, response)
	})
	return mux
}

func newHealthResponse(status raft.Status) healthResponse {
	return healthResponse{
		NodeID:      status.ID,
		Role:        status.State.String(),
		Term:        status.Term,
		LeaderID:    status.LeaderID,
		CommitIndex: status.CommitIndex,
		LastApplied: status.LastApplied,
		Ready:       status.LeaderID != "",
	}
}

func writeHealthResponse(w http.ResponseWriter, statusCode int, response healthResponse) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(response)
}
