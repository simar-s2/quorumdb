package main

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/simar-s2/quorumdb/internal/netproxy"
	"github.com/simar-s2/quorumdb/internal/resp"
)

// nodeCount is the cluster size; the nemesis and the checks assume three nodes.
const nodeCount = 3

// link is the traffic one node sends to another: its Raft RPCs and forwarded client commands.
type link struct{ from, to int }

// cluster is the system under test: three QuorumDB nodes, and the means to break them.
// localCluster runs them as processes on this machine; kubernetesCluster drives pods with kubectl.
type cluster interface {
	nodeName(node int) string
	redisAddress(node int) string
	// zone is the failure zone a node runs in. Each local node counts as its own zone.
	zone(node int) string

	// kill crashes a node with SIGKILL and reports whether it was running, and when the kill was sent.
	kill(node int) (killedAt time.Time, wasRunning bool)
	// restart brings a killed node back.
	restart(node int)
	// setFrozen pauses (SIGSTOP) or resumes (SIGCONT) a node and reports whether it was running.
	setFrozen(node int, frozen bool) bool
	// setLinks cuts the given links (mode Reset or Drop) or restores them (mode Pass).
	setLinks(links []link, mode netproxy.Mode)
	// linkModes lists the ways this cluster can cut a link.
	linkModes() []netproxy.Mode

	// healAll restores every link, resumes frozen nodes and restarts killed ones.
	healAll()
	// stop shuts the cluster down at the end of a run.
	stop()
	// saveLogs puts the nodes' logs since the run started into dir, for a failed run.
	saveLogs(dir string, since time.Time)
}

// query sends one command to a node with a short timeout.
func query(addr string, timeout time.Duration, args ...string) (resp.Value, error) {
	conn, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return resp.Value{}, err
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(timeout))
	if _, err := conn.Write(resp.AppendCommand(nil, args...)); err != nil {
		return resp.Value{}, err
	}
	return resp.NewReader(conn).ReadReply()
}

func infoField(info, field string) string {
	for _, line := range strings.Split(info, "\r\n") {
		if v, ok := strings.CutPrefix(line, field+":"); ok {
			return v
		}
	}
	return ""
}

// leader returns the index of the node leading with the highest term, or -1.
func leader(c cluster) int {
	node, _ := leaderAndTerm(c, -1)
	return node
}

// leaderAndTerm returns the node leading with the highest term and that term, asking every node
// except skip (-1 asks all). It returns -1 if no node it reached is leading.
func leaderAndTerm(c cluster, skip int) (node, term int) {
	best, bestTerm := -1, -1
	for i := 0; i < nodeCount; i++ {
		if i == skip {
			continue
		}
		v, err := query(c.redisAddress(i), 200*time.Millisecond, "INFO", "raft")
		if err != nil {
			continue
		}
		term, _ := strconv.Atoi(infoField(v.Str, "term"))
		if infoField(v.Str, "role") == "leader" && term > bestTerm {
			best, bestTerm = i, term
		}
	}
	return best, bestTerm
}

// answers reports whether a node replies to INFO, i.e. it is running, not frozen and reachable.
func answers(c cluster, node int) bool {
	_, err := query(c.redisAddress(node), 200*time.Millisecond, "INFO", "raft")
	return err == nil
}

func waitLeader(c cluster, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if leader(c) >= 0 {
			return true
		}
		time.Sleep(50 * time.Millisecond)
	}
	return false
}

// waitConverged waits until all nodes report the same applied index and state digest.
func waitConverged(c cluster, timeout time.Duration) (bool, string) {
	deadline := time.Now().Add(timeout)
	last := ""
	for time.Now().Before(deadline) {
		var applied, digests []string
		for i := 0; i < nodeCount; i++ {
			info, err1 := query(c.redisAddress(i), 500*time.Millisecond, "INFO", "raft")
			dig, err2 := query(c.redisAddress(i), 500*time.Millisecond, "DEBUG", "DIGEST")
			if err1 != nil || err2 != nil {
				applied = append(applied, "?")
				digests = append(digests, "?")
				continue
			}
			applied = append(applied, infoField(info.Str, "last_applied"))
			digests = append(digests, dig.Str)
		}
		last = fmt.Sprintf("applied=%v digests=%v", applied, digests)
		if applied[0] != "?" && applied[0] == applied[1] && applied[1] == applied[2] &&
			digests[0] == digests[1] && digests[1] == digests[2] {
			return true, last
		}
		time.Sleep(100 * time.Millisecond)
	}
	return false, last
}
