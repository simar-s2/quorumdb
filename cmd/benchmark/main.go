// Command benchmark runs redis-benchmark against a QuorumDB cluster's leader and one follower,
// repeats every setting, and saves each result as JSON. It runs the same way against the local
// Docker cluster and inside the GKE cluster.
package main

import (
	"bytes"
	"encoding/csv"
	"flag"
	"fmt"
	"net"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/simar-s2/quorumdb/internal/resp"
	"github.com/simar-s2/quorumdb/internal/results"
)

// setting is one row of the results table: which node the client talks to, and the pipeline depth.
type setting struct {
	target   string // "leader" or "follower" (which forwards every command to the leader)
	pipeline int
}

var settings = []setting{
	{target: "leader", pipeline: 1},
	{target: "leader", pipeline: 16},
	{target: "follower", pipeline: 1},
}

// valueBytes is the size of each SET value (redis-benchmark's default, passed explicitly).
const valueBytes = 3

func main() {
	var (
		nodeList    = flag.String("nodes", "n1=n1:6379,n2=n2:6379,n3=n3:6379", "id=host:port of every node")
		environment = flag.String("environment", "local", "label saved with the results: local or gke")
		description = flag.String("description", "", "free text saved with the results, e.g. the machine it ran on")
		requests    = flag.Int("requests", 200000, "requests per redis-benchmark run")
		clients     = flag.Int("clients", 50, "parallel connections")
		keyspace    = flag.Int("keyspace", 100000, "random keys are drawn from this many")
		repeats     = flag.Int("repeats", 3, "runs of every setting")
		outputPath  = flag.String("out", "-", "where to write the JSON results; - prints them to standard output")
	)
	flag.Parse()

	nodes, err := parseNodes(*nodeList)
	if err != nil {
		fatal("bad -nodes: %v", err)
	}
	version, err := exec.Command("redis-benchmark", "--version").Output()
	if err != nil {
		fatal("redis-benchmark not found: %v", err)
	}

	file := results.Benchmark{
		Kind:        "benchmark",
		Environment: *environment,
		Description: *description,
		StartedAt:   time.Now().UTC(),
		Settings: results.BenchmarkSettings{
			Requests:              *requests,
			Clients:               *clients,
			Keyspace:              *keyspace,
			ValueBytes:            valueBytes,
			Repeats:               *repeats,
			RedisBenchmarkVersion: strings.TrimSpace(string(version)),
		},
		Nodes:  nodes,
		Client: results.Client{OS: runtime.GOOS + "/" + runtime.GOARCH, CPUs: runtime.NumCPU()},
	}

	// Each repeat runs every setting once, rather than all repeats of one setting back to back, so
	// slow drift (a disk warming up, the dataset growing) affects every setting equally.
	for repeat := 1; repeat <= *repeats; repeat++ {
		for _, s := range settings {
			leader, follower, err := findLeaderAndFollower(nodes)
			if err != nil {
				fatal("%v", err)
			}
			target := leader
			if s.target == "follower" {
				target = follower
			}
			arguments := []string{
				"-h", host(target.Address), "-p", port(target.Address),
				"-t", "set,get", "-n", strconv.Itoa(*requests), "-c", strconv.Itoa(*clients),
				"-P", strconv.Itoa(s.pipeline), "-r", strconv.Itoa(*keyspace), "-d", strconv.Itoa(valueBytes), "--csv",
			}
			fmt.Fprintf(os.Stderr, "repeat %d/%d  %-8s %-10s pipeline %-2d  ", repeat, *repeats, s.target, target.ID, s.pipeline)
			output, err := exec.Command("redis-benchmark", arguments...).Output()
			if err != nil {
				fatal("redis-benchmark %s: %v", strings.Join(arguments, " "), err)
			}
			rows, err := parseCSV(output)
			if err != nil {
				fatal("parsing redis-benchmark output: %v\n%s", err, output)
			}

			leaderAfter, _, _ := findLeaderAndFollower(nodes)
			for _, row := range rows {
				row.Target, row.Node, row.Pipeline, row.Repeat = s.target, target.ID, s.pipeline, repeat
				row.LeaderChanged = leaderAfter.ID != leader.ID
				file.Runs = append(file.Runs, row)
				fmt.Fprintf(os.Stderr, "%s %9.0f req/s  p50 %6.3f ms  p99 %6.3f ms   ", row.Test, row.RequestsPerSecond, row.P50Ms, row.P99Ms)
			}
			fmt.Fprintln(os.Stderr)
		}
	}

	if err := results.Write(*outputPath, file); err != nil {
		fatal("writing results: %v", err)
	}
}

// parseCSV reads redis-benchmark --csv output, one header line and then one line per command:
//
//	"test","rps","avg_latency_ms","min_latency_ms","p50_latency_ms","p95_latency_ms","p99_latency_ms","max_latency_ms"
//	"SET","38292.94","1.208","0.320","1.111","2.159","5.127","13.287"
func parseCSV(output []byte) ([]results.BenchmarkRun, error) {
	records, err := csv.NewReader(bytes.NewReader(output)).ReadAll()
	if err != nil {
		return nil, err
	}
	if len(records) < 2 {
		return nil, fmt.Errorf("expected a header and at least one row")
	}
	column := map[string]int{}
	for i, name := range records[0] {
		column[name] = i
	}
	number := func(record []string, name string) float64 {
		value, _ := strconv.ParseFloat(record[column[name]], 64)
		return value
	}
	var rows []results.BenchmarkRun
	for _, record := range records[1:] {
		rows = append(rows, results.BenchmarkRun{
			Test:              record[column["test"]],
			RequestsPerSecond: number(record, "rps"),
			AverageMs:         number(record, "avg_latency_ms"),
			P50Ms:             number(record, "p50_latency_ms"),
			P95Ms:             number(record, "p95_latency_ms"),
			P99Ms:             number(record, "p99_latency_ms"),
			MaxMs:             number(record, "max_latency_ms"),
		})
	}
	return rows, nil
}

// findLeaderAndFollower asks every node for its Raft role and returns the leader and the first follower.
func findLeaderAndFollower(nodes []results.Node) (leader, follower results.Node, err error) {
	for _, node := range nodes {
		switch raftRole(node.Address) {
		case "leader":
			leader = node
		case "follower":
			if follower.ID == "" {
				follower = node
			}
		}
	}
	if leader.ID == "" || follower.ID == "" {
		return leader, follower, fmt.Errorf("need a leader and a follower; is the cluster up?")
	}
	return leader, follower, nil
}

// raftRole returns the role line of INFO raft ("leader", "follower", ...), or "" if the node does not answer.
func raftRole(address string) string {
	connection, err := net.DialTimeout("tcp", address, time.Second)
	if err != nil {
		return ""
	}
	defer connection.Close()
	connection.SetDeadline(time.Now().Add(2 * time.Second))
	if _, err := connection.Write(resp.AppendCommand(nil, "INFO", "raft")); err != nil {
		return ""
	}
	reply, err := resp.NewReader(connection).ReadReply()
	if err != nil {
		return ""
	}
	for _, line := range strings.Split(reply.Str, "\r\n") {
		if role, found := strings.CutPrefix(line, "role:"); found {
			return role
		}
	}
	return ""
}

func parseNodes(list string) ([]results.Node, error) {
	var nodes []results.Node
	for _, part := range strings.Split(list, ",") {
		id, address, found := strings.Cut(strings.TrimSpace(part), "=")
		if !found || id == "" || address == "" {
			return nil, fmt.Errorf("%q is not id=host:port", part)
		}
		nodes = append(nodes, results.Node{ID: id, Address: address})
	}
	return nodes, nil
}

func host(address string) string { h, _, _ := net.SplitHostPort(address); return h }
func port(address string) string { _, p, _ := net.SplitHostPort(address); return p }

func fatal(format string, arguments ...any) {
	fmt.Fprintf(os.Stderr, "benchmark: "+format+"\n", arguments...)
	os.Exit(1)
}
