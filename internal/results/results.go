// Package results defines the JSON files the benchmark and chaos tools write and cmd/results reads.
package results

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"
)

// Node is one QuorumDB node, and where it ran (zone and machine type are empty outside Kubernetes).
type Node struct {
	ID          string `json:"id"`
	Address     string `json:"address"`
	Zone        string `json:"zone,omitempty"`
	MachineType string `json:"machine_type,omitempty"`
}

// Client describes the machine the benchmark or chaos client ran on.
type Client struct {
	OS          string `json:"os"`
	CPUs        int    `json:"cpus"`
	Zone        string `json:"zone,omitempty"`
	MachineType string `json:"machine_type,omitempty"`
}

// Benchmark is one file written by cmd/benchmark: every redis-benchmark run of one session.
type Benchmark struct {
	Kind        string            `json:"kind"`        // always "benchmark"
	Environment string            `json:"environment"` // "local" or "gke"
	Description string            `json:"description"` // free text, e.g. the laptop model
	StartedAt   time.Time         `json:"started_at"`
	Settings    BenchmarkSettings `json:"settings"`
	Nodes       []Node            `json:"nodes"`
	Client      Client            `json:"client"`
	Runs        []BenchmarkRun    `json:"runs"`
}

type BenchmarkSettings struct {
	Requests              int    `json:"requests"`
	Clients               int    `json:"clients"`
	Keyspace              int    `json:"keyspace"`
	ValueBytes            int    `json:"value_bytes"`
	Repeats               int    `json:"repeats"`
	RedisBenchmarkVersion string `json:"redis_benchmark_version"`
}

// BenchmarkRun is one command (SET or GET) from one redis-benchmark run.
type BenchmarkRun struct {
	Target            string  `json:"target"` // "leader" or "follower"
	Node              string  `json:"node"`   // the node the client connected to
	Pipeline          int     `json:"pipeline"`
	Repeat            int     `json:"repeat"`
	Test              string  `json:"test"` // "SET" or "GET"
	RequestsPerSecond float64 `json:"requests_per_second"`
	AverageMs         float64 `json:"average_ms"`
	P50Ms             float64 `json:"p50_ms"`
	P95Ms             float64 `json:"p95_ms"`
	P99Ms             float64 `json:"p99_ms"`
	MaxMs             float64 `json:"max_ms"`
	// LeaderChanged is true if the leader was different after the run, so the run measured a failover.
	LeaderChanged bool `json:"leader_changed"`
}

// The lines around the JSON when it is printed to standard output, so a script can cut it out of a
// Kubernetes Job's log, where it is mixed with progress messages.
const (
	BeginMarker = "=== BEGIN RESULTS JSON ==="
	EndMarker   = "=== END RESULTS JSON ==="
)

// Write saves value as indented JSON to path, or prints it between the markers if path is "-".
func Write(path string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	if path == "-" {
		fmt.Printf("%s\n%s%s\n", BeginMarker, data, EndMarker)
		return nil
	}
	return os.WriteFile(path, data, 0o644)
}

// Read loads a JSON file written by Write into value.
func Read(path string, value any) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(data, value); err != nil {
		return fmt.Errorf("%s: %w", path, err)
	}
	return nil
}

// Kind returns the "kind" field of a results file ("benchmark" or "chaos").
func Kind(path string) (string, error) {
	var header struct {
		Kind string `json:"kind"`
	}
	if err := Read(path, &header); err != nil {
		return "", err
	}
	return strings.TrimSpace(header.Kind), nil
}
