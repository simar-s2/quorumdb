// Package results defines the JSON files the benchmark and chaos tools write and cmd/results reads.
package results

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
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

// Chaos is the summary written by cmd/chaos: totals over every run, then one entry per run.
type Chaos struct {
	Kind        string        `json:"kind"`        // always "chaos"
	Environment string        `json:"environment"` // "local" or "gke"
	StartedAt   time.Time     `json:"started_at"`
	WallSeconds float64       `json:"wall_seconds"`
	Settings    ChaosSettings `json:"settings"`
	Nodes       []Node        `json:"nodes,omitempty"`
	Client      Client        `json:"client"`

	Runs                      int `json:"runs"`
	Passed                    int `json:"passed"`
	LinearizabilityViolations int `json:"linearizability_violations"`
	PorcupineTimeouts         int `json:"porcupine_timeouts"`
	NotConverged              int `json:"not_converged"`
	CounterFailures           int `json:"counter_failures"`
	FinalReadFailures         int `json:"final_read_failures"`
	SetupFailures             int `json:"setup_failures"`

	Operations int `json:"operations"`
	// AcknowledgedWrites counts every write that got a success reply: the porcupine-checked
	// SET/DEL/INCR operations plus the write-once SETs.
	AcknowledgedWrites int `json:"acknowledged_writes"`
	AmbiguousWrites    int `json:"ambiguous_writes"`
	SuccessfulReads    int `json:"successful_reads"`
	FailedReads        int `json:"failed_reads"`
	// ConfirmedWritesChecked counts acknowledged writes whose survival was checked one by one: the
	// write-once keys read back at the end, and the acknowledged INCRs compared with the final
	// counter values. LostConfirmedWrites is how many of them were missing.
	ConfirmedWritesChecked int `json:"confirmed_writes_checked"`
	LostConfirmedWrites    int `json:"lost_confirmed_writes"`

	Faults      map[string]int `json:"faults"`
	TotalFaults int            `json:"total_faults"`

	// FailoverMilliseconds holds, per kind of fault that took out the leader (kill, pause, isolate,
	// leader-zone), how long the other two nodes took to elect a new leader after the fault was
	// applied. It is only measured when both other nodes were up. FailoverTimeouts counts the faults
	// after which no new leader appeared within 10 s.
	FailoverMilliseconds map[string][]float64 `json:"failover_milliseconds"`
	FailoverTimeouts     map[string]int       `json:"failover_timeouts"`

	PerRun []ChaosRun `json:"per_run"`
}

type ChaosSettings struct {
	Clients            int     `json:"clients"`
	Registers          int     `json:"registers"`
	Counters           int     `json:"counters"`
	FaultSecondsPerRun float64 `json:"fault_seconds_per_run"`
	SnapshotThreshold  int     `json:"snapshot_threshold"`
	Seed               uint64  `json:"seed"`
	NodeFlags          string  `json:"node_flags,omitempty"`
}

// ChaosRun is the outcome of one run.
type ChaosRun struct {
	Run                    int            `json:"run"`
	Passed                 bool           `json:"passed"`
	Seconds                float64        `json:"seconds"`
	Operations             int            `json:"operations"`
	AcknowledgedWrites     int            `json:"acknowledged_writes"`
	AmbiguousWrites        int            `json:"ambiguous_writes"`
	ConfirmedWritesChecked int            `json:"confirmed_writes_checked"`
	LostConfirmedWrites    int            `json:"lost_confirmed_writes"`
	Linearizable           string         `json:"linearizable"` // porcupine's verdict: Ok, Illegal or Unknown
	Converged              bool           `json:"converged"`
	Faults                 map[string]int `json:"faults"`
	Error                  string         `json:"error,omitempty"`
}

// Median returns the middle value (the mean of the two middle values for an even count), or 0.
func Median(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sorted := append([]float64(nil), values...)
	sort.Float64s(sorted)
	middle := len(sorted) / 2
	if len(sorted)%2 == 1 {
		return sorted[middle]
	}
	return (sorted[middle-1] + sorted[middle]) / 2
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
