// Command chaos runs the fault-injection test: kills, pauses and partitions, checked with porcupine.
package main

import (
	"bufio"
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/anishathalye/porcupine"
	"github.com/simar-s2/quorumdb/internal/chaos"
	"github.com/simar-s2/quorumdb/internal/results"
)

type config struct {
	clusterKind  string // "local" or "kubernetes"
	namespace    string
	environment  string
	bin          string
	runs         int
	duration     time.Duration
	clients      int
	registers    int
	counters     int
	seed         uint64
	out          string
	summaryPath  string
	snapshotAt   int
	checkTimeout time.Duration
	nodeFlags    []string
}

func main() {
	var cfg config
	var nodeFlags string
	flag.StringVar(&cfg.clusterKind, "cluster", "local", "local: start the nodes as processes here; kubernetes: test the QuorumDB pods (run from a pod in the cluster)")
	flag.StringVar(&cfg.namespace, "namespace", "quorumdb", "with -cluster kubernetes: the namespace of the QuorumDB StatefulSet")
	flag.StringVar(&cfg.environment, "environment", "", "label saved in the summary (default local, or gke with -cluster kubernetes)")
	flag.StringVar(&cfg.bin, "bin", "./bin/quorumdb", "with -cluster local: path to the quorumdb binary")
	flag.IntVar(&cfg.runs, "runs", 100, "number of independent runs")
	flag.DurationVar(&cfg.duration, "duration", 10*time.Second, "how long faults are injected in each run")
	flag.IntVar(&cfg.clients, "clients", 8, "concurrent clients")
	flag.IntVar(&cfg.registers, "keys", 4, "register keys (GET/SET/DEL)")
	flag.IntVar(&cfg.counters, "counters", 2, "counter keys (INCR/GET)")
	flag.Uint64Var(&cfg.seed, "seed", 0, "random seed (0 = time based)")
	flag.StringVar(&cfg.out, "out", "chaos-results", "directory for the summary and for logs of failed runs")
	flag.StringVar(&cfg.summaryPath, "summary", "", "where to write the JSON summary (default <out>/summary.json; - prints it)")
	flag.IntVar(&cfg.snapshotAt, "snapshot-threshold", 200, "node snapshot threshold; small so snapshots and InstallSnapshot happen often")
	flag.DurationVar(&cfg.checkTimeout, "check-timeout", 2*time.Minute, "porcupine time budget per run")
	flag.StringVar(&nodeFlags, "node-flags", "", "extra flags for every node, space separated (e.g. -unsafe-local-reads)")
	flag.Parse()
	if cfg.seed == 0 {
		cfg.seed = uint64(time.Now().UnixNano())
	}
	cfg.nodeFlags = strings.Fields(nodeFlags)

	// newCluster gives each run its cluster: three fresh processes, or the shared Kubernetes pods.
	var newCluster func(dir string) (cluster, error)
	switch cfg.clusterKind {
	case "local":
		if _, err := os.Stat(cfg.bin); err != nil {
			fatalf("node binary %s not found; run `make build` first", cfg.bin)
		}
		newCluster = func(dir string) (cluster, error) {
			c, err := startLocalCluster(cfg, dir)
			if err != nil {
				return nil, err
			}
			return c, nil
		}
	case "kubernetes":
		if len(cfg.nodeFlags) > 0 {
			fatalf("-node-flags only works with -cluster local")
		}
		if err := checkKubernetesSettings(cfg.namespace, cfg.snapshotAt); err != nil {
			fatalf("%v", err)
		}
		newCluster = func(string) (cluster, error) {
			c, err := connectKubernetesCluster(cfg.namespace)
			if err != nil {
				return nil, err
			}
			return c, nil
		}
	default:
		fatalf("-cluster must be local or kubernetes, not %q", cfg.clusterKind)
	}
	if cfg.environment == "" {
		cfg.environment = map[string]string{"local": "local", "kubernetes": "gke"}[cfg.clusterKind]
	}
	if err := os.MkdirAll(cfg.out, 0o755); err != nil {
		fatalf("%v", err)
	}
	if cfg.summaryPath == "" {
		cfg.summaryPath = filepath.Join(cfg.out, "summary.json")
	}

	interrupted := make(chan os.Signal, 1)
	signal.Notify(interrupted, os.Interrupt, syscall.SIGTERM)

	fmt.Printf("QuorumDB chaos test: %d runs x %s of faults, 3 nodes (%s), %d clients, seed %d\n",
		cfg.runs, cfg.duration, cfg.clusterKind, cfg.clients, cfg.seed)
	if len(cfg.nodeFlags) > 0 {
		fmt.Printf("node flags: %s\n", strings.Join(cfg.nodeFlags, " "))
	}
	fmt.Println()

	var runResults []*runResult
	start := time.Now()
	for i := 1; i <= cfg.runs; i++ {
		res := runOnce(cfg, i, newCluster, interrupted)
		runResults = append(runResults, res)
		fmt.Println(res.line(cfg.runs))
		if res.interrupted {
			break
		}
	}
	s := summarize(cfg, runResults, start)
	fmt.Println()
	fmt.Print(s.text)
	os.WriteFile(filepath.Join(cfg.out, "summary.txt"), []byte(s.text), 0o644)
	if err := results.Write(cfg.summaryPath, s.json); err != nil {
		fmt.Fprintf(os.Stderr, "chaos: writing the summary: %v\n", err)
	}
	if !s.passed {
		os.Exit(1)
	}
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "chaos: "+format+"\n", args...)
	os.Exit(2)
}

// --- One run ---

type runResult struct {
	run         int
	elapsed     time.Duration
	setupErr    string
	interrupted bool

	ops, acked, ambiguous, readsOK, readsFailed int
	faults                                      map[string]int

	// Acknowledged writes checked one by one at the end (write-once keys and INCRs), and how many were lost.
	confirmedChecked, lostWrites int

	linearizable porcupine.CheckResult
	converged    bool
	convergeInfo string
	counterErr   string
	finalErr     string
	dir          string
}

func (r *runResult) passed() bool {
	return r.setupErr == "" && r.finalErr == "" && r.linearizable == porcupine.Ok && r.converged && r.counterErr == "" &&
		r.lostWrites == 0
}

func mark(ok bool) string {
	if ok {
		return "ok"
	}
	return "FAIL"
}

func (r *runResult) line(total int) string {
	if r.setupErr != "" {
		return fmt.Sprintf("run %3d/%d  setup failed: %s", r.run, total, r.setupErr)
	}
	var fs []string
	for _, k := range sortedKeys(r.faults) {
		fs = append(fs, fmt.Sprintf("%s:%d", k, r.faults[k]))
	}
	s := fmt.Sprintf("run %3d/%d  ops %4d  acked %4d  ambiguous %3d  lost %d  [%s]  linearizable %s  converged %s  counters %s  %4.1fs",
		r.run, total, r.ops, r.acked, r.ambiguous, r.lostWrites, strings.Join(fs, " "),
		map[bool]string{true: "ok", false: string(r.linearizable)}[r.linearizable == porcupine.Ok],
		mark(r.converged), mark(r.counterErr == ""), r.elapsed.Seconds())
	for _, e := range []string{r.finalErr, r.counterErr} {
		if e != "" {
			s += "\n           " + e
		}
	}
	if !r.converged {
		s += "\n           not converged: " + r.convergeInfo
	}
	if !r.passed() {
		s += "\n           logs kept in " + r.dir
	}
	return s
}

func sortedKeys(m map[string]int) []string {
	ks := make([]string, 0, len(m))
	for k := range m {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	return ks
}

func runOnce(cfg config, run int, newCluster func(dir string) (cluster, error), interrupted <-chan os.Signal) *runResult {
	res := &runResult{run: run, faults: map[string]int{}}
	started := time.Now()
	defer func() { res.elapsed = time.Since(started) }()
	res.dir = filepath.Join(cfg.out, fmt.Sprintf("run-%03d", run))
	os.RemoveAll(res.dir)
	if err := os.MkdirAll(res.dir, 0o755); err != nil {
		res.setupErr = err.Error()
		return res
	}
	rng := rand.New(rand.NewPCG(cfg.seed, uint64(run)))

	c, err := newCluster(res.dir)
	if err != nil {
		res.setupErr = err.Error()
		return res
	}
	defer c.stop()
	if !waitLeader(c, 10*time.Second) {
		res.setupErr = "no leader elected within 10s"
		return res
	}

	// Keys look like "1790482999295790000-007/r2": the seed and run number make them new for every run,
	// so on a cluster shared by many runs, each run's history still starts from empty keys.
	keyPrefix := fmt.Sprintf("%d-%03d/", cfg.seed, run)
	rec := &recorder{start: time.Now(), ackedIncr: map[string]int{}, ambiguousIncr: map[string]int{},
		writeOnceAcked: map[string]string{}}
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < cfg.clients; i++ {
		cl := &client{id: i, keyPrefix: keyPrefix, c: c, rec: rec, rng: rand.New(rand.NewPCG(cfg.seed+uint64(run), uint64(i)))}
		wg.Add(1)
		go func() { defer wg.Done(); cl.run(cfg, stop) }()
	}

	nm := &nemesis{c: c, rng: rng, counts: res.faults, start: rec.start}
	done := make(chan struct{})
	go func() { nm.run(cfg.duration); close(done) }()
	select {
	case <-done:
	case <-interrupted:
		res.interrupted = true
		<-done
	}

	// Heal everything and keep clients running so the history also covers recovery.
	nm.logf("heal all")
	c.healAll()
	waitLeader(c, 10*time.Second)
	time.Sleep(2 * time.Second)
	close(stop)
	wg.Wait()

	// Final reads of every key, recorded like any other operation; a lost acked write fails the check.
	final := &client{id: cfg.clients, keyPrefix: keyPrefix, c: c, rec: rec, rng: rand.New(rand.NewPCG(cfg.seed, 99))}
	finalValues := map[string]chaos.Output{}
	var keys []string
	for i := 0; i < cfg.registers; i++ {
		keys = append(keys, fmt.Sprintf("%sr%d", keyPrefix, i))
	}
	for i := 0; i < cfg.counters; i++ {
		keys = append(keys, fmt.Sprintf("%sc%d", keyPrefix, i))
	}
	for _, k := range keys {
		ok := false
		for deadline := time.Now().Add(15 * time.Second); time.Now().Before(deadline); {
			rec.mu.Lock()
			n := len(rec.ops)
			rec.mu.Unlock()
			if final.do(chaos.Input{Op: chaos.OpGet, Key: k}) {
				rec.mu.Lock()
				finalValues[k] = rec.ops[n].Output.(chaos.Output)
				rec.mu.Unlock()
				ok = true
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
		if !ok {
			res.finalErr = "could not read final value of " + k
		}
	}
	// Every acknowledged write-once key must still be there with its value.
	lostWriteOnce, err := final.readBackWriteOnceKeys(rec.writeOnceAcked)
	if err != nil {
		res.finalErr = err.Error()
	}
	res.confirmedChecked += len(rec.writeOnceAcked)
	res.lostWrites += lostWriteOnce
	res.converged, res.convergeInfo = waitConverged(c, 20*time.Second)

	// Counter check: each final value must lie between acked and acked + ambiguous INCRs.
	for i := 0; i < cfg.counters; i++ {
		k := fmt.Sprintf("%sc%d", keyPrefix, i)
		out, ok := finalValues[k]
		if !ok {
			continue
		}
		var v int64
		if out.Exists {
			v, _ = strconv.ParseInt(out.Value, 10, 64)
		}
		lo, hi := int64(rec.ackedIncr[k]), int64(rec.ackedIncr[k]+rec.ambiguousIncr[k])
		// A final value below the acknowledged count means that many acknowledged INCRs were lost.
		res.confirmedChecked += rec.ackedIncr[k]
		if v < lo {
			res.lostWrites += int(lo - v)
		}
		if v < lo || v > hi {
			res.counterErr += fmt.Sprintf("counter %s = %d, expected between %d (acked) and %d (acked + ambiguous); ", k, v, lo, hi)
		}
	}

	// Ambiguous operations may take effect at any time after they started.
	end := rec.now() + 1
	for i := range rec.ops {
		if rec.ops[i].Return == pending {
			rec.ops[i].Return = end
		}
	}
	res.ops = len(rec.ops)
	res.acked, res.ambiguous, res.readsOK, res.readsFailed = rec.acked, rec.ambiguous, rec.readsOK, rec.readsFailed
	res.acked += len(rec.writeOnceAcked)
	res.ambiguous += len(rec.writeOnceAmbiguous)

	// Delete this run's keys, so a cluster shared by many runs does not keep growing (which would make
	// every snapshot bigger and later runs slower than earlier ones).
	runKeys := append(keys, rec.writeOnceAmbiguous...)
	for key := range rec.writeOnceAcked {
		runKeys = append(runKeys, key)
	}
	final.deleteKeys(runKeys)
	final.close()

	result, info := porcupine.CheckOperationsVerbose(chaos.Model, rec.ops, cfg.checkTimeout)
	res.linearizable = result

	os.WriteFile(filepath.Join(res.dir, "nemesis.log"), []byte(strings.Join(nm.events, "\n")+"\n"), 0o644)
	if result == porcupine.Illegal {
		porcupine.VisualizePath(chaos.Model, info, filepath.Join(res.dir, "linearizability.html"))
	}
	c.stop()
	if res.passed() {
		os.RemoveAll(res.dir)
	} else {
		saveHistory(filepath.Join(res.dir, "history.txt"), rec.ops)
		c.saveLogs(res.dir, started)
	}
	return res
}

func saveHistory(path string, ops []porcupine.Operation) {
	f, err := os.Create(path)
	if err != nil {
		return
	}
	defer f.Close()
	w := bufio.NewWriter(f)
	defer w.Flush()
	sorted := append([]porcupine.Operation(nil), ops...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Call < sorted[j].Call })
	for _, op := range sorted {
		fmt.Fprintf(w, "client %2d  [%12.3fms, %12.3fms]  %s\n", op.ClientId,
			float64(op.Call)/1e6, float64(op.Return)/1e6, chaos.Model.DescribeOperation(op.Input, op.Output))
	}
}

// --- Summary ---

type summary struct {
	text   string
	json   results.Chaos
	passed bool
}

// summary is this run's entry in the JSON summary.
func (r *runResult) summary() results.ChaosRun {
	var problems []string
	for _, problem := range []string{r.setupErr, r.finalErr, r.counterErr} {
		if problem != "" {
			problems = append(problems, problem)
		}
	}
	if r.setupErr == "" && !r.converged {
		problems = append(problems, "not converged: "+r.convergeInfo)
	}
	return results.ChaosRun{
		Run:                    r.run,
		Passed:                 r.passed(),
		Seconds:                r.elapsed.Seconds(),
		Operations:             r.ops,
		AcknowledgedWrites:     r.acked,
		AmbiguousWrites:        r.ambiguous,
		ConfirmedWritesChecked: r.confirmedChecked,
		LostConfirmedWrites:    r.lostWrites,
		Linearizable:           string(r.linearizable),
		Converged:              r.converged,
		Faults:                 r.faults,
		Error:                  strings.Join(problems, "; "),
	}
}

func summarize(cfg config, runResults []*runResult, start time.Time) summary {
	elapsed := time.Since(start)
	s := results.Chaos{
		Kind:        "chaos",
		Environment: cfg.environment,
		StartedAt:   start.UTC(),
		WallSeconds: elapsed.Seconds(),
		Settings: results.ChaosSettings{
			Clients:            cfg.clients,
			Registers:          cfg.registers,
			Counters:           cfg.counters,
			FaultSecondsPerRun: cfg.duration.Seconds(),
			SnapshotThreshold:  cfg.snapshotAt,
			Seed:               cfg.seed,
			NodeFlags:          strings.Join(cfg.nodeFlags, " "),
		},
		Client: results.Client{OS: runtime.GOOS + "/" + runtime.GOARCH, CPUs: runtime.NumCPU()},
		Runs:   len(runResults),
		Faults: map[string]int{},
	}
	if cfg.clusterKind == "kubernetes" {
		var placement results.Client
		s.Nodes, placement = kubernetesPlacement(cfg.namespace)
		s.Client.Zone, s.Client.MachineType = placement.Zone, placement.MachineType
	}
	for _, r := range runResults {
		s.PerRun = append(s.PerRun, r.summary())
		if r.passed() {
			s.Passed++
		}
		if r.setupErr != "" {
			s.SetupFailures++
			continue
		}
		switch r.linearizable {
		case porcupine.Illegal:
			s.LinearizabilityViolations++
		case porcupine.Unknown:
			s.PorcupineTimeouts++
		}
		if !r.converged {
			s.NotConverged++
		}
		if r.counterErr != "" {
			s.CounterFailures++
		}
		if r.finalErr != "" {
			s.FinalReadFailures++
		}
		s.Operations += r.ops
		s.AcknowledgedWrites += r.acked
		s.AmbiguousWrites += r.ambiguous
		s.SuccessfulReads += r.readsOK
		s.FailedReads += r.readsFailed
		s.ConfirmedWritesChecked += r.confirmedChecked
		s.LostConfirmedWrites += r.lostWrites
		for kind, count := range r.faults {
			s.Faults[kind] += count
			s.TotalFaults += count
		}
	}

	checked := s.Runs - s.SetupFailures
	var faultList []string
	for _, kind := range sortedKeys(s.Faults) {
		faultList = append(faultList, fmt.Sprintf("%s %d", kind, s.Faults[kind]))
	}
	var b strings.Builder
	fmt.Fprintf(&b, "==== QuorumDB chaos summary ====\n")
	fmt.Fprintf(&b, "runs passed                 %d / %d\n", s.Passed, s.Runs)
	fmt.Fprintf(&b, "linearizability violations  %d   (porcupine: %d ok, %d illegal, %d timed out)\n",
		s.LinearizabilityViolations, checked-s.LinearizabilityViolations-s.PorcupineTimeouts, s.LinearizabilityViolations, s.PorcupineTimeouts)
	fmt.Fprintf(&b, "lost confirmed writes       %d   (of %d checked one by one: write-once keys read back, INCRs vs final counters)\n",
		s.LostConfirmedWrites, s.ConfirmedWritesChecked)
	fmt.Fprintf(&b, "replicas converged          %d / %d\n", checked-s.NotConverged, checked)
	fmt.Fprintf(&b, "counter checks passed       %d / %d\n", checked-s.CounterFailures, checked)
	fmt.Fprintf(&b, "operations recorded         %d\n", s.Operations)
	fmt.Fprintf(&b, "  acknowledged writes       %d\n", s.AcknowledgedWrites)
	fmt.Fprintf(&b, "  ambiguous writes          %d   (timed out or failed; may or may not have applied)\n", s.AmbiguousWrites)
	fmt.Fprintf(&b, "  successful reads          %d   (%d failed reads not recorded)\n", s.SuccessfulReads, s.FailedReads)
	fmt.Fprintf(&b, "faults injected             %d   (%s)\n", s.TotalFaults, strings.Join(faultList, ", "))
	fmt.Fprintf(&b, "setup                       3 nodes, %d clients, %d registers + %d counters, %s of faults per run, snapshot every %d entries\n",
		cfg.clients, cfg.registers, cfg.counters, cfg.duration, cfg.snapshotAt)
	fmt.Fprintf(&b, "seed                        %d\n", cfg.seed)
	fmt.Fprintf(&b, "client                      %s, %d CPUs %s %s\n", s.Client.OS, s.Client.CPUs, s.Client.MachineType, s.Client.Zone)
	for _, node := range s.Nodes {
		fmt.Fprintf(&b, "node                        %s in %s (%s)\n", node.ID, node.Zone, node.MachineType)
	}
	fmt.Fprintf(&b, "wall time                   %s\n", elapsed.Round(time.Second))
	if len(cfg.nodeFlags) > 0 {
		fmt.Fprintf(&b, "node flags                  %s\n", strings.Join(cfg.nodeFlags, " "))
	}
	return summary{text: b.String(), json: s, passed: s.Passed == s.Runs && s.Runs == cfg.runs}
}
