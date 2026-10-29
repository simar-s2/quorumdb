// Command chaos runs the fault-injection test: kills, pauses and partitions, checked with porcupine.
package main

import (
	"bufio"
	"encoding/json"
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
)

type config struct {
	bin          string
	runs         int
	duration     time.Duration
	clients      int
	registers    int
	counters     int
	seed         uint64
	out          string
	snapshotAt   int
	checkTimeout time.Duration
	nodeFlags    []string
}

func main() {
	var cfg config
	var nodeFlags string
	flag.StringVar(&cfg.bin, "bin", "./bin/quorumdb", "path to the quorumdb binary")
	flag.IntVar(&cfg.runs, "runs", 100, "number of independent runs")
	flag.DurationVar(&cfg.duration, "duration", 10*time.Second, "how long faults are injected in each run")
	flag.IntVar(&cfg.clients, "clients", 8, "concurrent clients")
	flag.IntVar(&cfg.registers, "keys", 4, "register keys (GET/SET/DEL)")
	flag.IntVar(&cfg.counters, "counters", 2, "counter keys (INCR/GET)")
	flag.Uint64Var(&cfg.seed, "seed", 0, "random seed (0 = time based)")
	flag.StringVar(&cfg.out, "out", "chaos-results", "directory for the summary and for logs of failed runs")
	flag.IntVar(&cfg.snapshotAt, "snapshot-threshold", 200, "node snapshot threshold; small so snapshots and InstallSnapshot happen often")
	flag.DurationVar(&cfg.checkTimeout, "check-timeout", 2*time.Minute, "porcupine time budget per run")
	flag.StringVar(&nodeFlags, "node-flags", "", "extra flags for every node, space separated (e.g. -unsafe-local-reads)")
	flag.Parse()
	if cfg.seed == 0 {
		cfg.seed = uint64(time.Now().UnixNano())
	}
	cfg.nodeFlags = strings.Fields(nodeFlags)
	if _, err := os.Stat(cfg.bin); err != nil {
		fatalf("node binary %s not found; run `make build` first", cfg.bin)
	}
	if err := os.MkdirAll(cfg.out, 0o755); err != nil {
		fatalf("%v", err)
	}

	interrupted := make(chan os.Signal, 1)
	signal.Notify(interrupted, os.Interrupt, syscall.SIGTERM)

	fmt.Printf("QuorumDB chaos test: %d runs x %s of faults, 3 nodes, %d clients, seed %d\n",
		cfg.runs, cfg.duration, cfg.clients, cfg.seed)
	if len(cfg.nodeFlags) > 0 {
		fmt.Printf("node flags: %s\n", strings.Join(cfg.nodeFlags, " "))
	}
	fmt.Println()

	var results []*runResult
	start := time.Now()
	for i := 1; i <= cfg.runs; i++ {
		res := runOnce(cfg, i, interrupted)
		results = append(results, res)
		fmt.Println(res.line(cfg.runs))
		if res.interrupted {
			break
		}
	}
	s := summarize(cfg, results, time.Since(start))
	fmt.Println()
	fmt.Print(s.text)
	os.WriteFile(filepath.Join(cfg.out, "summary.txt"), []byte(s.text), 0o644)
	if b, err := json.MarshalIndent(s.json, "", "  "); err == nil {
		os.WriteFile(filepath.Join(cfg.out, "summary.json"), b, 0o644)
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

	linearizable porcupine.CheckResult
	converged    bool
	convergeInfo string
	counterErr   string
	finalErr     string
	dir          string
}

func (r *runResult) passed() bool {
	return r.setupErr == "" && r.finalErr == "" && r.linearizable == porcupine.Ok && r.converged && r.counterErr == ""
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
	s := fmt.Sprintf("run %3d/%d  ops %4d  acked %4d  ambiguous %3d  [%s]  linearizable %s  converged %s  counters %s  %4.1fs",
		r.run, total, r.ops, r.acked, r.ambiguous, strings.Join(fs, " "),
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

func runOnce(cfg config, run int, interrupted <-chan os.Signal) *runResult {
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

	c, err := startLocalCluster(cfg, res.dir)
	if err != nil {
		res.setupErr = err.Error()
		return res
	}
	defer c.stop()
	if !waitLeader(c, 10*time.Second) {
		res.setupErr = "no leader elected within 10s"
		return res
	}

	rec := &recorder{start: time.Now(), ackedIncr: map[string]int{}, ambiguousIncr: map[string]int{}}
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < cfg.clients; i++ {
		cl := &client{id: i, c: c, rec: rec, rng: rand.New(rand.NewPCG(cfg.seed+uint64(run), uint64(i)))}
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
	final := &client{id: cfg.clients, c: c, rec: rec, rng: rand.New(rand.NewPCG(cfg.seed, 99))}
	finalValues := map[string]chaos.Output{}
	var keys []string
	for i := 0; i < cfg.registers; i++ {
		keys = append(keys, fmt.Sprintf("r%d", i))
	}
	for i := 0; i < cfg.counters; i++ {
		keys = append(keys, fmt.Sprintf("c%d", i))
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
	final.close()
	res.converged, res.convergeInfo = waitConverged(c, 20*time.Second)

	// Counter check: each final value must lie between acked and acked + ambiguous INCRs.
	for i := 0; i < cfg.counters; i++ {
		k := fmt.Sprintf("c%d", i)
		out, ok := finalValues[k]
		if !ok {
			continue
		}
		var v int64
		if out.Exists {
			v, _ = strconv.ParseInt(out.Value, 10, 64)
		}
		lo, hi := int64(rec.ackedIncr[k]), int64(rec.ackedIncr[k]+rec.ambiguousIncr[k])
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
	json   map[string]any
	passed bool
}

func summarize(cfg config, results []*runResult, elapsed time.Duration) summary {
	var passed, setupFailed, illegal, unknown, notConverged, counterBad, finalBad int
	var ops, acked, ambiguous, readsOK, readsFailed int
	faults := map[string]int{}
	for _, r := range results {
		if r.passed() {
			passed++
		}
		if r.setupErr != "" {
			setupFailed++
			continue
		}
		switch r.linearizable {
		case porcupine.Illegal:
			illegal++
		case porcupine.Unknown:
			unknown++
		}
		if !r.converged {
			notConverged++
		}
		if r.counterErr != "" {
			counterBad++
		}
		if r.finalErr != "" {
			finalBad++
		}
		ops += r.ops
		acked += r.acked
		ambiguous += r.ambiguous
		readsOK += r.readsOK
		readsFailed += r.readsFailed
		for k, v := range r.faults {
			faults[k] += v
		}
	}
	checked := len(results) - setupFailed
	var fs []string
	total := 0
	for _, k := range sortedKeys(faults) {
		fs = append(fs, fmt.Sprintf("%s %d", k, faults[k]))
		total += faults[k]
	}
	var b strings.Builder
	fmt.Fprintf(&b, "==== QuorumDB chaos summary ====\n")
	fmt.Fprintf(&b, "runs passed                 %d / %d\n", passed, len(results))
	fmt.Fprintf(&b, "linearizability violations  %d   (porcupine: %d ok, %d illegal, %d timed out)\n", illegal, checked-illegal-unknown, illegal, unknown)
	fmt.Fprintf(&b, "replicas converged          %d / %d\n", checked-notConverged, checked)
	fmt.Fprintf(&b, "counter checks passed       %d / %d\n", checked-counterBad, checked)
	fmt.Fprintf(&b, "operations recorded         %d\n", ops)
	fmt.Fprintf(&b, "  acknowledged writes       %d\n", acked)
	fmt.Fprintf(&b, "  ambiguous writes          %d   (timed out or failed; may or may not have applied)\n", ambiguous)
	fmt.Fprintf(&b, "  successful reads          %d   (%d failed reads not recorded)\n", readsOK, readsFailed)
	fmt.Fprintf(&b, "faults injected             %d   (%s)\n", total, strings.Join(fs, ", "))
	fmt.Fprintf(&b, "setup                       3 nodes, %d clients, %d registers + %d counters, %s of faults per run, snapshot every %d entries\n",
		cfg.clients, cfg.registers, cfg.counters, cfg.duration, cfg.snapshotAt)
	fmt.Fprintf(&b, "seed                        %d\n", cfg.seed)
	fmt.Fprintf(&b, "host                        %s/%s, %d CPUs\n", runtime.GOOS, runtime.GOARCH, runtime.NumCPU())
	fmt.Fprintf(&b, "wall time                   %s\n", elapsed.Round(time.Second))
	if len(cfg.nodeFlags) > 0 {
		fmt.Fprintf(&b, "node flags                  %s\n", strings.Join(cfg.nodeFlags, " "))
	}
	ok := passed == len(results) && len(results) == cfg.runs
	return summary{
		text:   b.String(),
		passed: ok,
		json: map[string]any{
			"runs": len(results), "passed": passed, "linearizability_violations": illegal,
			"porcupine_timeouts": unknown, "not_converged": notConverged, "counter_failures": counterBad,
			"final_read_failures": finalBad, "setup_failures": setupFailed,
			"operations": ops, "acknowledged_writes": acked, "ambiguous_writes": ambiguous,
			"successful_reads": readsOK, "failed_reads": readsFailed, "faults": faults,
			"clients": cfg.clients, "fault_seconds_per_run": cfg.duration.Seconds(), "seed": cfg.seed,
			"node_flags": strings.Join(cfg.nodeFlags, " "), "wall_seconds": elapsed.Seconds(),
		},
	}
}
