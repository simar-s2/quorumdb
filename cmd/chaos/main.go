// Command chaos runs the fault-injection test: kills, pauses and partitions, checked with porcupine.
package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math/rand/v2"
	"net"
	"os"
	"os/exec"
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
	"github.com/simar-s2/quorumdb/internal/netproxy"
	"github.com/simar-s2/quorumdb/internal/resp"
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

// --- Cluster processes ---

type node struct {
	id        string
	redisAddr string
	args      []string
	logPath   string

	mu     sync.Mutex
	cmd    *exec.Cmd
	exited chan struct{}
	paused bool
}

func (n *node) start() error {
	n.mu.Lock()
	defer n.mu.Unlock()
	logf, err := os.OpenFile(n.logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	cmd := exec.Command(n.args[0], n.args[1:]...)
	cmd.Stdout, cmd.Stderr = logf, logf
	if err := cmd.Start(); err != nil {
		logf.Close()
		return err
	}
	exited := make(chan struct{})
	go func() { cmd.Wait(); logf.Close(); close(exited) }()
	n.cmd, n.exited, n.paused = cmd, exited, false
	return nil
}

func (n *node) alive() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.cmd == nil {
		return false
	}
	select {
	case <-n.exited:
		return false
	default:
		return true
	}
}

// kill sends SIGKILL, so nothing gets flushed or shut down cleanly.
func (n *node) kill() {
	n.mu.Lock()
	cmd, exited := n.cmd, n.exited
	n.cmd = nil
	n.mu.Unlock()
	if cmd == nil {
		return
	}
	cmd.Process.Signal(syscall.SIGCONT)
	cmd.Process.Kill()
	<-exited
}

func (n *node) setPaused(p bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.cmd == nil || n.paused == p {
		return
	}
	sig := syscall.SIGCONT
	if p {
		sig = syscall.SIGSTOP
	}
	n.cmd.Process.Signal(sig)
	n.paused = p
}

type cluster struct {
	nodes   []*node
	proxies [3][3]*netproxy.Proxy // proxies[a][b] carries a's traffic to b
}

func freePort() (string, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", err
	}
	defer ln.Close()
	return ln.Addr().String(), nil
}

func startCluster(cfg config, dir string) (*cluster, error) {
	c := &cluster{}
	var redis, raft [3]string
	for i := 0; i < 3; i++ {
		var err error
		if redis[i], err = freePort(); err != nil {
			return nil, err
		}
		if raft[i], err = freePort(); err != nil {
			return nil, err
		}
	}
	for a := 0; a < 3; a++ {
		for b := 0; b < 3; b++ {
			if a == b {
				continue
			}
			p, err := netproxy.Listen("127.0.0.1:0", raft[b])
			if err != nil {
				c.stop()
				return nil, err
			}
			c.proxies[a][b] = p
		}
	}
	for i := 0; i < 3; i++ {
		var peers []string
		for j := 0; j < 3; j++ {
			addr := raft[j]
			if j != i {
				addr = c.proxies[i][j].Addr()
			}
			peers = append(peers, fmt.Sprintf("n%d=%s", j+1, addr))
		}
		args := []string{cfg.bin,
			"-id", fmt.Sprintf("n%d", i+1),
			"-redis-addr", redis[i],
			"-raft-addr", raft[i],
			"-peers", strings.Join(peers, ","),
			"-data", filepath.Join(dir, fmt.Sprintf("n%d", i+1)),
			"-heartbeat", "50ms",
			"-election-timeout", "300ms",
			"-snapshot-threshold", strconv.Itoa(cfg.snapshotAt),
			"-command-timeout", "2s",
		}
		args = append(args, cfg.nodeFlags...)
		n := &node{id: fmt.Sprintf("n%d", i+1), redisAddr: redis[i], args: args, logPath: filepath.Join(dir, fmt.Sprintf("n%d.log", i+1))}
		c.nodes = append(c.nodes, n)
		if err := n.start(); err != nil {
			c.stop()
			return nil, err
		}
	}
	return c, nil
}

func (c *cluster) stop() {
	for _, n := range c.nodes {
		n.kill()
	}
	for a := range c.proxies {
		for b := range c.proxies[a] {
			if p := c.proxies[a][b]; p != nil {
				p.Close()
			}
		}
	}
}

// healAll heals every link, resumes paused nodes and restarts dead ones.
func (c *cluster) healAll() {
	for a := range c.proxies {
		for b := range c.proxies[a] {
			if p := c.proxies[a][b]; p != nil {
				p.SetMode(netproxy.Pass)
			}
		}
	}
	for _, n := range c.nodes {
		if n.alive() {
			n.setPaused(false)
		} else {
			n.start()
		}
	}
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
func (c *cluster) leader() int {
	best, bestTerm := -1, -1
	for i, n := range c.nodes {
		if !n.alive() {
			continue
		}
		v, err := query(n.redisAddr, 200*time.Millisecond, "INFO", "raft")
		if err != nil {
			continue
		}
		term, _ := strconv.Atoi(infoField(v.Str, "term"))
		if infoField(v.Str, "role") == "leader" && term > bestTerm {
			best, bestTerm = i, term
		}
	}
	return best
}

func (c *cluster) waitLeader(timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if c.leader() >= 0 {
			return true
		}
		time.Sleep(50 * time.Millisecond)
	}
	return false
}

// waitConverged waits until all nodes report the same applied index and state digest.
func (c *cluster) waitConverged(timeout time.Duration) (bool, string) {
	deadline := time.Now().Add(timeout)
	last := ""
	for time.Now().Before(deadline) {
		var applied, digests []string
		for _, n := range c.nodes {
			info, err1 := query(n.redisAddr, 500*time.Millisecond, "INFO", "raft")
			dig, err2 := query(n.redisAddr, 500*time.Millisecond, "DEBUG", "DIGEST")
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

// --- Clients ---

type recorder struct {
	start time.Time
	mu    sync.Mutex
	ops   []porcupine.Operation

	acked, ambiguous, readsOK, readsFailed int
	ackedIncr, ambiguousIncr               map[string]int
}

func (r *recorder) now() int64 { return time.Since(r.start).Nanoseconds() }

// pending marks an operation with an unknown outcome; it gets a return time after the history ends.
const pending = int64(-1)

func (r *recorder) record(client int, in chaos.Input, call, ret int64, out chaos.Output) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ops = append(r.ops, porcupine.Operation{ClientId: client, Input: in, Call: call, Output: out, Return: ret})
	switch {
	case !in.Op.IsWrite():
		r.readsOK++
	case out.Unknown:
		r.ambiguous++
		if in.Op == chaos.OpIncr {
			r.ambiguousIncr[in.Key]++
		}
	default:
		r.acked++
		if in.Op == chaos.OpIncr && out.Err == "" {
			r.ackedIncr[in.Key]++
		}
	}
}

func (r *recorder) failedRead() {
	r.mu.Lock()
	r.readsFailed++
	r.mu.Unlock()
}

type client struct {
	id   int
	c    *cluster
	rng  *rand.Rand
	rec  *recorder
	conn net.Conn
	rd   *resp.Reader
	seq  int
}

func (cl *client) close() {
	if cl.conn != nil {
		cl.conn.Close()
		cl.conn = nil
	}
}

func (cl *client) send(timeout time.Duration, args ...string) (resp.Value, error) {
	if cl.conn == nil {
		n := cl.c.nodes[cl.rng.IntN(len(cl.c.nodes))]
		conn, err := net.DialTimeout("tcp", n.redisAddr, 300*time.Millisecond)
		if err != nil {
			return resp.Value{}, errNotSent
		}
		cl.conn, cl.rd = conn, resp.NewReader(conn)
	}
	cl.conn.SetDeadline(time.Now().Add(timeout))
	if _, err := cl.conn.Write(resp.AppendCommand(nil, args...)); err != nil {
		cl.close()
		return resp.Value{}, err
	}
	v, err := cl.rd.ReadReply()
	if err != nil {
		cl.close()
	}
	return v, err
}

// errNotSent means the client could not connect, so the operation never happened.
var errNotSent = errors.New("not sent")

// do runs one operation and records it in the history.
func (cl *client) do(in chaos.Input) bool {
	args := []string{strings.ToUpper(in.Op.String()), in.Key}
	if in.Op == chaos.OpSet {
		args = append(args, in.Value)
	}
	call := cl.rec.now()
	v, err := cl.send(3*time.Second, args...)
	ret := cl.rec.now()
	if errors.Is(err, errNotSent) {
		return false
	}

	if !in.Op.IsWrite() {
		if err != nil || v.IsError() {
			cl.rec.failedRead()
			if err == nil {
				cl.close() // try another node next time
			}
			return false
		}
		cl.rec.record(cl.id, in, call, ret, chaos.Output{Exists: !v.Null, Value: v.Str})
		return true
	}

	var out chaos.Output
	switch {
	case err != nil:
		out.Unknown = true
	case v.IsError() && in.Op == chaos.OpIncr && strings.Contains(v.Str, "not an integer"):
		out.Err = v.Str
	case v.IsError():
		// TIMEOUT / TRYAGAIN / CLUSTERDOWN: may or may not have executed.
		out.Unknown = true
		cl.close()
	case in.Op == chaos.OpSet:
		if v.Str != "OK" {
			out.Unknown = true
		}
	default:
		out.Int = v.Int
	}
	if out.Unknown {
		ret = pending
	}
	cl.rec.record(cl.id, in, call, ret, out)
	return !out.Unknown
}

func (cl *client) run(cfg config, stop <-chan struct{}) {
	defer cl.close()
	for {
		select {
		case <-stop:
			return
		default:
		}
		var in chaos.Input
		reg := fmt.Sprintf("r%d", cl.rng.IntN(cfg.registers))
		ctr := fmt.Sprintf("c%d", cl.rng.IntN(cfg.counters))
		switch p := cl.rng.IntN(100); {
		case p < 40:
			cl.seq++
			in = chaos.Input{Op: chaos.OpSet, Key: reg, Value: fmt.Sprintf("%d-%d", cl.id, cl.seq)}
		case p < 72:
			in = chaos.Input{Op: chaos.OpGet, Key: reg}
		case p < 78:
			in = chaos.Input{Op: chaos.OpDel, Key: reg}
		case p < 92:
			in = chaos.Input{Op: chaos.OpIncr, Key: ctr}
		default:
			in = chaos.Input{Op: chaos.OpGet, Key: ctr}
		}
		if !cl.do(in) {
			time.Sleep(time.Duration(5+cl.rng.IntN(20)) * time.Millisecond)
		}
	}
}

// --- Nemesis ---

type nemesis struct {
	c      *cluster
	rng    *rand.Rand
	counts map[string]int
	events []string
	start  time.Time
}

func (nm *nemesis) logf(format string, args ...any) {
	nm.events = append(nm.events, fmt.Sprintf("%6.2fs  %s", time.Since(nm.start).Seconds(), fmt.Sprintf(format, args...)))
}

// target picks the leader half the time and a random node otherwise.
func (nm *nemesis) target() int {
	if nm.rng.IntN(2) == 0 {
		if l := nm.c.leader(); l >= 0 {
			return l
		}
	}
	return nm.rng.IntN(3)
}

func (nm *nemesis) linkMode() netproxy.Mode {
	if nm.rng.IntN(2) == 0 {
		return netproxy.Reset
	}
	return netproxy.Drop
}

func (nm *nemesis) cut(a, b int, m netproxy.Mode) { nm.c.proxies[a][b].SetMode(m) }

// inject applies one random fault and returns the function that heals it.
func (nm *nemesis) inject() func() {
	n := nm.target()
	node := nm.c.nodes[n]
	switch kind := nm.rng.IntN(6); kind {
	case 0:
		if !node.alive() {
			return func() {}
		}
		nm.counts["kill"]++
		nm.logf("kill -9 %s", node.id)
		node.kill()
		return func() { nm.logf("restart %s", node.id); node.start() }
	case 1:
		if !node.alive() {
			return func() {}
		}
		nm.counts["pause"]++
		nm.logf("pause (SIGSTOP) %s", node.id)
		node.setPaused(true)
		return func() { nm.logf("resume %s", node.id); node.setPaused(false) }
	case 2:
		m := nm.linkMode()
		nm.counts["isolate"]++
		nm.logf("isolate %s (%s)", node.id, m)
		for o := 0; o < 3; o++ {
			if o != n {
				nm.cut(n, o, m)
				nm.cut(o, n, m)
			}
		}
		return func() {
			nm.logf("heal %s", node.id)
			for o := 0; o < 3; o++ {
				if o != n {
					nm.cut(n, o, netproxy.Pass)
					nm.cut(o, n, netproxy.Pass)
				}
			}
		}
	case 3:
		// Bridge: two nodes lose each other but both still reach the third.
		o := (n + 1 + nm.rng.IntN(2)) % 3
		m := nm.linkMode()
		nm.counts["bridge"]++
		nm.logf("cut n%d <-> n%d (%s)", n+1, o+1, m)
		nm.cut(n, o, m)
		nm.cut(o, n, m)
		return func() {
			nm.logf("heal n%d <-> n%d", n+1, o+1)
			nm.cut(n, o, netproxy.Pass)
			nm.cut(o, n, netproxy.Pass)
		}
	default:
		// One-way link failure, in either direction relative to the target.
		o := (n + 1 + nm.rng.IntN(2)) % 3
		from, to := n, o
		if kind == 5 {
			from, to = o, n
		}
		m := nm.linkMode()
		nm.counts["one-way"]++
		nm.logf("cut n%d -> n%d (%s)", from+1, to+1, m)
		nm.cut(from, to, m)
		return func() { nm.logf("heal n%d -> n%d", from+1, to+1); nm.cut(from, to, netproxy.Pass) }
	}
}

func (nm *nemesis) run(d time.Duration) {
	end := time.Now().Add(d)
	for time.Now().Before(end) {
		time.Sleep(time.Duration(200+nm.rng.IntN(600)) * time.Millisecond)
		heals := []func(){nm.inject()}
		if nm.rng.IntN(4) == 0 {
			// Sometimes overlap a second fault, which can leave no majority.
			heals = append(heals, nm.inject())
		}
		time.Sleep(time.Duration(500+nm.rng.IntN(2000)) * time.Millisecond)
		for i := len(heals) - 1; i >= 0; i-- {
			heals[i]()
		}
	}
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
	s := fmt.Sprintf("run %3d/%d  ops %5d  acked writes %4d  ambiguous %3d  faults [%s]  linearizable %s  converged %s  counters %s  %4.1fs",
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

	c, err := startCluster(cfg, res.dir)
	if err != nil {
		res.setupErr = err.Error()
		return res
	}
	defer c.stop()
	if !c.waitLeader(10 * time.Second) {
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
	c.waitLeader(10 * time.Second)
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
	res.converged, res.convergeInfo = c.waitConverged(20 * time.Second)

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
