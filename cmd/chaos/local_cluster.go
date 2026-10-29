package main

import (
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/simar-s2/quorumdb/internal/netproxy"
)

// node is one QuorumDB process started by the local cluster.
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

// localCluster runs three QuorumDB processes on this machine, with a TCP proxy on each of the six
// directed links between them, so the nemesis can cut any link in either direction.
type localCluster struct {
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

func startLocalCluster(cfg config, dir string) (*localCluster, error) {
	c := &localCluster{}
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

func (c *localCluster) nodeName(i int) string     { return c.nodes[i].id }
func (c *localCluster) redisAddress(i int) string { return c.nodes[i].redisAddr }
func (c *localCluster) zone(i int) string         { return c.nodes[i].id }

func (c *localCluster) kill(i int) (time.Time, bool) {
	if !c.nodes[i].alive() {
		return time.Time{}, false
	}
	killedAt := time.Now()
	c.nodes[i].kill()
	return killedAt, true
}

func (c *localCluster) restart(i int) { c.nodes[i].start() }

func (c *localCluster) setFrozen(i int, frozen bool) bool {
	if !c.nodes[i].alive() {
		return false
	}
	c.nodes[i].setPaused(frozen)
	return true
}

// linkModes: the proxies can refuse connections (like a TCP reset) or silently drop traffic.
func (c *localCluster) linkModes() []netproxy.Mode {
	return []netproxy.Mode{netproxy.Reset, netproxy.Drop}
}

func (c *localCluster) setLinks(links []link, mode netproxy.Mode) {
	for _, l := range links {
		c.proxies[l.from][l.to].SetMode(mode)
	}
}

func (c *localCluster) healAll() {
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

// saveLogs does nothing: the nodes already write their logs into the run directory.
func (c *localCluster) saveLogs(dir string, since time.Time) {}

func (c *localCluster) stop() {
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
