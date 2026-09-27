package server

import (
	"fmt"
	"io"
	"log/slog"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/simar-s2/quorumdb/internal/raft"
	"github.com/simar-s2/quorumdb/internal/resp"
)

// testCluster runs three real nodes over TCP on localhost.
type testCluster struct {
	t       *testing.T
	ids     []string
	peers   map[string]string
	clients []string
	dirs    []string
	nodes   []*Server
}

func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().String()
}

func newTestCluster(t *testing.T) *testCluster {
	c := &testCluster{t: t, peers: map[string]string{}}
	base := t.TempDir()
	for i := 1; i <= 3; i++ {
		id := fmt.Sprintf("n%d", i)
		c.ids = append(c.ids, id)
		c.peers[id] = freeAddr(t)
		c.clients = append(c.clients, freeAddr(t))
		c.dirs = append(c.dirs, filepath.Join(base, id))
	}
	c.nodes = make([]*Server, 3)
	for i := range c.ids {
		c.start(i)
	}
	t.Cleanup(func() {
		for i := range c.nodes {
			c.stop(i)
		}
	})
	return c
}

func (c *testCluster) start(i int) {
	c.t.Helper()
	s, err := New(Config{
		ID:        c.ids[i],
		RedisAddr: c.clients[i],
		RaftAddr:  c.peers[c.ids[i]],
		Peers:     c.peers,
		DataDir:   c.dirs[i],
		Raft: raft.Config{
			HeartbeatInterval: 20 * time.Millisecond,
			ElectionTimeout:   150 * time.Millisecond,
			SnapshotThreshold: 50,
		},
		CommandTimeout: 3 * time.Second,
		Logger:         slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		c.t.Fatal(err)
	}
	c.nodes[i] = s
}

func (c *testCluster) stop(i int) {
	if c.nodes[i] != nil {
		c.nodes[i].Close()
		c.nodes[i] = nil
	}
}

func (c *testCluster) leader() int {
	c.t.Helper()
	for try := 0; try < 100; try++ {
		for i, s := range c.nodes {
			if s != nil {
				if _, ok := s.Raft().Leader(); ok {
					return i
				}
			}
		}
		time.Sleep(30 * time.Millisecond)
	}
	c.t.Fatal("no leader")
	return -1
}

type client struct {
	t    *testing.T
	conn net.Conn
	rd   *resp.Reader
}

func (c *testCluster) dial(i int) *client {
	c.t.Helper()
	conn, err := net.Dial("tcp", c.clients[i])
	if err != nil {
		c.t.Fatal(err)
	}
	c.t.Cleanup(func() { conn.Close() })
	return &client{t: c.t, conn: conn, rd: resp.NewReader(conn)}
}

func (cl *client) do(args ...string) resp.Value {
	cl.t.Helper()
	cl.conn.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := cl.conn.Write(resp.AppendCommand(nil, args...)); err != nil {
		cl.t.Fatal(err)
	}
	v, err := cl.rd.ReadReply()
	if err != nil {
		cl.t.Fatal(err)
	}
	return v
}

// pipeline sends all commands in one write and reads all replies.
func (cl *client) pipeline(cmds ...[]string) []resp.Value {
	cl.t.Helper()
	var b []byte
	for _, c := range cmds {
		b = resp.AppendCommand(b, c...)
	}
	cl.conn.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := cl.conn.Write(b); err != nil {
		cl.t.Fatal(err)
	}
	out := make([]resp.Value, len(cmds))
	for i := range out {
		v, err := cl.rd.ReadReply()
		if err != nil {
			cl.t.Fatal(err)
		}
		out[i] = v
	}
	return out
}

func expect(t *testing.T, got resp.Value, want string) {
	t.Helper()
	if s := got.String(); s != want {
		t.Fatalf("got %s, want %s", s, want)
	}
}

func TestCommandsThroughFollower(t *testing.T) {
	c := newTestCluster(t)
	l := c.leader()
	f := (l + 1) % 3
	cl := c.dial(f)

	expect(t, cl.do("PING"), `"PONG"`)
	expect(t, cl.do("SET", "k", "v"), `"OK"`)
	expect(t, cl.do("SET", "k", "other", "NX"), "(nil)")
	expect(t, cl.do("GET", "k"), `"v"`)
	expect(t, cl.do("INCR", "n"), "(integer) 1")
	expect(t, cl.do("DECR", "n"), "(integer) 0")
	expect(t, cl.do("INCR", "k"), "(error) ERR value is not an integer or out of range")
	expect(t, cl.do("MSET", "a", "1", "b", "2"), `"OK"`)
	expect(t, cl.do("MGET", "a", "b", "zz"), `["1" "2" (nil)]`)
	expect(t, cl.do("EXISTS", "a", "b", "zz"), "(integer) 2")
	expect(t, cl.do("DEL", "a", "zz"), "(integer) 1")
	expect(t, cl.do("EXPIRE", "b", "100"), "(integer) 1")
	expect(t, cl.do("TTL", "b"), "(integer) 100")
	expect(t, cl.do("TTL", "k"), "(integer) -1")
	expect(t, cl.do("TTL", "zz"), "(integer) -2")
	expect(t, cl.do("SET", "e", "v", "PX", "50"), `"OK"`)
	time.Sleep(80 * time.Millisecond)
	expect(t, cl.do("GET", "e"), "(nil)")
	expect(t, cl.do("SET", "e", "v", "EX", "0"), "(error) ERR invalid expire time in 'set' command")
	expect(t, cl.do("GET"), "(error) ERR wrong number of arguments for 'get' command")

	// The leader sees the forwarded writes.
	lc := c.dial(l)
	expect(t, lc.do("GET", "k"), `"v"`)
	if n := c.nodes[f].forwarded.Load(); n == 0 {
		t.Fatal("follower did not forward anything")
	}
}

// TestPipelineOrder checks that each read in a pipeline sees exactly the writes before it.
func TestPipelineOrder(t *testing.T) {
	c := newTestCluster(t)
	for _, node := range []int{c.leader(), (c.leader() + 1) % 3} {
		cl := c.dial(node)
		key := fmt.Sprintf("p%d", node)
		got := cl.pipeline(
			[]string{"SET", key, "1"},
			[]string{"GET", key},
			[]string{"INCR", key},
			[]string{"INCR", key},
			[]string{"GET", key},
			[]string{"PING"},
			[]string{"DEL", key},
			[]string{"GET", key},
		)
		want := []string{`"OK"`, `"1"`, "(integer) 2", "(integer) 3", `"3"`, `"PONG"`, "(integer) 1", "(nil)"}
		for i := range want {
			if got[i].String() != want[i] {
				t.Fatalf("node %d reply %d = %s, want %s", node, i, got[i], want[i])
			}
		}
	}
}

func TestFailoverKeepsData(t *testing.T) {
	c := newTestCluster(t)
	l := c.leader()
	cl := c.dial((l + 1) % 3)
	for i := 0; i < 120; i++ { // enough to trigger snapshots (threshold 50)
		expect(t, cl.do("SET", fmt.Sprintf("key%d", i), fmt.Sprint(i)), `"OK"`)
	}
	c.stop(l)
	nl := c.leader()
	if nl == l {
		t.Fatal("stopped node is still leader")
	}
	cl = c.dial(nl)
	expect(t, cl.do("GET", "key7"), `"7"`)
	expect(t, cl.do("SET", "after", "failover"), `"OK"`)

	// The old leader restarts from its WAL and snapshot and catches up.
	c.start(l)
	deadline := time.Now().Add(5 * time.Second)
	for {
		d1 := c.dial(l).do("DEBUG", "DIGEST").Str
		d2 := c.dial(nl).do("DEBUG", "DIGEST").Str
		if d1 == d2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("restarted node did not converge: %s vs %s", d1, d2)
		}
		time.Sleep(50 * time.Millisecond)
	}
	expect(t, c.dial(l).do("GET", "after"), `"failover"`)
	info := c.dial(l).do("INFO", "raft").Str
	if !strings.Contains(info, "snapshot_index:") || strings.Contains(info, "snapshot_index:0\r") {
		t.Fatalf("expected a snapshot after 120 writes, INFO:\n%s", info)
	}
}

func TestNoQuorumTimesOut(t *testing.T) {
	c := newTestCluster(t)
	l := c.leader()
	c.stop((l + 1) % 3)
	c.stop((l + 2) % 3)
	cl := c.dial(l)
	v := cl.do("SET", "k", "v")
	if !v.IsError() {
		t.Fatalf("write without a majority returned %s", v)
	}
	v = cl.do("GET", "k")
	if !v.IsError() {
		t.Fatalf("read without a majority returned %s", v)
	}
}
