package main

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/anishathalye/porcupine"
	"github.com/simar-s2/quorumdb/internal/chaos"
	"github.com/simar-s2/quorumdb/internal/resp"
)

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
	c    cluster
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
		addr := cl.c.redisAddress(cl.rng.IntN(nodeCount))
		conn, err := net.DialTimeout("tcp", addr, 300*time.Millisecond)
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
