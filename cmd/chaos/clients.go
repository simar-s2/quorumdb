package main

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"net"
	"sort"
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

	// writeOnceAcked maps every acknowledged write-once key to its value; all must survive.
	writeOnceAcked     map[string]string
	writeOnceAmbiguous int
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

func (r *recorder) recordWriteOnce(key, value string, acknowledged bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if acknowledged {
		r.writeOnceAcked[key] = value
	} else {
		r.writeOnceAmbiguous++
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

// writeOnce sets a key that nothing else ever writes, such as once/3-17 = 3-17. Every acknowledged
// one must still be there at the end of the run, so a missing one is a lost write, counted directly.
func (cl *client) writeOnce() bool {
	cl.seq++
	key := fmt.Sprintf("once/%d-%d", cl.id, cl.seq)
	value := fmt.Sprintf("%d-%d", cl.id, cl.seq)
	v, err := cl.send(3*time.Second, "SET", key, value)
	if errors.Is(err, errNotSent) {
		return false
	}
	if err == nil && !v.IsError() && v.Str == "OK" {
		cl.rec.recordWriteOnce(key, value, true)
		return true
	}
	// No reply, or an error such as TIMEOUT: the write may or may not have happened, so it is not checked.
	if err == nil {
		cl.close() // try another node next time
	}
	cl.rec.recordWriteOnce(key, value, false)
	return false
}

// readBackWriteOnceKeys reads every acknowledged write-once key, 100 per MGET, and returns how many
// are missing or hold a different value: the number of acknowledged writes that were lost.
func (cl *client) readBackWriteOnceKeys(acked map[string]string) (lost int, err error) {
	keys := make([]string, 0, len(acked))
	for key := range acked {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for start := 0; start < len(keys); start += 100 {
		batch := keys[start:min(start+100, len(keys))]
		values, err := cl.mgetWithRetry(batch)
		if err != nil {
			return lost, err
		}
		for i, key := range batch {
			if values[i].Null || values[i].Str != acked[key] {
				lost++
			}
		}
	}
	return lost, nil
}

// mgetWithRetry keeps trying an MGET, on other nodes if needed, for up to 15 s. Reads are safe to retry.
func (cl *client) mgetWithRetry(keys []string) ([]resp.Value, error) {
	deadline := time.Now().Add(15 * time.Second)
	for {
		v, err := cl.send(3*time.Second, append([]string{"MGET"}, keys...)...)
		if err == nil && !v.IsError() && len(v.Array) == len(keys) {
			return v.Array, nil
		}
		if err == nil {
			cl.close()
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("could not read back the write-once keys: %v %s", err, v.Str)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (cl *client) run(cfg config, stop <-chan struct{}) {
	defer cl.close()
	for {
		select {
		case <-stop:
			return
		default:
		}
		reg := fmt.Sprintf("r%d", cl.rng.IntN(cfg.registers))
		ctr := fmt.Sprintf("c%d", cl.rng.IntN(cfg.counters))
		var ok bool
		switch p := cl.rng.IntN(100); {
		case p < 30:
			cl.seq++
			ok = cl.do(chaos.Input{Op: chaos.OpSet, Key: reg, Value: fmt.Sprintf("%d-%d", cl.id, cl.seq)})
		case p < 40:
			ok = cl.writeOnce()
		case p < 72:
			ok = cl.do(chaos.Input{Op: chaos.OpGet, Key: reg})
		case p < 78:
			ok = cl.do(chaos.Input{Op: chaos.OpDel, Key: reg})
		case p < 92:
			ok = cl.do(chaos.Input{Op: chaos.OpIncr, Key: ctr})
		default:
			ok = cl.do(chaos.Input{Op: chaos.OpGet, Key: ctr})
		}
		if !ok {
			time.Sleep(time.Duration(5+cl.rng.IntN(20)) * time.Millisecond)
		}
	}
}
