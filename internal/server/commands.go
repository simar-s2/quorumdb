package server

import (
	"bytes"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/simar-s2/quorumdb/internal/kv"
	"github.com/simar-s2/quorumdb/internal/resp"
)

// class says where a command runs.
type class uint8

const (
	// classLocal commands are answered by the node the client is connected
	// to, without touching the replicated state (PING, INFO, ...).
	classLocal class = iota
	// classRead commands read the state machine on the leader after a
	// ReadIndex round, which makes them linearizable.
	classRead
	// classWrite commands become Raft log entries and are acknowledged once
	// a majority has committed them.
	classWrite
)

type command struct {
	name  string
	arity int // Redis convention: >0 exact count, <0 minimum count (both include the name)
	class class
	local func(s *Server, b []byte, args [][]byte) []byte
	read  func(s *Server, b []byte, args [][]byte, now int64) []byte
	// write validates arguments and builds the state machine command. It
	// returns an error reply instead for invalid input, which is then never
	// proposed.
	write func(args [][]byte, now int64) (*kv.Command, []byte)
}

var commands = map[string]*command{}

func register(c *command) { commands[c.name] = c }

func init() {
	register(&command{name: "ping", arity: -1, class: classLocal, local: cmdPing})
	register(&command{name: "echo", arity: 2, class: classLocal, local: cmdEcho})
	register(&command{name: "quit", arity: -1, class: classLocal, local: func(_ *Server, b []byte, _ [][]byte) []byte { return resp.AppendSimple(b, "OK") }})
	register(&command{name: "select", arity: 2, class: classLocal, local: cmdSelect})
	register(&command{name: "info", arity: -1, class: classLocal, local: cmdInfo})
	register(&command{name: "command", arity: -1, class: classLocal, local: func(_ *Server, b []byte, _ [][]byte) []byte { return resp.AppendArray(b, 0) }})
	register(&command{name: "config", arity: -2, class: classLocal, local: cmdConfig})
	register(&command{name: "debug", arity: -2, class: classLocal, local: cmdDebug})

	register(&command{name: "get", arity: 2, class: classRead, read: cmdGet})
	register(&command{name: "mget", arity: -2, class: classRead, read: cmdMGet})
	register(&command{name: "exists", arity: -2, class: classRead, read: cmdExists})
	register(&command{name: "ttl", arity: 2, class: classRead, read: cmdTTL(false)})
	register(&command{name: "pttl", arity: 2, class: classRead, read: cmdTTL(true)})
	register(&command{name: "dbsize", arity: 1, class: classRead, read: func(s *Server, b []byte, _ [][]byte, _ int64) []byte {
		return resp.AppendInt(b, int64(s.store.Len()))
	}})

	register(&command{name: "set", arity: -3, class: classWrite, write: cmdSet})
	register(&command{name: "mset", arity: -3, class: classWrite, write: cmdMSet})
	register(&command{name: "del", arity: -2, class: classWrite, write: func(args [][]byte, now int64) (*kv.Command, []byte) {
		return &kv.Command{Op: kv.OpDel, Now: now, Args: args[1:]}, nil
	}})
	register(&command{name: "incr", arity: 2, class: classWrite, write: incrBy(1, false)})
	register(&command{name: "decr", arity: 2, class: classWrite, write: incrBy(-1, false)})
	register(&command{name: "incrby", arity: 3, class: classWrite, write: incrBy(1, true)})
	register(&command{name: "decrby", arity: 3, class: classWrite, write: incrBy(-1, true)})
	register(&command{name: "expire", arity: -3, class: classWrite, write: cmdExpire(time.Second)})
	register(&command{name: "pexpire", arity: -3, class: classWrite, write: cmdExpire(time.Millisecond)})
}

// lookup finds the command for args and checks its arity. On failure it
// returns the error reply to send.
func lookup(args [][]byte) (*command, []byte) {
	name := strings.ToLower(string(args[0]))
	c, ok := commands[name]
	if !ok {
		var sb strings.Builder
		for _, a := range args[1:] {
			if sb.Len() > 128 {
				break
			}
			fmt.Fprintf(&sb, "'%s' ", a)
		}
		return nil, resp.AppendError(nil, fmt.Sprintf("ERR unknown command '%s', with args beginning with: %s", args[0], sb.String()))
	}
	if c.arity > 0 && len(args) != c.arity || c.arity < 0 && len(args) < -c.arity {
		return nil, wrongArgs(c.name)
	}
	return c, nil
}

func wrongArgs(name string) []byte {
	return resp.AppendError(nil, fmt.Sprintf("ERR wrong number of arguments for '%s' command", name))
}

var (
	errSyntax    = resp.AppendError(nil, "ERR syntax error")
	errNotInt    = resp.AppendError(nil, "ERR value is not an integer or out of range")
	errOverflow  = resp.AppendError(nil, "ERR increment or decrement would overflow")
	errBadExpire = func(cmd string) []byte {
		return resp.AppendError(nil, fmt.Sprintf("ERR invalid expire time in '%s' command", cmd))
	}
)

func eq(arg []byte, s string) bool { return bytes.EqualFold(arg, []byte(s)) }

// Local commands.

func cmdPing(_ *Server, b []byte, args [][]byte) []byte {
	switch len(args) {
	case 1:
		return resp.AppendSimple(b, "PONG")
	case 2:
		return resp.AppendBulk(b, args[1])
	}
	return append(b, wrongArgs("ping")...)
}

func cmdEcho(_ *Server, b []byte, args [][]byte) []byte { return resp.AppendBulk(b, args[1]) }

func cmdSelect(_ *Server, b []byte, args [][]byte) []byte {
	if n, ok := kv.ParseInt(args[1]); !ok {
		return append(b, errNotInt...)
	} else if n != 0 {
		return resp.AppendError(b, "ERR DB index is out of range")
	}
	return resp.AppendSimple(b, "OK")
}

// cmdConfig answers CONFIG GET for the parameters redis-benchmark asks about
// at startup, so it runs without warnings.
func cmdConfig(_ *Server, b []byte, args [][]byte) []byte {
	if !eq(args[1], "get") || len(args) != 3 {
		return resp.AppendError(b, "ERR CONFIG subcommand not supported (only CONFIG GET)")
	}
	known := map[string]string{"save": "", "appendonly": "no"}
	v, ok := known[strings.ToLower(string(args[2]))]
	if !ok {
		return resp.AppendArray(b, 0)
	}
	b = resp.AppendArray(b, 2)
	b = resp.AppendBulk(b, bytes.ToLower(args[2]))
	return resp.AppendBulkString(b, v)
}

func cmdDebug(s *Server, b []byte, args [][]byte) []byte {
	if eq(args[1], "digest") {
		return resp.AppendBulkString(b, s.store.Digest())
	}
	return resp.AppendError(b, "ERR DEBUG subcommand not supported (only DEBUG DIGEST)")
}

// Reads.

func cmdGet(s *Server, b []byte, args [][]byte, now int64) []byte {
	if v, ok := s.store.Get(args[1], now); ok {
		return resp.AppendBulk(b, v)
	}
	return resp.AppendNull(b)
}

func cmdMGet(s *Server, b []byte, args [][]byte, now int64) []byte {
	vals := s.store.MGet(args[1:], now)
	b = resp.AppendArray(b, len(vals))
	for _, v := range vals {
		if v == nil {
			b = resp.AppendNull(b)
		} else {
			b = resp.AppendBulk(b, v)
		}
	}
	return b
}

func cmdExists(s *Server, b []byte, args [][]byte, now int64) []byte {
	return resp.AppendInt(b, s.store.Exists(args[1:], now))
}

func cmdTTL(millis bool) func(*Server, []byte, [][]byte, int64) []byte {
	return func(s *Server, b []byte, args [][]byte, now int64) []byte {
		ms := s.store.PTTL(args[1], now)
		if ms < 0 || millis {
			return resp.AppendInt(b, ms)
		}
		return resp.AppendInt(b, (ms+500)/1000)
	}
}

// Writes.

// cmdSet parses SET key value [NX|XX] [EX s|PX ms|EXAT ts|PXAT ms-ts|KEEPTTL].
// Relative expiries are turned into absolute times here, on the leader, so
// every replica applies the same deadline.
func cmdSet(args [][]byte, now int64) (*kv.Command, []byte) {
	c := &kv.Command{Op: kv.OpSet, Now: now, Args: args[1:3]}
	haveExpiry := false
	for i := 3; i < len(args); i++ {
		opt := args[i]
		switch {
		case eq(opt, "nx") && c.Flags&kv.FlagXX == 0:
			c.Flags |= kv.FlagNX
		case eq(opt, "xx") && c.Flags&kv.FlagNX == 0:
			c.Flags |= kv.FlagXX
		case eq(opt, "keepttl") && !haveExpiry:
			c.Flags |= kv.FlagKeepTTL
			haveExpiry = true
		case (eq(opt, "ex") || eq(opt, "px") || eq(opt, "exat") || eq(opt, "pxat")) && !haveExpiry && i+1 < len(args):
			i++
			n, ok := kv.ParseInt(args[i])
			if !ok {
				return nil, errNotInt
			}
			if n <= 0 {
				return nil, errBadExpire("set")
			}
			at, ok := absoluteExpiry(strings.ToLower(string(opt)), n, now)
			if !ok {
				return nil, errBadExpire("set")
			}
			c.ExpireAt = at
			haveExpiry = true
		default:
			return nil, errSyntax
		}
	}
	return c, nil
}

func absoluteExpiry(opt string, n, now int64) (int64, bool) {
	switch opt {
	case "ex":
		if n > (math.MaxInt64-now)/1000 {
			return 0, false
		}
		return now + n*1000, true
	case "px":
		if n > math.MaxInt64-now {
			return 0, false
		}
		return now + n, true
	case "exat":
		if n > math.MaxInt64/1000 {
			return 0, false
		}
		return n * 1000, true
	default: // pxat
		return n, true
	}
}

func cmdMSet(args [][]byte, now int64) (*kv.Command, []byte) {
	if len(args)%2 != 1 {
		return nil, wrongArgs("mset")
	}
	return &kv.Command{Op: kv.OpMSet, Now: now, Args: args[1:]}, nil
}

func incrBy(sign int64, explicit bool) func([][]byte, int64) (*kv.Command, []byte) {
	return func(args [][]byte, now int64) (*kv.Command, []byte) {
		delta := int64(1)
		if explicit {
			n, ok := kv.ParseInt(args[2])
			if !ok {
				return nil, errNotInt
			}
			if sign < 0 && n == math.MinInt64 {
				return nil, errOverflow
			}
			delta = n
		}
		return &kv.Command{Op: kv.OpIncrBy, Now: now, Delta: sign * delta, Args: args[1:2]}, nil
	}
}

// cmdExpire parses EXPIRE/PEXPIRE key amount [NX|XX|GT|LT].
func cmdExpire(unit time.Duration) func([][]byte, int64) (*kv.Command, []byte) {
	name := "expire"
	if unit == time.Millisecond {
		name = "pexpire"
	}
	return func(args [][]byte, now int64) (*kv.Command, []byte) {
		n, ok := kv.ParseInt(args[2])
		if !ok {
			return nil, errNotInt
		}
		mult := int64(unit / time.Millisecond)
		if n > (math.MaxInt64-now)/mult {
			return nil, errBadExpire(name)
		}
		if n < -1<<40 {
			n = -1 << 40 // any deadline this far in the past just deletes the key
		}
		c := &kv.Command{Op: kv.OpExpire, Now: now, ExpireAt: now + n*mult, Args: args[1:2]}
		for _, opt := range args[3:] {
			switch {
			case eq(opt, "nx"):
				c.Flags |= kv.FlagNX
			case eq(opt, "xx"):
				c.Flags |= kv.FlagXX
			case eq(opt, "gt"):
				c.Flags |= kv.FlagGT
			case eq(opt, "lt"):
				c.Flags |= kv.FlagLT
			default:
				return nil, resp.AppendError(nil, "ERR Unsupported option "+string(opt))
			}
		}
		if c.Flags&kv.FlagNX != 0 && c.Flags&(kv.FlagXX|kv.FlagGT|kv.FlagLT) != 0 ||
			c.Flags&kv.FlagGT != 0 && c.Flags&kv.FlagLT != 0 {
			return nil, resp.AppendError(nil, "ERR NX and XX, GT or LT options at the same time are not compatible")
		}
		if c.ExpireAt <= 0 {
			c.ExpireAt = 1 // deadline in the past: delete the key
		}
		return c, nil
	}
}

// encodeResult turns a state machine result into a RESP reply.
func encodeResult(b []byte, res any) []byte {
	switch v := res.(type) {
	case kv.Status:
		return resp.AppendSimple(b, string(v))
	case nil:
		return resp.AppendNull(b)
	case int64:
		return resp.AppendInt(b, v)
	case []byte:
		return resp.AppendBulk(b, v)
	case error:
		return resp.AppendError(b, v.Error())
	}
	return resp.AppendError(b, fmt.Sprintf("ERR unexpected result type %T", res))
}
