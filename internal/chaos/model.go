// Package chaos holds the porcupine model used by the fault-injection test.
package chaos

import (
	"fmt"
	"strconv"

	"github.com/anishathalye/porcupine"
)

type OpKind uint8

const (
	OpGet OpKind = iota
	OpSet
	OpDel
	OpIncr
)

func (k OpKind) String() string {
	return [...]string{"get", "set", "del", "incr"}[k]
}

func (k OpKind) IsWrite() bool { return k != OpGet }

// Input is what a client asked for.
type Input struct {
	Op    OpKind
	Key   string
	Value string // for set
}

// Output is what the client observed; Unknown means the outcome was never learned.
type Output struct {
	Unknown bool
	Exists  bool   // get: key present
	Value   string // get: value
	Int     int64  // del: keys removed; incr: new value
	Err     string // incr: error reply (non-integer value)
}

// state is the model's view of one key.
type state struct {
	exists bool
	value  string
}

// Model is a per-key register supporting GET, SET, DEL and INCR, partitioned by key.
var Model = porcupine.Model{
	Partition: func(history []porcupine.Operation) [][]porcupine.Operation {
		byKey := map[string][]porcupine.Operation{}
		var keys []string
		for _, op := range history {
			k := op.Input.(Input).Key
			if _, ok := byKey[k]; !ok {
				keys = append(keys, k)
			}
			byKey[k] = append(byKey[k], op)
		}
		out := make([][]porcupine.Operation, 0, len(keys))
		for _, k := range keys {
			out = append(out, byKey[k])
		}
		return out
	},
	Init: func() interface{} { return state{} },
	Step: func(st, in, out interface{}) (bool, interface{}) {
		s, i, o := st.(state), in.(Input), out.(Output)
		switch i.Op {
		case OpGet:
			if o.Unknown {
				return true, s
			}
			return o.Exists == s.exists && (!s.exists || o.Value == s.value), s
		case OpSet:
			return true, state{exists: true, value: i.Value}
		case OpDel:
			removed := int64(0)
			if s.exists {
				removed = 1
			}
			return o.Unknown || o.Int == removed, state{}
		case OpIncr:
			var n int64
			if s.exists {
				v, err := strconv.ParseInt(s.value, 10, 64)
				if err != nil {
					// Server rejects INCR on a non-integer and changes nothing.
					return o.Unknown || o.Err != "", s
				}
				n = v
			}
			next := state{exists: true, value: strconv.FormatInt(n+1, 10)}
			return o.Unknown || o.Err == "" && o.Int == n+1, next
		}
		return false, s
	},
	DescribeOperation: func(in, out interface{}) string {
		i, o := in.(Input), out.(Output)
		var res string
		switch {
		case o.Unknown:
			res = "?"
		case o.Err != "":
			res = "error: " + o.Err
		case i.Op == OpGet && !o.Exists:
			res = "nil"
		case i.Op == OpGet:
			res = strconv.Quote(o.Value)
		case i.Op == OpSet:
			res = "OK"
		default:
			res = strconv.FormatInt(o.Int, 10)
		}
		if i.Op == OpSet {
			return fmt.Sprintf("set(%s, %q) -> %s", i.Key, i.Value, res)
		}
		return fmt.Sprintf("%s(%s) -> %s", i.Op, i.Key, res)
	},
	DescribeState: func(st interface{}) string {
		s := st.(state)
		if !s.exists {
			return "nil"
		}
		return strconv.Quote(s.value)
	},
}
