// Package kv is the replicated state machine: an in-memory string key-value
// store whose mutations arrive as Raft log entries.
//
// Every mutation is encoded as a Command by the leader and applied in log
// order on every node. Commands carry the leader's clock reading (Now) and
// absolute expiry times, so applying the same log produces the same state on
// every replica regardless of local clocks.
package kv

import (
	"encoding/binary"
	"errors"
)

type Op uint8

const (
	OpSet Op = iota + 1
	OpDel
	OpIncrBy
	OpExpire
	OpMSet
	OpSweep // delete keys whose expiry is <= Now
)

// Condition flags for SET and EXPIRE.
const (
	FlagNX uint8 = 1 << iota
	FlagXX
	FlagKeepTTL
	FlagGT
	FlagLT
)

// Command is one state machine mutation.
type Command struct {
	Op       Op
	Flags    uint8
	Now      int64 // leader clock in unix milliseconds when the command was proposed
	ExpireAt int64 // absolute expiry in unix milliseconds, 0 means none
	Delta    int64 // INCRBY/DECRBY amount
	Args     [][]byte
}

var errCorrupt = errors.New("kv: corrupt command encoding")

// Encode serializes the command as:
// op | flags | varint now | varint expireAt | varint delta | uvarint nargs | (uvarint len | bytes)*
func (c *Command) Encode() []byte {
	n := 2 + 4*binary.MaxVarintLen64
	for _, a := range c.Args {
		n += binary.MaxVarintLen64 + len(a)
	}
	b := make([]byte, 0, n)
	b = append(b, byte(c.Op), c.Flags)
	b = binary.AppendVarint(b, c.Now)
	b = binary.AppendVarint(b, c.ExpireAt)
	b = binary.AppendVarint(b, c.Delta)
	b = binary.AppendUvarint(b, uint64(len(c.Args)))
	for _, a := range c.Args {
		b = binary.AppendUvarint(b, uint64(len(a)))
		b = append(b, a...)
	}
	return b
}

// DecodeCommand parses an encoded command. Args alias b, which is fine
// because log entry data is never modified after it is created.
func DecodeCommand(b []byte) (*Command, error) {
	if len(b) < 2 {
		return nil, errCorrupt
	}
	c := &Command{Op: Op(b[0]), Flags: b[1]}
	b = b[2:]
	var ok bool
	if c.Now, b, ok = readVarint(b); !ok {
		return nil, errCorrupt
	}
	if c.ExpireAt, b, ok = readVarint(b); !ok {
		return nil, errCorrupt
	}
	if c.Delta, b, ok = readVarint(b); !ok {
		return nil, errCorrupt
	}
	nargs, b, ok := readUvarint(b)
	if !ok || nargs > uint64(len(b)) {
		return nil, errCorrupt
	}
	c.Args = make([][]byte, nargs)
	for i := range c.Args {
		var l uint64
		if l, b, ok = readUvarint(b); !ok || l > uint64(len(b)) {
			return nil, errCorrupt
		}
		c.Args[i] = b[:l:l]
		b = b[l:]
	}
	return c, nil
}

func readVarint(b []byte) (int64, []byte, bool) {
	v, n := binary.Varint(b)
	if n <= 0 {
		return 0, nil, false
	}
	return v, b[n:], true
}

func readUvarint(b []byte) (uint64, []byte, bool) {
	v, n := binary.Uvarint(b)
	if n <= 0 {
		return 0, nil, false
	}
	return v, b[n:], true
}
