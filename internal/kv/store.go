package kv

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"maps"
	"math"
	"sort"
	"strconv"
	"sync"
)

// Status is a simple-string reply such as OK.
type Status string

const OK Status = "OK"

var (
	ErrNotInteger = errors.New("ERR value is not an integer or out of range")
	ErrOverflow   = errors.New("ERR increment or decrement would overflow")
)

type item struct {
	val []byte
	exp int64 // absolute unix ms, 0 = no expiry
}

func (it item) expired(now int64) bool { return it.exp != 0 && it.exp <= now }

// Store is the key-value state machine. Apply is called by the Raft applier
// goroutine only; reads may run concurrently from client goroutines.
type Store struct {
	mu   sync.RWMutex
	data map[string]item
	ttl  map[string]struct{} // keys that have an expiry, for sweeping
}

func NewStore() *Store {
	return &Store{data: make(map[string]item), ttl: make(map[string]struct{})}
}

// Apply executes one committed command and returns its reply value: Status,
// nil (null reply), int64 or error.
func (s *Store) Apply(_ uint64, data []byte) any {
	cmd, err := DecodeCommand(data)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	now := cmd.Now
	switch cmd.Op {
	case OpSet:
		key := string(cmd.Args[0])
		cur, exists := s.lookup(key, now)
		if cmd.Flags&FlagNX != 0 && exists || cmd.Flags&FlagXX != 0 && !exists {
			return nil
		}
		exp := cmd.ExpireAt
		if cmd.Flags&FlagKeepTTL != 0 && exists {
			exp = cur.exp
		}
		s.put(key, item{val: cmd.Args[1], exp: exp})
		return OK

	case OpMSet:
		for i := 0; i+1 < len(cmd.Args); i += 2 {
			s.put(string(cmd.Args[i]), item{val: cmd.Args[i+1]})
		}
		return OK

	case OpDel:
		var n int64
		for _, k := range cmd.Args {
			if _, exists := s.lookup(string(k), now); exists {
				s.remove(string(k))
				n++
			}
		}
		return n

	case OpIncrBy:
		key := string(cmd.Args[0])
		cur, exists := s.lookup(key, now)
		var n int64
		if exists {
			v, ok := parseInt(cur.val)
			if !ok {
				return ErrNotInteger
			}
			n = v
		}
		d := cmd.Delta
		if d > 0 && n > math.MaxInt64-d || d < 0 && n < math.MinInt64-d {
			return ErrOverflow
		}
		n += d
		s.put(key, item{val: strconv.AppendInt(nil, n, 10), exp: cur.exp})
		return n

	case OpExpire:
		key := string(cmd.Args[0])
		cur, exists := s.lookup(key, now)
		if !exists || !expireAllowed(cmd.Flags, cur.exp, cmd.ExpireAt) {
			return int64(0)
		}
		if cmd.ExpireAt <= now {
			s.remove(key)
			return int64(1)
		}
		cur.exp = cmd.ExpireAt
		s.put(key, cur)
		return int64(1)

	case OpSweep:
		var n int64
		for k := range s.ttl {
			if s.data[k].expired(now) {
				s.remove(k)
				n++
			}
		}
		return n
	}
	return errors.New("ERR unknown state machine operation")
}

// expireAllowed implements the NX/XX/GT/LT options of EXPIRE. A key with no
// expiry counts as an infinite TTL, as in Redis.
func expireAllowed(flags uint8, cur, next int64) bool {
	switch {
	case flags&FlagNX != 0:
		return cur == 0
	case flags&FlagXX != 0:
		return cur != 0
	case flags&FlagGT != 0:
		return cur != 0 && next > cur
	case flags&FlagLT != 0:
		return cur == 0 || next < cur
	}
	return true
}

// lookup returns the live item for key. Expired items are removed, which is
// deterministic because now comes from the log entry. Callers hold s.mu.
func (s *Store) lookup(key string, now int64) (item, bool) {
	it, ok := s.data[key]
	if !ok {
		return item{}, false
	}
	if it.expired(now) {
		s.remove(key)
		return item{}, false
	}
	return it, true
}

func (s *Store) put(key string, it item) {
	s.data[key] = it
	if it.exp != 0 {
		s.ttl[key] = struct{}{}
	} else {
		delete(s.ttl, key)
	}
}

func (s *Store) remove(key string) {
	delete(s.data, key)
	delete(s.ttl, key)
}

// Reads. now is the reading node's clock; expired keys are treated as absent.

func (s *Store) Get(key []byte, now int64) ([]byte, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	it, ok := s.data[string(key)]
	if !ok || it.expired(now) {
		return nil, false
	}
	return it.val, true
}

// MGet reads all keys under one lock so the result is a consistent snapshot.
// Missing keys are returned as nil.
func (s *Store) MGet(keys [][]byte, now int64) [][]byte {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([][]byte, len(keys))
	for i, k := range keys {
		if it, ok := s.data[string(k)]; ok && !it.expired(now) {
			out[i] = it.val
		}
	}
	return out
}

func (s *Store) Exists(keys [][]byte, now int64) int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var n int64
	for _, k := range keys {
		if it, ok := s.data[string(k)]; ok && !it.expired(now) {
			n++
		}
	}
	return n
}

// PTTL returns the remaining time to live in milliseconds, -1 if the key has
// no expiry and -2 if it does not exist.
func (s *Store) PTTL(key []byte, now int64) int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	it, ok := s.data[string(key)]
	switch {
	case !ok || it.expired(now):
		return -2
	case it.exp == 0:
		return -1
	default:
		return it.exp - now
	}
}

// Len returns the number of stored keys, including expired keys that have
// not been swept yet (same as Redis DBSIZE).
func (s *Store) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.data)
}

// HasExpired reports whether any key is past its expiry, so the leader knows
// when a sweep is worth proposing.
func (s *Store) HasExpired(now int64) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for k := range s.ttl {
		if s.data[k].expired(now) {
			return true
		}
	}
	return false
}

func (s *Store) sortedKeys() []string {
	keys := make([]string, 0, len(s.data))
	for k := range s.data {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

const snapshotVersion = 1

// Snapshot takes a shallow copy of the map, which is a consistent point-in-
// time view because stored values are never modified in place (Apply always
// replaces an item). The returned function encodes that copy as:
// version | uvarint count | (uvarint klen | key | uvarint vlen | value | varint exp)*
func (s *Store) Snapshot() func() ([]byte, error) {
	s.mu.RLock()
	data := maps.Clone(s.data)
	s.mu.RUnlock()
	return func() ([]byte, error) {
		b := []byte{snapshotVersion}
		b = binary.AppendUvarint(b, uint64(len(data)))
		for k, it := range data {
			b = binary.AppendUvarint(b, uint64(len(k)))
			b = append(b, k...)
			b = binary.AppendUvarint(b, uint64(len(it.val)))
			b = append(b, it.val...)
			b = binary.AppendVarint(b, it.exp)
		}
		return b, nil
	}
}

// Restore replaces the store's contents with a snapshot.
func (s *Store) Restore(b []byte) error {
	if len(b) == 0 || b[0] != snapshotVersion {
		return errors.New("kv: unknown snapshot version")
	}
	b = b[1:]
	n, b, ok := readUvarint(b)
	if !ok {
		return errCorrupt
	}
	data := make(map[string]item, n)
	ttl := make(map[string]struct{})
	for i := uint64(0); i < n; i++ {
		var kl, vl uint64
		var exp int64
		if kl, b, ok = readUvarint(b); !ok || kl > uint64(len(b)) {
			return errCorrupt
		}
		key := string(b[:kl])
		b = b[kl:]
		if vl, b, ok = readUvarint(b); !ok || vl > uint64(len(b)) {
			return errCorrupt
		}
		val := append([]byte(nil), b[:vl]...)
		b = b[vl:]
		if exp, b, ok = readVarint(b); !ok {
			return errCorrupt
		}
		data[key] = item{val: val, exp: exp}
		if exp != 0 {
			ttl[key] = struct{}{}
		}
	}
	s.mu.Lock()
	s.data, s.ttl = data, ttl
	s.mu.Unlock()
	return nil
}

// Digest hashes the full state. Replicas that applied the same log prefix
// have the same digest; the chaos harness uses this to check convergence.
func (s *Store) Digest() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	h := sha256.New()
	var buf [binary.MaxVarintLen64]byte
	for _, k := range s.sortedKeys() {
		it := s.data[k]
		h.Write(binary.AppendUvarint(buf[:0], uint64(len(k))))
		h.Write([]byte(k))
		h.Write(binary.AppendUvarint(buf[:0], uint64(len(it.val))))
		h.Write(it.val)
		h.Write(binary.AppendVarint(buf[:0], it.exp))
	}
	return hex.EncodeToString(h.Sum(nil)[:20])
}

// parseInt accepts the same strict integer syntax as Redis: an optional
// minus sign, no leading zeros or plus sign, and a value that fits in int64.
func parseInt(b []byte) (int64, bool) {
	if len(b) == 0 || len(b) > 20 {
		return 0, false
	}
	if len(b) == 1 && b[0] == '0' {
		return 0, true
	}
	digits := b
	if b[0] == '-' {
		digits = b[1:]
	}
	if len(digits) == 0 || digits[0] < '1' || digits[0] > '9' {
		return 0, false
	}
	for _, c := range digits {
		if c < '0' || c > '9' {
			return 0, false
		}
	}
	n, err := strconv.ParseInt(string(b), 10, 64)
	return n, err == nil
}

// ParseInt is exported for the command layer, which validates arguments the
// same way the state machine does.
func ParseInt(b []byte) (int64, bool) { return parseInt(b) }
