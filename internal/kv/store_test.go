package kv

import (
	"bytes"
	"math"
	"strconv"
	"testing"
)

type harness struct {
	t     *testing.T
	s     *Store
	index uint64
}

func newHarness(t *testing.T) *harness { return &harness{t: t, s: NewStore()} }

func (h *harness) apply(c Command) any {
	h.index++
	return h.s.Apply(h.index, c.Encode())
}

func args(ss ...string) [][]byte {
	out := make([][]byte, len(ss))
	for i, s := range ss {
		out[i] = []byte(s)
	}
	return out
}

func TestCommandRoundTrip(t *testing.T) {
	in := Command{Op: OpSet, Flags: FlagNX | FlagKeepTTL, Now: 1700000000123, ExpireAt: -5, Delta: math.MinInt64, Args: args("k", "", "v\x00")}
	out, err := DecodeCommand(in.Encode())
	if err != nil {
		t.Fatal(err)
	}
	if out.Op != in.Op || out.Flags != in.Flags || out.Now != in.Now || out.ExpireAt != in.ExpireAt || out.Delta != in.Delta || len(out.Args) != 3 {
		t.Fatalf("round trip mismatch: %+v", out)
	}
	for i := range in.Args {
		if !bytes.Equal(in.Args[i], out.Args[i]) {
			t.Errorf("arg %d: %q != %q", i, out.Args[i], in.Args[i])
		}
	}
	if _, err := DecodeCommand([]byte{1}); err == nil {
		t.Error("expected error for truncated command")
	}
}

func TestSetConditions(t *testing.T) {
	h := newHarness(t)
	if r := h.apply(Command{Op: OpSet, Flags: FlagXX, Args: args("k", "a")}); r != nil {
		t.Fatalf("SET XX on missing key = %v, want nil", r)
	}
	if r := h.apply(Command{Op: OpSet, Flags: FlagNX, Args: args("k", "a")}); r != OK {
		t.Fatalf("SET NX on missing key = %v", r)
	}
	if r := h.apply(Command{Op: OpSet, Flags: FlagNX, Args: args("k", "b")}); r != nil {
		t.Fatalf("SET NX on existing key = %v, want nil", r)
	}
	if v, _ := h.s.Get([]byte("k"), 0); string(v) != "a" {
		t.Fatalf("value = %q, want a", v)
	}
}

func TestExpiryUsesCommandClock(t *testing.T) {
	h := newHarness(t)
	h.apply(Command{Op: OpSet, Now: 1000, ExpireAt: 2000, Args: args("k", "v")})
	if _, ok := h.s.Get([]byte("k"), 1999); !ok {
		t.Fatal("key expired early")
	}
	if _, ok := h.s.Get([]byte("k"), 2000); ok {
		t.Fatal("key visible after expiry")
	}
	if got := h.s.PTTL([]byte("k"), 1500); got != 500 {
		t.Fatalf("PTTL = %d, want 500", got)
	}
	// SET NX at a time when the key has logically expired must succeed.
	if r := h.apply(Command{Op: OpSet, Now: 2500, Flags: FlagNX, Args: args("k", "new")}); r != OK {
		t.Fatalf("SET NX after expiry = %v", r)
	}
	if got := h.s.PTTL([]byte("k"), 2500); got != -1 {
		t.Fatalf("plain SET should clear TTL, PTTL = %d", got)
	}
	if got := h.s.PTTL([]byte("missing"), 0); got != -2 {
		t.Fatalf("PTTL missing = %d", got)
	}
}

func TestKeepTTL(t *testing.T) {
	h := newHarness(t)
	h.apply(Command{Op: OpSet, Now: 0, ExpireAt: 5000, Args: args("k", "v")})
	h.apply(Command{Op: OpSet, Now: 10, Flags: FlagKeepTTL, Args: args("k", "v2")})
	if got := h.s.PTTL([]byte("k"), 1000); got != 4000 {
		t.Fatalf("KEEPTTL lost expiry, PTTL = %d", got)
	}
}

func TestIncr(t *testing.T) {
	h := newHarness(t)
	if r := h.apply(Command{Op: OpIncrBy, Delta: 1, Args: args("n")}); r != int64(1) {
		t.Fatalf("INCR new key = %v", r)
	}
	if r := h.apply(Command{Op: OpIncrBy, Delta: -3, Args: args("n")}); r != int64(-2) {
		t.Fatalf("DECRBY = %v", r)
	}
	h.apply(Command{Op: OpSet, Args: args("s", "abc")})
	if r := h.apply(Command{Op: OpIncrBy, Delta: 1, Args: args("s")}); r != ErrNotInteger {
		t.Fatalf("INCR on string = %v", r)
	}
	h.apply(Command{Op: OpSet, Args: args("big", strconv.FormatInt(math.MaxInt64, 10))})
	if r := h.apply(Command{Op: OpIncrBy, Delta: 1, Args: args("big")}); r != ErrOverflow {
		t.Fatalf("INCR overflow = %v", r)
	}
	// INCR keeps an existing TTL.
	h.apply(Command{Op: OpSet, Now: 0, ExpireAt: 9000, Args: args("t", "5")})
	h.apply(Command{Op: OpIncrBy, Now: 1, Delta: 1, Args: args("t")})
	if got := h.s.PTTL([]byte("t"), 1000); got != 8000 {
		t.Fatalf("INCR dropped TTL, PTTL = %d", got)
	}
}

func TestParseInt(t *testing.T) {
	for in, want := range map[string]bool{
		"0": true, "-1": true, "42": true, "9223372036854775807": true,
		"": false, "01": false, "+1": false, "-0": false, " 1": false, "1a": false, "9223372036854775808": false,
	} {
		if _, ok := parseInt([]byte(in)); ok != want {
			t.Errorf("parseInt(%q) ok = %v, want %v", in, ok, want)
		}
	}
}

func TestExpireFlags(t *testing.T) {
	h := newHarness(t)
	h.apply(Command{Op: OpSet, Args: args("k", "v")})
	if r := h.apply(Command{Op: OpExpire, Flags: FlagXX, ExpireAt: 100, Args: args("k")}); r != int64(0) {
		t.Fatalf("EXPIRE XX without ttl = %v", r)
	}
	if r := h.apply(Command{Op: OpExpire, Flags: FlagNX, ExpireAt: 100, Args: args("k")}); r != int64(1) {
		t.Fatalf("EXPIRE NX = %v", r)
	}
	if r := h.apply(Command{Op: OpExpire, Flags: FlagGT, ExpireAt: 50, Args: args("k")}); r != int64(0) {
		t.Fatalf("EXPIRE GT smaller = %v", r)
	}
	if r := h.apply(Command{Op: OpExpire, Flags: FlagLT, ExpireAt: 50, Args: args("k")}); r != int64(1) {
		t.Fatalf("EXPIRE LT smaller = %v", r)
	}
	// A deadline in the past deletes the key.
	if r := h.apply(Command{Op: OpExpire, Now: 10, ExpireAt: 5, Args: args("k")}); r != int64(1) {
		t.Fatalf("EXPIRE past = %v", r)
	}
	if h.s.Exists(args("k"), 0) != 0 {
		t.Fatal("key should be gone")
	}
	if r := h.apply(Command{Op: OpExpire, ExpireAt: 100, Args: args("missing")}); r != int64(0) {
		t.Fatalf("EXPIRE missing = %v", r)
	}
}

func TestDelMSetSweep(t *testing.T) {
	h := newHarness(t)
	h.apply(Command{Op: OpMSet, Args: args("a", "1", "b", "2")})
	h.apply(Command{Op: OpSet, Now: 0, ExpireAt: 10, Args: args("c", "3")})
	if got := h.s.MGet(args("a", "b", "x"), 0); string(got[0]) != "1" || string(got[1]) != "2" || got[2] != nil {
		t.Fatalf("MGET = %q", got)
	}
	if r := h.apply(Command{Op: OpDel, Args: args("a", "a", "x")}); r != int64(1) {
		t.Fatalf("DEL = %v", r)
	}
	if !h.s.HasExpired(10) || h.s.HasExpired(9) {
		t.Fatal("HasExpired wrong")
	}
	if r := h.apply(Command{Op: OpSweep, Now: 10}); r != int64(1) {
		t.Fatalf("sweep removed %v keys, want 1", r)
	}
	if h.s.Len() != 1 {
		t.Fatalf("Len = %d, want 1", h.s.Len())
	}
}

func TestSnapshotRestoreAndDigest(t *testing.T) {
	h := newHarness(t)
	h.apply(Command{Op: OpMSet, Args: args("a", "1", "b", "")})
	h.apply(Command{Op: OpSet, ExpireAt: 12345, Args: args("c", "x")})
	snap, err := h.s.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	other := NewStore()
	other.Apply(1, (&Command{Op: OpSet, Args: args("junk", "1")}).Encode())
	if err := other.Restore(snap); err != nil {
		t.Fatal(err)
	}
	if other.Digest() != h.s.Digest() {
		t.Fatal("digest differs after restore")
	}
	if other.PTTL([]byte("c"), 45) != 12300 || other.Len() != 3 {
		t.Fatal("restored state differs")
	}
	h.apply(Command{Op: OpSet, Args: args("a", "2")})
	if other.Digest() == h.s.Digest() {
		t.Fatal("digest should change when state changes")
	}
}
