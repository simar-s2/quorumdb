package chaos

import (
	"math"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"
)

type hist struct{ ops []porcupine.Operation }

func (h *hist) add(client int, call, ret int64, in Input, out Output) {
	h.ops = append(h.ops, porcupine.Operation{ClientId: client, Input: in, Call: call, Output: out, Return: ret})
}

func check(t *testing.T, h *hist) porcupine.CheckResult {
	t.Helper()
	return porcupine.CheckOperationsTimeout(Model, h.ops, 10*time.Second)
}

func TestModelAcceptsLinearizableHistory(t *testing.T) {
	h := &hist{}
	h.add(0, 0, 10, Input{Op: OpSet, Key: "x", Value: "a"}, Output{})
	h.add(1, 5, 15, Input{Op: OpGet, Key: "x"}, Output{Exists: true, Value: "a"}) // concurrent, sees it
	h.add(0, 20, 30, Input{Op: OpIncr, Key: "n"}, Output{Int: 1})
	h.add(1, 25, 35, Input{Op: OpIncr, Key: "n"}, Output{Int: 2})
	h.add(0, 40, 50, Input{Op: OpDel, Key: "x"}, Output{Int: 1})
	h.add(1, 60, 70, Input{Op: OpGet, Key: "x"}, Output{Exists: false})
	if r := check(t, h); r != porcupine.Ok {
		t.Fatalf("got %s, want Ok", r)
	}
}

// The property the chaos test exists to protect: an acknowledged write must
// be visible to every later read.
func TestModelDetectsLostAcknowledgedWrite(t *testing.T) {
	h := &hist{}
	h.add(0, 0, 10, Input{Op: OpSet, Key: "x", Value: "a"}, Output{})
	h.add(0, 20, 30, Input{Op: OpSet, Key: "x", Value: "b"}, Output{}) // acknowledged
	h.add(1, 40, 50, Input{Op: OpGet, Key: "x"}, Output{Exists: true, Value: "a"})
	if r := check(t, h); r != porcupine.Illegal {
		t.Fatalf("got %s, want Illegal", r)
	}
}

func TestModelDetectsStaleRead(t *testing.T) {
	h := &hist{}
	h.add(0, 0, 10, Input{Op: OpSet, Key: "x", Value: "new"}, Output{})
	h.add(1, 20, 30, Input{Op: OpGet, Key: "x"}, Output{Exists: false})
	if r := check(t, h); r != porcupine.Illegal {
		t.Fatalf("got %s, want Illegal", r)
	}
}

func TestModelDetectsDoubleIncrement(t *testing.T) {
	h := &hist{}
	h.add(0, 0, 10, Input{Op: OpIncr, Key: "n"}, Output{Int: 1})
	h.add(1, 20, 30, Input{Op: OpGet, Key: "n"}, Output{Exists: true, Value: "2"})
	if r := check(t, h); r != porcupine.Illegal {
		t.Fatalf("got %s, want Illegal", r)
	}
}

// An unacknowledged write may or may not have happened; both outcomes are
// linearizable, but later reads must be consistent with a single choice.
func TestModelAmbiguousWrites(t *testing.T) {
	end := int64(math.MaxInt32)
	took := &hist{}
	took.add(0, 0, end, Input{Op: OpSet, Key: "x", Value: "maybe"}, Output{Unknown: true})
	took.add(1, 20, 30, Input{Op: OpGet, Key: "x"}, Output{Exists: true, Value: "maybe"})
	if r := check(t, took); r != porcupine.Ok {
		t.Fatalf("applied ambiguous write: got %s, want Ok", r)
	}

	never := &hist{}
	never.add(0, 0, end, Input{Op: OpSet, Key: "x", Value: "maybe"}, Output{Unknown: true})
	never.add(1, 20, 30, Input{Op: OpGet, Key: "x"}, Output{Exists: false})
	if r := check(t, never); r != porcupine.Ok {
		t.Fatalf("dropped ambiguous write: got %s, want Ok", r)
	}

	flipflop := &hist{}
	flipflop.add(0, 0, end, Input{Op: OpSet, Key: "x", Value: "maybe"}, Output{Unknown: true})
	flipflop.add(1, 20, 30, Input{Op: OpGet, Key: "x"}, Output{Exists: true, Value: "maybe"})
	flipflop.add(1, 40, 50, Input{Op: OpGet, Key: "x"}, Output{Exists: false})
	if r := check(t, flipflop); r != porcupine.Illegal {
		t.Fatalf("value appeared then vanished: got %s, want Illegal", r)
	}
}
