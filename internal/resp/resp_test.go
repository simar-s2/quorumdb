package resp

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"
)

func TestReadCommandArray(t *testing.T) {
	r := NewReader(strings.NewReader("*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nva\r\nl\r\n"))
	args, err := r.ReadCommand()
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"SET", "key", "va\r\nl"}
	if len(args) != len(want) {
		t.Fatalf("got %d args, want %d", len(args), len(want))
	}
	for i := range want {
		if string(args[i]) != want[i] {
			t.Errorf("arg %d = %q, want %q", i, args[i], want[i])
		}
	}
}

func TestReadCommandInlineAndPipelined(t *testing.T) {
	input := "PING\r\n\r\n*1\r\n$4\r\nPING\r\nECHO   hi  \n"
	r := NewReader(strings.NewReader(input))
	var got []string
	for {
		args, err := r.ReadCommand()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		parts := make([]string, len(args))
		for i, a := range args {
			parts[i] = string(a)
		}
		got = append(got, strings.Join(parts, " "))
	}
	want := []string{"PING", "PING", "ECHO hi"}
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestReadCommandProtocolErrors(t *testing.T) {
	for _, in := range []string{
		"*x\r\n",
		"*1\r\n:3\r\n",
		"*1\r\n$-5\r\n",
		"*1\r\n$3\r\nabcde\r\n",
	} {
		_, err := NewReader(strings.NewReader(in)).ReadCommand()
		if !errors.Is(err, ErrProtocol) {
			t.Errorf("%q: got %v, want protocol error", in, err)
		}
	}
}

func TestReplyRoundTrip(t *testing.T) {
	var b []byte
	b = AppendSimple(b, "OK")
	b = AppendError(b, "ERR boom")
	b = AppendInt(b, -42)
	b = AppendBulk(b, []byte("hello"))
	b = AppendNull(b)
	b = AppendArray(b, 2)
	b = AppendBulkString(b, "a")
	b = AppendNull(b)

	r := NewReader(bytes.NewReader(b))
	checks := []func(Value) bool{
		func(v Value) bool { return v.Kind == KindSimple && v.Str == "OK" },
		func(v Value) bool { return v.IsError() && v.Str == "ERR boom" },
		func(v Value) bool { return v.Kind == KindInt && v.Int == -42 },
		func(v Value) bool { return v.Kind == KindBulk && v.Str == "hello" },
		func(v Value) bool { return v.Kind == KindBulk && v.Null },
		func(v Value) bool {
			return v.Kind == KindArray && len(v.Array) == 2 && v.Array[0].Str == "a" && v.Array[1].Null
		},
	}
	for i, check := range checks {
		v, err := r.ReadReply()
		if err != nil {
			t.Fatalf("reply %d: %v", i, err)
		}
		if !check(v) {
			t.Errorf("reply %d: unexpected value %+v", i, v)
		}
	}
}

func TestAppendCommandParses(t *testing.T) {
	b := AppendCommand(nil, "MSET", "a", "1", "b", "")
	args, err := NewReader(bytes.NewReader(b)).ReadCommand()
	if err != nil {
		t.Fatal(err)
	}
	if len(args) != 5 || string(args[0]) != "MSET" || len(args[4]) != 0 {
		t.Fatalf("unexpected args %q", args)
	}
}
