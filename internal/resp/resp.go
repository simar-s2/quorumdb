// Package resp implements RESP2: command parsing, reply encoding and reply parsing.
package resp

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"strconv"
)

const (
	maxBulkLen   = 512 << 20 // same limit as Redis' proto-max-bulk-len default
	maxArrayLen  = 1 << 20
	readerBufLen = 64 << 10
)

// ErrProtocol is returned for malformed input; the server replies with an error and closes.
var ErrProtocol = errors.New("protocol error")

func protoErr(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrProtocol, fmt.Sprintf(format, args...))
}

// Reader parses RESP from a buffered stream.
type Reader struct {
	br *bufio.Reader
}

func NewReader(r io.Reader) *Reader {
	return &Reader{br: bufio.NewReaderSize(r, readerBufLen)}
}

// Buffered returns how many bytes can be parsed without blocking, used to batch pipelined commands.
func (r *Reader) Buffered() int { return r.br.Buffered() }

// ReadCommand reads one command, as a RESP array or an inline command, skipping empty ones.
func (r *Reader) ReadCommand() ([][]byte, error) {
	for {
		line, err := r.readLine()
		if err != nil {
			return nil, err
		}
		if len(line) == 0 {
			continue
		}
		if line[0] != '*' {
			args := bytes.Fields(line)
			if len(args) == 0 {
				continue
			}
			out := make([][]byte, len(args))
			for i, a := range args {
				out[i] = append([]byte(nil), a...)
			}
			return out, nil
		}
		n, err := parseInt(line[1:])
		if err != nil || n > maxArrayLen {
			return nil, protoErr("invalid multibulk length")
		}
		if n <= 0 {
			continue
		}
		args := make([][]byte, n)
		for i := range args {
			line, err := r.readLine()
			if err != nil {
				return nil, err
			}
			if len(line) == 0 || line[0] != '$' {
				return nil, protoErr("expected '$', got '%s'", firstByte(line))
			}
			l, err := parseInt(line[1:])
			if err != nil || l < 0 || l > maxBulkLen {
				return nil, protoErr("invalid bulk length")
			}
			if args[i], err = r.readBulk(int(l)); err != nil {
				return nil, err
			}
		}
		return args, nil
	}
}

func (r *Reader) readBulk(n int) ([]byte, error) {
	buf := make([]byte, n+2)
	if _, err := io.ReadFull(r.br, buf); err != nil {
		return nil, err
	}
	if buf[n] != '\r' || buf[n+1] != '\n' {
		return nil, protoErr("bulk string not terminated by CRLF")
	}
	return buf[:n:n], nil
}

// readLine returns a line without its CRLF; the slice is valid until the next read.
func (r *Reader) readLine() ([]byte, error) {
	line, err := r.br.ReadSlice('\n')
	if err == bufio.ErrBufferFull {
		return nil, protoErr("line too long")
	}
	if err != nil {
		return nil, err
	}
	line = line[:len(line)-1]
	if len(line) > 0 && line[len(line)-1] == '\r' {
		line = line[:len(line)-1]
	}
	return line, nil
}

func firstByte(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return string(b[:1])
}

func parseInt(b []byte) (int64, error) {
	return strconv.ParseInt(string(b), 10, 64)
}

// Reply encoding appends to a caller-owned buffer so a batch of replies needs one write.

func AppendSimple(b []byte, s string) []byte {
	b = append(b, '+')
	b = append(b, s...)
	return append(b, '\r', '\n')
}

func AppendError(b []byte, s string) []byte {
	b = append(b, '-')
	b = append(b, s...)
	return append(b, '\r', '\n')
}

func AppendInt(b []byte, n int64) []byte {
	b = append(b, ':')
	b = strconv.AppendInt(b, n, 10)
	return append(b, '\r', '\n')
}

func AppendBulk(b []byte, p []byte) []byte {
	b = append(b, '$')
	b = strconv.AppendInt(b, int64(len(p)), 10)
	b = append(b, '\r', '\n')
	b = append(b, p...)
	return append(b, '\r', '\n')
}

func AppendBulkString(b []byte, s string) []byte {
	b = append(b, '$')
	b = strconv.AppendInt(b, int64(len(s)), 10)
	b = append(b, '\r', '\n')
	b = append(b, s...)
	return append(b, '\r', '\n')
}

func AppendNull(b []byte) []byte { return append(b, "$-1\r\n"...) }

func AppendArray(b []byte, n int) []byte {
	b = append(b, '*')
	b = strconv.AppendInt(b, int64(n), 10)
	return append(b, '\r', '\n')
}

// AppendCommand encodes a client command as an array of bulk strings.
func AppendCommand(b []byte, args ...string) []byte {
	b = AppendArray(b, len(args))
	for _, a := range args {
		b = AppendBulkString(b, a)
	}
	return b
}

// Kind identifies the RESP2 reply type.
type Kind byte

const (
	KindSimple Kind = '+'
	KindError  Kind = '-'
	KindInt    Kind = ':'
	KindBulk   Kind = '$'
	KindArray  Kind = '*'
)

// Value is a parsed reply. Null bulk strings and null arrays have Null set.
type Value struct {
	Kind  Kind
	Str   string
	Int   int64
	Null  bool
	Array []Value
}

func (v Value) IsError() bool { return v.Kind == KindError }

func (v Value) String() string {
	switch v.Kind {
	case KindInt:
		return fmt.Sprintf("(integer) %d", v.Int)
	case KindError:
		return "(error) " + v.Str
	case KindArray:
		if v.Null {
			return "(nil)"
		}
		parts := make([]string, len(v.Array))
		for i, e := range v.Array {
			parts[i] = e.String()
		}
		return fmt.Sprint(parts)
	default:
		if v.Null {
			return "(nil)"
		}
		return strconv.Quote(v.Str)
	}
}

// ReadReply parses one server reply.
func (r *Reader) ReadReply() (Value, error) {
	line, err := r.readLine()
	if err != nil {
		return Value{}, err
	}
	if len(line) == 0 {
		return Value{}, protoErr("empty reply line")
	}
	kind, rest := Kind(line[0]), line[1:]
	switch kind {
	case KindSimple, KindError:
		return Value{Kind: kind, Str: string(rest)}, nil
	case KindInt:
		n, err := parseInt(rest)
		if err != nil {
			return Value{}, protoErr("invalid integer reply")
		}
		return Value{Kind: kind, Int: n}, nil
	case KindBulk:
		n, err := parseInt(rest)
		if err != nil || n > maxBulkLen {
			return Value{}, protoErr("invalid bulk length")
		}
		if n < 0 {
			return Value{Kind: kind, Null: true}, nil
		}
		b, err := r.readBulk(int(n))
		if err != nil {
			return Value{}, err
		}
		return Value{Kind: kind, Str: string(b)}, nil
	case KindArray:
		n, err := parseInt(rest)
		if err != nil || n > maxArrayLen {
			return Value{}, protoErr("invalid array length")
		}
		if n < 0 {
			return Value{Kind: kind, Null: true}, nil
		}
		v := Value{Kind: kind, Array: make([]Value, n)}
		for i := range v.Array {
			if v.Array[i], err = r.ReadReply(); err != nil {
				return Value{}, err
			}
		}
		return v, nil
	default:
		return Value{}, protoErr("unknown reply type %q", line[0])
	}
}
