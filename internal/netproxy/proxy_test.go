package netproxy

import (
	"bufio"
	"net"
	"testing"
	"time"
)

func echoServer(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				r := bufio.NewReader(c)
				for {
					line, err := r.ReadString('\n')
					if err != nil {
						return
					}
					c.Write([]byte(line))
				}
			}()
		}
	}()
	return ln.Addr().String()
}

func roundTrip(conn net.Conn, r *bufio.Reader, timeout time.Duration) error {
	conn.SetDeadline(time.Now().Add(timeout))
	if _, err := conn.Write([]byte("ping\n")); err != nil {
		return err
	}
	_, err := r.ReadString('\n')
	return err
}

func dial(t *testing.T, addr string) (net.Conn, *bufio.Reader) {
	t.Helper()
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c, bufio.NewReader(c)
}

func TestModes(t *testing.T) {
	p, err := Listen("127.0.0.1:0", echoServer(t))
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	c, r := dial(t, p.Addr())
	if err := roundTrip(c, r, time.Second); err != nil {
		t.Fatalf("pass mode: %v", err)
	}

	p.SetMode(Drop)
	if err := roundTrip(c, r, 100*time.Millisecond); err == nil {
		t.Fatal("drop mode delivered data")
	}
	c2, r2 := dial(t, p.Addr()) // accepted but blackholed
	if err := roundTrip(c2, r2, 100*time.Millisecond); err == nil {
		t.Fatal("new connection in drop mode delivered data")
	}

	p.SetMode(Pass)
	c3, r3 := dial(t, p.Addr())
	if err := roundTrip(c3, r3, time.Second); err != nil {
		t.Fatalf("new connection after heal: %v", err)
	}

	p.SetMode(Reset)
	if err := roundTrip(c3, r3, time.Second); err == nil {
		t.Fatal("reset mode kept the connection open")
	}
	c4, r4 := dial(t, p.Addr())
	if err := roundTrip(c4, r4, time.Second); err == nil {
		t.Fatal("reset mode accepted a working connection")
	}
}
