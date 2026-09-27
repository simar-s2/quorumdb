// Package netproxy is a TCP proxy the chaos test uses to cut or blackhole links between nodes.
package netproxy

import (
	"net"
	"sync"
	"sync/atomic"
	"time"
)

type Mode int32

const (
	// Pass forwards traffic normally.
	Pass Mode = iota
	// Reset closes connections and refuses new ones, like a TCP RST.
	Reset
	// Drop silently discards traffic; blackholed connections stay dead after the link heals.
	Drop
)

func (m Mode) String() string {
	switch m {
	case Reset:
		return "reset"
	case Drop:
		return "drop"
	}
	return "pass"
}

type Proxy struct {
	ln     net.Listener
	target string
	mode   atomic.Int32

	mu     sync.Mutex
	closed bool
	pairs  map[*pair]struct{}
}

type pair struct {
	client, upstream net.Conn // upstream is nil for connections accepted while dropping
	black            atomic.Bool
	once             sync.Once
}

func (p *pair) close() {
	p.once.Do(func() {
		p.client.Close()
		if p.upstream != nil {
			p.upstream.Close()
		}
	})
}

// Listen starts a proxy on listenAddr that forwards to target.
func Listen(listenAddr, target string) (*Proxy, error) {
	ln, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return nil, err
	}
	p := &Proxy{ln: ln, target: target, pairs: map[*pair]struct{}{}}
	go p.acceptLoop()
	return p, nil
}

func (p *Proxy) Addr() string { return p.ln.Addr().String() }

func (p *Proxy) Mode() Mode { return Mode(p.mode.Load()) }

// SetMode changes how the link behaves from now on.
func (p *Proxy) SetMode(m Mode) {
	p.mode.Store(int32(m))
	p.mu.Lock()
	defer p.mu.Unlock()
	for pr := range p.pairs {
		switch m {
		case Reset:
			pr.close()
			delete(p.pairs, pr)
		case Drop:
			pr.black.Store(true)
		}
	}
}

func (p *Proxy) Close() {
	p.mu.Lock()
	p.closed = true
	for pr := range p.pairs {
		pr.close()
	}
	p.pairs = nil
	p.mu.Unlock()
	p.ln.Close()
}

func (p *Proxy) acceptLoop() {
	for {
		c, err := p.ln.Accept()
		if err != nil {
			p.mu.Lock()
			closed := p.closed
			p.mu.Unlock()
			if closed {
				return
			}
			time.Sleep(5 * time.Millisecond)
			continue
		}
		go p.handle(c)
	}
}

func (p *Proxy) handle(c net.Conn) {
	pr := &pair{client: c}
	switch p.Mode() {
	case Reset:
		c.Close()
		return
	case Drop:
		pr.black.Store(true)
	default:
		up, err := net.DialTimeout("tcp", p.target, time.Second)
		if err != nil {
			c.Close()
			return
		}
		pr.upstream = up
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		pr.close()
		return
	}
	p.pairs[pr] = struct{}{}
	p.mu.Unlock()

	done := func() {
		pr.close()
		p.mu.Lock()
		delete(p.pairs, pr)
		p.mu.Unlock()
	}
	if pr.upstream == nil {
		go func() { copyUnless(pr, nil, c); done() }()
		return
	}
	go func() { copyUnless(pr, pr.upstream, c); done() }()
	go func() { copyUnless(pr, c, pr.upstream); done() }()
}

// copyUnless copies src to dst, discarding data while the pair is blackholed.
func copyUnless(pr *pair, dst, src net.Conn) {
	buf := make([]byte, 32<<10)
	for {
		n, err := src.Read(buf)
		if n > 0 && dst != nil && !pr.black.Load() {
			if _, werr := dst.Write(buf[:n]); werr != nil {
				return
			}
		}
		if err != nil {
			return
		}
	}
}
