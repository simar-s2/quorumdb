package main

import (
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/simar-s2/quorumdb/internal/netproxy"
)

type nemesis struct {
	c      cluster
	rng    *rand.Rand
	counts map[string]int
	events []string
	start  time.Time
}

func (nm *nemesis) logf(format string, args ...any) {
	nm.events = append(nm.events, fmt.Sprintf("%6.2fs  %s", time.Since(nm.start).Seconds(), fmt.Sprintf(format, args...)))
}

// target picks the leader half the time and a random node otherwise.
func (nm *nemesis) target() int {
	if nm.rng.IntN(2) == 0 {
		if l := leader(nm.c); l >= 0 {
			return l
		}
	}
	return nm.rng.IntN(nodeCount)
}

func (nm *nemesis) linkMode() netproxy.Mode {
	modes := nm.c.linkModes()
	return modes[nm.rng.IntN(len(modes))]
}

// bothWays returns the links from a to b and from b to a.
func bothWays(a, b int) []link { return []link{{from: a, to: b}, {from: b, to: a}} }

// inject applies one random fault and returns the function that heals it.
func (nm *nemesis) inject() func() {
	n := nm.target()
	name := nm.c.nodeName(n)
	switch kind := nm.rng.IntN(6); kind {
	case 0:
		if !nm.c.kill(n) {
			return func() {}
		}
		nm.counts["kill"]++
		nm.logf("kill -9 %s", name)
		return func() { nm.logf("restart %s", name); nm.c.restart(n) }
	case 1:
		if !nm.c.setFrozen(n, true) {
			return func() {}
		}
		nm.counts["pause"]++
		nm.logf("pause (SIGSTOP) %s", name)
		return func() { nm.logf("resume %s", name); nm.c.setFrozen(n, false) }
	case 2:
		m := nm.linkMode()
		var links []link
		for o := 0; o < nodeCount; o++ {
			if o != n {
				links = append(links, bothWays(n, o)...)
			}
		}
		nm.counts["isolate"]++
		nm.logf("isolate %s (%s)", name, m)
		nm.c.setLinks(links, m)
		return func() { nm.logf("heal %s", name); nm.c.setLinks(links, netproxy.Pass) }
	case 3:
		// Bridge: two nodes lose each other but both still reach the third.
		o := (n + 1 + nm.rng.IntN(2)) % nodeCount
		m := nm.linkMode()
		links := bothWays(n, o)
		nm.counts["bridge"]++
		nm.logf("cut %s <-> %s (%s)", name, nm.c.nodeName(o), m)
		nm.c.setLinks(links, m)
		return func() {
			nm.logf("heal %s <-> %s", name, nm.c.nodeName(o))
			nm.c.setLinks(links, netproxy.Pass)
		}
	default:
		// One-way link failure, in either direction relative to the target.
		o := (n + 1 + nm.rng.IntN(2)) % nodeCount
		from, to := n, o
		if kind == 5 {
			from, to = o, n
		}
		m := nm.linkMode()
		links := []link{{from: from, to: to}}
		nm.counts["one-way"]++
		nm.logf("cut %s -> %s (%s)", nm.c.nodeName(from), nm.c.nodeName(to), m)
		nm.c.setLinks(links, m)
		return func() {
			nm.logf("heal %s -> %s", nm.c.nodeName(from), nm.c.nodeName(to))
			nm.c.setLinks(links, netproxy.Pass)
		}
	}
}

func (nm *nemesis) run(d time.Duration) {
	end := time.Now().Add(d)
	for time.Now().Before(end) {
		time.Sleep(time.Duration(200+nm.rng.IntN(600)) * time.Millisecond)
		heals := []func(){nm.inject()}
		if nm.rng.IntN(4) == 0 {
			// Sometimes overlap a second fault, which can leave no majority.
			heals = append(heals, nm.inject())
		}
		time.Sleep(time.Duration(500+nm.rng.IntN(2000)) * time.Millisecond)
		for i := len(heals) - 1; i >= 0; i-- {
			heals[i]()
		}
	}
}
