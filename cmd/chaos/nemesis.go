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

	// failovers holds, per fault kind, how many milliseconds the other nodes took to elect a new
	// leader after the fault took out the old one. failoverTimeouts counts the times they did not
	// elect one within failoverTimeout.
	failovers        map[string][]float64
	failoverTimeouts map[string]int
}

const failoverTimeout = 10 * time.Second

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

// leaderBeforeFault returns the current leader and its term if node is that leader and both other
// nodes answer, so a new leader can be elected once the fault takes node out. Otherwise it returns -1
// and the failover is not timed.
func (nm *nemesis) leaderBeforeFault(node int) (oldLeader, oldTerm int) {
	oldLeader, oldTerm = leaderAndTerm(nm.c, -1)
	if oldLeader != node {
		return -1, 0
	}
	for other := 0; other < nodeCount; other++ {
		if other != node && !answers(nm.c, other) {
			return -1, 0
		}
	}
	return oldLeader, oldTerm
}

// timeFailover waits until a node other than oldLeader leads in a newer term, and records how long
// that took. The clock starts just before the command that applies the fault was sent (faultSent),
// so on Kubernetes the time includes kubectl's latency and NetworkPolicy propagation: it is an upper
// bound on the real failover. It does nothing if oldLeader is -1.
func (nm *nemesis) timeFailover(kind string, oldLeader, oldTerm int, faultSent time.Time) {
	if oldLeader < 0 {
		return
	}
	for time.Since(faultSent) < failoverTimeout {
		if newLeader, term := leaderAndTerm(nm.c, oldLeader); newLeader >= 0 && term > oldTerm {
			elapsed := time.Since(faultSent)
			nm.failovers[kind] = append(nm.failovers[kind], float64(elapsed.Microseconds())/1000)
			nm.logf("new leader %s after %s", nm.c.nodeName(newLeader), elapsed.Round(time.Millisecond))
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	nm.failoverTimeouts[kind]++
	nm.logf("no new leader within %s", failoverTimeout)
}

// inject applies one random fault and returns the function that heals it.
func (nm *nemesis) inject() func() {
	n := nm.target()
	name := nm.c.nodeName(n)
	kind := nm.rng.IntN(7)
	if kind >= 2 && len(nm.c.linkModes()) == 0 {
		return func() {} // kinds 2 to 6 cut links, which this cluster cannot do
	}
	switch kind {
	case 0:
		oldLeader, oldTerm := nm.leaderBeforeFault(n)
		killedAt, wasRunning := nm.c.kill(n)
		if !wasRunning {
			return func() {}
		}
		nm.counts["kill"]++
		nm.logf("kill -9 %s", name)
		nm.timeFailover("kill", oldLeader, oldTerm, killedAt)
		return func() { nm.logf("restart %s", name); nm.c.restart(n) }
	case 1:
		oldLeader, oldTerm := nm.leaderBeforeFault(n)
		pausedAt := time.Now()
		if !nm.c.setFrozen(n, true) {
			return func() {}
		}
		nm.counts["pause"]++
		nm.logf("pause (SIGSTOP) %s", name)
		nm.timeFailover("pause", oldLeader, oldTerm, pausedAt)
		return func() { nm.logf("resume %s", name); nm.c.setFrozen(n, false) }
	case 6:
		return nm.cutOffLeaderZone()
	case 2:
		m := nm.linkMode()
		var links []link
		for o := 0; o < nodeCount; o++ {
			if o != n {
				links = append(links, bothWays(n, o)...)
			}
		}
		oldLeader, oldTerm := nm.leaderBeforeFault(n)
		nm.counts["isolate"]++
		nm.logf("isolate %s (%s)", name, m)
		cutAt := time.Now()
		nm.c.setLinks(links, m)
		nm.timeFailover("isolate", oldLeader, oldTerm, cutAt)
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

// cutOffLeaderZone cuts every link between the leader's zone and the other zones, as if that zone's
// network failed, and times the failover. With one node per zone this isolates the leader, but it is
// worked out from zones, so it would also cut a zone holding several nodes.
func (nm *nemesis) cutOffLeaderZone() func() {
	oldLeader := leader(nm.c)
	if oldLeader < 0 {
		return func() {}
	}
	zone := nm.c.zone(oldLeader)
	var links []link
	for inside := 0; inside < nodeCount; inside++ {
		for outside := 0; outside < nodeCount; outside++ {
			if nm.c.zone(inside) == zone && nm.c.zone(outside) != zone {
				links = append(links, bothWays(inside, outside)...)
			}
		}
	}
	m := nm.linkMode()
	measuredLeader, oldTerm := nm.leaderBeforeFault(oldLeader)
	nm.counts["leader-zone"]++
	nm.logf("cut off zone %s, where the leader %s is (%s)", zone, nm.c.nodeName(oldLeader), m)
	cutAt := time.Now()
	nm.c.setLinks(links, m)
	nm.timeFailover("leader-zone", measuredLeader, oldTerm, cutAt)
	return func() { nm.logf("heal zone %s", zone); nm.c.setLinks(links, netproxy.Pass) }
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
