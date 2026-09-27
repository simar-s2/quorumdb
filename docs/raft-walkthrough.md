# Raft walkthrough

This maps each part of the Raft paper ("In Search of an Understandable Consensus
Algorithm") and Ongaro's thesis to the code in `internal/raft`, and explains the
design choices that go beyond the paper. Function names are stable; line numbers
are not, so they are not used here.

## The pieces

| Concern | Where | Paper / thesis |
|---|---|---|
| Persistent state (term, vote, log) | `storage.go` (`saveMeta`), `wal.go`, `log.go` | Figure 2 |
| Leader election | `campaign`, `HandleRequestVote`, `becomeLeader`, `becomeFollower`, `tick` | §5.2, §5.4.1 |
| Pre-vote | `campaign(pre=true)`, pre-vote branch of `HandleRequestVote` | thesis §9.6 |
| Check-quorum | `checkQuorum` | thesis §6.2 |
| Log replication (leader) | `appendLocal`, `replicate`, `replicateOnce`, `backtrack`, `advanceCommit` | §5.3, §5.4.2 |
| Log replication (follower) | `HandleAppendEntries` | Figure 2 |
| Leader's own disk write in parallel | `runPersister` | thesis §10.2.1 |
| Applying to the state machine | `runApplier`, `resolveFuture` | Figure 2 ("All servers") |
| Snapshots and compaction | `takeSnapshot`, `persistSnapshot`, `memLog.compact`, `wal.pickStart`, `wal.removeBefore` | §7 |
| InstallSnapshot | `sendSnapshot`, `HandleInstallSnapshot`, `restoreSnapshot` | §7, Figure 13 |
| Linearizable reads | `ReadIndex`, `resolveReads`, `WaitApplied` | thesis §6.4 |
| Client writes | `Propose`, `Future.Wait` | §8 |

## Concurrency model

One mutex (`Raft.mu`) guards all Raft state. Long-running goroutines:

- **ticker**: every `HeartbeatInterval/5`, fires elections when the randomized
  deadline passes; on the leader, runs check-quorum.
- **replicate** (one per follower, only while leader): sends AppendEntries or
  InstallSnapshot, one RPC in flight. Entries that arrive while an RPC is in flight
  go out together in the next one, which batches under load.
- **persister** (leader): fsyncs the leader's own WAL. The leader counts itself
  toward a majority only for entries that are on its disk.
- **applier**: applies committed entries to the state machine in order, resolves
  client futures, and triggers snapshots.

Network I/O and fsync never happen while holding `mu`, except for rare events
(term changes and votes, which must be durable before the reply leaves).

## Leader election

1. A follower whose election deadline passes (randomized in `[T, 2T)`, T = 300 ms
   by default) becomes a **pre-candidate** and asks everyone: "would you vote for
   me in term+1?". It does not change its term.
2. A peer grants a pre-vote only if the candidate's log is at least as up to date
   (last term, then last index) **and** the peer has not heard from a leader within
   the election timeout.
3. With a majority of pre-votes it becomes a real candidate: increments the term,
   votes for itself, **persists term and vote**, and requests votes.
4. A voter grants at most one vote per term (`votedFor` is persisted before
   replying) and only to an up-to-date candidate (the election restriction), which
   guarantees every leader holds every committed entry.
5. The winner appends a **no-op entry** in its own term. A leader may not decide
   that entries from earlier terms are committed just by counting replicas
   (Figure 8); committing its own no-op commits everything before it.

Why pre-vote: without it, a node cut off by a partition keeps timing out and
bumping its term. When it comes back, its higher term forces the healthy leader
to step down for nothing. `TestPreVoteNoDisruption` checks that the isolated
node's term stays put.

Why check-quorum: a leader stuck in a minority partition would otherwise keep
accepting client requests that can never commit. With check-quorum it steps down
after one election timeout without hearing from a majority, and clients get an
error quickly.

## Log replication

The leader keeps `nextIndex` (what to send next) and `matchIndex` (what is known
to be replicated) per follower. A follower accepts AppendEntries only if its log
has an entry at `PrevLogIndex` with term `PrevLogTerm` (the consistency check). By
induction this gives the Log Matching property: if two logs have an entry with the
same index and term, they are identical up to that point.

On a mismatch the follower returns a hint: the conflicting term and the first index
of that term (or its log length, if the log is too short). The leader then jumps
back a whole term per round trip instead of one entry at a time (`backtrack`,
exercised by `TestBackup`).

The follower truncates only at the first **conflicting** entry. It never truncates
when the entries it received are already present: a delayed, shorter
AppendEntries must not erase entries it already acknowledged
(`TestFollowerAppendRules`).

**Commit rule** (`advanceCommit`): the highest index stored durably on a majority,
provided the entry at that index is from the current term. With three nodes the
leader commits as soon as any two of {leader, follower 1, follower 2} have the
entry on disk. The leader's own fsync runs in parallel with replication, so a slow
local disk does not block commits.

## Durability

A node acknowledges nothing before it is on disk:

- **Term and vote** go to `meta` (write temp file, fsync, rename, fsync directory)
  before a vote reply or any reply that carries a newer term.
- **Log entries** go to the WAL. A follower replies success to AppendEntries only
  after `syncTo` has fsynced the records. `syncTo` is a group commit: whoever holds
  the sync lock flushes everything buffered so far, so one fsync acknowledges many
  requests.

### WAL format and replay (`wal.go`)

Segment files hold CRC-protected records. Truncation is implicit: when a follower
replaces a conflicting suffix, the new entries are appended with lower indexes than
the last record, and replay applies "an entry at index i replaces everything from i
on". Replaying records in order rebuilds exactly the in-memory log, so there is no
separate truncate operation that could be lost or misordered. A torn record at the
end of the newest segment (a crash mid-write) is cut off; corruption anywhere else
is an error. Every restart continues in a new segment, so only the newest segment
can ever hold a torn write.

## Snapshots and log compaction

When `lastApplied - snapshotIndex` passes a threshold (default 10,000 entries):

1. On the applier goroutine, the state machine captures its state. The KV store
   shallow-copies its map, which is a consistent point-in-time view because stored
   values are never mutated in place.
2. In the background the copy is serialized and written as `snapshot` (atomic
   rename). The file records `walStart`, the first WAL segment still needed.
3. The in-memory log drops entries up to the snapshot index, segments before
   `walStart` are deleted, and the WAL rotates to a new segment.

Which segment is safe to replay from? Every segment header records the log's last
index when that segment was created. A segment whose start index is at or below the
snapshot index is enough, because everything after the snapshot was written after
that segment began (`pickStart`, `TestWALCompactionWithSnapshot`).

If a follower needs entries the leader has already compacted, the leader sends its
snapshot (`sendSnapshot`). The follower (`HandleInstallSnapshot`) keeps its log
suffix if it contains the snapshot's last entry. Otherwise it discards the whole log
and starts a fresh WAL segment (`wal.reset`), so replay after a crash cannot
resurrect the discarded entries. The applier then restores the state machine from
the snapshot (`TestInstallSnapshot`).

## Linearizable reads: ReadIndex

A leader cannot simply read its local state: it might have been deposed without
knowing it (for example, paused by GC), and a new leader might already have
committed newer writes. `ReadIndex`:

1. Records `readIndex = max(commitIndex, noopIndex)`, which covers every write
   acknowledged before the read arrived.
2. Confirms it is still leader: a majority must acknowledge a heartbeat that was
   sent **after** the read arrived. Each request carries a heartbeat sequence
   number; `resolveReads` releases reads once a majority has acknowledged a
   sequence at or above theirs. Concurrent reads share heartbeat rounds.
3. The server waits until `lastApplied >= readIndex`, then reads the local state
   machine.

No clock assumptions are needed (unlike leader leases). `TestReadIndexStaleLeader`
checks that an isolated leader cannot confirm a read, and the chaos test pauses
leaders with SIGSTOP to hit exactly this case.

## Client requests (`internal/server`)

- **Writes**: `Propose` appends to the leader's log and returns a future. The
  future resolves when the applier applies that index. If a different leader's
  entry ends up at that index, the future resolves with `ErrLeadershipLost`, and
  the client gets an error that says the outcome is unknown.
- **Followers forward** to the leader over the peer transport. A forwarded request
  is retried only when it provably did not execute (the dial failed, or the target
  said "not leader" before proposing anything), so a retry can never apply a write
  twice.
- **Pipelining**: commands already buffered on a connection are executed as a
  batch. Consecutive writes are proposed together; consecutive reads share one
  ReadIndex round; a read waits for the writes before it, so per-connection order
  is preserved (`TestPipelineOrder`).
- **Expiry is deterministic**: the leader turns relative TTLs into absolute
  timestamps and stamps each command with its clock reading, so every replica
  applying the same log reaches the same state. Reads hide expired keys; a periodic
  sweep entry reclaims their memory on all replicas.

## Questions worth being ready for

- *What if the leader crashes after committing but before replying?* The entry is
  on a majority. The next leader has it (election restriction) and commits it via
  its no-op. The client saw an error or timeout, so it must treat the outcome as
  unknown. The chaos checker models exactly this.
- *Can two leaders exist at once?* Yes, in different terms: an old leader that
  hasn't noticed it was deposed. It can't commit (no majority will accept its term),
  and ReadIndex stops it from serving stale reads.
- *Why fsync on followers before acknowledging?* Commitment means "on disk on a
  majority". If followers acknowledged from memory, a simultaneous crash of the
  leader and one follower could lose an acknowledged write.
- *Why does a candidate persist its vote before sending RequestVote?* Otherwise a
  crash and restart could let it vote for someone else in the same term, allowing
  two leaders in one term.
- *Why not pipeline AppendEntries?* It was tried. With fsync-bound followers it
  increased the number of small fsyncs and made latency worse on this setup. See
  `docs/benchmarks.md`.
