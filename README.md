# raft-replication-log

**Component 9 of 11 (Distribution & High Availability, 1 of 2)** in the [`strata-database-engine`](https://github.com/juanmmm21/strata-database-engine) ecosystem.
Repo: [`github.com/juanmmm21/raft-replication-log`](https://github.com/juanmmm21/raft-replication-log)

A from-scratch implementation of the Raft consensus algorithm, written in Python: randomized-timeout leader election with monotonically increasing terms, log replication committed only after majority quorum, network-partition simulation with split-brain prevention, and snapshotting to bound log growth.

---

## What it is and what problem it solves

A single database node is a single point of failure: if the disk fails, or the process crashes mid-write, the data it held is gone. The standard fix is replication — keep the same log on several independent nodes — but replication introduces its own hard problem: if two nodes ever both believe they are the leader in the same term, they can each accept conflicting writes, and the cluster silently diverges. This is split-brain, and it is the single most dangerous failure mode in distributed storage.

Raft (Ongaro & Ousterhout, *"In Search of an Understandable Consensus Algorithm"*, 2014) solves this with a small set of formally verified safety rules: at most one leader per term, entries only committed once a strict majority of nodes durably hold them, and a candidate can only win an election if its log is at least as up-to-date as a majority of the cluster. This project implements that algorithm directly — leader election, log replication, and snapshotting — over a simulated in-memory cluster, with a property-based test suite that fuzzes ticks, client proposals, network partitions, and node crashes together across thousands of steps and asserts the paper's safety invariants hold at every single step, not just at the end.

## Role in `strata-database-engine`

```text
write-ahead-log-recovery       (produces the logical log format this module
        │                       conceptually replicates -- no direct import;
        │ (no direct import)    see "Data format" below)
        ▼
┌───────────────────────────┐
│    raft-replication-log    │  (this repo)
│  leader election · terms    │
│  log replication · quorum    │
│  partition safety · snapshot  │
└──────────────┬────────────────┘
               │ replicated log + "current leader per shard"
               ▼
     distributed-query-router
               │
               ▼
            nanosql
```

This repo does **not** import `write-ahead-log-recovery`, `bplus-tree-storage-engine`, `lsm-tree-engine`, or any other sibling project — the ecosystem rule is that cross-repo integration only happens inside `nanosql`. It consumes nothing concrete from any other repo; it produces two things `distributed-query-router` will eventually consume: a replicated, consistent log, and the identity of the current leader per shard (`Cluster.current_leader()`). See [Data format / interface exposed to the ecosystem](#data-format--interface-exposed-to-the-ecosystem) for the exact contract.

## Goal / skills demonstrated

- A complete implementation of Raft's leader election (§5.2), log replication (§5.3), and the safety rule that a leader may only *directly* commit entries from its own term (§5.4.2) — including the no-op-on-election technique the paper prescribes to avoid a leader getting permanently stuck unable to commit inherited entries.
- Log compaction via snapshotting (§7), with the `InstallSnapshot` RPC used to catch up a follower that has fallen behind the leader's compacted log boundary.
- A simulated 3–5 node in-memory cluster with an injectable `Transport` protocol, deliberate network-partition and node-crash simulation, and a quorum rule that provably prevents split-brain — verified directly, not assumed.
- Deterministic, seeded randomness throughout: every election timeout comes from an injected `random.Random`, never the global `random` module, so every test scenario is exactly reproducible.
- A property-based test suite that fuzzes a random sequence of ticks, client proposals, partitions, crashes, and recoveries against a real cluster and checks Raft's formal safety invariants (election safety, log matching, state machine safety) after every single step — not a hand-picked set of scenarios.
- A real liveness bug found and fixed by that same property suite before publishing (see [Troubleshooting](#troubleshooting) and the commit history): without the no-op-on-election entry, a newly elected leader with no incoming client traffic could stay permanently unable to commit entries inherited from a previous term.

## How it works

### Leader election (§5.2)

Every node starts as a `FOLLOWER`. Time is modeled as discrete logical ticks (`RaftNode.tick()`), not wall-clock time — the same technique used by production Raft libraries (e.g. etcd's `raft` package) to make consensus fully deterministic and testable without `time.sleep`. Each follower has an election timeout drawn from an injected `random.Random` within a configurable range (`election_timeout_ticks`, default `(10, 20)`); if that many ticks pass with no valid heartbeat or vote request from a current leader, it becomes a `CANDIDATE`, increments its term, votes for itself, and requests votes from every peer. A peer grants its vote only if it has not already voted in that term for someone else, and the candidate's log is at least as up-to-date as its own (last-term first, then last-index — the "leader completeness" check that guarantees a candidate that wins an election necessarily has every previously committed entry). A candidate that collects votes from a strict majority — including itself — becomes `LEADER`. The randomized timeout range is what keeps split votes rare and self-resolving: two nodes starting an election in the exact same tick is unlikely, and if it happens anyway, the next round's independently redrawn timeouts resolve it.

### Log replication and quorum commit (§5.3)

The leader appends a proposed command to its own log immediately, then sends `AppendEntries` to every follower carrying (`prev_log_index`, `prev_log_term`) — a consistency check the follower must pass before accepting new entries, walking backward through a fast-backtrack optimization (`conflict_index`/`conflict_term` in the response) instead of decrementing one index at a time on every mismatch. Once a majority of nodes (leader included) report `match_index >= N` for some index `N`, and — critically — the entry at `N` belongs to the leader's *own current term*, the leader advances `commit_index` to `N` and applies every newly committed entry to its state machine. That "own current term" qualifier is §5.4.2's safety rule: without it, a leader could commit an entry from an older term purely because a majority happens to still hold it, even though that entry might be silently overwritten by a future leader with a different (but also majority-backed) history — replicating it again under the *current* term is what makes the commit durable against every possible future leader.

### The no-op-on-election technique

A direct consequence of that same rule: a brand-new leader that inherits an *uncommitted* entry from a previous term cannot commit it by itself, no matter how many followers already have it — it needs at least one entry of its own term to reach quorum first, which then indirectly carries every earlier entry forward with it. If no client ever proposes anything new, that leader is stuck forever. Raft's prescribed fix (§5.4.2, end) is for a leader to commit a blank no-op entry into its own log the moment it is elected — `RaftNode.become_leader()` does exactly this before sending its first `AppendEntries` round. The no-op is stored and replicated like any other log entry (so it still occupies a real log index) but is filtered out before being handed to the state machine, so `StateMachine.apply()` never sees it.

### Network partitions and split-brain prevention

`Cluster.partition(*groups)` splits the simulated cluster into mutually unreachable groups; any RPC between two nodes in different groups is silently dropped, exactly as a real node would observe an unreachable peer — a timeout, not an error. No special-cased partition-detection logic exists anywhere in `election.py`/`replication.py`: split-brain prevention falls directly out of the quorum arithmetic in `RaftNode.quorum_size()`, which is always computed against the cluster's *total* configured membership, never the size of whatever group happens to be reachable right now. A minority partition — even one that still contains the previous leader, who keeps believing it is leader in its own old term — can never gather a majority of votes or of `AppendEntries` acknowledgements, so it can neither elect a new leader nor commit new entries. The majority partition, meanwhile, safely elects a new leader in a higher term and keeps making progress. This is verified directly (`tests/test_cluster.py::test_isolated_minority_cannot_commit_entries_on_its_own`), not just asserted in a docstring.

### Snapshotting (§7)

`snapshot.create_snapshot()` asks the pluggable `StateMachine` for an opaque serialized state (`take_snapshot()`), records `(last_included_index, last_included_term)`, and discards every log entry at or below that index — only entries already applied to the state machine may ever be compacted away, since compacting an uncommitted entry would destroy information no snapshot has captured yet. When a leader's `next_index` for a lagging follower falls at or below its own snapshot boundary, it sends `InstallSnapshot` instead of `AppendEntries`; the follower restores its state machine from the snapshot's opaque payload and either keeps or discards its own log suffix, depending on whether it already happened to have the exact same entry at the snapshot boundary. `RaftNode(..., snapshot_threshold=N)` triggers this automatically once `N` entries have been applied since the last snapshot — used by both the `snapshot-demo` CLI command and the automatic-compaction test.

### The simulated cluster: no threads, no sockets, one process

`Cluster` is the single implementation of the `Transport` protocol nodes are injected with. Every "RPC" a node sends is, in reality, a direct synchronous function call — `node.transport.send_append_entries(...)` immediately calls the target `RaftNode`'s handler and returns its response (or `None`, standing in for a dropped/timed-out message). This is a deliberate scope decision, spelled out in `cluster.py`'s module docstring: this repo models *the consensus protocol itself*, not a real network stack, so there is nothing to gain from sockets or `asyncio` — and, unlike `lock-manager-deadlock-detector` (which genuinely needs OS threads to model concurrent transactions racing each other), there is no real parallelism here to model in the first place. The whole simulation runs in one thread with one deterministic order of execution, which is exactly what makes the property-based fuzz testing possible: the same seed always reproduces the same sequence of elections, partitions, and crashes. The simulated cluster does not persist anything to disk: its node crash/recovery (`Cluster.crash_node`/`recover_node`) is modeled as temporary unreachability rather than process restart. Persistence and real sockets live in the separate real-deployment modules described next, so the simulation stays deterministic.

### Real deployment: TCP transport and on-disk log

The simulation above is unchanged and still drives the property-based
tests. Next to it, three modules run the same protocol core as a real
cluster of 3–5 processes with a static membership:

* **`network.py` — `NetworkNode`.** One listening TCP socket per node,
  one outbound link per peer, a ticker thread (10 ms ticks, elections
  after 150–300 ms, heartbeats every 50 ms by default). `TcpTransport`
  implements `Transport` *asynchronously*: `send_*` only enqueues and
  returns `None` — exactly what the core already treats as "no response
  (yet)" — and responses, when they arrive over the network, go through
  the very same `handle_*_response` functions the simulation uses, which
  already discard stale replies by term. One `RLock` per node serialises
  every state mutation (tick, inbound RPC, response, client proposal) and
  **no network I/O ever happens while holding it** (only enqueueing), so
  two nodes messaging each other cannot deadlock. Lost connections drop
  their queued messages and reconnect with backoff; Raft tolerates the
  loss.
* **`storage.py` — `DiskStorage`.** Persists `currentTerm`/`votedFor`
  (atomic `meta.json`: temp file, fsync, rename, fsync of the directory),
  the log (`log.bin`, append-only records `[len][crc32][index][term][command]`)
  and the snapshot (`snapshot.bin`, atomic, with CRC; installing or taking
  one rewrites the log atomically with the entries that follow it). The
  protocol core still mutates its state in memory and knows nothing about
  disks: `sync(node)` diffs that state against what was last persisted and
  writes only the difference (truncated suffix + appended entries, new
  term/vote, new snapshot), with `fsync`. On load, a torn last record (a
  crash in the middle of an append) is detected by length/CRC and cut off
  — that entry was never acknowledged to anyone, because acknowledgement
  happens after the fsync. A corrupt snapshot is an error, never silence.
* **The persist-then-publish rule.** After every operation, still under
  the node lock, `NetworkNode` calls `storage.sync(node)` and only then
  hands the produced messages to the network and answers the inbound RPC:
  a vote, an appended entry or a new term reaches stable storage before
  any other node can observe it (§5.1).
* **`codec.py`.** Length-prefixed JSON frames (opaque bytes in base64) for
  the six RPCs; malformed frames raise `CodecError` at the boundary.

Restarting a node with the same data directory restores term, vote, log
and snapshot (`storage.restore_node`); it then rejoins and catches up like
any lagging follower. `tests/test_network.py` exercises this with real
sockets: election and replication on 3 nodes, follower rejection with a
leader hint, leader crash and fail-over, restart from disk and catch-up,
whole-cluster restart keeping the committed log, and a 5-node cluster
surviving two failures. `nanosql` builds its multi-process SQL cluster and
its chaos/linearizability harness on top of `NetworkNode`.

## Architecture

```text
src/raft_replication_log/
├── __init__.py
├── models.py          # NodeId/Term/LogIndex/Command aliases, RaftRole, LogEntry,
│                       # Snapshot, the six RPC request/response dataclasses
├── errors.py           # RaftError hierarchy (NotLeaderError, SnapshotOutOfRangeError, ...)
├── protocols.py         # Transport (pluggable RPC channel), StateMachine (pluggable apply/snapshot)
├── state_machine.py      # InMemoryStateMachine: default StateMachine, length-prefixed snapshot format
├── node.py                # RaftNode: all persistent/volatile state, log navigation with
│                          # snapshot-boundary awareness, role transitions, tick()
├── election.py              # start_election, handle_request_vote(_response)
├── replication.py             # propose, replicate_to_peer, handle_append_entries(_response),
│                              # the §5.4.2 quorum-commit rule
├── snapshot.py                  # create_snapshot, handle_install_snapshot(_response)
├── cluster.py                    # Cluster: the simulated Transport, partition/crash injection
├── codec.py                       # length-prefixed JSON frames for the six RPCs
├── storage.py                     # DiskStorage: term/vote, append-only log, snapshot, torn-tail recovery
├── network.py                     # NetworkNode + async TcpTransport: real 3-5 node clusters over TCP
├── pipeline.py                     # build_cluster (seeded), elect_leader, replicate_command
└── __main__.py                      # demo / partition-demo / snapshot-demo / benchmark CLI
```

Per-domain logic (`election.py`, `replication.py`, `snapshot.py`) is factored as free functions that take a `RaftNode` as their first argument and mutate its state directly — the same separation `vectorized-execution-engine` uses for `joins.py`/`aggregate.py`/`sort.py` split out of `operators.py`. `RaftNode` itself owns all state and exposes the handful of methods those modules call into (`step_down`, `become_leader`, `conflict_hint`, `apply_committed`, ...); they are not prefixed with an underscore even though they are not part of the class's public API, because Python has no notion of "package-private" and forcing that convention here would only get in the way of the sibling modules that call them constantly. Only `RaftNode._offset_of` is genuinely private to `node.py` itself.

## Requirements and installation

- Python `>=3.11`, no runtime dependencies.

```bash
git clone https://github.com/juanmmm21/raft-replication-log.git
cd raft-replication-log
python3 -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"  # or: pip install -e . pytest mypy ruff
```

## Usage

### CLI

```bash
# leader election + 3 client commands replicated to commit across a 5-node cluster
python -m raft_replication_log demo

# partitions the cluster, proves the isolated minority (with the old leader) commits
# nothing on its own, then heals the partition and shows the cluster reunify
python -m raft_replication_log partition-demo

# replicates enough entries to trigger automatic snapshotting and shows the truncated log
python -m raft_replication_log snapshot-demo

# ticks-to-election and replication throughput across cluster sizes
python -m raft_replication_log benchmark --sizes 3 5 --trials 20 --commands 200
```

### Programmatic usage

```python
from raft_replication_log import pipeline

# seeded RNG end to end: every election timeout in the cluster traces back to this seed
cluster = pipeline.build_cluster([f"node-{i}" for i in range(5)], seed=1)

leader_id = pipeline.elect_leader(cluster)
index = pipeline.replicate_command(cluster, b"SET x=1")  # blocks (in simulated ticks) until quorum commits

leader = cluster.nodes[leader_id]
assert leader.commit_index >= index
assert leader.state_machine.applied_commands == (b"SET x=1",)  # type: ignore[attr-defined]

# simulate a network partition and confirm the isolated minority cannot commit alone
cluster.partition({leader_id}, set(cluster.nodes) - {leader_id})
cluster.tick(50)
cluster.heal_partition()
```

Lower-level access (used by the test suite) constructs `RaftNode`s directly with an injected `Transport` and `random.Random`, useful for unit-testing a single node's election/replication logic in isolation without a full cluster.

## Data format / interface exposed to the ecosystem

This module never imports `write-ahead-log-recovery` — the cross-repo integration rule for this ecosystem is that it only happens inside `nanosql`. Instead, `models.LogEntry` redefines, locally and independently, the minimal shape Raft itself needs for any entry it replicates:

```python
@dataclass(frozen=True, slots=True)
class LogEntry:
    index: int      # absolute Raft log index, 1-indexed, monotonic, never reused
    term: int        # the term of the leader that originally proposed this entry
    command: bytes     # opaque payload -- Raft never inspects its contents
```

`command` is intentionally opaque: in a real assembly inside `nanosql`, it would hold a serialized `write_ahead_log_recovery.models.LogRecord` (or an equivalent higher-level operation description) — this repo's job ends at replicating and committing that payload by majority, not at interpreting it. The two things `distributed-query-router` will eventually consume from this module are:

- **The replicated log itself**, via the injectable `StateMachine` protocol (`apply(command: bytes) -> None`) — a real assembly supplies an implementation that applies each committed command to actual table storage instead of `InMemoryStateMachine`'s "record it in a list" default.
- **The current leader per shard**, via `Cluster.current_leader() -> NodeId | None` — `distributed-query-router` is expected to run one `Cluster` per shard and query this to route writes to the right node.

## Development

```bash
pytest
ruff check .
ruff format --check .
mypy --strict src/
```

The test suite (77 tests) covers — besides the 15 real-deployment tests (`test_codec.py`, `test_storage.py`, `test_network.py`) described above — leader election in isolation (randomized-timeout bounds and determinism, vote-granting rules including the log-up-to-date and vote-once-per-term checks, term monotonicity across repeated elections) — 11 tests; log replication (the `AppendEntries` consistency check and fast-backtrack conflict hints, safe truncation that never discards a duplicate resend, and the §5.4.2 direct-vs-indirect commit rule tested explicitly by hand-building a leader with an inherited uncommitted entry) — 11 tests; snapshotting (compaction boundaries, `InstallSnapshot` restoring state and either keeping or discarding a follower's log suffix depending on whether it matches, automatic threshold-triggered compaction, and a lagging node catching up entirely via `InstallSnapshot` after recovering from a crash) — 12 tests; full cluster scenarios matching every item in this project's Definition of Done — election in 3- and 5-node clusters, normal replication reaching every node, recovery after a follower crash and after a leader crash (with leader completeness verified explicitly), a network partition never producing two leaders in the same term, and the isolated minority (even holding the old leader) proven unable to commit anything on its own — 13 tests; and a property-based suite (15 seeds × 500 randomized steps each, mixing ticks, client proposals, partitions, heals, crashes and recoveries) asserting Raft's formal safety invariants — election safety, log matching, state machine safety — after *every single step*, not just at the end, plus a final convergence check once the cluster is healed and left to run — 15 tests.

Two real bugs were found by this test suite before publishing, not hidden:

1. **`fix(cluster)`** — `Cluster.current_leader()` did not exclude crashed nodes. A crashed node's `role` attribute never changes on its own (it stops receiving ticks and RPCs, so nothing ever tells it to step down), so without this exclusion a killed leader kept being reported as "the current leader" forever, and `pipeline.elect_leader()` could never detect that a real new leader had already been elected elsewhere. Caught by `test_recovery_after_leader_crash_elects_new_leader_and_keeps_committed_entries`.
2. **`fix(node)`** — no no-op entry was committed on election (see [How it works](#the-no-op-on-election-technique)). Without it, a newly elected leader with an inherited uncommitted entry from an older term, and no incoming client traffic, would stay stuck forever unable to commit it. This was not caught by any hand-written scenario test — it only surfaced when the property-suite's seed count was raised from 10 to 30 and one seed's post-heal convergence check failed after 1,000 ticks that should have been more than enough.

## Benchmarks

Measured with the `benchmark` subcommand on the reference development machine:

**Ticks to leader election** (average over 30 independently seeded clusters per size, `election_timeout_ticks=(10, 20)`, `heartbeat_interval_ticks=3`):

| Cluster size | Avg. ticks to stable leader |
|---|---|
| 3 nodes | 12.0 |
| 5 nodes | 11.6 |

**Replication throughput** (500 client commands proposed and committed sequentially, single cluster per size):

| Cluster size | Commands committed | Elapsed | Throughput |
|---|---|---|---|
| 3 nodes | 500 | 0.004s | ~122,300 commands/s |
| 5 nodes | 500 | 0.007s | ~72,400 commands/s |

Both numbers are dominated by simulation overhead (Python function-call cost per simulated RPC), not by anything resembling real network latency — this is expected and by design: the point of this benchmark is to characterize the algorithm's *behavior* (how many logical ticks an election typically needs, how replication cost scales with cluster size as every additional node adds one more `AppendEntries` round trip per replicated command) rather than to produce a number comparable to a real deployed Raft cluster, whose latency would be dominated by actual network round trips instead. The near-2x throughput gap between 3 and 5 nodes tracks the expected cost model directly: each `propose()` call fans out one `AppendEntries` to every peer synchronously before returning, so a 5-node cluster does noticeably more per-command work than a 3-node one for the same quorum guarantee.

## Troubleshooting

- **`NotLeaderError` when calling `RaftNode.propose()`:** only the current leader accepts new commands. Use `Cluster.current_leader()` (or `pipeline.replicate_command`, which resolves the leader automatically and retries against a new one if leadership changes mid-flight) instead of proposing against an arbitrary node.
- **`Cluster(...)` raises `ValueError` about cluster size:** this module intentionally only supports 3–5 node clusters (`cluster.MIN_CLUSTER_SIZE`/`MAX_CLUSTER_SIZE`), matching this project's Definition of Done — outside that range, construct `RaftNode`s directly instead of going through `Cluster`.
- **A cluster seems to make no progress after a partition is healed:** call `cluster.tick(...)` a few more times — healing a partition does not itself force an immediate re-election or heartbeat; nodes only notice the network is sane again on their next scheduled tick, exactly as timeouts would in a real deployment.
- **A follower's `applied_commands` lags behind the leader's right after `pipeline.replicate_command` returns:** this is expected, not a bug — that function only guarantees the *leader* has committed by majority (the real definition of "committed" in Raft), not that every follower's own `commit_index` has caught up yet, since that only happens on the *next* `AppendEntries` a follower receives. Tick the cluster a little further (`cluster.tick(heartbeat_interval_ticks + 1)` is enough) before asserting cluster-wide convergence, exactly as the `demo` CLI subcommand does.
- **`SnapshotOutOfRangeError` from `create_snapshot`:** only entries already applied to the state machine (`index <= last_applied`) can be compacted — this guards against destroying an entry no snapshot has captured a record of yet, in case it later needs to be discarded and replaced during normal replication.

## License

MIT — see [`LICENSE`](./LICENSE).
