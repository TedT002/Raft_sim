# raftsim

[![CI](https://github.com/TedT002/Raft_sim/actions/workflows/ci.yml/badge.svg)](https://github.com/TedT002/Raft_sim/actions/workflows/ci.yml)

A Raft consensus implementation being built together with a deterministic simulation test harness,
designed so that any run can be replayed exactly from a single seed (given the same configuration,
pinned toolchain and `Cargo.lock`).

## Status

Work in progress — Phase 4: clients and linearizability. A leader replicates client commands with
consistency-checked `AppendEntries`, starts every term with a no-op entry and commits only entries
of its own term by counting replicas (§5.4.2, the Figure 8 trap); followers answer clients with
`NotLeader` and a hint. Every request carries a client session `(client, seq)`, so a retried request
is applied at most once (§8). Simulated clients retry on timeouts, follow leader hints and lose some
replies on purpose, and every run's client history is checked for linearizability. All five safety
properties of Figure 3 (Election Safety, Leader Append-Only, Log Matching, Leader Completeness,
State Machine Safety) are checked after every simulated event across hundreds of seeds with message
loss, duplication, partitions, crashes and a disk that loses unsynced writes. The chaos CLI
(`fuzz`/`replay`), seed shrinking and mutation testing are next.

## Why

Consensus bugs hide in timing and fault interleavings that are hard to reproduce with real clocks,
threads and sockets. `raftsim` simulates the network, the clock, the disk, crashes and randomness
instead, so that a single `u64` seed deterministically replays an entire run, including injected
message loss, delay, reordering, duplication, partitions, node crashes and lost disk writes. The
approach follows the deterministic simulation testing style popularized by FoundationDB and
TigerBeetle.

## Architecture

The Raft state machine is sans-IO: it has one entry point and returns a list of effects for the host
to execute, instead of performing any I/O itself.

```rust
pub fn step(&mut self, input: Input) -> Vec<Output>
```

Output order is semantic: the host must execute a step's outputs in order, and an `Output::Persist`
must be durable (written and fsynced) before any later output of the same step is executed. This is
how the core honors Raft's rule that `currentTerm`, `votedFor` and `log[]` are updated on stable
storage before responding to RPCs (Figure 2 of the paper). A `Persist` carries only the change (the
term and vote, plus the log suffix to replace), the way a real node appends to a log file; after a
crash the host hands the full persistent state back to the core.

The simulator is protocol-agnostic: it drives any node with the same sans-IO shape
(`SimNode::step(NodeInput) -> Vec<NodeOutput>`), and Raft is plugged in through an adapter
(`RaftCluster`) that checks invariants after every event. Events are ordered by `(time, seq)`, so
simultaneous events run in insertion order; every component draws from its own ChaCha8 stream
derived from one master seed; and every processed event is folded into an FNV-1a trace hash, so two
runs with the same seed produce byte-identical traces. Partitions follow a "cut cable" model: a
message is delivered only if its endpoints stayed connected for its whole flight, so messages sent
during a partition never arrive, and messages in flight across a cut are lost even if the partition
heals before they are due. A crashed node keeps only its disk: it gets no ticks, messages arriving
while it is down are dropped, and a restart rebuilds it from what it persisted. The simulated disk
keeps every write pending until its fsync completes a few ticks later, and the simulator holds back
the messages and state machine applications that follow a write until the write is durable. A crash
loses pending writes (or, at random, lets a prefix of them land), just as on real hardware. A
message emitted before the write it depends on cannot be held back, and only an unlucky crash would
expose it, so the Raft adapter flags such an output order in the step that produces it.

Invariants are checked after every simulated event, not only at the end of a run, so a transient
violation cannot hide. All five safety properties of Figure 3 are checked by independent oracles
that never see `raft-core`'s types: Election Safety, Leader Append-Only, Log Matching, Leader
Completeness and State Machine Safety. The log oracles (Log Matching and Leader Completeness) judge
what is durable rather than what is in memory: a change that a crash wipes out before its fsync
never left the node, so recording it would describe a history that never happened (a single-node
cluster, for instance, elects itself and advances its commit index before either reaches its disk).
Leadership and what a leader does to its own log are judged in memory, at the moment a write is
issued, so a leader that corrupts its log and steps down before the write is durable is still
caught; a crash makes the Append-Only oracle forget the log snapshots of a node's leaderships
(Election Safety keeps its record), because only a node whose term never became durable can win the
same term again. Cheaper checks also look at memory when a write is issued: every live node's memory
must match what it wrote, so a state change that was not persisted is caught at once instead of
waiting for an unlucky crash; a step must persist before it sends or applies anything; and no node
may rewrite its log at or below the commit index the checker has seen for it.

Clients reach nodes directly (not through the simulated network), but a seeded fraction of replies
is lost, so a committed request often goes unanswered and is retried under the same `(client, seq)`.
The key-value state machine keeps the last sequence number and result of every client session and
answers a duplicate from the session instead of applying it again (§8). Operations are `Put`, `Get`,
`Delete` and `Append`; reads go through the log. `Append` is not idempotent, so a request applied
twice is visible to a later read, which is exactly what the linearizability checker looks for. The
history records the first call and the first reply of every operation; an operation whose client
gave up is indeterminate (it may or may not have taken effect), and one whose attempts were all
rejected is known to have failed and is left out. The checker follows Wing & Gong's search with
Lowe's just-in-time linearization and memoization, splits the history per key (P-compositionality)
and skips indeterminate writes whose values no read ever saw, which cannot change the verdict.

Crates and their dependency direction (`raft-core` depends on no workspace crate):

| Crate | Responsibility |
|---|---|
| `raft-core` | Pure, sans-IO Raft state machine (Figure 2): leader election, log replication, `NotLeader` replies and the leader's no-op entry (§8) |
| `sim` | Deterministic simulator: virtual clock, event queue, seeded network and disk, crash/restart, trace hash; Raft adapter with a session-aware key-value state machine, checking invariants after every event; simulated clients that record their history |
| `checker` | The five Raft safety properties of Figure 3 and a linearizability checker for key-value histories, independent of `raft-core`'s types |
| `cli` | `raftsim` binary: planned `fuzz` and `replay --seed N` subcommands |

```
cli -> sim
cli -> checker
sim -> raft-core
sim -> checker
```

## Roadmap

- [x] Phase 0 — Skeleton: workspace, crate boundaries, `raft-core` API skeleton, CI gates
- [x] Phase 1 — Simulator: virtual clock, event queue, simulated network (loss, duplication, delay,
      partitions), trace hashing
- [x] Phase 2 — Leader election: roles, terms, randomized timeouts, persisting term/vote,
      crash/restart
- [x] Phase 3 — Log replication: `AppendEntries`, commit rule, state machine application, simulated
      disk (writes pending until fsync)
- [x] Phase 4 — Client interface: request dedup, `NotLeader` responses, leader no-op,
      linearizability checking
- [ ] Phase 5 — Chaos and proof: `raftsim fuzz`/`replay --seed N`, seed shrinking, mutation testing
- [ ] Phase 6 (optional) — Snapshots, cluster membership changes, a real network runner

## Development

The toolchain version, components and profile are pinned in `rust-toolchain.toml`; on first use, run
`rustup toolchain install` (no arguments; rustup 1.28 or newer) to install exactly what it
specifies. The helper scripts need bash 4.4+, `jq` and GNU `find`/`sort` (all preinstalled on
GitHub's Ubuntu runners; CI also runs `shellcheck` on them). All gates below must pass before a
phase is considered done:

```
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo test --workspace -- --test-threads=1
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps
./scripts/check_forbidden.sh
./scripts/check_forbidden.sh crates/sim
./scripts/test_check_forbidden.sh
./scripts/check_deps.sh
./scripts/check_line_length.sh
```

## License

Licensed under either of Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE)) or MIT
license ([LICENSE-MIT](LICENSE-MIT)) at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in the
work by you, as defined in the Apache-2.0 license, shall be dual licensed as above, without any
additional terms or conditions.
