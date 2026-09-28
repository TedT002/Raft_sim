# raftsim

[![CI](https://github.com/TedT002/Raft_sim/actions/workflows/ci.yml/badge.svg)](https://github.com/TedT002/Raft_sim/actions/workflows/ci.yml)

A Raft consensus implementation being built together with a deterministic simulation test harness,
designed so that any run can be replayed exactly from a single seed (given the same configuration,
pinned toolchain and `Cargo.lock`).

## Status

Work in progress — Phase 1: the deterministic simulator runs; there is no Raft logic yet. The simulator
currently drives a toy ping/pong protocol that lives only in its tests, and `RaftNode::step` is still an
intentional no-op.

## Why

Consensus bugs hide in timing and fault interleavings that are hard to reproduce with real clocks,
threads and sockets. `raftsim` simulates the network, the clock and randomness instead (a simulated disk
and crashes come next), so that a single `u64` seed deterministically replays an entire run, including
injected message loss, delay, reordering, duplication and partitions. The approach follows the
deterministic simulation testing style popularized by FoundationDB and TigerBeetle.

## Architecture

The Raft state machine is sans-IO: it has one entry point and returns a list of effects for the host to
execute, instead of performing any I/O itself.

```rust
pub fn step(&mut self, input: Input) -> Vec<Output>
```

Output order is semantic: the host must execute a step's outputs in order, and an `Output::Persist`
must be durable (written and fsynced) before any later output of the same step is executed. This is
how the core honors Raft's rule that `currentTerm`, `votedFor` and `log[]` are updated on stable
storage before responding to RPCs (Figure 2 of the paper).

The simulator is protocol-agnostic: it drives any node with the same sans-IO shape
(`SimNode::step(NodeInput) -> Vec<NodeOutput>`), and Raft will be plugged in through an adapter. Events
are ordered by `(time, seq)`, so simultaneous events run in insertion order; every component draws
from its own ChaCha8 stream derived from one master seed; and every processed event is folded into an
FNV-1a trace hash, so two runs with the same seed produce byte-identical traces. Partitions follow a
"cut cable" model: a message is delivered only if its endpoints stayed connected for its whole
flight, so messages sent during a partition never arrive, and messages in flight across a cut are
lost even if the partition heals before they are due.

Crates and their dependency direction (`raft-core` depends on no workspace crate):

| Crate | Responsibility |
|---|---|
| `raft-core` | Pure, sans-IO Raft state machine (Figure 2); currently a type skeleton |
| `sim` | Deterministic simulator: virtual clock, event queue, seeded network, trace hash; disk and crashes planned |
| `checker` | Planned: Raft safety invariants and linearizability checker, independent of `raft-core`'s types |
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
- [ ] Phase 2 — Leader election: roles, terms, randomized timeouts, persisting term/vote, crash/restart
- [ ] Phase 3 — Log replication: `AppendEntries`, commit rule, state machine application, simulated
      disk (writes pending until fsync)
- [ ] Phase 4 — Client interface: request dedup, `NotLeader` responses, linearizability checking
- [ ] Phase 5 — Chaos and proof: `raftsim fuzz`/`replay --seed N`, seed shrinking, mutation testing
- [ ] Phase 6 (optional) — Snapshots, cluster membership changes, a real network runner

## Development

The toolchain version, components and profile are pinned in `rust-toolchain.toml`; on first use, run
`rustup toolchain install` (no arguments; rustup 1.28 or newer) to install exactly what it specifies.
The helper scripts need bash 4.4+, `jq` and GNU `find`/`sort` (all preinstalled on GitHub's Ubuntu
runners; CI also runs `shellcheck` on them). All gates below must pass before a phase is considered
done:

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
```

## License

Licensed under either of Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE)) or MIT license
([LICENSE-MIT](LICENSE-MIT)) at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in the
work by you, as defined in the Apache-2.0 license, shall be dual licensed as above, without any
additional terms or conditions.
