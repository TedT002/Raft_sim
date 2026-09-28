# raftsim

[![CI](https://github.com/TedT002/Raft_sim/actions/workflows/ci.yml/badge.svg)](https://github.com/TedT002/Raft_sim/actions/workflows/ci.yml)

A Raft consensus implementation being built together with a deterministic simulation test harness,
designed so that any run can be replayed exactly from a single seed (given the same configuration,
pinned toolchain and `Cargo.lock`).

## Status

Work in progress — Phase 0 skeleton: the workspace builds and CI gates run; there is no Raft logic and
no simulator yet. The `raft-core` crate exposes only its type skeleton, and `RaftNode::step` is an
intentional no-op.

## Why

Consensus bugs hide in timing and fault interleavings that are hard to reproduce with real clocks,
threads and sockets. `raftsim` is designed to simulate the network, clock, disk and randomness instead,
so that a single `u64` seed will deterministically replay an entire run, including injected message
loss, delay, reordering, partitions and crashes. The approach follows the deterministic simulation
testing style popularized by FoundationDB and TigerBeetle.

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

Crates and their dependency direction (`raft-core` depends on no workspace crate):

| Crate | Responsibility |
|---|---|
| `raft-core` | Pure, sans-IO Raft state machine (Figure 2); currently a type skeleton |
| `sim` | Planned: deterministic simulation (virtual clock, event queue, simulated network/disk, faults) |
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
- [ ] Phase 1 — Simulator: virtual clock, event queue, simulated network (loss, duplication, delay,
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
./scripts/check_forbidden.sh
./scripts/test_check_forbidden.sh
./scripts/check_deps.sh
```

## License

Licensed under either of Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE)) or MIT license
([LICENSE-MIT](LICENSE-MIT)) at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in the
work by you, as defined in the Apache-2.0 license, shall be dual licensed as above, without any
additional terms or conditions.
