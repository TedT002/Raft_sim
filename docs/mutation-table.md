# Mutation table

Each row is a bug planted on purpose behind a Cargo feature (`mutation-<name>`, never part of a
default build). The fuzzer must catch every one of them. On every push (CI job `Mutations`),
`scripts/check_mutations.sh` replays each row's seed with its feature, where the run must fail with
exactly the listed check, and without it, where the same run must pass. It also requires a row for
every `mutation-*` feature. The Seed and Caught by columns therefore cannot silently go stale; the
two count columns are a measurement snapshot (see the notes).

| Feature | Planted bug | Raft rule broken | Caught by | Profile | Seed | Caught (chaos) | Caught (figure8) |
|---|---|---|---|---|---|---|---|
| `mutation-no-election-restriction` | a node votes for a candidate with a stale log (one vote per term still holds) | election restriction (§5.4.1) | `violation: leader completeness` | chaos | 1 | 797 / 1000 | 997 / 1000 |
| `mutation-commit-old-terms` | a leader commits entries of earlier terms by counting replicas | commitment rule (§5.4.2, Figure 8) | `violation: leader completeness` | figure8 | 3 | 0 / 1000 | 16 / 1000 |
| `mutation-forget-vote` | `votedFor` is not persisted | persistent state (Figure 2) | `violation: durability` | chaos | 0 | 1000 / 1000 | 1000 / 1000 |
| `mutation-truncate-on-append` | every AppendEntries truncates the log after `prevLogIndex` | delete only on a real conflict (§5.3) | `violation: committed entry rewritten` | chaos | 0 | 1000 / 1000 | 1000 / 1000 |
| `mutation-skip-prev-log-term` | a follower accepts AppendEntries whose `prevLogTerm` differs from its own entry at `prevLogIndex` (the length check stays) | AppendEntries consistency check (§5.3) | `violation: log matching` | chaos | 0 | 297 / 1000 | 926 / 1000 |
| `mutation-apply-before-commit` | entries are applied up to the last log index, not the commit index | apply only committed entries (Figure 2) | `violation: state machine safety` | chaos | 0 | 605 / 1000 | 1000 / 1000 |
| `mutation-no-dedup` | the state machine re-applies a retried request | client sessions apply a request once (§8) | `linearizability` | chaos | 0 | 562 / 1000 | 149 / 1000 |

Reproduce any row (the run fails with the listed check; without the feature the same seed passes):

```
cargo run -p cli --features mutation-commit-old-terms -- replay --seed 3 --profile figure8
```

Notes:

- **Caught by** is the failure signature printed by `raftsim`: the violated invariant, or the
  linearizability checker. `durability` and `committed entry rewritten` are the simulator's
  write-time checks (memory must match what was written; a node never rewrites its log at or below
  its commit index); they catch those bugs at the first faulty step, before the damage could surface
  as a safety violation.
- **Caught (chaos / figure8)** counts the failing seeds among `0..1000` for each profile
  (`raftsim fuzz --seeds 0..1000 --profile <p>` built with the feature). The default chaos profile
  misses `mutation-commit-old-terms`: the leader's no-op entry (§8) closes the Figure 8 window
  quickly. The `figure8` profile sends one entry per AppendEntries and crashes leaders often, which
  reopens it.
- The seeds are those of the current scenario generator. Changing the generator, the simulator's
  event order or the protocol changes them; regenerate the table with the fuzz commands above.
