# Mutation table

Each row is a bug planted on purpose behind a Cargo feature (`mutation-<name>`, never part of a
default build). The fuzzer must catch every one of them. On every push (CI job `Mutations`),
`scripts/check_mutations.sh` replays each row's seed with its feature, where the run must fail with
exactly the listed check, and without it, where the same run must pass. It also requires a row for
every `mutation-*` feature. The Seed and Caught by columns therefore cannot silently go stale; the
two count columns are a measurement snapshot (see the notes).

| Feature | Planted bug | Raft rule broken | Caught by | Profile | Seed | Caught (chaos) | Caught (figure8) | Caught (reads) | Caught (snapshots) |
|---|---|---|---|---|---|---|---|---|---|
| `mutation-no-election-restriction` | a node votes for a candidate with a stale log (one vote per term still holds) | election restriction (§5.4.1) | `violation: leader completeness` | chaos | 1 | 797 / 1000 | 997 / 1000 | 669 / 1000 | 788 / 1000 |
| `mutation-commit-old-terms` | a leader commits entries of earlier terms by counting replicas | commitment rule (§5.4.2, Figure 8) | `violation: leader completeness` | figure8 | 3 | 0 / 1000 | 16 / 1000 | 0 / 1000 | 0 / 1000 |
| `mutation-forget-vote` | `votedFor` is not persisted | persistent state (Figure 2) | `violation: durability` | chaos | 0 | 1000 / 1000 | 1000 / 1000 | 1000 / 1000 | 1000 / 1000 |
| `mutation-truncate-on-append` | every AppendEntries truncates the log after `prevLogIndex` | delete only on a real conflict (§5.3) | `violation: committed entry rewritten` | chaos | 0 | 1000 / 1000 | 1000 / 1000 | 1000 / 1000 | 1000 / 1000 |
| `mutation-skip-prev-log-term` | a follower accepts AppendEntries whose `prevLogTerm` differs from its own entry at `prevLogIndex` (the length check stays) | AppendEntries consistency check (§5.3) | `violation: log matching` | chaos | 0 | 297 / 1000 | 926 / 1000 | 589 / 1000 | 226 / 1000 |
| `mutation-apply-before-commit` | entries are applied up to the last log index, not the commit index | apply only committed entries (Figure 2) | `violation: state machine safety` | chaos | 0 | 605 / 1000 | 1000 / 1000 | 767 / 1000 | 958 / 1000 |
| `mutation-no-dedup` | the state machine re-applies a retried request | client sessions apply a request once (§8) | `linearizability` | chaos | 0 | 562 / 1000 | 149 / 1000 | 353 / 1000 | 538 / 1000 |
| `mutation-read-without-quorum` | a leader serves a ReadIndex read without confirming its leadership with a majority | confirm leadership before serving a read (ReadIndex, thesis §6.4) | `linearizability` | reads | 62 | 0 / 1000 | 0 / 1000 | 37 / 1000 | 0 / 1000 |
| `mutation-read-before-term-commit` | a new leader serves reads before it has committed an entry of its own term | wait for the term's first commit before reading (thesis §6.4, §8) | `linearizability` | reads | 0 | 0 / 1000 | 0 / 1000 | 122 / 1000 | 0 / 1000 |
| `mutation-install-stale-snapshot` | a follower installs a snapshot older than its commit index, rolling back its state machine | install only a newer snapshot (§7, Figure 13) | `violation: state machine safety` | snapshots | 0 | 0 / 1000 | 0 / 1000 | 0 / 1000 | 782 / 1000 |
| `mutation-snapshot-without-sessions` | the state machine's snapshot leaves out the client sessions | a snapshot includes the client sessions (thesis §6.3, §7) | `violation: snapshot safety` | snapshots | 0 | 0 / 1000 | 0 / 1000 | 0 / 1000 | 972 / 1000 |

Reproduce any row (the run fails with the listed check; without the feature the same seed passes):

```
cargo run -p cli --features mutation-commit-old-terms -- replay --seed 3 --profile figure8
```

Notes:

- **Caught by** is the failure signature printed by `raftsim`: the violated invariant, or the
  linearizability checker. `durability` and `committed entry rewritten` are the simulator's
  write-time checks (memory must match what was written; a node never rewrites its log at or below
  its commit index); they catch those bugs at the first faulty step, before the damage could surface
  as a safety violation. `snapshot safety` compares state machines: with compaction on, every
  replica's state machine after applying index `i`, and the state machine rebuilt from every
  snapshot through `i` that a node installs from its leader or loads from its disk at restart, must
  equal the state the first replica had at `i`. A snapshot is checked when it is used, not when it
  is taken: its bytes are opaque until they are decoded. The comparison uses a fingerprint of the
  decoded state (table and client sessions) rather than the snapshot's own encoding, so a snapshot
  that leaves out the sessions is caught as soon as a replica is rebuilt from it, before a retried
  request is applied twice.
- **Caught (chaos / figure8 / reads / snapshots)** counts the failing seeds among `0..1000` for
  each profile (`raftsim fuzz --seeds 0..1000 --profile <p>` built with the feature). The default
  chaos profile misses `mutation-commit-old-terms`: the leader's no-op entry (§8) closes the Figure
  8 window quickly. The `figure8` profile sends one entry per AppendEntries and crashes leaders
  often, which reopens it. The two read mutations only matter where reads skip the log: only the
  `reads` profile issues ReadIndex reads, and the linearizability checker catches the stale values
  they return. Likewise only the `snapshots` profile compacts logs, so the two snapshot mutations
  are caught there and nowhere else.
- Random fault injection has limits. Code review found a bug in the first ReadIndex version: a
  follower answered a probe of an earlier term with that probe's round number, so a delayed answer
  could confirm a newer read of the same leader. It needs a delayed probe, a delayed answer and a
  change of leadership to line up; even with the `reads` profile's long-tail delays the fuzzer did
  not find it in 1000 seeds (nor with 20% of messages delayed). It is guarded by deterministic tests
  instead (`an_answer_to_an_earlier_terms_probe_never_confirms_a_read` and the step-contract test in
  `raft-core`), not by a row here.
- The seeds are those of the current scenario generator. Changing the generator, the simulator's
  event order or the protocol changes them; regenerate the table with the fuzz commands above.
