# raft-tests

Cross-module integration tests. Each test brings up real
[`raft-core`](../raft-core) state machines, real
[`raft-transport-grpc`](../raft-transport-grpc) gRPC servers (on
ephemeral localhost ports), and real
[`raft-storage-rocksdb`](../raft-storage-rocksdb) RocksDB stores
(in `@TempDir`s), then exercises end-to-end scenarios.

This module is **not** published. It exists separately so:

- it can depend on every runtime module without polluting any of them
  with a transitive test dependency on the others;
- the slow integration suite can be skipped by inner-loop builds with
  `-P fast` while still running in CI;
- a failure here points at a wiring problem (host/transport/storage
  contract violation) rather than at any single module's logic.

## Tests

| Class | What it proves |
|---|---|
| [`SingleNodeIntegrationTest`](src/test/java/io/github/xinfra/lab/raft/tests/SingleNodeIntegrationTest.java) | A single-node cluster boots over real gRPC + real RocksDB, becomes leader, accepts proposals, applies them in order. |
| [`ThreeNodeClusterIntegrationTest`](src/test/java/io/github/xinfra/lab/raft/tests/ThreeNodeClusterIntegrationTest.java) | A 3-node localhost cluster elects a leader, all peers converge on the same leader id, and proposals replicate to all three apply logs in the same order. |
| [`RestartFromDiskIntegrationTest`](src/test/java/io/github/xinfra/lab/raft/tests/RestartFromDiskIntegrationTest.java) | Persistence: a node that's accepted proposals can be closed and re-opened against the same RocksDB directory. The restarted node re-elects, recovers commit/applied state from disk, and does **not** re-apply old entries. |
| [`LeaderFailoverIntegrationTest`](src/test/java/io/github/xinfra/lab/raft/tests/LeaderFailoverIntegrationTest.java) | Kill the current leader of a 3-node cluster. The two survivors form a quorum, elect a new leader (different id from the old one), accept new proposals, and continue replicating. |
| [`SnapshotInstallIntegrationTest`](src/test/java/io/github/xinfra/lab/raft/tests/SnapshotInstallIntegrationTest.java) | Partition a follower, commit + snapshot + compact past its match index, then heal. The follower can no longer be caught up entry-by-entry and must install a `MsgSnapshot`, after which it converges and applies post-heal proposals. |
| [`DynamicMembershipIntegrationTest`](src/test/java/io/github/xinfra/lab/raft/tests/DynamicMembershipIntegrationTest.java) | ConfChange V2 end to end: removing a follower converges the persisted voter set to the remaining two; a brand-new node joins as a learner, catches up via replication, is promoted to voter, and the 4-voter cluster keeps committing. |
| [`PartitionIntegrationTest`](src/test/java/io/github/xinfra/lab/raft/tests/PartitionIntegrationTest.java) | A minority partition cannot commit while the majority keeps progressing; after heal the whole cluster converges. A 20%-loss link still converges because raft retransmits. |
| [`SoakStabilityTest`](src/test/java/io/github/xinfra/lab/raft/tests/SoakStabilityTest.java) | (`@Tag("soak")`, off by default) Sustained proposals + periodic snapshot/compaction; asserts continuous commit progress, no apply backlog, and no thread leak. |

## Fault injection

Partition / loss tests use a [`ChaosTransport`](src/test/java/io/github/xinfra/lab/raft/tests/chaos/ChaosTransport.java)
decorator that wraps the real gRPC transport and consults a shared
[`ChaosController`](src/test/java/io/github/xinfra/lab/raft/tests/chaos/ChaosController.java)
to drop messages on both the send and receive paths. It models only the
faults raft is designed to tolerate (loss, partition, isolation) — never
corruption or reordering. Build a chaos-wrapped peer with
`IntegrationTestSupport.chaosPeer(...)`.

## Run

```bash
# from the repo root:
mvn install                    # full reactor including these tests
mvn -pl raft-tests -am test    # just the integration tests
mvn -pl raft-tests -P fast test  # skip them on inner loop
mvn -pl raft-tests -P soak test  # ONLY the soak tests (long-running)
mvn -pl raft-tests -P soak -Dsoak.durationSeconds=120 test  # longer soak
```

Each test allocates ephemeral free ports just before binding, so
parallel test runs in different processes won't collide. Tests inside a
single JVM run sequentially (`forkCount=1`) to keep timing
deterministic.

## What's not covered yet

- ReadIndex / linearizable reads.
- Asymmetric (one-way) reachability faults — `ChaosController` currently
  models symmetric link blocks; one-directional delay/loss is future work.

These are tracked in [`raft/TODO.md`](../raft-core/TODO.md) Step 3.5.
