# Testing Strategy

[English](testing.md) | [中文](testing.zh.md)

## Test Pyramid Overview

x-raft-lib employs a multi-layered testing strategy to verify Raft protocol correctness from algorithmic invariants to full-stack distributed scenarios.

```
                    ┌─────────────┐
                    │   Soak /    │  Weekly: sustained load,
                    │   Chaos     │  resource leak detection
                    ├─────────────┤
                  ┌─┤ Integration │  Per-PR: real gRPC + RocksDB,
                  │ │   Tests     │  multi-node scenarios
                  │ ├─────────────┤
                  │ │    Fuzz     │  Nightly: Jazzer coverage-guided
                  │ │   Tests     │  fuzzing on step() + parse
                  │ ├─────────────┤
                  │ │  Property   │  Per-PR: jqwik property-based
                  │ │   Tests     │  testing on quorum/confchange
                ┌─┤ ├─────────────┤
                │ │ │  Datadriven │  Per-PR: scenario-driven
                │ │ │   Tests     │  interaction tests
                │ ├─┼─────────────┤
                │ │ │    Unit     │  Per-PR: 396+ tests,
                │ │ │   Tests     │  85%+ coverage
                └─┴─┴─────────────┘
```

| Layer | Count | Module | Runs |
|-------|-------|--------|------|
| Unit & functional tests | 396+ | raft-core | Every PR |
| Property-based tests | 11 | raft-core | Every PR |
| Datadriven tests | 28 scenarios | raft-core | Every PR |
| Fuzz tests | 2 harnesses | raft-core | Nightly |
| Integration tests | 25+ | raft-tests | Every PR |
| Chaos stress | 6 test classes × 5 iterations | raft-tests | Weekly |
| Soak tests | 2 | raft-tests | Weekly |

---

## Unit Tests (raft-core)

Core protocol tests ported from etcd-raft, covering every branch of the state machine:

| Test Class | What It Covers |
|------------|---------------|
| `RaftTest` | Leader election, log replication, commit advancement, message handling |
| `RaftPaperTest` | Raft paper Figure 2 invariants — election safety, leader append-only, log matching, leader completeness, state machine safety |
| `RaftSnapTest` | Snapshot creation, install, restore, compaction interaction |
| `RaftFlowControlTest` | MaxInflightMsgs, MaxSizePerMsg, Inflights sliding window, backpressure |
| `UnstableTest` | Unstable log buffer: stableTo, truncateAndAppend, shrink |
| `RaftLogTest` | Log management across Unstable + Storage boundary |
| `ReadOnlyTest` | ReadIndex safe and lease-based modes |
| `RawNodeTest` | RawNode API: bootstrap, propose, conf change, ready, advance |
| `ConfChangeTest` | V1/V2 membership change, joint consensus enter/leave |

### How to Run

```bash
mvn -pl raft-core test                     # all unit tests + JaCoCo gate
mvn -pl raft-core test -Dtest='RaftTest'   # single class
```

---

## Property-Based Tests (jqwik)

Randomized property tests that explore edge cases beyond hand-written scenarios:

| Test Class | Properties Verified |
|------------|-------------------|
| `ChangerPropertyTest` | ConfChange operations preserve quorum invariants across random voter/learner configurations |
| `InflightsPropertyTest` | Inflights add/freeFirstOne/freeLE/reset maintain correct count and capacity under random operation sequences |
| `MajorityConfigPropertyTest` | CommittedIndex computation is correct for arbitrary quorum sizes and match-index distributions |

---

## Datadriven Tests

The `InteractionTest` framework (ported from CockroachDB's datadriven framework) defines Raft scenarios in `.txt` files:

```
# example: three_node_election.txt
add-nodes 3 voters=(1,2,3) ...
campaign 1
stabilize
----
> 1 becomes leader
```

Current 28 scenarios: `single_node`, `single_node_log_state`, `single_node_propose_batch`, `single_node_snapshot_compact`, `two_node_election`, `three_node_election`, `three_node_propose_replicate`, `partition_recovery`, `forget_leader`, `leader_transfer`, `confchange_v2_joint`, `confchange_add_learner`, `confchange_add_voter`, `confchange_demote_voter_to_learner`, `confchange_learner_lifecycle`, `confchange_promote_learner`, `confchange_remove_learner`, `confchange_remove_then_readd`, `confchange_remove_voter`, `prevote_no_term_bump`, `checkquorum_leader_steps_down`, `heartbeat_resp_recovers_from_probing`, `snapshot_install_after_compact`, `snapshot_and_compact_errors`, `snapshot_succeed_via_app_resp`, `replicate_pause`, `lagging_commit`, `progress_after_replication`.

### Rewrite Mode

Write scenario skeletons (commands + `----`), then run:

```bash
mvn -pl raft-core test -Dtest='InteractionTest' -Ddatadriven.rewrite=true
```

This auto-captures actual output and fills in expected results.

---

## Fuzz Tests (Jazzer)

Coverage-guided fuzzing on the protocol boundary:

| Harness | Target | Goal |
|---------|--------|------|
| `EraftpbParseFuzzTest` | Protobuf message deserialization | Catch malformed-input panics, OOM from crafted payloads |
| `RaftStepFuzzTest` | `Raft.step(Message)` entry point | Discover invariant violations under arbitrary message sequences |

Each harness runs 30 minutes per session, twice daily in the [fuzz-nightly](.github/workflows/fuzz-nightly.yml) workflow. Corpus findings are uploaded as artifacts.

### How to Run Locally

```bash
mvn -P fuzz -pl raft-core test -Dtest='EraftpbParseFuzzTest'
mvn -P fuzz -pl raft-core test -Dtest='RaftStepFuzzTest'
```

---

## Integration Tests (raft-tests)

End-to-end tests with real gRPC sockets, real RocksDB stores, and ephemeral ports:

| Test Class | What It Proves |
|------------|---------------|
| `SingleNodeIntegrationTest` | Single-node boot, leader election, proposal, apply |
| `ThreeNodeClusterIntegrationTest` | 3-node cluster election, leader convergence, ordered replication |
| `RestartFromDiskIntegrationTest` | RocksDB persistence: close, re-open, recover state, no re-apply |
| `LeaderFailoverIntegrationTest` | Kill leader, survivors re-elect, continue committing |
| `SnapshotInstallIntegrationTest` | Partition follower past compaction, heal, MsgSnapshot install |
| `DynamicMembershipIntegrationTest` | ConfChangeV2: remove voter, add learner, promote to voter |
| `PartitionIntegrationTest` | Minority partition cannot commit; majority progresses; heal converges |
| `ChaosFaultInjectionIntegrationTest` | Random fault injection (loss, partition, isolation), recovery |
| `KvLinearizabilityIntegrationTest` | Concurrent KV ops with Jepsen-style linearizability checker |
| `ZeroCopySnapshotStreamingTest` | 64 MiB byte-for-byte streaming snapshot verification |
| `MultiNodeRestartIntegrationTest` | All-node restart + majority restart with minority survivor |
| `LeaderTransferIntegrationTest` | Leadership transfer to follower; transfer during active proposals |
| `ConfChangeCrashIntegrationTest` | Leader crash during conf change; scale up/down (3->5->3) |

### How to Run

```bash
mvn -pl raft-tests -am test                # all integration tests
mvn -pl raft-tests -am test -P fast        # skip them for inner loop
```

---

## Chaos Testing

### ChaosController / ChaosTransport

The `ChaosTransport` decorator wraps a real gRPC transport and consults a shared `ChaosController` to inject faults on both send and receive paths.

**Fault model** (only faults Raft is designed to tolerate):

| Fault | API | Effect |
|-------|-----|--------|
| Isolation | `chaos.isolate(nodeId)` | Drop all messages to/from a node |
| Partition | `chaos.partition(setA, setB)` | Block messages between two groups |
| Link block | `chaos.blockLink(from, to)` | Block one specific directional link |
| Drop probability | `chaos.dropProbability(from, to, p)` | Randomly drop messages with probability p |
| Latency | `chaos.latency(from, to, ms)` | Add delay to messages on a link |
| Duplicate | `chaos.duplicate(from, to, n)` | Duplicate messages n times |
| Heal | `chaos.healAll()` | Remove all faults |

Build a chaos-wrapped peer with `IntegrationTestSupport.chaosPeer(...)`.

### ChaosFaultInjectionIntegrationTest

Exercises random fault combinations against a live cluster:
1. Apply random faults (isolation, partition, drops)
2. Attempt proposals during chaos
3. Heal all faults
4. Verify cluster recovers and converges

---

## Linearizability Checking

`KvLinearizabilityIntegrationTest` runs concurrent KV operations against a 3-node cluster and verifies results with a linearizability checker (Jepsen-style history validation). This catches consistency violations that only manifest under concurrent access.

---

## Soak Tests

Long-running stability tests tagged `@Tag("soak")`, excluded from per-PR CI:

| Test | Duration | What It Checks |
|------|----------|---------------|
| `SoakStabilityTest` | Configurable (default 60s, CI 30min) | Sustained proposals + periodic snapshot/compaction. Asserts continuous commit progress, no apply backlog, no thread leak |
| `ElectionCycleSoakIntegrationTest` | Configurable cycles (default 20) | Repeated leader crash -> re-election -> restart loop. Asserts no committed entries lost, no commit regression, no thread leak |

### How to Run

```bash
# default duration
mvn -P soak -pl raft-tests test

# 30-minute soak
mvn -P soak -pl raft-tests test -Dsoak.durationSeconds=1800

# custom election cycles
mvn -P soak -pl raft-tests test -Dsoak.electionCycles=50
```

---

## Coverage Gates

JaCoCo coverage thresholds enforced per-PR:

| Module | Instruction | Branch | Line | Method |
|--------|:-----------:|:------:|:----:|:------:|
| raft-core | >= 85% | >= 80% | >= 88% | >= 85% |
| raft-transport-grpc | >= 75% | >= 60% | >= 75% | >= 80% |
| raft-storage-rocksdb | >= 80% | >= 70% | >= 80% | >= 95% |

Project-level Codecov: project >= 80%, patch >= 75% (1pp tolerance).
