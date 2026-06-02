# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
once it reaches `1.0.0`. Pre-`1.0` releases may break compatibility.

## [Unreleased]

## [0.1.0-RC1] - 2026-06-02

First public release. Protocol parity with etcd-io/raft, plus the
out-of-band snapshot streaming, bounded queues, pluggable metrics,
gRPC transport, and RocksDB storage referenced below.

### Changed
- **Extracted to a standalone repository** — the Raft modules were lifted
  out of the original monorepo into a dedicated repo at
  `github.com/x-infra-lab/x-raft-lib`. The aggregator artifact was renamed
  to `x-raft-lib`, the project's display name unified to **x-raft-lib**,
  and governance files (CONTRIBUTING, CODE_OF_CONDUCT, SECURITY, CHANGELOG,
  NOTICE) surfaced at the repository root. Maven Central publishing wired up
  via the Sonatype `central-publishing-maven-plugin` in a `release` profile
  plus a `workflow_dispatch` GitHub Actions job. Unused `lombok` dependency
  removed.
- **Repo housekeeping** — the core module directory was renamed `raft/`
  → `raft-core/` to match its `artifactId`; doc cross-references updated
  to suit. Added a root `.gitignore` and removed checked-in IDE/build
  cruft (`.idea/`, `*.iml`, `target/`, `.DS_Store`).
- **`raft-examples` fleshed out** — from a single-node `RaftPeer`
  scaffold into a runnable distributed KV system with a `main()` demo
  (`KvClusterDemo`) and a RocksDB-backed state machine. See below.

### Added
- **`raft-tests` module** — cross-module integration tests against real
  gRPC sockets + real RocksDB stores. Six tests covering single-node
  bootstrap+apply, 3-node cluster election + replication, persistence
  across restart, and leader failover with quorum loss + recovery.
- **`RocksDbStorage.getApplied()` / `setApplied(long)`** — persisted
  apply-index watermark in the `state` CF. Hosts pass it back as
  `Config.applied` on restart so raft does not re-deliver
  previously-applied entries (the bug the restart integration test
  caught).
- **`RocksDbStorage.setConfState(ConfState)`** — persisted current
  cluster membership, preferred over snapshot-metadata in
  `initialState()`. Hosts call it after each ConfChange apply so a
  restart recovers voters without replaying the log into raft.
- **`RaftPeer`** now persists both watermarks: `setConfState` after
  every ConfChange apply, `setApplied` after each Ready cycle's batch
  of committed entries. The single-node restart test verifies this
  end-to-end.
- **Multi-module split (Step 3 partial)**:
  - `Transport` interface added to `raft-core` (zero implementation,
    raft-core stays I/O-free).
  - **New module `raft-transport-grpc`** — gRPC implementation:
    unary RPC for hot path (`Send`), client-streaming RPC for snapshots
    (`InstallSnapshot`) so multi-GB blobs bypass protobuf's 2 GiB
    single-message ceiling. `SnapshotChunk` proto encodes the
    `MsgSnapshot` envelope length-prefixed in chunk 0; `snapshot.data`
    is sliced across chunks.
  - **New module `raft-storage-rocksdb`** — RocksDB implementation
    with three column families (log, state, snapshot). Atomic
    Ready-cycle persistence via `RocksDbStorage.writeBatched(entries,
    hardState, snapshot)` (one fsync per Ready). Reopen-from-disk
    verified by tests.
  - **New module `raft-examples`** — a runnable distributed KV system.
    `RaftPeer` wires the three modules together; `RocksKvStore` is a
    RocksDB-backed application state machine and `KvCommand` is the
    replicated mutation. `KvClusterDemo` has a `main()` that brings up
    an in-process N-node cluster over real gRPC + RocksDB, runs a
    scripted workload through the leader, and prints each node's
    converged KV snapshot; `KvClusterDemoTest` asserts convergence.
  - Aggregator `pom.xml` at the repository root defines parent +
    `dependencyManagement` for shared versions. Java 17 / Maven 3.x.
  - **GitHub Actions CI** at `.github/workflows/ci.yml`: JDK
    17/21 matrix, full reactor `mvn install`, jacoco summary +
    surefire artifact upload on failure.
- **Production hardening (Step 2 partial)**:
  - Bounded queues for `Raft.pendingReadIndexMessages` and
    `Raft.readStates` (default 1024, drop-oldest + warn + metric).
    New `Config.maxPendingReadIndexMessages` and `Config.maxReadStates`.
  - Pluggable `RaftMetrics` interface (zero external dep). 8 callbacks
    cover the most common raft events; javadoc lists the 12 recommended
    counter / gauge / histogram bindings for Micrometer, Prometheus,
    OpenTelemetry, etc. Wire-points: propose accepted/dropped/stopped,
    leader change, read-index drop, read-state evict, tick skipped,
    peer unreachable, ready emitted.
  - `Node.stop(long, TimeUnit)` overload that returns `false` on
    timeout instead of hanging when the event loop is wedged.
  - `Node.basicStatus()` lock-free liveness snapshot (term / lead /
    commit / applied / lastIndex / state). Does not go through the
    events queue; readable even when the loop is stuck.
  - `Node.registerLeaderObserver(LeaderObserver)` callback API;
    returned `Runnable` deregisters. Hosts can now react to leader
    changes (e.g. quiesce client writes) without polling
    `Ready.softState`.
  - `Config.daemonEventLoop` (default `true` for test friendliness;
    production should set `false` to keep the event loop alive across
    JVM shutdown until the host explicitly calls `stop()`).
  - `Storage` interface extended with `append`, `setHardState`,
    `applySnapshot`, `createSnapshot`, `compact`, `close` as
    `default` methods (throw `UnsupportedOperationException`).
    Storage javadoc now documents atomicity, durability, concurrency,
    error-code semantics, the async-storage path, and the `mustSync`
    hint.
- Apache-2.0 LICENSE, NOTICE crediting etcd-io/raft, license headers on
  all source files.
- English `README.md`; Chinese version moved to `README.zh.md`.
- `CONTRIBUTING.md`, `CODE_OF_CONDUCT.md`, `SECURITY.md`, this
  `CHANGELOG.md`.
- `TODO.md` roadmap to production / PB-scale / open-source readiness.
- Sonatype-ready pom metadata (`<licenses>`, `<developers>`, `<scm>`,
  `<issueManagement>`, release profile with sources/javadoc/gpg).

### Changed
- `Config.validate()` now hard-rejects `maxSizePerMsg == 0` (used to
  silently accept and let the leader pack the full log into one
  MsgAppend). Logs a `warn` when `maxUncommittedEntriesSize` defaults
  to `NO_LIMIT` (no propose backpressure) and when
  `electionTick < 10 * heartbeatTick` (recommended ratio per the
  Raft thesis).
- `Raft.electionElapsed` and `Raft.heartbeatElapsed` widened from
  `int` to `long` so a long-lived learner that never gets promoted
  can't overflow the counter (~248 days at 100 Hz tick).
- Maven coordinates: `io.github.xinfra.lab:raft:1.0-SNAPSHOT` →
  `io.github.xinfra.lab:raft-core:0.1.0-RC1`.
- `Raft.java`: `MsgAppendResponse` handler unconditionally clears
  `pr.msgAppFlowPaused` (mirrors etcd-raft); `maybeSendAppend` falls
  back to snapshot when `entries()` raises (silent swallow fixed).
- `RaftException`: introduced `Code` enum + `equals/hashCode` based
  on code; callers use `e.is(Code.X)` instead of identity comparison.
- Invariant violations throw new `RaftInvariantException` (a
  `RuntimeException`) — separated from recoverable `RaftException`.
- `DefaultNode`: event-loop cleanup wrapped in `try-finally` so a
  Throwable escaping the inner catches can't strand producers blocked
  on a full events queue. `propose` preserves `InterruptedException`.
- `MemoryStorage`: test-only accessors are now `synchronized` and
  return immutable copies; new `setEntries(List)` for atomic seed.
- `Unstable.maybe{First,Last}Index/Term` and `ReadOnly.ackedIndex` /
  `AckedIndexer.ackedIndex`: nullable `Long` → `OptionalLong`.
- `confChangeToMsg` moved from `Raft` to
  `confchange.Changer.toMessage(...)`.
- Java-idiomatic clean-up: `SoftState` proper `equals/hashCode`;
  duplicate `Progress.setIsLearner` removed; wildcard imports replaced;
  `DefaultNode.submitWithResult` and `RawNode.stepLocal` helpers
  introduced; `Raft` gets small `appendRespAccept/Reject/voteResp`
  builder helpers.

### Fixed
- `RaftFlowControlTest.testMsgAppFlowControlMoveForward` strengthened:
  asserts match/next/inflights count + emitted-message absence on
  out-of-date responses (the previous `isPaused()`-only assertion
  would not have caught a regression that silently advances match).
- `Util.describeEntry`: render protobuf parse errors as
  `<parse-error: ...>` instead of letting them masquerade as legit
  entry content in logs.

### Deliberate non-changes
- `Changer.initProgress`: `Next = max(lastIndex, 1)` retained instead
  of moving to etcd-raft's `lastIndex + 1`. Verified the +1 form
  worsens convergence in the fresh-peer-with-compacted-leader case
  and breaks the snapshot_install_after_compact test scenario.
