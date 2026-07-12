# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
once it reaches `1.0.0`. Pre-`1.0` releases may break compatibility.

## [Unreleased]

### Fixed

#### `raft-transport-grpc` — GrpcTransport

- **快照发送线程池隔离，消除心跳饥饿 (P0)**

  **问题根因：** `GrpcTransport` 中所有 peer 发送（心跳、日志复制、快照）
  共享同一个固定线程池 `sendExecutor`（大小 `max(2, cpus/2)`）。
  `PeerChannel.sendSnapshot()` 使用 blocking stub 同步等待 server ack，
  最长阻塞 60 秒。当集群中有两个 follower 同时需要快照时（例如同时加入
  两个 learner），两个快照发送占满线程池，导致心跳（`MsgHeartbeat`）和日志
  复制（`MsgAppend`）全部排队等待，触发 election timeout → 选举风暴。

  **修复方案：**
  - 新增 `snapshotExecutor` 字段（2 线程固定池，线程名
    `raft-grpc-snap-{localId}`），专门用于快照发送。
  - `send(long, Message)` 方法中，根据 `msg.getMsgType()` 路由：
    `MsgSnapshot` 类型消息提交到 `snapshotExecutor`，其余消息仍走
    `sendExecutor`。
  - `sendSnapshot(long, Message, InputStream, Callback)`（流式快照
    发送入口）改为提交到 `snapshotExecutor`。
  - `close()` 方法中增加对 `snapshotExecutor` 的 shutdown +
    awaitTermination + shutdownNow 处理，与 `sendExecutor` 并行关闭。

  **影响范围：** `GrpcTransport.java` — 构造器、`send()`、
  `sendSnapshot()`、`close()` 四处修改。

- **inline 快照发送异常不再被吞没，unreachable 回调能正确触发 (P0)**

  **问题根因：** `PeerChannel.sendSnapshot()` 内部有两个 catch 块——
  `TimeoutException`（第 428 行）和 `Throwable`（第 431 行），都只记日志
  不重新抛出。外层 `PeerChannel.send()` 的 catch 块（第 328 行）负责调用
  `unreachableListener.onUnreachable(peerId)`，但由于 `sendSnapshot()` 吞掉
  了所有异常，这段代码对快照发送失败永远不可达。结果：leader 向一个已挂掉
  的 follower 发快照，raft 核心永远收不到 `MsgUnreachable` 通知，
  progress tracker 可能长时间处于 `StateSnapshot`（暂停复制）状态。

  **修复方案：**
  - `TimeoutException` catch 块：在 `req.onError(te)` 之后增加
    `throw new RuntimeException(te)`。
  - `Throwable` catch 块：在日志之后增加重新抛出逻辑——
    `RuntimeException` 直接 rethrow，`Error` 直接 rethrow，checked
    exception 包装为 `RuntimeException` 抛出。
  - 异常传播到外层 `send()` 的 catch，正常触发
    `unreachableListener.onUnreachable(peerId)`。

  **影响范围：** `GrpcTransport.java` — `PeerChannel.sendSnapshot()`
  方法的两个 catch 块。

- **向未注册 peer 发消息时触发 unreachable 回调 (P1)**

  **问题根因：** `send(long, Message)` 中 `peers.get(peerId)` 返回 null 时，
  只记了一条 WARN 日志就 return，未调用 `unreachableListener`。raft 核心
  依赖 `MsgUnreachable`（由 unreachable 回调触发）来将 progress 从
  `StateReplicate` 降级为 `StateProbe`，静默丢弃意味着 leader 会持续向不存在
  的 peer 发送消息而得不到任何反馈。

  **修复方案：**
  - 在 `send()` 方法的 null-peer 分支中，log.warn 之后增加
    `UnreachableListener l = unreachableListener; if (l != null) l.onUnreachable(peerId);`。
  - 更新测试 `GrpcTransportLifecycleTest.sendToUnknownPeerIsLoggedAndDropped()`：
    原断言 `assertThat(unreachableCount.get()).isZero()`（期望不触发）
    改为 `assertThat(unreachableCount.get()).isEqualTo(1)`（期望触发 1 次）。

  **影响范围：** `GrpcTransport.java` — `send()` 方法；
  `GrpcTransportLifecycleTest.java` — 断言修改。

#### `raft-storage-rocksdb` — RocksDbStorage

- **修复 JNI 原生内存泄漏：`ColumnFamilyOptions` 和 `DBOptions` (P1)**

  **问题根因：** `RocksDbStorage` 构造器中创建了 `ColumnFamilyOptions cfOpts`
  和 `DBOptions dbOpts` 作为局部变量。这两个对象底层通过 JNI 分配 C++ 原生
  内存（RocksDB native objects），不受 Java GC 管理，必须显式调用 `close()`
  释放。由于是局部变量，构造器执行完后 Java 引用丢失，但 C++ 侧内存永远
  不会被释放——每次创建 `RocksDbStorage` 实例都会泄漏数十 KB 到数百 KB
  的原生内存（取决于 bloom filter、block cache 配置等）。长时间运行或频繁
  重启存储实例的场景下会导致进程 RSS 持续增长。

  **修复方案：**
  - 新增两个 `final` 字段：`private final ColumnFamilyOptions cfOpts` 和
    `private final DBOptions dbOpts`。
  - 构造器中将局部变量赋值改为 `this.cfOpts = ...` 和 `this.dbOpts = ...`。
  - `close()` 方法中，在 `db.close()` **之后**（options 必须比 DB 存活更久）
    依次关闭 `dbOpts.close()` 和 `cfOpts.close()`，每个都包裹在独立的
    try-catch 中，确保一个关闭失败不影响其余资源释放。
  - 关闭顺序：`cfHandles` → `writeOpts` → `db` → `dbOpts` → `cfOpts`
    → `bloomFilter` → `blockCache`。

  **影响范围：** `RocksDbStorage.java` — 字段声明、构造器、`close()` 方法。

#### `raft-examples` — RaftKVNode

- **异步模式下持久化/应用失败现在是致命错误 (P0)**

  **问题根因：** `RaftKVNode` 的异步存储写入模式使用两个独立线程
  （`persistExecutor` 和 `applyExecutor`）分别处理 `MsgStorageAppend` 和
  `MsgStorageApply`。`handleStorageAppend()` 和 `handleStorageApply()` 各有
  一个 `catch (Exception e)` 块，但只调用了 `LOG.error("async persist/apply failed", e)`
  就返回了。

  持久化失败意味着已被 raft 共识确认的数据**没有被持久化**——这不是可恢复
  的错误，而是数据丢失。如果继续运行，后续的 `MsgStorageAppendResp` 永远
  不会发出（因为 responses 在 try 块内的 deliverOrSend 之前失败了），raft
  核心将认为持久化仍在进行中，整个 Ready 管线停滞。同时 `MsgStorageApplyResp`
  也不会发出，applied index 不再推进，pending proposals 永远不会完成。

  **修复方案：**
  - 两个 catch 块中，日志级别改为
    `LOG.error("fatal: async persist/apply failed, stopping node", e)`。
  - 增加 `running = false`（使 `readyLoop()` 的 `while (running)` 退出）。
  - 增加 `applier.interrupt()`（中断可能正阻塞在 `node.ready()` 上的
    applier 线程，使其立即退出）。
  - `readyLoop()` 退出后，`finally` 块中的 `drainPendingFutures()` 会将
    所有 `pendingProposals`、`pendingReads`、`waitingForApply`、
    `pendingConfChanges` 以 `RaftException(UNAVAILABLE, "node shutting down")`
    异常完成，不会有调用者无限等待。

  **影响范围：** `RaftKVNode.java` — `handleStorageAppend()` 和
  `handleStorageApply()` 的 catch 块。

- **inline 快照模式不再导致 StateSnapshot 饥饿**

  **问题根因：** 当 leader 通过 `maybeSendSnapshot()` 发送快照后，
  对应 follower 的 progress 会进入 `StateSnapshot`（`isPaused()` 始终返回
  `true`），日志复制完全暂停。只有收到 `MsgSnapStatus`（由
  `node.reportSnapshot()` 发出）后，progress 才会通过 `becomeProbe()`
  恢复到 `StateProbe`。

  在流式快照模式下，`sendSnapshotOutOfBand()` 的回调会调用
  `node.reportSnapshot(to, SnapshotFinish/SnapshotFailure)`。但在 inline
  模式下，`MsgSnapshot` 直接通过 `transport.send()` 发出，是 fire-and-forget
  的——没有任何地方调用 `reportSnapshot()`。

  结果：leader 的 progress tracker 中该 follower 永远停留在
  `StateSnapshot`，`isPaused()=true`，`maybeSendAppend()` 返回 false，
  `MsgHeartbeatResponse` 处理中的 `sendAppend()` 也被 `isPaused()` 阻塞。
  该 follower 永远无法通过正常复制追上 leader。

  etcd 原版（Go HTTP transport）在 inline 发送后也调用了 `ReportSnapshot`，
  但本项目的 Java gRPC transport 遗漏了这一关键步骤。

  **修复方案：**
  - 在 `RaftKVNode.sendPeerMessage()` 方法中，`transport.send(m.getTo(), m)`
    之后增加判断：如果 `m.getMsgType() == MsgSnapshot`，立即调用
    `node.reportSnapshot(m.getTo(), SnapshotStatus.SnapshotFinish)`。
  - 使用 `SnapshotFinish`（而非 `SnapshotFailure`）是因为 inline 模式下
    消息已交给 transport，transport 层面的失败会通过 `unreachableListener`
    另行处理。`SnapshotFinish` 和 `SnapshotFailure` 在 raft 核心中效果相同：
    都调用 `pr.becomeProbe()`，唯一区别是日志级别。

  **影响范围：** `RaftKVNode.java` — `sendPeerMessage()` 方法。

- **`checkSelfRemoval()` 修复 joint consensus 场景下的漏检**

  **问题根因：** 原实现只检查当前 ConfChangeV2 的 `changesList` 是否包含
  `ConfChangeRemoveNode(self)`。对于 joint consensus 的第二步
  （leaveJoint），raft 核心自动生成一条**空的** ConfChangeV2（`changesList`
  为空），原代码中 `removingSelf` 为 false → 直接 return，永远不会检查
  节点是否已被移出集群——被替换的节点无法感知自己被移除，不会主动关闭。

  同时，原实现在检测到自身被移除后直接调用 `close()`，这会在 apply 线程
  中同步关闭整个节点（包括 raft node、transport、storage），可能导致
  正在进行的 persist/apply 操作被中断。

  **修复方案：**
  - 增加 `everAdmitted` 标记，在节点首次出现在 ConfState 中时置为 true，
    防止节点重放历史配置变更（如 bootstrap entries）时误判自己被移除。
  - 增加 `isLeaveJoint` 判断：`cc.getChangesList().isEmpty()` 时也执行
    检查（空 ConfChangeV2 在 raft 协议中唯一的语义就是 leaveJoint）。
  - 检测到自身被移除时不再直接 `close()`，而是设置 `running = false` 并
    `removedFuture.complete(null)`，由 `KvServer` 的
    `raftKvNode.onRemoved().thenRunAsync(this::close)` 异步完成关闭。

  **影响范围：** `RaftKVNode.java` — `checkSelfRemoval()` 方法、新增
  `everAdmitted` 字段。

### Changed

#### `raft-examples` — RaftKVNode / KvServer / KvServerBootstrap

- **快照阈值从硬编码常量改为构造器参数**

  - `RaftKVNode`：删除 `private static final long SNAPSHOT_ENTRIES_THRESHOLD = 10_000`，
    新增构造器参数 `long snapshotThreshold` 和同名 `final` 字段。
    `maybeSnapshot()` 中的判断条件改为 `applied - lastSnapshotIndex < snapshotThreshold`。
  - `KvServer`：构造器透传 `snapshotThreshold` 参数给 `RaftKVNode`。
  - `KvServerBootstrap`：`main()` 中创建 `KvServer` 时传入默认值 `10_000`。

- **`KvServer.onRemoved()` 回调改为异步执行**

  原代码使用 `raftKvNode.onRemoved().thenRun(this::close)`，
  `close()` 在 `removedFuture` 的完成线程上同步执行。如果 `removedFuture`
  在 apply 线程中被 complete，`close()` 会关闭 transport 和 storage，
  而 apply 线程可能还在使用它们。改为 `thenRunAsync(this::close)` 使关闭
  在独立线程上执行，避免死锁或资源竞争。

#### `raft-examples` — KvServerIntegrationTest

- **从 9 阶段扩展为 11 阶段全功能集成测试**

  原测试以 3 节点静态 bootstrap 启动集群，覆盖基本 CRUD、ReadIndex、
  leader transfer、动态成员变更（加入/移除）和 joint consensus。

  重写为单节点 bootstrap → 逐节点动态加入的模式（更贴近生产场景），
  新增阶段：

  | 阶段 | 覆盖内容 |
  |------|---------|
  | Phase 1 | 单节点 bootstrap，验证自选举为 leader |
  | Phase 2 | 动态添加 node 2：learner → 等待追赶 → 提升为 voter（2 节点集群）|
  | Phase 3 | 动态添加 node 3：同上流程（3 节点集群）|
  | Phase 4 | Propose PUT/DELETE + 全节点状态收敛验证 |
  | Phase 5 | ReadIndex 线性一致读（命中 + 未命中）|
  | Phase 6 | Leader transfer + 新 leader propose 验证 |
  | Phase 7 | 添加 node 4 为 learner → 批量写入 60 条触发 snapshot → 验证 snapshot 创建 + learner 数据收敛 |
  | Phase 8 | node 4 提升为 voter（4 节点集群）|
  | Phase 9 | gRPC 层 CRUD（put/get/delete）+ Admin 接口（getClusterInfo 验证 4 voters）|
  | Phase 10 | 移除 node 4 → 验证 3 节点集群继续工作 |
  | Phase 11 | Joint consensus 原子替换 voter（移除 victim + 添加 node 5）→ 批量写入触发新 snapshot（使新 snapshot 的 ConfState 包含 node 5）→ 全节点收敛 |

  - `SNAPSHOT_THRESHOLD` 设为 50（降低触发门槛，加速测试）。
  - 参数化矩阵覆盖 3 种模式组合：
    `(sync+inline)`, `(sync+streaming)`, `(async+inline)`。
  - 新增 `createServer()` 和 `serverById()` 辅助方法。
  - Phase 11 中批量写入 `SNAPSHOT_THRESHOLD + 10` 条 entries，确保
    joint consensus 完成后生成包含 node 5 的新 snapshot——raft 核心会
    拒绝 ConfState 中不包含目标节点的 snapshot，这是该阶段能通过的关键。

### Docs

- **`CHANGELOG.md`** — 新增 `[Unreleased]` 条目，详细记录所有修复和变更。
- **`docs/code-qa.zh.md`** — Q26 更新快照触发条件为可配置的
  `snapshotThreshold` 参数；Q27 更新传输细节——快照发送使用独立
  `snapshotExecutor` 线程池，新增 inline 模式 `reportSnapshot` 说明。

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
- Project roadmap documentation.
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
