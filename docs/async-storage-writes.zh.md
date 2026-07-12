# 异步存储写入（Async Storage Writes）

[English](async-storage-writes.md) | [中文](async-storage-writes.zh.md)

本文档结合 `raft-examples` 中的 `RaftKVNode` 详解异步存储写入模式的使用方式，以及为什么这种方式是安全正确的。

## 1. 问题背景：同步模式的瓶颈

在默认的同步模式中，Ready 处理是严格串行的：

```
持久化 entries/hardState → 发送 messages → 应用 committedEntries → advance()
```

关键约束是 **`advance()` 构成一道屏障**：在 `advance()` 返回之前，DefaultNode 事件循环不会发射下一个 Ready（`waitingAdvance = true`）。这意味着：

```
Ready₁ 持久化 ──┬── Ready₁ 应用 ──── advance₁ ──── Ready₂ 持久化 ──┬── Ready₂ 应用 ...
               │                                                  │
               └── 持久化和应用串行，无法流水线化 ─────────────────────┘
```

当持久化是高延迟操作（如 fsync 到 SSD）时，应用状态机必须等待持久化完成后的 advance 才能处理下一批条目，造成 **吞吐量瓶颈**。

## 2. 异步模式的核心思想

AsyncStorageWrites 将「持久化」与「应用」的完成通知从 `advance()` 中解耦出来，改用消息驱动：

```
Ready.messages() 中包含：
├── MsgStorageAppend  → 「请持久化这些 entries/hardState/snapshot」
│   └── Responses: [MsgStorageAppendResp(自回环), MsgAppResp(给 peers), ...]
├── MsgStorageApply   → 「请应用这些 committedEntries」
│   └── Responses: [MsgStorageApplyResp(自回环)]
└── 其他正常消息       → MsgAppend, MsgHeartbeat 等
```

**不再调用 `advance()`。** 取而代之，当应用完成持久化/应用后，将 `Responses` 中的消息投递回 raft（自回环的通过 `node.step()`，发给 peer 的通过 `transport.send()`），raft 收到这些响应后自行推进状态。

这使得架构上可以实现真正的流水线化：

```
                  ┌── 持久化线程 ──┐     ┌── 持久化线程 ──┐
Ready₁ ──┤                      ├──── Ready₂ ──┤                      ├── ...
                  └── 应用线程 ────┘     └── 应用线程 ────┘
                  (可并行执行)             (可并行执行)
```

## 3. 在 raft-examples 中的使用

### 3.1 启用方式

通过 `Config.builder().asyncStorageWrites(true)` 启用：

```java
// RaftKVNode 构造函数
Config cfg = Config.builder()
        .id(id)
        .electionTick(10)
        .heartbeatTick(1)
        .storage(storage)
        .asyncStorageWrites(asyncStorageWrites)  // ← 开关
        .applied(storage.getApplied())
        .build();
```

启用后，DefaultNode 事件循环不再设置 `waitingAdvance = true`（`DefaultNode.java:206`），允许连续发射多个 Ready 而无需等待上一个完成。

### 3.2 三线程架构

`RaftKVNode` 在异步模式下使用三个线程：

| 线程 | 职责 | 触发源 |
|------|------|--------|
| readyLoop | 取 Ready，将 `MsgStorageAppend`/`MsgStorageApply` 分发给工作线程，立即发送其他 peer 消息 | `node.ready()` |
| persistExecutor | **从 MsgStorageAppend 中**持久化 entries/hardState/snapshot，完成后投递 Responses | readyLoop 分发的 `MsgStorageAppend` |
| applyExecutor | **从 MsgStorageApply 中**应用 committedEntries 到状态机，完成后投递 Responses | readyLoop 分发的 `MsgStorageApply` |

readyLoop 线程 **不做任何持久化和应用** —— 它只负责分发任务后立即返回，等待下一个 Ready。这正是流水线化的关键：当 persistExecutor 在持久化 Ready₁ 时，readyLoop 已经可以取出 Ready₂ 并分发。

### 3.3 processReadyAsync：分发逻辑

```java
private void processReadyAsync(Ready rd) {
    boolean hasSnapshot = rd.snapshot().getMetadata().getIndex() > 0;
    CompletableFuture<Void> persistDone = CompletableFuture.completedFuture(null);

    // ReadStates 在 readyLoop 线程上立即处理（使用最新已知 applied index）
    processReadStates(rd.readStates(), 0);

    for (Eraftpb.Message m : rd.messages()) {
        switch (m.getMsgType()) {
            case MsgStorageAppend -> {
                // ① 提交到持久化线程
                persistDone = CompletableFuture.runAsync(
                        () -> handleStorageAppend(m), persistExecutor);
            }
            case MsgStorageApply -> {
                // ② 提交到应用线程
                Runnable applyTask = () -> handleStorageApply(m);
                if (hasSnapshot) {
                    // 有快照时，必须等持久化完成（快照恢复到 SM 后）再应用
                    persistDone.thenRunAsync(applyTask, applyExecutor);
                } else {
                    // 无快照时，持久化和应用可并行执行
                    CompletableFuture.runAsync(applyTask, applyExecutor);
                }
            }
            default -> sendPeerMessage(m); // ③ peer 消息立即发送
        }
    }
    // readyLoop 线程立即返回，不调用 advance()，不等待持久化/应用完成
}
```

关键点：readyLoop 只做 **分发** —— 将 `MsgStorageAppend` 提交到持久化线程，将 `MsgStorageApply` 提交到应用线程。所有 I/O 操作都发生在工作线程上。

### 3.4 handleStorageAppend：在专用线程上持久化

持久化线程从 **MsgStorageAppend 消息本身**提取 entries、HardState 和 snapshot，完成持久化后投递附带的 Responses：

```java
private void handleStorageAppend(Eraftpb.Message m) {
    // 从 MsgStorageAppend 的字段中重建 HardState（term/vote/commit）
    Eraftpb.HardState hs = (m.getTerm() != 0 || m.getVote() != 0 || m.getCommit() != 0)
            ? Eraftpb.HardState.newBuilder()
                    .setTerm(m.getTerm()).setVote(m.getVote()).setCommit(m.getCommit()).build()
            : Eraftpb.HardState.getDefaultInstance();

    // 持久化 entries + hardState + snapshot
    storage.writeBatched(m.getEntriesList(), hs, m.getSnapshot());

    // 若有快照，恢复到状态机
    applySnapshotToStateMachine(m.getSnapshot());

    // 持久化完成后，投递附带的响应
    for (Eraftpb.Message resp : m.getResponsesList())
        deliverOrSend(resp);
}
```

**HardState 编码方式：** protobuf `Message` 类型有 `term`、`vote` 和 `commit` 字段。RawNode 在构建 `MsgStorageAppend` 时将 HardState 写入这些字段（见 `RawNode.newStorageAppendMsg()`），因此应用层从中重建 HardState。

### 3.5 handleStorageApply：在专用线程上应用

应用线程从 **MsgStorageApply 消息**中取出 committedEntries 应用到状态机，完成后投递 `MsgStorageApplyResp`：

```java
private void handleStorageApply(Eraftpb.Message m) {
    // 从 MsgStorageApply 中应用 committedEntries 到状态机
    long highestApplied = applyEntries(m.getEntriesList(), 0);

    // 排空等待 apply 的读请求
    drainWaitingReads(highestApplied);

    // 可能触发快照创建
    maybeSnapshot(highestApplied);

    // 应用完成后，投递 MsgStorageApplyResp
    for (Eraftpb.Message resp : m.getResponsesList())
        deliverOrSend(resp);
}
```

### 3.6 deliverOrSend：响应路由

`MsgStorageAppend` 的 `Responses` 中包含两类消息：
- **自回环消息**（`resp.getTo() == self`）：如 `MsgStorageAppendResp`，通过 `node.step()` 投递回 raft 核心
- **Peer 消息**（`resp.getTo() != self`）：如 `MsgAppResp`，通过 `transport.send()` 发给对端

```java
private void deliverOrSend(Eraftpb.Message resp) throws InterruptedException {
    if (resp.getTo() == id) {
        node.step(resp);      // 自回环：告知 raft 持久化/应用完成
    } else {
        sendPeerMessage(resp); // 发给 peer
    }
}
```

### 3.7 快照时的顺序保证

当 Ready 中包含快照时（`hasSnapshot = true`），持久化和应用 **不能** 并行执行 —— 快照必须先被持久化并恢复到状态机，然后才能应用后续的 committed entries。

这通过 `CompletableFuture` 链式调用来保证：

```java
if (hasSnapshot) {
    persistDone.thenRunAsync(applyTask, applyExecutor);
} else {
    CompletableFuture.runAsync(applyTask, applyExecutor);
}
```

普通场景（无快照）下，持久化和应用完全并行以获得最大吞吐。

### 3.8 CLI 与测试

命令行启动时通过 `--async-storage-writes` 开启：

```bash
java -cp ... KvServerBootstrap \
    --id=1 --raft-port=8081 --kv-port=9001 \
    --data-dir=/tmp/node1 \
    --peers=1=localhost:8081,2=localhost:8082,3=localhost:8083 \
    --bootstrap --async-storage-writes
```

集成测试使用 `@MethodSource` 参数化，覆盖三种组合：

| snapshotStreaming | asyncStorageWrites | 说明 |
|---|---|---|
| false | false | 同步 + inline 快照（基线） |
| true | false | 同步 + streaming 快照 |
| false | true | 异步 + inline 快照 |

## 4. 为什么异步模式是安全正确的

### 4.1 核心不变量：Responses 编码了因果依赖

异步模式的安全性来自一个关键设计：**RawNode 将所有因果依赖编码在 Responses 列表中**。

在同步模式下，`advance()` 内部回放 `stepsOnAdvance`（`MsgStorageAppendResp` + `MsgStorageApplyResp`）。这些自回环消息告诉 raft 核心「持久化/应用已完成」，raft 据此推进 commit/applied。

异步模式做的是 **同样的事**，只是换了载体：

| 同步模式 | 异步模式 |
|----------|----------|
| `acceptReady()` 收集 stepsOnAdvance | `readyWithoutAccept()` 生成 MsgStorageAppend/Apply |
| `advance()` 回放 `raft.step(appendResp)` | 应用持久化后投递 `node.step(MsgStorageAppendResp)` |
| `advance()` 回放 `raft.step(applyResp)` | 应用 apply 后投递 `node.step(MsgStorageApplyResp)` |

**Raft 核心收到的消息完全相同，状态转换完全一致**。区别仅在于消息的投递时机由 `advance()` 隐式触发变为应用显式投递。

### 4.2 持久化先于 Peer 响应：不可能丢失已承诺的数据

Raft 安全性的关键约束是：**节点在告知其他节点「我已持久化某日志」之前，必须确保该日志已落盘**。

在异步模式中，这由 `MsgStorageAppend.Responses` 的结构保证：

```java
// RawNode.newStorageAppendMsg() 构建逻辑
mb.addAllEntries(rd.entries());                    // 要持久化的 entries
mb.addAllResponses(r.msgsAfterAppend());           // MsgAppResp 等 peer 消息
mb.addResponses(newStorageAppendRespMsg(r, rd));    // 自回环 appendResp
```

`msgsAfterAppend`（如 `MsgAppendResponse`、`MsgVoteResponse`）被打包在 `MsgStorageAppend` 的 Responses 中。`handleStorageAppend` 的处理顺序是：

1. **先** `storage.writeBatched()` 持久化 entries
2. **后** 遍历 `m.getResponsesList()` 投递

因此 `MsgAppResp`（告知 Leader「我已持久化到 index N」）一定在本地持久化之后才被发送。这与同步模式中「持久化 → 发送 messages（其中包含 msgsAfterAppend）→ advance」的语义完全等价。

### 4.3 DefaultNode 事件循环的保证

`DefaultNode.run()` 中对异步模式的特殊处理（`DefaultNode.java:206-208`）：

```java
if (!rn.asyncStorageWrites) {
    waitingAdvance = true;    // 同步模式：阻塞下一个 Ready
}
// 异步模式：不设置 waitingAdvance，允许连续发射 Ready
```

这是安全的，因为：

- **raft 核心自身维护了正确的状态**：`acceptReady()` 调用 `raftLog.acceptUnstable()` 和 `raftLog.acceptApplying()` 标记了哪些 entries 正在处理中，`hasReady()` 不会重复输出相同的 entries
- **MsgStorageAppendResp 携带了 lastIndex/lastTerm**：raft 核心据此执行 `stableTo()`，将 entries 从 unstable 移入 stable，确保不会重复持久化
- **MsgStorageApplyResp 携带了已应用的 entries**：raft 核心据此推进 `raftLog.applied`，确保不会重复应用

### 4.4 三线程模型的线程安全保证

三个线程共享若干资源，均为并发安全：

| 共享资源 | 安全机制 |
|----------|----------|
| `pendingProposals`、`pendingConfChanges`、`peerAddresses` | `ConcurrentHashMap` |
| `waitingForApply` | `ConcurrentLinkedQueue` |
| `node.step()` | 通过 DefaultNode 内部事件队列序列化 |
| `transport.send()` | `GrpcTransport` 线程安全 |
| `RocksDbStorage` | RocksDB 内部线程安全 |
| `lastSnapshotIndex` | `volatile` 保证跨线程可见性 |

单线程的 `persistExecutor` 和单线程的 `applyExecutor` 各自保证了其工作项的顺序执行，避免了对 storage 和状态机的并发修改。

## 5. 与同步模式的完整对比

### 5.1 数据流对比

**同步模式：**

```
                    RawNode                          应用层
                    ───────                          ──────
readyWithoutAccept ─── 收割 entries/committed/msgs
                        构建 Ready
acceptReady ─────────── 更新 prev 状态
                        收集 stepsOnAdvance              ← advance 时回放
                        (appendResp, applyResp)

                                                     rd = node.ready()
                                                     storage.writeBatched(entries, hs, snap)
                                                     transport.send(messages)   ← 含 msgsAfterAppend
                                                     apply(committedEntries)
                                                     node.advance()
                                                       ↓
                    advance() ──── raft.step(appendResp)
                                   raft.step(applyResp)
                                   → 推进 stable/applied
```

**异步模式：**

```
                    RawNode                          readyLoop 线程
                    ───────                          ──────────────
readyWithoutAccept ─── 收割 entries/committed/msgs
                        构建 Ready
                        生成 MsgStorageAppend
                          entries + hardState + snapshot
                          responses: [msgsAfterAppend, appendResp]
                        生成 MsgStorageApply
                          entries: committedEntries
                          responses: [applyResp]
acceptReady ─────────── 更新 prev 状态
                        不收集 stepsOnAdvance

                                                     rd = node.ready()
                                                     // 只分发，不做 I/O
                                                     for m in messages:
                                                       MsgStorageAppend → 提交到 persistExecutor
                                                       MsgStorageApply  → 提交到 applyExecutor
                                                       其他消息         → transport.send(m)
                                                     // 立即返回，取下一个 Ready

                    persistExecutor 线程             applyExecutor 线程
                    ────────────────────             ──────────────────
                    storage.writeBatched(            apply(m.entries)
                      m.entries, hs, snap)           drainWaitingReads()
                    applySnapshot(snap)              maybeSnapshot()
                    for resp in responses:           for resp in responses:
                      deliverOrSend(resp)              deliverOrSend(resp)
                        ↓                                ↓
node.step(appendResp) ─── 推进 stable          node.step(applyResp) ─── 推进 applied
transport.send(MsgAppResp) → peer
```

### 5.2 对比总结

| 维度 | 同步模式 | 异步模式 |
|------|----------|----------|
| 启用方式 | 默认 | `Config.asyncStorageWrites(true)` |
| 完成通知 | `advance()` 隐式投递 | 应用显式投递 Responses |
| DefaultNode 背压 | `waitingAdvance = true` 阻塞下一 Ready | 不阻塞，允许流水线 |
| 持久化来源 | `rd.entries()` + `rd.hardState()` | `MsgStorageAppend.getEntriesList()` + 消息字段 |
| 应用来源 | `rd.committedEntries()` | `MsgStorageApply.getEntriesList()` |
| peer 响应（如 MsgAppResp） | 在 `rd.messages()` 中直接发送 | 在 `MsgStorageAppend.Responses` 中，持久化后发送 |
| `advance()` 调用 | 必须调用 | **禁止调用**（会抛异常） |
| 线程模型 | 单个 readyLoop 线程 | readyLoop + persistExecutor + applyExecutor |
| 并行潜力 | 无（严格串行） | 持久化与应用可并行 |

## 6. 常见误区

### 误区 1：在 readyLoop 线程上做持久化

**错误做法。** 如果在 readyLoop 线程上调用 `storage.writeBatched()`，然后只在工作线程上投递 `MsgStorageAppend` 的 Responses，那么 **并没有实现真正的异步** —— 持久化仍然会阻塞下一个 Ready 的获取。关键是：**持久化必须发生在 persistExecutor 线程上**，由 MsgStorageAppend 消息驱动。

### 误区 2：异步模式下可以调用 advance()

**禁止。** `RawNode.advance()` 在异步模式下直接抛出 `RaftInvariantException("Advance must not be called when using AsyncStorageWrites")`。因为 advance 的职责已被 Responses 消息取代，重复调用会导致状态混乱。

### 误区 3：MsgStorageAppend 的 Responses 可以不投递

**不可以。** Responses 中包含：
- `MsgStorageAppendResp`（自回环）：raft 核心需要它来执行 `stableTo()`，否则 unstable 日志永远不会被标记为 stable
- `MsgAppResp` / `MsgVoteResp` 等（发给 peers）：这些是必须在持久化之后才能发送的消息，不投递则 Leader 无法推进 commitIndex

### 误区 4：可以直接从 rd.messages() 发送 peer 消息

**不可以。** `msgsAfterAppend` 中的 peer 消息（如 Follower 的 `MsgAppendResponse`）被 **有意** 打包在 `MsgStorageAppend.Responses` 中。这些消息必须在持久化后发送 —— 这正是 `msgsAfterAppend` 存在的意义。如果直接从 `rd.messages()` 中发送它们（像同步模式那样），会绕过持久化保证。

### 误区 5：有快照时持久化和应用并行执行

**危险。** 当 Ready 中包含快照时，快照必须 **先被持久化并恢复到状态机**，然后才能应用 committed entries。应使用 `persistDone.thenRunAsync(applyTask, applyExecutor)` 来保证顺序。只有在无快照的场景下，持久化和应用才可以完全并行。
