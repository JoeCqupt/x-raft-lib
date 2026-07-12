# Raft / RawNode / Node 三层架构

[English](raft-node-layers.md) | [中文](raft-node-layers.zh.md)

x-raft-lib 的核心 API 分为三层，从底到顶依次是：**Raft** (纯状态机) → **RawNode** (单线程 Ready 管理) → **Node / DefaultNode** (线程安全封装)。上层应用根据自身的线程模型选择在哪一层接入。

## 分层总览

```
┌─────────────────────────────────────────────────────────┐
│  应用层 (RaftKVNode / 你的应用)                           │
│  定时 tick() · 投递 propose() / step()                   │
│  消费 ready() → 持久化 → 发送 → 应用 → advance()          │
└───────────────────────┬─────────────────────────────────┘
                        │  Node 接口 (线程安全)
                        ▼
┌─────────────────────────────────────────────────────────┐
│  DefaultNode                                            │
│  单线程事件循环，有界队列 (1024)，背压                      │
│  Tick burst 限制 (128)，Drain 模型                       │
│  Leader change 通知，Lock-free liveness mirror           │
└───────────────────────┬─────────────────────────────────┘
                        │  独占调用 (事件循环线程)
                        ▼
┌─────────────────────────────────────────────────────────┐
│  RawNode                                                │
│  管理 Ready 生命周期：hasReady → ready → advance          │
│  维护 prevSoftSt / prevHardSt 实现增量 Ready              │
│  处理 AsyncStorageWrites 合成消息                         │
└───────────────────────┬─────────────────────────────────┘
                        │  raft.step() / raft.tick()
                        ▼
┌─────────────────────────────────────────────────────────┐
│  Raft (internal)                                        │
│  纯状态机：选举 · 日志复制 · 提交推进 · 心跳 · ReadIndex    │
│  零 I/O、零网络、零定时器                                  │
│  输出积累在 msgs / msgsAfterAppend / readStates           │
└─────────────────────────────────────────────────────────┘
```

## 各层职责

### Raft — 纯共识状态机

**位置：** `raft-core/.../internal/Raft.java`（package-private 字段，跨包通过访问器读取）

Raft 类实现了完整的 Raft 共识算法，是整个库的计算核心。它不做任何 I/O —— 不写磁盘、不发网络、不管定时器。

**核心状态：**

| 字段 | 说明 |
|------|------|
| `term` / `vote` / `lead` | 当前任期、投票、已知 Leader |
| `state` | 角色：Leader / Follower / Candidate / PreCandidate |
| `raftLog` | 日志管理器（Unstable + Storage 两层） |
| `trk` (ProgressTracker) | 每个 Follower 的复制进度、Inflights 流控 |
| `msgs` / `msgsAfterAppend` | 累积的待发送消息 |
| `readStates` | ReadIndex 结果队列 |

**角色切换：** 通过函数指针 `tickFn` / `stepFn` 实现零 if-else 分发：

```
becomeFollower()  → tickFn = tickElection,  stepFn = stepFollower
becomeCandidate() → tickFn = tickElection,  stepFn = stepCandidate
becomeLeader()    → tickFn = tickHeartbeat, stepFn = stepLeader
```

**输入/输出模型：**

- **输入：** `tick()` 推进逻辑时钟，`step(Message)` 投递消息（本地或远程）
- **输出：** 不主动推送。输出积累在内部列表中，等待 RawNode 在 Ready 周期中收割

### RawNode — 单线程 Ready 管理器

**位置：** `raft-core/.../RawNode.java`（public API）

RawNode 是 Raft 的薄封装层，不引入任何并发机制。它的核心职责是管理 **Ready 生命周期** —— 将 Raft 的内部状态变化打包成一个不可变的 `Ready` 快照，交给上层处理。

**关键方法：**

| 方法 | 职责 |
|------|------|
| `tick()` | 直接委托 `raft.tick()` |
| `propose(data)` | 构造 MsgPropose，调用 `raft.step()` |
| `step(msg)` | 校验消息来源后委托 `raft.step()` |
| `hasReady()` | 检查是否有新的输出需要处理 |
| `readyWithoutAccept()` | 从 Raft 收割所有输出，构建 Ready |
| `acceptReady(rd)` | 更新 prev 状态，清空 Raft 缓冲区 |
| `advance(rd)` | 回放 stepsOnAdvance，通知 Raft 持久化已完成 |
| `ready()` | `readyWithoutAccept()` + `acceptReady()` 的组合 |

**增量检测：** RawNode 维护 `prevSoftSt` 和 `prevHardSt`，只有当 Raft 的 SoftState/HardState 与上一次不同时才包含在 Ready 中，避免上层重复处理。

**防御性拷贝：** `readyWithoutAccept()` 对 `unstableEnts` 和 `committedEnts` 做 `new ArrayList<>(...)` 拷贝。因为 Raft 内部的 unstable buffer 是 subList 视图，后续 step() 可能触发 `stableTo()` 修改底层列表，导致 `ConcurrentModificationException`。

### Node / DefaultNode — 线程安全封装

**位置：** `Node.java`（public 接口）+ `internal/DefaultNode.java`（实现）

DefaultNode 用一个独立的事件循环线程封装 RawNode，让多个生产者线程可以安全地并发调用 `propose()`、`step()`、`tick()` 等方法。

**工厂方法：**

```java
Node n = Node.startNode(config, peers);    // 首次启动
Node n = Node.restartNode(config);          // 从 Storage 恢复
```

两个方法内部都是：`new RawNode()` → `bootstrap()`（仅 startNode）→ 启动事件循环线程。

**事件队列：**

所有外部 API 调用被转换为 Event 对象，放入一个有界的 `LinkedBlockingQueue<Event>`（默认容量 1024）。事件循环线程是唯一消费者。

| Event 类型 | 来源 | 入队方式 | 结果通知 |
|------------|------|----------|----------|
| `TickEvent` | `tick()` | `offer()`（非阻塞，溢出丢弃） | 无 |
| `ProposeEvent` | `propose()` | `put()`（阻塞） | `CompletableFuture<Void>` |
| `RecvEvent` | `step()` / `campaign()` | `put()`（阻塞） | 无（火忘） |
| `ConfChangeEvent` | `applyConfChange()` | `put()`（阻塞） | `CompletableFuture<ConfState>` |
| `StatusEvent` | `status()` | `put()`（阻塞） | `CompletableFuture<Status>` |
| `WakeEvent` | `advance()` | `offer()`（非阻塞） | 无（唤醒信号） |

**Tick 特殊处理：** Tick 使用 `offer()` 而非 `put()`，避免定时器线程被阻塞。同时通过 `AtomicInteger pendingTicks` 做 burst 限制（上限 128），超出后丢弃并告警。这对应 etcd-raft 中 `tickc` 的 buffered channel 语义。

**Drain 模型：**

```
while (!stopped) {
    ① 如果 rn.hasReady() 且不在等 advance → 发射 Ready 到 readyc
    ② 检测 leader 变化 → 通知 observers + 更新 liveness mirror
    ③ events.take() 阻塞等第一个事件
    ④ events.poll() 非阻塞批量取最多 128 个后续事件
    ⑤ 每个事件处理后检查 advancePending 标志
}
```

这个模型的关键设计：先发射 Ready 再处理事件，使得前一批 drain 积累的多个 propose 可以合并到一个 Ready 中，实现 group commit —— 一次 fsync 持久化多条日志。

**Advance 机制：** 应用调用 `advance()` 时，不直接入事件队列（避免与 propose/step 竞争队列容量），而是设置 `AtomicBoolean advancePending` 标志 + `offer(WakeEvent)` 唤醒事件循环。事件循环在每次 take/poll 后都会检查这个标志。

**Lock-free Liveness Mirror：** `basicStatus()` 通过 `AtomicLong` 字段（`liveTerm`、`liveLead`、`liveCommit` 等）提供 best-effort 的状态快照，不走事件队列。用于 health endpoint 和 liveness probe，即使事件循环卡死也能响应。

## Ready 数据流

`Ready` 是 Raft 状态机的一次输出快照，包含上层需要处理的所有内容：

| 字段 | 说明 | 处理顺序 |
|------|------|----------|
| `softState` | Leader 和角色变化（非持久化） | 仅通知 |
| `hardState` | term / vote / commit（需持久化） | ① 持久化 |
| `entries` | 未持久化的日志条目 | ① 持久化 |
| `snapshot` | 快照（需持久化） | ① 持久化 |
| `messages` | 待发送的消息 | ② 发送 |
| `readStates` | ReadIndex 结果 | ③ 通知等待中的读请求 |
| `committedEntries` | 已提交待应用的日志条目 | ④ 应用到状态机 |
| `mustSync` | 是否需要 fsync | ① 持久化时参考 |

**处理顺序很重要：** 必须先持久化 entries 和 hardState，再发送 messages。否则节点崩溃后可能丢失已告知其他节点「我已持久化」的日志条目，破坏 Raft 安全性。

### Ready 生命周期（sync 模式）

```
上层应用                    RawNode                          Raft
────────                    ───────                          ────
                 ┌─── hasReady() ────────── 比较 softState/hardState
                 │                          检查 msgs, readStates,
                 │                          unstableEnts, committedEnts
                 │
                 │    readyWithoutAccept() ─── 防御性拷贝所有输出
                 │                             构建 Ready record
                 │
rd = ready() ◄───┤    acceptReady(rd) ─────── 更新 prevSoftSt/prevHardSt
                 │                             drainMsgs()
                 │                             raftLog.acceptUnstable()
                 └                             收集 stepsOnAdvance

persist(rd)
send(rd.messages)
apply(rd.committedEntries)

advance(rd) ───────── advance(rd) ─────────── 回放 stepsOnAdvance
                                               raft.step(appendResp)
                                               raft.step(applyResp)
```

### Ready 生命周期（async 模式）

AsyncStorageWrites 模式下，`advance()` 不再由应用显式调用。RawNode 在 `readyWithoutAccept()` 中自动生成合成消息：

- `MsgStorageAppend` → 通知上层持久化 entries + hardState + snapshot
- `MsgStorageApply` → 通知上层应用 committedEntries

上层完成后通过 `step()` 投递对应的响应消息：

- `MsgStorageAppendResp` → 告知 Raft 持久化完成
- `MsgStorageApplyResp` → 告知 Raft 应用完成

这使得持久化和应用可以在不同线程中并行执行，提高吞吐。DefaultNode 在异步模式下不设置 `waitingAdvance` 标志，允许连续发射多个 Ready。

`raft-examples` 中的 `RaftKVNode` 通过 `--async-storage-writes` 开关演示了完整的异步模式用法。详见 [异步存储写入文档](async-storage-writes.zh.md)。

## 两种使用模式

### 模式一：直接使用 RawNode

适合已有事件循环或 actor 框架的场景。应用自己管理线程，在循环中驱动 RawNode。

```java
RawNode rn = RawNode.newRawNode(config);
rn.bootstrap(List.of(new Peer(1), new Peer(2), new Peer(3)));

while (running) {
    rn.tick();
    for (Message msg : received) rn.step(msg);

    if (rn.hasReady()) {
        Ready rd = rn.ready();
        storage.writeBatched(rd.entries(), rd.hardState(), rd.snapshot());
        transport.send(rd.messages());
        stateMachine.apply(rd.committedEntries());
        rn.advance(rd);
    }
}
```

### 模式二：通过 Node / DefaultNode（推荐）

生产环境推荐。DefaultNode 提供线程安全的 API 和内置的事件循环。

```java
Node node = Node.startNode(config, List.of(new Peer(1), new Peer(2), new Peer(3)));

// 定时器线程：定时驱动逻辑时钟
scheduler.scheduleAtFixedRate(node::tick, 100, 100, TimeUnit.MILLISECONDS);

// 网络接收线程：投递入站消息
transport.setReceiver(msg -> node.step(msg));

// 应用线程：消费 Ready
while (running) {
    Ready rd = node.ready();                           // 阻塞等待
    storage.writeBatched(rd.entries(), rd.hardState(), rd.snapshot());
    transport.send(rd.messages());
    stateMachine.apply(rd.committedEntries());
    node.advance();                                    // 通知完成
}

node.stop();
```

`raft-examples` 中的 `RaftKVNode` 就是模式二的完整实现，包含了 propose 跟踪、ReadIndex、ConfChange、快照管理等完整的生产逻辑。

## 关键设计决策

### 为什么分三层而不是两层？

Raft 状态机必须保持纯粹 —— 零 I/O、确定性、可测试。但直接暴露 Raft 会要求使用者理解 `msgs` / `msgsAfterAppend` 分离、unstable 日志管理等内部细节。RawNode 作为中间层封装了这些复杂性，提供了干净的 `Ready` 抽象。

DefaultNode 再加一层是因为大多数应用需要多线程安全 —— 网络回调线程要能调用 `step()`，定时器线程要能调用 `tick()`，同时 Ready 消费在另一个线程。用事件循环把这些序列化到单线程，既安全又高效。

### 为什么 msgs 和 msgsAfterAppend 要分开？

`msgs` 包含 MsgAppend、MsgHeartbeat 等消息，可以在持久化之前发送。`msgsAfterAppend` 包含 MsgAppendResponse、MsgVoteResponse 等消息，必须在本地日志持久化之后才能发送。RawNode 在构建 Ready 时将两者合并到 `messages` 字段中，但内部保证了正确的排序 —— 上层只需按顺序发送即可。

### 为什么 advance() 要回放 stepsOnAdvance？

在 sync 模式下，Raft 需要知道持久化和应用已经完成，才能推进 commitIndex 和 appliedIndex。`acceptReady()` 收集了需要在 advance 时投递的自回环消息（`MsgStorageAppendResp`、`MsgStorageApplyResp`），`advance()` 将它们通过 `raft.step()` 投递回状态机，完成闭环。
