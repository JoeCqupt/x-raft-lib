# x-raft-lib 代码问答（Code Q&A）

[English](code-qa.md) | [中文](code-qa.zh.md)

本文档整理了围绕 x-raft-lib 核心代码的一系列问答，聚焦于 **Ready 消息机制**、**异步存储写入**、**快照来源** 以及 **Joint Consensus 配置变更** 等易混淆的实现细节。每个问题都附带源码定位，便于对照阅读。

---

## 目录

- [一、Ready 消息机制](#一ready-消息机制)
  - [Q1: `msgsAfterAppend` 是干什么的？](#q1-msgsafterappend-是干什么的)
  - [Q2: 为什么 Ready 输出的 `messages` 把两种消息都放进去了？](#q2-为什么-ready-输出的-messages-把两种消息都放进去了)
  - [Q3: `asyncStorageWrites` 分支在做什么？](#q3-asyncstoragewrites-分支在做什么)
- [二、Ready 快照来源](#二ready-快照来源)
  - [Q4: Ready 里的 snapshot 是如何来的？](#q4-ready-里的-snapshot-是如何来的)
- [三、Joint Consensus 配置变更](#三joint-consensus-配置变更)
  - [Q5: 对于配置变更，`completeConfChangeFuture` 是否没有意义？](#q5-对于配置变更completeconfchangefuture-是否没有意义)
  - [Q6: `checkSelfRemoval` 为什么不检查 `learners_next`？](#q6-checkselfremoval-为什么不检查-learners_next)
  - [Q7: `checkSelfRemoval` 会在 joint 第一步执行吗？（附修复方案）](#q7-checkselfremoval-会在-joint-第一步执行吗附修复方案)
  - [Q8: `appliedTo` 中的 autoLeave 代码在干什么？](#q8-appliedto-中的-autoleave-代码在干什么)
  - [Q9: 整个库只有一个地方触发离开 joint 共识吗？](#q9-整个库只有一个地方触发离开-joint-共识吗)

---

## 一、Ready 消息机制

### Q1: `msgsAfterAppend` 是干什么的？

`msgsAfterAppend` 是 Raft 核心中的一个**延迟发送消息队列**，用于存放那些**必须在日志持久化（append）之后才能发送**的消息。

在 `Raft.send()` 中，以下三种响应消息会被放入 `msgsAfterAppend` 而非普通的 `msgs`：

| 消息类型 | 为什么必须等持久化后再发 |
|---------|------------------------|
| `MsgAppendResponse` | Follower 必须先把日志落盘，才能告诉 Leader "已收到"，否则 Leader 可能过早推进 commitIndex |
| `MsgRequestVoteResponse` | 必须先持久化投票记录（防止重启后重复投票），才能回复投票结果 |
| `MsgRequestPreVoteResponse` | 预投票响应，同理 |

**与 `msgs` 的区别：**

- `msgs`：立即可发送（如 `MsgAppend`、`MsgHeartbeat`、`MsgSnapshot`）
- `msgsAfterAppend`：必须等日志 / 状态落盘后才能发送的响应

**源码位置：** `Raft.send()`（`raft-core/.../internal/Raft.java`）

---

### Q2: 为什么 Ready 输出的 `messages` 把两种消息都放进去了？

`msgs` 和 `msgsAfterAppend` 确实在 `Ready.messages` 中被合并到同一个列表，但这是**刻意且安全**的设计。

在 `RawNode.readyWithoutAccept()` 中：

```java
// 先放入 msgs（立即可发的消息）
List<Eraftpb.Message> messages = new ArrayList<>(r.msgs());
...
} else {
    // 再把 msgsAfterAppend 中"目标不是自己"的消息追加进去
    for (Eraftpb.Message m : r.msgsAfterAppend()) {
        if (m.getTo() != r.id()) {
            messages.add(m);
        }
    }
}
```

**"延迟发送"的语义由 Ready 处理契约保证：** 应用层拿到 Ready 后，处理顺序固定为「先持久化 `entries` + `hardState`，再发送 `messages`」。因此整个 `messages` 列表本身就是在持久化之后才发送的，合并到同一列表是安全的。

**发给自己的消息如何处理？** 注意过滤条件 `m.getTo() != r.id()`——发给自己的消息不会进入 `Ready.messages`，而是在 `acceptReady()` 中被收集到 `stepsOnAdvance`，等应用层调用 `advance()` 时才 `step()` 回状态机。

| 消息 | 去向 | 处理时机 |
|------|------|---------|
| `msgs`（如 MsgAppend） | 远端节点 | Ready.messages，持久化后发送 |
| `msgsAfterAppend` 发给远端的 | 远端节点 | 也在 Ready.messages，持久化后发送 |
| `msgsAfterAppend` 发给自己的 | 自身 | `stepsOnAdvance`，`advance()` 时回放 |

**源码位置：** `RawNode.readyWithoutAccept()` / `RawNode.acceptReady()`

---

### Q3: `asyncStorageWrites` 分支在做什么？

这是**异步存储写入模式**的处理逻辑，与默认同步模式完全不同。

- **默认模式（`asyncStorageWrites = false`）：** 应用层按顺序同步处理 → 持久化 → 发消息 → 应用 → `advance()`。
- **异步模式（`asyncStorageWrites = true`）：** Raft 把工作打包成消息发给虚拟本地线程，持久化与状态机应用可以**并行、异步**进行，且**不再调用 `advance()`**。

```java
if (asyncStorageWrites) {
    if (needStorageAppendMsg(r, rd)) {
        messages.add(newStorageAppendMsg(r, rd));   // 发给 LOCAL_APPEND_THREAD
    }
    if (needStorageApplyMsg(rd)) {
        messages.add(newStorageApplyMsg(r, rd));    // 发给 LOCAL_APPLY_THREAD
    }
}
```

**`MsgStorageAppend` 的结构（见 `newStorageAppendMsg()`）：**

```
MsgStorageAppend {
    to: LOCAL_APPEND_THREAD
    entries / hardState / snapshot: 需持久化的内容
    responses: [
        msgsAfterAppend 中的消息,   ← 持久化完才能发的响应
        MsgStorageAppendResp        ← 通知 Raft 持久化完成
    ]
}
```

**核心设计意图：** 把"先持久化、再发响应"的因果约束**编码在消息结构中**——`msgsAfterAppend` 被嵌入 `MsgStorageAppend.responses`，应用层完成持久化后才递送这些 responses，天然保证顺序，同时让持久化和应用得以并行以提升吞吐。

**源码位置：** `RawNode.newStorageAppendMsg()` / `RawNode.newStorageApplyMsg()`

---

## 二、Ready 快照来源

### Q4: Ready 里的 snapshot 是如何来的？

`Ready.snapshot` 完全来自 **`Unstable`（不稳定日志）中缓存的待持久化快照**。

**来源链路：**

```
Ready.snapshot
  ← RawNode.readyWithoutAccept()  (b.snapshot(...))
  ← raftLog.nextUnstableSnapshot()
  ← unstable.nextSnapshot()
  ← unstable.snapshot 字段
```

**快照唯一进入 unstable 的入口：** Follower 收到 Leader 的 `MsgSnapshot` → `handleSnapshot()` → `restore()` → `raftLog.restore(s)` → `Unstable.restore(s)`：

```java
public void restore(Eraftpb.Snapshot s) {
    offset = s.getMetadata().getIndex() + 1;
    offsetInProgress = offset;
    entries = new ArrayList<>();
    snapshot = s;                 // 快照存入 unstable.snapshot
    snapshotInProgress = false;   // 标记为"未开始持久化"
}
```

**生命周期：**

| 阶段 | 动作 | 状态变化 |
|------|------|---------|
| 1. 收到 MsgSnapshot | `restore(s)` | `snapshot=s`, `snapshotInProgress=false` |
| 2. 生成 Ready | `nextSnapshot()` 返回 s | 放入 `Ready.snapshot` |
| 3. acceptReady | `acceptInProgress()` | `snapshotInProgress=true`（防重复输出）|
| 4. 应用层落盘完 | `stableSnapTo(index)` | `snapshot=null`（清除）|

**关键点：**

1. **只有 Follower 会产生 `Ready.snapshot`**——Leader 从不 restore 自己的快照，它是发送方（`maybeSendSnapshot`）。
2. **`snapshotInProgress` 防止重复**——一旦 Ready 被 accept，同一快照不会在下一轮 Ready 再次出现。
3. **与 Leader 侧区分**：`Ready.snapshot` 是"本 Follower 需要安装的快照"；Leader 发给别人的快照走 `msgs` 中的 `MsgSnapshot`，两者完全不同。

**源码位置：** `Unstable.nextSnapshot()` / `Raft.handleSnapshot()` / `Raft.restore()`

---

## 三、Joint Consensus 配置变更

### Q5: 对于配置变更，`completeConfChangeFuture` 是否没有意义？

**仍然有意义**，但它的语义不是"配置变更彻底生效"，而是"**用户提交的那条 ConfChange 日志已被 apply**"。

Joint Consensus 下一次配置变更实际是两条日志：

```
第1条: 用户提交的 ConfChangeV2（进入 joint，带用户 context）
第2条: 自动生成的空 ConfChangeV2（离开 joint，context 为空）
```

`completeConfChangeFuture` 在**第1条日志 apply 时**就触发。第2条是 Raft 内部自动生成、context 为空，因此 `completeConfChangeFuture` 中 `context.size() < 8` 会直接 return，不会匹配任何 pending future。

**这个设计是合理的，因为：**

1. 与普通 propose 语义一致——entry 被 apply 即 complete，不等后续效果；
2. 第2步是自动且确定发生的（只要 Leader 存活）；
3. 进入 joint 状态已意味着新配置开始参与投票，第2步只是清理。

若需要**严格等待离开 joint**，则要额外机制（轮询 ConfState 或第2条 apply 时通知），当前实现不提供该保证。

**源码位置：** `RaftKVNode.completeConfChangeFuture()`（`raft-examples`）

---

### Q6: `checkSelfRemoval` 为什么不检查 `learners_next`？

因为存在一条严格不变量：**`learners_next ⊆ voters_outgoing`**，检查 `voters_outgoing` 已隐含覆盖 `learners_next`。

`learners_next` 里的节点是"当前仍是 outgoing config 的 voter、将在离开 joint 后变成 learner"的节点。在 `Changer.makeLearner()` 中：

```java
if (cfg.getVoters().outgoing().contains(id)) {
    cfg.getLearnersNext().add(id);   // 仍是 outgoing voter → 进 LearnersNext
} else {
    cfg.getLearners().add(id);
}
```

`Changer.checkInvariants()` 强制校验：

```java
for (long id : cfg.getLearnersNext()) {
    if (!cfg.getVoters().outgoing().contains(id)) {
        throw new IllegalStateException(id + " is in LearnersNext, but not Voters[1]");
    }
}
```

因此，若节点在 `learners_next`，它一定也在 `voters_outgoing`，`checkSelfRemoval` 中检查 `voters_outgoing` 已足够完备。

**源码位置：** `Changer.makeLearner()` / `Changer.checkInvariants()`（`raft-core/.../internal/confchange`）

---

### Q7: `checkSelfRemoval` 会在 joint 第一步执行吗？（附修复方案）

**会执行，但不会触发 shutdown**——这里存在一个需要修复的不完备之处。

**enterJoint（第1步）：** 移除节点 3 时，ConfState 为 `voters=[1,2]`、`voters_outgoing=[1,2,3]`。此时 `removingSelf=true`，但因节点仍在 `voters_outgoing`，不 shutdown（正确）。

**leaveJoint（第2步）：** Raft 自动提交**空** ConfChangeV2，其 `changesList` 为空 → `removingSelf=false` → 原代码直接 return，**节点永远不会主动 shutdown**（缺陷）。

**修复方案：** 当 `changesList` 为空（即 leaveJoint）时也执行检查：

```java
private void checkSelfRemoval(Eraftpb.ConfChangeV2 cc, Eraftpb.ConfState cs) {
    boolean removingSelf = false;
    for (Eraftpb.ConfChangeSingle change : cc.getChangesList()) {
        if (change.getNodeId() == id
                && change.getType() == Eraftpb.ConfChangeType.ConfChangeRemoveNode) {
            removingSelf = true;
            break;
        }
    }

    // leaveJoint: 空的 ConfChangeV2 表示离开 joint 状态
    boolean isLeaveJoint = cc.getChangesList().isEmpty();

    if (!removingSelf && !isLeaveJoint) return;

    if (!cs.getVotersList().contains(id)
            && !cs.getLearnersList().contains(id)
            && !cs.getVotersOutgoingList().contains(id)) {
        LOG.info("node {} removed from cluster, shutting down", id);
        removedFuture.complete(null);
        running = false;
    }
}
```

**兼容性验证（ConfV1 与 ConfV2 全场景）：**

| 场景 | `removingSelf` | `isLeaveJoint` | 结果 |
|------|:-:|:-:|------|
| ConfV1 单步移除自己 | true | false | ConfState 中已消失 → shutdown |
| ConfV2 enterJoint（移除自己）| true | false | 仍在 voters_outgoing → 不 shutdown |
| ConfV2 leaveJoint（空 changes）| false | true | 不在任何集合 → shutdown |
| ConfV2 无关变更（如 AddNode）| false | false | 直接 return，无副作用 |

> 注：`changesList` 为空的 ConfChangeV2 在 Raft 协议中只有 leaveJoint 一种语义，普通变更至少含一个 change，故不会误判。

**源码位置：** `RaftKVNode.checkSelfRemoval()`（`raft-examples`，已应用修复）

---

### Q8: `appliedTo` 中的 autoLeave 代码在干什么？

这段代码负责**自动发起 joint consensus 的第二步——离开联合配置**。

```java
if (trk.getConfig().isAutoLeave() && newApplied >= pendingConfIndex && state == RaftStateType.StateLeader) {
    Eraftpb.Message m = Changer.toMessage(null);
    try {
        step(m);
        logger.info("initiating automatic transition out of joint configuration {}", trk.getConfig());
    } catch (RaftException err) {
        logger.debug("not initiating automatic transition out of joint configuration {}: {}", trk.getConfig(), err);
    }
}
```

**触发条件（三者都要满足）：**

1. `isAutoLeave()`——当前处于 joint 且标记了自动离开（仅 Auto / JointImplicit 模式为 true）；
2. `newApplied >= pendingConfIndex`——进入 joint 的第1条日志已被 apply；
3. `state == StateLeader`——只有 Leader 能 propose。

**动作：** `Changer.toMessage(null)` 构造一条 **data 为空的 `EntryConfChangeV2` 类型 MsgPropose**，通过 `step()` 提交。这条空 ConfChangeV2 就是 leaveJoint 信号。

**为什么失败只记 debug？** 该自动 propose 允许失败（如恰有其他 pending 变更时会被 drop）。下一次 `appliedTo` 会再次尝试，直到成功离开 joint，因此失败是可自愈的、预期内的。

**源码位置：** `Raft.appliedTo()` / `Changer.toMessage()`

---

### Q9: 整个库只有一个地方触发离开 joint 共识吗？

需区分**执行离开**与**发起离开**两个层面。

**实际执行离开：只有 1 处**——`Raft.applyConfChange()` 中：

```java
if (leaveJoint(cc)) {
    cr = changer.leaveJoint();   // 唯一执行离开 joint 的地方
}
```

任何路径最终都要在 apply 一条空 ConfChangeV2 时经过这里。

**发起离开：有 2 个入口：**

1. **自动发起**——`Raft.appliedTo()` 中的 `Changer.toMessage(null)`（仅 `autoLeave=true` 模式，即 Q8 的代码）；
2. **手动发起**——`ConfChangeTransitionJointExplicit`（显式）模式下 `autoLeave=false`，自动触发不会执行，用户须通过 `RawNode.proposeConfChange()` / `DefaultNode` 自行 propose 一个空 ConfChangeV2。

| 维度 | 数量 | 位置 |
|------|:-:|------|
| 实际执行离开（`changer.leaveJoint()`）| 1 处 | `Raft.applyConfChange()` |
| 自动发起离开 | 1 处 | `Raft.appliedTo()`（仅 autoLeave 模式）|
| 手动发起离开 | 通用 propose 通道 | explicit 模式下用户自行 propose 空 ConfChangeV2 |

**结论：** 若指"Raft 自动离开 joint"，确实只有 `appliedTo` 这一处；但 explicit 模式下用户可手动发起，两条发起路径最终都汇聚到唯一的 `changer.leaveJoint()` 执行点。

**源码位置：** `Raft.applyConfChange()` / `Raft.appliedTo()` / `RawNode.proposeConfChange()`

---

## 相关文档

- [架构与设计](architecture.zh.md)
- [Raft / RawNode / Node 三层架构](raft-node-layers.zh.md)
- [快照流程](snapshot-flow.zh.md)
