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
- [四、日志复制流程](#四日志复制流程)
  - [Q10: Leader 首次启动后 `maybeSendAppend` 如何执行？](#q10-leader-首次启动后-maybesendappend-如何执行)
  - [Q11: `handleAppendEntries` 中 Follower 如何处理日志？](#q11-handleappendentries-中-follower-如何处理日志)
  - [Q12: Leader 收到 `MsgAppendResponse` 后做了什么？](#q12-leader-收到-msgappendresponse-后做了什么)
  - [Q13: `maybeDecrTo` 回退的设计思路是什么？](#q13-maybedecrto-回退的设计思路是什么)
  - [Q14: Leader 收到 `MsgHeartbeatResponse` 后做了什么？](#q14-leader-收到-msgheartbeatresponse-后做了什么)
- [五、Unstable 与持久化](#五unstable-与持久化)
  - [Q15: `offsetInProgress` 是干什么的？](#q15-offsetinprogress-是干什么的)
  - [Q16: 能否让不依赖持久化的消息先发送？etcd 有这个优化吗？](#q16-能否让不依赖持久化的消息先发送etcd-有这个优化吗)
- [六、配置变更提案校验](#六配置变更提案校验)
  - [Q17: Leader 处理 `MsgPropose` 时对 ConfChange 做了哪些校验？](#q17-leader-处理-msgpropose-时对-confchange-做了哪些校验)
- [七、线性一致读 ReadIndex](#七线性一致读-readindex)
  - [Q18: `heartbeatCtx` 与 `recvAck` 在 ReadIndex 中的作用？](#q18-heartbeatctx-与-recvack-在-readindex-中的作用)
- [八、启动与初始化](#八启动与初始化)
  - [Q19: bootstrap 为什么会“添加两次”配置变更？](#q19-bootstrap-为什么会添加两次配置变更)
  - [Q20: bootstrap 为什么可以直接设置 committed，不需多数派确认？](#q20-bootstrap-为什么可以直接设置-committed不需多数派确认)
  - [Q21: `newRaft()` 中 `Changer.restore` 在做什么？](#q21-newraft-中-changerrestore-在做什么)
  - [Q22: Follower 节点何时变为 active（recentActive 生命周期）？](#q22-follower-节点何时变为-activerecentactive-生命周期)
- [九、配置变更内部机制](#九配置变更内部机制)
  - [Q23: `applyConfChange` 的完整逻辑是什么？](#q23-applyconfchange-的完整逻辑是什么)
  - [Q24: `ProgressTracker.Config` 里有什么？](#q24-progresstrackerconfig-里有什么)
  - [Q25: `learnersNext` 是计算得到的还是参数传入的？](#q25-learnersnext-是计算得到的还是参数传入的)
- [十、流式快照完整流程](#十流式快照完整流程)
  - [Q26: 流式快照如何创建？生成什么文件？](#q26-流式快照如何创建生成什么文件)
  - [Q27: Leader 何时、如何发送快照？](#q27-leader-何时如何发送快照)
  - [Q28: Follower 如何接收并 stage 快照？](#q28-follower-如何接收并-stage-快照)
  - [Q29: Follower 如何 apply snapshot（restore → writeBatched）？](#q29-follower-如何-apply-snapshotrestore--writebatched)
  - [Q30: 如何 advance？side-car 快照文件何时删除？](#q30-如何-advanceside-car-快照文件何时删除)
- [十一、存储实现细节与并发](#十一存储实现细节与并发)
  - [Q31: side-car 临时文件不怕重名吗？存在并发竞态吗？](#q31-side-car-临时文件不怕重名吗存在并发竞态吗)
  - [Q32: `applySnapshot` 什么时候使用？为何代码里没看到调用？](#q32-applysnapshot-什么时候使用为何代码里没看到调用)
  - [Q33: `createSnapshot` 和 `applySnapshot` 的使用方式是什么？只有 Leader 创建快照、只有 Follower 安装快照吗？](#q33-createsnapshot-和-applysnapshot-的使用方式是什么只有-leader-创建快照只有-follower-安装快照吗)

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

## 四、日志复制流程

### Q10: Leader 首次启动后 `maybeSendAppend` 如何执行？

`becomeLeader()` 中会先 `reset()`，再对**自身** Progress 做特殊处理，然后追加一条 noop entry。

**reset() 后各节点 Progress 初始状态：**

```
match = 0（非自身）/ lastIndex（自身）
next  = lastIndex + 1
state = StateProbe
recentActive = false
```

**Leader 对自身的特殊处理：**

```java
Progress pr = trk.getProgress().get(id);
pr.becomeReplicate();      // 自身不需探测，直接进入高速复制
pr.setRecentActive(true);  // 自身显然活跃，避免 CheckQuorum 误判
```

**首次向 follower 发送的流程（`maybeSendAppend`）：**

```
1. pr.isPaused() → false（刚重置）
2. prevIndex = pr.next - 1 = lastIndex
3. termResult(prevIndex) → 取 prevTerm
4. ents = raftLog.entries(pr.next, maxMsgSize) → 取出 noop entry
5. send MsgAppend(prevIndex, prevTerm, entries=[noop], commit)
6. pr.sentEntries(...) → StateProbe 下设 msgAppFlowPaused=true（发一条就暂停）
```

Follower 回复 accept 后，leader 通过 `maybeUpdate` 推进 match，并 `StateProbe → becomeReplicate()` 进入 pipeline 高速模式。

**源码位置：** `Raft.becomeLeader()` / `Raft.maybeSendAppend()`

---

### Q11: `handleAppendEntries` 中 Follower 如何处理日志？

Follower 收到 Leader 的 `MsgAppend` 后的处理分支：

```
prevIndex < committed?
  └── YES → 回复 accept(committed)   [过时消息，快速回复]

matchTerm(prev) 通过?  (一致性检查)
  ├── YES → findConflict → 从冲突点覆盖 → commitTo
  │         → 回复 accept(lastNewIndex)
  └── NO  → findConflictByTerm 计算 hint（加速回退）
            → 回复 reject(index, hintIndex, hintTerm)
```

**`maybeAppend` 内部四步：**

1. `matchTerm(prev)` —— 检查 follower 在 prevIndex 处是否有 term == prevTerm 的日志；
2. `findConflict(entries)` —— 逐条比对，找到第一个冲突的 entry（返回 0 表示全部已存在）；
3. 从冲突点开始用 leader 的 entries 覆盖（`unstable.truncateAndAppend`），已提交的 entry 冲突则抛不变量异常；
4. `commitTo(min(leaderCommit, lastNewIndex))` —— 推进 committed，但不超过本批最新 entry。

**拒绝时的加速回退：** `findConflictByTerm(hintIndex, logTerm)` 向前搜索 follower 日志中 term ≤ logTerm 的最大 index，让 leader 一次跳过整段冲突 term，而非逐条回退。

**源码位置：** `Raft.handleAppendEntries()` / `RaftLog.maybeAppend()` / `RaftLog.findConflictByTerm()`

---

### Q12: Leader 收到 `MsgAppendResponse` 后做了什么？

**公共前置：** 无论接受/拒绝，都 `setRecentActive(true)` + `setMsgAppFlowPaused(false)`（清除流控，避免陈旧/重复响应导致复制停滞）。

**分支一：拒绝（reject=true）——日志不一致**

```
1. 用 follower 的 hint 计算回退位置（logTerm>0 时再 findConflictByTerm 双方配合加速）
2. maybeDecrTo() 回退 next 指针
3. 若当前是 StateReplicate → 降级为 StateProbe
4. 立即 sendAppend 重新探测
```

**分支二：接受（reject=false）——复制成功**

```
1. maybeUpdate(index) → 推进 match
2. 状态推进：
   - StateProbe → becomeReplicate()（探测成功，切高速模式）
   - StateSnapshot → 若已追上 firstIndex → becomeProbe → becomeReplicate
   - StateReplicate → inflights.freeLE(index)（释放滑动窗口）
3. maybeCommit()？
   - YES → bcastAppend 广播新 commit + 释放 pending ReadIndex
   - NO  → 若该 follower 可推进其 commit → 单独 sendAppend
4. while(maybeSendAppend) → pipeline 尽量多发
5. 若是 leadTransferee 且已追上 lastIndex → sendTimeoutNow（立即选举）
```

**源码位置：** `Raft.stepLeader()` → `case MsgAppendResponse`

---

### Q13: `maybeDecrTo` 回退的设计思路是什么？

两种状态采用不同回退策略：

**StateReplicate（pipeline 高速模式）：**

```java
if (rejected <= match) return false;  // 过时拒绝，忽略
next = match + 1;                     // 保守退到已确认位置
```

多条 inflight 可能乱序到达，最安全就是退到 `match+1`；调用方随后 `becomeProbe()` 降级。

**StateProbe（逐条探测模式）：**

```java
if (next - 1 != rejected) return false;              // 过时响应，忽略
next = max(min(rejected, matchHint + 1), match + 1); // 利用 hint 跳跃
```

- `matchHint + 1`：follower 建议的位置；
- `match + 1`：安全下界，绝不低于此；
- 一次只发一条、响应有序，可精确利用 hint 高效跳过冲突段。

**源码位置：** `Progress.maybeDecrTo()`

---

### Q14: Leader 收到 `MsgHeartbeatResponse` 后做了什么？

心跳回复身兼两职——**日志复制推进** 与 **ReadIndex 确认**。

**① 日志复制推进：**

```java
pr.setRecentActive(true);
pr.setMsgAppFlowPaused(false);
if (pr.getMatch() < r.raftLog.lastIndex() || pr.getState() == StateProbe) {
    r.sendAppend(m.getFrom());  // follower 落后或在探测 → 趁机推进
}
```

**② ReadIndex 安全确认（仅 ReadOnlySafe 且携带 ctx）：**

```java
r.readOnly.recvAck(m.getFrom(), m.getContext().toByteArray());  // 记录该 follower 确认位置
List<ReadIndexRequest> rss = r.readOnly.maybeAdvance(voters);   // 多数派确认了吗？
if (rss != null) {
    for (ReadIndexRequest rs : rss) {
        // 逐个响应已确认的 ReadIndex 请求（本地写 readStates 或回复 MsgReadIndexResp）
    }
}
```

**源码位置：** `Raft.stepLeader()` → `case MsgHeartbeatResponse`

---

## 五、Unstable 与持久化

### Q15: `offsetInProgress` 是干什么的？

`offsetInProgress` 是 `Unstable` 中的**持久化分界标记**，区分 unstable entries 中哪些**已交给 Storage 持久化（in-progress）**、哪些**还没开始持久化**。

```
entries 数组:
     offset          offsetInProgress      offset + entries.size()
       │                   │                       │
       ▼                   ▼                       ▼
  ┌──────┬──────┬──────┬──────┬──────┬──────┐
  │  e1  │  e2  │  e3  │  e4  │  e5  │  e6  │
  └──────┴──────┴──────┴──────┴──────┴──────┘
  ├──── 正在持久化中 ────┤├── 尚未开始持久化 ──┤
```

**关键方法：**

| 方法 | 作用 |
|------|------|
| `nextEntries()` | 返回 `[offsetInProgress:]`，即尚未开始持久化的新 entries |
| `acceptInProgress()` | 推进标记，表示"当前所有 entries 已开始持久化" |
| `stableTo(id)` | 持久化完成确认，删除已稳定的前缀（并保证 `offsetInProgress >= offset`）|

**解决的问题：** 支持异步持久化时，"已经交出去但还没写完的 entries 不要重复发送"。`truncateAndAppend` 中若新 entries 完全覆盖旧数据（`fromIndex <= offset`），会重置 `offsetInProgress = offset`，表示"没有任何 entry 正在持久化中"。

**源码位置：** `Unstable.java` → `offsetInProgress` 字段 / `nextEntries()` / `acceptInProgress()` / `stableTo()`

---

### Q16: 能否让不依赖持久化的消息先发送？etcd 有这个优化吗？

**可以，而且 etcd 已经在应用层实现了这个优化。**

**原理（Raft 论文 §10.2.1）：** Leader 发 `MsgAppend` 给 follower 不依赖自身持久化——commit 需要**多数派**持久化，leader 只是其中一员。即使 leader 在持久化前 crash，entries 还没 commit，安全性不受影响。因此 leader 的磁盘写入可与 follower 的复制**并行**。

**但 follower 不行**——follower 的 `MsgAppendResponse` 是"我已持久化"的承诺，必须先落盘再回复，否则 leader 可能误判多数派而错误 commit。

**etcd server 的做法（`server/etcdserver/raft.go`）：**

```go
// the leader can write to its disk in parallel with replicating to the followers
if islead {
    r.transport.Send(r.processMessages(rd.Messages))  // Leader：持久化前先发
}
r.storage.Save(rd.HardState, rd.Entries)              // 再持久化
if !islead {
    msgs := r.processMessages(rd.Messages)
    notifyc <- struct{}{}
    r.transport.Send(msgs)                            // Follower：持久化后才发
}
```

**分层对比：**

| 层级 | 做法 |
|------|------|
| etcd/raft 库 | 所有消息混在 `Ready.Messages`，把时序决策权交给使用者 |
| etcd server（应用层） | `if islead` 判断，leader 提前发、follower 延后发 |
| CockroachDB | 在库层引入 `AsyncStorageWrites`（本项目已有此路径），用 `MsgStorageAppend.responses` 精确编码时序 |

**对本项目的启示：** 可在应用层按 leader/follower 区分发送时机；或在 `Ready` 中拆分 `messages` / `messagesAfterAppend` 两组；或直接使用已有的 `asyncStorageWrites` 模式（最彻底）。

**源码位置：** `RawNode.readyWithoutAccept()`（对比 etcd `server/etcdserver/raft.go`）

---

## 六、配置变更提案校验

### Q17: Leader 处理 `MsgPropose` 时对 ConfChange 做了哪些校验？

这是 Raft 关键不变量的守护者：**一次只允许一个配置变更在进行中（in-flight）**。

**第一步：统一解析为 V2。** V1（单变更）与 V2（批量/联合共识）统一为 `ConfChangeV2` 表示，便于一致处理。解析失败视为日志损坏，抛 `RaftInvariantException`。

**第二步：三项安全检查。**

```java
boolean alreadyPending  = r.pendingConfIndex > r.raftLog.applied;              // 上一次变更还没 apply
boolean alreadyJoint    = !r.trk.getConfig().getVoters().getConfigs()[1].isEmpty(); // 当前在联合共识中
boolean wantsLeaveJoint = ccv2.getChangesCount() == 0;                          // 本次是要离开 joint
```

| 失败条件 | 拒绝原因 |
|---------|---------|
| `alreadyPending` | 一次只能有一个 in-flight 配置变更 |
| `alreadyJoint && !wantsLeaveJoint` | 必须先离开联合状态才能做下一次变更 |
| `!alreadyJoint && wantsLeaveJoint` | 不在 joint 却要离开，无意义操作 |

**第三步：处理结果。** 校验失败时**不丢弃整个提案**，而是把该 ConfChange entry 替换为空的 `EntryNormal`——保证日志序号不出现空洞，同时静默忽略这次变更；校验通过则记录 `pendingConfIndex` 防止后续并发。

**源码位置：** `Raft.stepLeader()` → `case MsgPropose`

---

## 七、线性一致读 ReadIndex

### Q18: `heartbeatCtx` 与 `recvAck` 在 ReadIndex 中的作用？

`ReadOnlySafe` 模式下，Leader 收到 ReadIndex 请求后需确认自己仍是合法 leader（防脑裂读到旧数据），机制如下。

**`heartbeatCtx()`** 返回一个**单调递增的位置标记**（8 字节小端 long），代表"到目前为止所有待确认 ReadIndex 请求的终止位置"：

```java
long unconfirmedReadPosition = confirmedReads + unconfirmedReads.size();
```

**`recvAck(id, ctx)`** 记录某 voter 已确认到的位置（`acks.merge(from, val, Math::max)`）。Leader 给自己发 `recvAck(self, heartbeatCtx())` 相当于**自动给自己投票**（leader 本身是 voter，不需发心跳给自己）。

**完整流程：**

```
客户端 ReadIndex 请求
  → addRequest(committed, req)   记录请求 + 当时 committed
  → recvAck(self, ctx)           leader 自 ack
  → bcastHeartbeat(ctx)          心跳携带 ctx 发给 followers
  → follower 回复心跳(原样带回 ctx)
  → recvAck(follower, ctx)       记录 follower 确认
  → maybeAdvance(voters)         多数派确认 → 安全返回读结果
```

**设计巧妙之处：** 用单调递增数字**批量确认**——`ctx=5` 意味着"确认了位置 ≤5 的所有读请求"，多个 ReadIndex 可被同一轮心跳批量确认，减少心跳轮次。

**源码位置：** `ReadOnly.heartbeatCtx()` / `ReadOnly.recvAck()` / `ReadOnly.maybeAdvance()` / `Raft.sendMsgReadIndexResponse()`

---

## 八、启动与初始化

### Q19: bootstrap 为什么会“添加两次”配置变更？

`RawNode.bootstrap(peers)` 确实对每个 peer 做了两件事，这是**刻意设计**（与 etcd/raft 原版一致）：

```java
// 第一步：把每个 peer 写成一条 EntryConfChange 日志，append 到 raftLog
for (int i = 0; i < peers.size(); i++) {
    ... ents.add(EntryConfChange(index=i+1, ConfChangeAddNode)); 
}
raft.raftLog().append(ents);
raft.raftLog().committed = ents.size();

// 第二步：逐个 applyConfChange 到内存的 ProgressTracker
for (Peer peer : peers) {
    raft.applyConfChange(ConfChangeV2(ConfChangeAddNode));
}
```

**为什么需要两次？因为两次作用于不同层面：**

| 步骤 | 作用对象 | 目的 |
|------|---------|------|
| 第一步 `append` | **持久化日志**（raftLog）| 创世纪配置作为正式日志写入，后续 Ready 循环会把它们当作 committedEntries 再 apply 一次 |
| 第二步 `applyConfChange` | **内存集群配置**（ProgressTracker）| 让节点**立即**知道集群成员，不用等 Ready 循环就能 `campaign()` 发起选举 |

etcd 原版的注释说得很清楚：

> Now apply them, mainly so that the application can call **Campaign immediately after Bootstrap** in tests. Note that these nodes will be **added to raft twice**: here and when the application's Ready loop calls ApplyConfChange.

**会不会重复生效？不会。** `applyConfChange` 是幂等的——同一个节点 AddNode 两次，第二次在 `Changer` 里发现已存在就是空操作，不会破坏配置。

**源码位置：** `RawNode.bootstrap()`

---

### Q20: bootstrap 为什么可以直接设置 committed，不需多数派确认？

```java
raft.raftLog().committed = ents.size();  // 直接把创世纪配置全部标为已提交
```

因为 bootstrap 是**创世纪（genesis）的特殊语义**：

1. **前提是空 Storage**——`lastIndex != 0` 会直接报错，保证只在集群首次初始化时调用；
2. **所有节点用相同的 peers 列表调 bootstrap**——等价于全体节点对初始配置达成先验一致，无需在运行期凑齐多数派；
3. 若不直接 commit，集群启动时没有 leader、无法推进 commitIndex，会陷入“无法选举→无法 commit→无法确认成员”的死锁。

这是 Raft 初始化公认的“创世纪配置直接生效”手法，与运行期的配置变更（需多数派）不同。

**源码位置：** `RawNode.bootstrap()`

---

### Q21: `newRaft()` 中 `Changer.restore` 在做什么？

节点重启或创建时，需要把持久化的集群配置（`ConfState`）**反序列化重建到内存的 `ProgressTracker`**，这就是 `Changer.restore(ConfState)` 的职责。

**restore 内部根据 ConfState 选择重建路径：**

```
votersOutgoing 为空？
  ├── YES → simple 路径：逐个 AddNode(voters) + AddLearner(learners)
  └── NO  → joint 路径：
            1. 先用 outgoing 配置重建为起点
            2. enterJoint(...) 重新进入联合共识
            3. 恢复 incoming voters / learners / learnersNext
```

**关键点：** 若节点在 crash 前正处于 joint 状态，`ConfState.votersOutgoing` 非空，restore 会忠实地**重建回 joint 状态**，不会丢失未完成的配置变更。

**源码位置：** `Changer.restore()` / `Raft.newRaft()`

---

### Q22: Follower 节点何时变为 active（recentActive 生命周期）？

`Progress.recentActive` 是 CheckQuorum 机制的**滑动窗口存活标记**，leader 用它判断自己是否仍能联系到多数派。

**置为 true（“最近活跃”）的时机：**

- 收到该 follower 的 `MsgAppendResponse`；
- 收到该 follower 的 `MsgHeartbeatResponse`；
- Leader 对自身：`becomeLeader()` 中 `setRecentActive(true)`。

**置为 false（重置窗口）的时机：**

- 每个 electionTimeout 触发一次 `MsgCheckQuorum`，leader 把除自己外所有 Progress 的 `recentActive` 重置为 false；
- 下一个周期内若仍未收到回复，则该节点被视为“失联”。

**作用：** 若一个 electionTimeout 周期内，`recentActive=true` 的 voter 不足多数派，leader 会主动退位为 follower（CheckQuorum），避免网络分区下旧 leader 继续提供服务。

**源码位置：** `Raft.becomeLeader()` / `Raft` → `MsgCheckQuorum` 处理 / `Progress.recentActive`

---

## 九、配置变更内部机制

### Q23: `applyConfChange` 的完整逻辑是什么？

`Raft.applyConfChange(ConfChangeV2)` 把一条已 apply 的配置变更日志真正作用到内存集群配置，分**两阶段**：

**阶段一：`Changer` 计算新配置（纯函数，不副作用）**

```
leaveJoint(cc)?  → changer.leaveJoint()      // 空 ConfChangeV2，离开联合共识
enterJoint(cc)? → changer.enterJoint(...)   // 多个变更或显式 joint，进入联合共识
否则            → changer.simple(...)        // 单一变更，直接生效
```

**阶段二：`switchToConfig(cfg, progressMap)` 切换到新配置**

```
1. 用新的 ProgressTracker.Config 替换旧配置
2. 处理自身角色变化：
   - 自己不再是 voter → 可能需要退位
   - 自己被移除 → 不再发起选举
3. 重算 commitIndex（新配置下多数派变了）
4. 若是 leader 且进入 autoLeave 的 joint → 记录 pendingConfIndex
```

**返回新的 `ConfState`**，由上层持久化，供重启时 `Changer.restore`（见 Q21）使用。

**源码位置：** `Raft.applyConfChange()` / `Raft.switchToConfig()`

---

### Q24: `ProgressTracker.Config` 里有什么？

`ProgressTracker.Config` 是集群成员配置的**内存快照**，字段如下：

| 字段 | 类型 | 含义 |
|------|------|------|
| `voters` | `JointConfig` | 投票成员，含 `incoming`（新）与 `outgoing`（旧）两个集合；outgoing 非空即处于 joint |
| `autoLeave` | `boolean` | 是否自动离开 joint（Auto / JointImplicit 模式为 true）|
| `learners` | `Set<Long>` | 学习者（不投票、只同步日志）|
| `learnersNext` | `Set<Long>` | “离开 joint 后将变为 learner”的节点（见 Q25）|

`JointConfig.committedIndex(matchIndexes)` 同时要求 incoming 与 outgoing **两个多数派都满足**，这正是联合共识安全性的基础。

**源码位置：** `ProgressTracker.Config` / `JointConfig`

---

### Q25: `learnersNext` 是计算得到的还是参数传入的？

**是 `Changer` 在处理变更时计算得到的**，不是用户传入的参数。

当对一个**当前仍是 outgoing voter** 的节点执行 `makeLearner` 时，不能立即把它变成 learner（否则会破坏 outgoing 多数派），而是先放进 `learnersNext`，等离开 joint 后再真正变为 learner：

```java
if (cfg.getVoters().outgoing().contains(id)) {
    cfg.getLearnersNext().add(id);   // 仍是 outgoing voter → 暂存 LearnersNext
} else {
    cfg.getLearners().add(id);       // 否则直接变 learner
}
```

并受不变量 `learnersNext ⊆ votersOutgoing` 约束（`Changer.checkInvariants()`）——这也是 Q6 中 `checkSelfRemoval` 不需单独检查 `learnersNext` 的原因。

**源码位置：** `Changer.makeLearner()` / `Changer.checkInvariants()`

---

## 十、流式快照完整流程

> 覆盖「打快照 → 发快照 → 发 meta → Follower apply → advance」全链路。详尽版见 [快照流程](snapshot-flow.zh.md)。

### Q26: 流式快照如何创建？生成什么文件？

**触发条件：** 宿主每轮 `processReady` 尾部调用 `maybeSnapshot(applied)`，当 `applied - lastSnapshotIndex >= SNAPSHOT_ENTRIES_THRESHOLD`（默认 10,000）时创建。

**`createSnapshotStreaming` 三阶段：**

```
Phase 1  短写锁校验 index/term（快速临界区）
Phase 2  无锁文件 I/O：写 side-car 文件
         snap-{index}-{term}.data.tmp → fsync → 原子 rename → fsyncDir
Phase 3  短写锁提交元数据到 RocksDB：
         put(cfSnap,  KEY_SNAPSHOT,      metadata_only)  // 仅元数据，无 payload
         put(cfState, KEY_SNAPSHOT_FILE, fileName)        // side-car 指针
         deleteOldSidecar(prevFile)                       // 删旧 side-car
```

**文件是什么？** `snapshots/snap-{index}-{term}.data`——独立的 side-car 文件，存放状态机序列化后的 payload；`Snapshot.data` 字段清空，只在 RocksDB 存元数据。

**两种创建方式对比：**

| 方式 | payload 去向 | `Snapshot.data` | 适用 |
|------|------------|-----------------|------|
| `createSnapshot`（inline）| 写入 RocksDB `cfSnap` | 携带完整数据 | 小状态机（示例默认）|
| `createSnapshotStreaming` | 独立 side-car 文件 | 空 | 超大状态机，避免 OOM |

创建后调用 `storage.compact(applied)` **截断** applied 之前的日志，`firstIndex` 前移——这正是后续 Leader 需给落后 Follower 发快照的根因。

**源码位置：** `RaftKVNode.maybeSnapshot()` / `RocksDbStorage.createSnapshotStreaming()` / `sidecarName()`

---

### Q27: Leader 何时、如何发送快照？

**触发时机：`maybeSendAppend` 发现 follower 需要的 entry 已被 compact**

```java
long prevIndex = pr.getNext() - 1;
if (raftLog.termResult(prevIndex).err() != null) {
    return maybeSendSnapshot(to, pr);   // prevIndex 被 compact
}
try {
    ents = raftLog.entries(pr.getNext(), maxMsgSize);
} catch (RaftException e) {
    return maybeSendSnapshot(to, pr);   // entries 被 compact
}
```

**`maybeSendSnapshot` 核心动作：**

```java
if (!pr.isRecentActive()) return false;        // 对方不活跃则不发
snapshot = raftLog.snapshot();                 // 从 Storage 取元数据
pr.becomeSnapshot(sindex);                     // 进入 StateSnapshot，isPaused=true 暂停复制
send(MsgSnapshot(to, snapshot));               // 放入 r.msgs() → Ready.messages
```

**宿主发送分流（Ready 循环）：**

```java
if (snapshotStreaming && m.getMsgType() == MsgSnapshot) {
    sendSnapshotOutOfBand(m);        // 带外流式
} else {
    transport.send(m.getTo(), m);    // 普通消息（inline 快照也走这里）
}
```

**`sendSnapshotOutOfBand`：**

```java
InputStream in = storage.openSnapshotData(m.getSnapshot());  // 打开 side-car
transport.sendSnapshot(to, m, in, (ok, err) ->
    node.reportSnapshot(to, ok ? SnapshotFinish : SnapshotFailure));  // 回调上报结果
```

**传输细节（`GrpcTransport.sendSnapshotStreaming`）：** 固定 buffer 边读 InputStream 边发 `SnapshotChunk`（client-streaming RPC），多 GB 快照不整体入堆；首个 chunk = header（4B 长度 + metaMsg），后续为 payload 分片；在 `sendExecutor` 线程池异步执行，不阻塞 Ready 循环。

**源码位置：** `Raft.maybeSendAppend()` / `Raft.maybeSendSnapshot()` / `RaftKVNode.sendSnapshotOutOfBand()` / `GrpcTransport.sendSnapshotStreaming()`

---

## 十一、存储实现细节与并发

### Q31: side-car 临时文件不怕重名吗？存在并发竞态吗？

**事实：临时文件名只由 `(index, term)` 决定，没有唯一性后缀。**

```java
private static String sidecarName(long index, long term) {
    return "snap-" + index + "-" + term + ".data";
}
// tmp = snapDir/("snap-{index}-{term}.data" + ".tmp")
```

三个写 side-car 的方法（`createSnapshotStreaming` / `applySnapshot(meta,data)` / `stageSnapshotData`）对同一份 `(index,term)` 算出**完全相同的 tmp 路径**，且文件 I/O 都在**锁外**（`writeLock` 只护 RocksDB 元数据的 Phase 1/3，Phase 2 文件写入无锁）。因此**文件层本身没有**处理多线程写同一文件的能力。

**为什么正常情况不撞车（靠上层协议约束，而非文件层防护）：**

- **Leader 创建端**：`createSnapshotStreaming` 只由宿主 `readyLoop` 单线程调用，且快照 index 单调递增，同一 `(index,term)` 不会被并发创建；
- **Follower 接收端**：进入 `StateSnapshot` 后 `isPaused()=true`，Leader 暂停对该 follower 的复制，正常路径下同一时刻只有一个快照在途；
- `installSnapshot` 单流内有 `expectedFrom` 校验，防止不同来源 chunk 交错。

**真正的竞态窗口：** `installSnapshot` 每个 RPC 都**新建一个 worker 线程**调用 `stageSnapshotData`。若发生 **Leader 超时重试 / 网络重复投递**，导致针对同一 `(index,term)` 的**两个并发 installSnapshot RPC**，就会有两个线程同时写同一个 tmp：

```java
Files.newOutputStream(tmpPath, CREATE, TRUNCATE_EXISTING, WRITE)  // 两线程都 TRUNCATE
→ fsync(tmpPath)
→ Files.move(tmpPath, finalPath, ATOMIC_MOVE)   // 一个成功后 tmp 消失
```

**后果：**
1. 两个流交错写同一 tmp（`TRUNCATE_EXISTING` 会互相截断）；
2. 先完成的线程 `ATOMIC_MOVE` 走 tmp 后，另一线程的 `fsyncFile` / `move` 会抛 `NoSuchFileException` → 该 install 失败 → follower 回 error → leader 重试。

**减轻因素（但不能作为正确性依据）：** 同一 `(index,term)` 的两个流写的是**同一份 payload**，字节相同，即使交错最终 finalPath 内容大概率仍正确——但这属于“侥幸幂等”。

**读侧安全：** `openSnapshotData` 在 `readLock` 下、按已提交的 `KEY_SNAPSHOT_FILE` 指针打开 finalPath，**不碰 tmp**，所以读侧安全；竞态只在**写 tmp** 这一段。

**建议修复：** 给每个写者一个独占 tmp（带唯一后缀），再各自原子 rename 到相同 finalPath（rename 幂等，最后写者赢，内容相同）：

```java
Path tmpPath = snapDir.resolve(fileName + ".tmp." +
        Long.toUnsignedString(ThreadLocalRandom.current().nextLong()));
```

**源码位置：** `RocksDbStorage.sidecarName()` / `createSnapshotStreaming()` / `stageSnapshotData()` / `RaftServiceImpl.installSnapshot()`

---

### Q32: `applySnapshot` 什么时候使用？为何代码里没看到调用？

**结论：生产宿主代码确实不调用它，只有测试在用。**

`applySnapshot(Eraftpb.Snapshot snap)`（单参数）是 `Storage` 接口的**标准“安装快照” API**（对应 etcd raft 的 `MemoryStorage.ApplySnapshot`）：安装快照、截断到 `snap.metadata.index` 之前的日志，持久化后 raft 才视为安装完成。

**为何生产代码不用？** 因为宿主选择了 **`writeBatched(entries, hardState, snapshot)`** 作为落盘入口，把三者合并进**一个 RocksDB `WriteBatch` 原子写入**：

```java
// RaftKVNode / TestRaftNode 的 readyLoop:
storage.writeBatched(rd.entries(), rd.hardState(), rd.snapshot());
```

`writeBatched` 内部对 snapshot 的处理等价于 `applySnapshot`（提交 `KEY_SNAPSHOT` + 关联 side-car + `deleteRange` 截断日志），但把它和 entries/hardState 揉进同一个原子批次，**原子性更强、I/O 更省**，因此取代了 `applySnapshot`。

**谁在调用它？——全是测试：**

| 调用点 | 用途 |
|--------|------|
| `MemoryStorageTest` / `RaftLogTest` / `NodeTest` | 单元测试构造快照场景 |
| `InteractionEnv` | etcd datadriven 交互测试框架移植 |
| `AsyncStorageWritesTest` | 异步存储写入测试 |

这些测试多用 `MemoryStorage`，它实现了单参数 `applySnapshot`，用起来比 `writeBatched` 更直接。

**孪生方法：** 双参数流式版 `applySnapshot(Snapshot meta, InputStream data)` 同样在生产**未被直接调用**——Follower 流式安装走的是 `stageSnapshotData`（先落 side-car）+ 下一轮 `writeBatched`（关联 side-car）这条路径。它们都是 `Storage` 接口为通用性提供的**备选 API**（附带默认 `UnsupportedOperationException`）。

**一句话总结：** `applySnapshot` 是 Storage 接口的标准快照安装 API，`RocksDbStorage` 生产路径用功能等价、但原子性/性能更优的 `writeBatched` 取代了它，因此目前只被测试使用。

**源码位置：** `Storage.applySnapshot()` / `RocksDbStorage.writeBatched()` / `MemoryStorage.applySnapshot()`

---

### Q33: `createSnapshot` 和 `applySnapshot` 的使用方式是什么？只有 Leader 创建快照、只有 Follower 安装快照吗？

**常见误解：** "只有 Leader 调用 `createSnapshot`，只有 Follower/Learner 调用 `applySnapshot`。"

这个理解**对了一半、错了一半**，核心混淆在于把「快照创建」（本地行为）和「快照传输」（网络行为）揉在了一起。

#### `createSnapshot`：所有节点都调，不是只有 Leader

看 `RaftKVNode.processReady()` 末尾：

```java
// 6. Maybe snapshot + compact.
if (highestApplied > 0) {
    maybeSnapshot(highestApplied);   // 没有任何 isLeader() 判断！
}
```

`maybeSnapshot` 在 `processReady` 中被**无条件调用**，Leader、Follower、Learner 都会在自己的 `applied - lastSnapshotIndex >= SNAPSHOT_ENTRIES_THRESHOLD`（默认 10,000）时独立调用 `createSnapshot`。

这是正确的 Raft 语义：**快照创建是每个节点的本地行为**，每个节点各自对自己的状态机打快照、各自 compact 日志，彼此独立。

#### 需要区分两个概念

| 行为 | 谁做 | 说明 |
|------|------|------|
| **创建快照** `createSnapshot` | **所有节点** | 每个节点对自己的状态机本地打快照 |
| **发送快照** `MsgSnapshot` | **只有 Leader** | Leader 发现 Follower 落后太多、日志已 compact，才把自己的已有快照发给 Follower |

Leader 发快照时**不会新建快照**，而是读取自己**之前已经创建好的**快照（`storage.snapshot()` / `openSnapshotData`），通过 `MsgSnapshot` 发出去。

#### `applySnapshot`：确实只有 Follower 侧需要安装，生产用 `writeBatched` 替代

"只有 Follower/Learner 才会安装快照"——**对**，因为只有 Follower 才会收到 Leader 的 `MsgSnapshot` 并 `restore`。

"生产代码没人调用 `applySnapshot`，用 `writeBatched` 替代"——**也对**，但替代关系分两种情况：

**Inline 模式（快照数据在 proto 里）：**

```
Follower 收到 MsgSnapshot → restore() → Ready.snapshot → writeBatched(entries, hardState, snapshot)
```

`writeBatched` 内部做的事和 `applySnapshot` **完全等价**（写 `KEY_SNAPSHOT` + `deleteRange` 截断日志），但揉进同一个 `WriteBatch`，**原子性更强**。

**Streaming 模式（快照数据在 side-car 文件里）：**

```
Follower 收到 MsgSnapshot → SnapshotSink.install → stageSnapshotData（只落 side-car 文件，不提交元数据）
                          → restore() → Ready.snapshot → writeBatched（关联 side-car + 截断日志）
```

这条路径连 `applySnapshot(Snapshot, InputStream)` 双参数流式版都没用，而是拆成了 `stageSnapshotData` + `writeBatched` 两步。

#### 正确的心智模型

```
快照创建（本地）         快照传输（网络）         快照安装（本地）
┌─────────────┐       ┌──────────────┐       ┌─────────────────┐
│ 所有节点     │       │ 只有 Leader   │       │ 只有 Follower    │
│ createSnapshot│      │ maybeSendSnapshot│    │ restore() →      │
│ + compact    │  ──→  │ MsgSnapshot   │  ──→  │ Ready.snapshot → │
│              │       │ (发已有快照)   │       │ writeBatched     │
└─────────────┘       └──────────────┘       └─────────────────┘
```

**一句话总结：** `createSnapshot` 所有节点都调（本地打快照）；`applySnapshot` 生产不调，被 `writeBatched` 替代，但"安装快照"这个动作确实只发生在 Follower 侧。

**源码位置：** `RaftKVNode.maybeSnapshot()` / `RaftKVNode.processReady()` / `RocksDbStorage.writeBatched()` / `Raft.handleSnapshot()`

---

## 相关文档

- [架构与设计](architecture.zh.md)
- [Raft / RawNode / Node 三层架构](raft-node-layers.zh.md)
- [快照流程](snapshot-flow.zh.md)
