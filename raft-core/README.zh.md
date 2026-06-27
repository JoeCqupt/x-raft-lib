# x-raft-lib / raft-core

> ⚠️ **最新发布版本：`0.1.0-RC1`**（已发布到 Maven Central）；**`main` 分支为
> `0.1.0-SNAPSHOT`**（面向 `0.1.0` GA 的未发布工作）。协议正确性已对齐
> etcd/raft，配套能力（可插拔 metrics、错误分级、gRPC 传输
> [raft-transport-grpc](../raft-transport-grpc)、RocksDB 存储
> [raft-storage-rocksdb](../raft-storage-rocksdb)）均已就绪。

[English](README.md) | 中文

Java 实现的 Raft 共识算法核心库，从 [etcd-io/raft](https://github.com/etcd-io/raft) 移植。

**设计理念**：纯状态机、零 I/O。Raft 核心不写磁盘、不发网络、不管定时器；所有
I/O 由上层应用驱动。Raft 的全部输出打包为 `Ready`，由上层按顺序处理。

## 状态

- **396+ 测试**（unit / property / datadriven / system / trace / async / integration / chaos / linearizability）
- **覆盖率**：instruction ≥85% / branch ≥80% / line ≥88% / method ≥85%（jacoco gate）
- **JMH baseline**：见 [`benchmarks/baseline.md`](./benchmarks/baseline.md)
- 完整对齐 etcd/raft 的核心语义（election、log replication、snapshot、conf change、ReadIndex、PreVote、CheckQuorum、ForgetLeader、TransferLeader、AsyncStorageWrites）

## 模块结构

```
io.github.xinfra.lab.raft              // 公开 API
├── Node.java                 # 线程安全 API 入口 (startNode/restartNode)
├── RawNode.java              # 单线程 API
├── Config.java               # Raft 配置 + validate()
├── Ready.java                # 输出结构体
├── Storage.java              # 持久化存储接口
├── Transport.java            # 传输层接口
├── MemoryStorage.java        # 内存 Storage 实现（测试用）
├── Status.java, SoftState.java, ReadState.java, Peer.java, ...
│
└── internal/                          // 非公开 API — 见 package-info.java
    ├── Raft.java             # 状态机（选举、日志复制、心跳）
    ├── RaftLog.java          # 日志管理（unstable + storage）
    ├── Unstable.java         # 未持久化日志缓冲（in-place shrink）
    ├── DefaultNode.java      # Node 线程安全封装（事件循环）
    ├── ReadOnly.java         # ReadIndex 实现
    ├── confchange/Changer.java   # V1/V2 joint consensus
    ├── quorum/MajorityConfig.java, JointConfig.java
    └── tracker/Progress.java, ProgressTracker.java, Inflights.java
```

## 使用方式（RawNode，单线程）

```java
MemoryStorage storage = new MemoryStorage();
Config cfg = Config.builder()
        .id(1)
        .electionTick(10)
        .heartbeatTick(1)
        .storage(storage)
        .maxSizePerMsg(Long.MAX_VALUE)
        .maxInflightMsgs(256)
        // 可选：.preVote(true).checkQuorum(true)
        // 可选：.asyncStorageWrites(true)
        .build();

RawNode rn = RawNode.newRawNode(cfg);
rn.bootstrap(List.of(new Peer(1)));   // 或 storage 已经有 ConfState

while (running) {
    rn.tick();                         // 外部驱动时钟
    for (Message msg : received) rn.step(msg);
    if (rn.hasReady()) {
        Ready rd = rn.ready();
        persist(rd.entries(), rd.hardState());     // 1. 持久化
        send(rd.messages());                       // 2. 网络发送
        if (!Util.isEmptySnap(rd.snapshot())) {
            restoreFromSnapshot(rd.snapshot());     // 3. 应用快照
        }
        apply(rd.committedEntries());              // 4. 应用到状态机
        rn.advance(rd);                            // 5. 通知 raft 完成
    }
}
```

## 使用方式（DefaultNode，多线程）

`DefaultNode` 用单独的事件循环线程封装 `RawNode`，API 线程安全：

```java
Node n = DefaultNode.startNode(cfg, List.of(new Peer(1)));

// 任意线程可调用以下方法：
n.tick();                    // 非阻塞，burst 限制 128
n.propose("data".getBytes());                // 阻塞直到 leader 处理
n.proposeConfChange(ccv2);
n.readIndex("ctx".getBytes());
n.transferLeadership(myId, targetId);
n.forgetLeader();
n.reportUnreachable(peerId);
n.reportSnapshot(peerId, status);
Status st = n.status();

// 消费 Ready 的循环（单一消费者）：
while (running) {
    Ready rd = n.ready();
    persist(rd.entries(), rd.hardState());           // 1. 持久化
    send(rd.messages());                             // 2. 网络发送
    if (!Util.isEmptySnap(rd.snapshot())) {
        restoreFromSnapshot(rd.snapshot());           // 3. 应用快照
    }
    apply(rd.committedEntries());                    // 4. 应用到状态机
    n.advance(rd);                                   // 5. 通知完成
}

n.stop();   // 阻塞直到事件循环退出 + 所有 pending future 完成
```

**`events` 队列默认容量 1024**，超出后 producer 阻塞（背压；等价 etcd unbuffered chan）。
Tick 是例外：用独立计数器 + offer，超 128 burst 直接丢弃 + warn。

## 配置选项

| 字段 | 默认 | 说明 |
|------|------|------|
| `id` | (必填) | 节点 ID，不能为 0 或 LOCAL_*_THREAD |
| `electionTick` | (必填) | 选举超时（tick 数）；必须 > heartbeatTick |
| `heartbeatTick` | (必填) | 心跳间隔（tick 数）；必须 > 0 |
| `storage` | (必填) | 持久化存储 |
| `maxSizePerMsg` | 0 | MsgApp 单条最大字节数 |
| `maxCommittedSizePerReady` | maxSizePerMsg | 单个 Ready 中 committed entries 最大字节数 |
| `maxUncommittedEntriesSize` | NO_LIMIT | 未提交 entries 总字节上限 |
| `maxInflightMsgs` | (必填) | 单 follower 在飞 MsgApp 数上限 |
| `maxInflightBytes` | NO_LIMIT | 单 follower 在飞字节数上限 |
| `preVote` | false | 启用 PreVote 防止 term 膨胀 |
| `checkQuorum` | false | 启用 leader 主动 quorum 检查 |
| `readOnlyOption` | ReadOnlySafe | ReadIndex 实现方式 |
| `disableProposalForwarding` | false | 禁用 follower 转发 propose |
| `stepDownOnRemoval` | false | leader 被移除时主动下台 |
| `asyncStorageWrites` | false | 持久化走 MsgStorageAppend/Apply 异步通道 |

## 测试

```bash
mvn test                     # 跑 raft-core 测试 + jacoco gates
mvn test -Dtest='RaftTest'   # 跑单类
mvn test -Dtest='InteractionTest' -Ddatadriven.rewrite=true   # 重写 testdata
```

### Datadriven framework

`src/test/java/io/github/xinfra/lab/raft/datadriven/Datadriven.java` 是简化版
cockroachdb datadriven 的 Java 移植。`.txt` 文件描述命令 + 期望输出，跑测时
diff 报告。25+ 个命令支持完整的 raft 场景。

**Rewrite 模式**：写场景骨架（命令 + `----`），跑 `mvn test -Ddatadriven.rewrite=true`
自动捕获实际输出填入 expected。审查 diff，确认无误后正常跑。

当前 28 个 scenarios：single_node、single_node_log_state、single_node_propose_batch、
single_node_snapshot_compact、two_node_election、three_node_election、
three_node_propose_replicate、partition_recovery、forget_leader、leader_transfer、
confchange_v2_joint、confchange_add_learner、confchange_add_voter、
confchange_demote_voter_to_learner、confchange_learner_lifecycle、
confchange_promote_learner、confchange_remove_learner、confchange_remove_then_readd、
confchange_remove_voter、prevote_no_term_bump、checkquorum_leader_steps_down、
heartbeat_resp_recovers_from_probing、snapshot_install_after_compact、
snapshot_and_compact_errors、snapshot_succeed_via_app_resp、replicate_pause、
lagging_commit、progress_after_replication。

## Benchmark

```bash
mvn test-compile
CP=$(mvn -q dependency:build-classpath -DincludeScope=test -Dmdep.outputFile=/dev/stdout)
java -cp "target/test-classes:target/classes:$CP" io.github.xinfra.lab.raft.RaftBenchmarks
```

完整 perf 数据 + A/B 复现步骤见 [`benchmarks/baseline.md`](./benchmarks/baseline.md)。

## CI

GitHub Actions：[`.github/workflows/ci.yml`](../.github/workflows/ci.yml)，
JDK 17/21 矩阵 + 全 reactor `mvn install` + jacoco 汇总。本地跑 `mvn test`
会触发 JaCoCo gates（INSTRUCTION≥85%, BRANCH≥80%, LINE≥88%, METHOD≥85%）。

## 技术栈

- Java 17
- Protobuf 4.35（消息序列化）
- SLF4J + Logback（日志）
- JUnit 5 + AssertJ（测试）
- jqwik（property-based 测试）
- JMH（性能基准）
- JaCoCo（覆盖率 + 门槛）

## 与 etcd/raft 的差异

完整对齐核心算法。Java 化做了几处适配（详见各类文件头注释）：

- **错误处理**：`RaftException` 携带 `Code` 枚举 + `equals/hashCode` 基于 code，调用方用 `e.is(Code.X)`；`RaftInvariantException`（RuntimeException 子类）表示协议不变量被破坏，与可恢复错误分离
- **随机源**：`ThreadLocalRandom` 替代 etcd 的 `math/rand`（更快、不阻塞 cold start）
- **状态默认值**：`RaftStateType state = StateFollower`（Java 没有 Go 的 enum 零值习惯，显式初始化避免 NPE）
- **Ready 字段**：`Ready.entries` / `committedEntries` 做防御性 ArrayList copy（防 subList 视图在 step 后失效，async 模式必需）
- **Unstable.stableTo**：in-place `removeRange` + shrink-on-empty（避免每次 ArrayList 重建，hot path +140%）
- **DefaultNode 异常处理**：try-finally 保证 done/drain/notify 在 Throwable 逃逸时也执行；producer 用 `submitWithResult` 解决 stop 竞态
- **AsyncStorageWrites**：完整支持 etcd 后续添加的异步存储路径，含 multi-node 收敛测试
- **Changer.initProgress**：`Next = max(lastIndex, 1)` 而非 etcd 的 `lastIndex+1`（验证后选择保留差异，详见 Changer.java 注释）

无差异的核心：选举、log replication、commit 推进、ReadIndex（safe + lease-based）、
Joint Consensus、Snapshot install/restore、PreVote、CheckQuorum、ForgetLeader、
TransferLeader、Inflights 流控。

## License

[Apache License 2.0](./LICENSE)。本项目从 [etcd-io/raft](https://github.com/etcd-io/raft)（Apache-2.0）
移植，attribution 详见 [`NOTICE`](./NOTICE)。
