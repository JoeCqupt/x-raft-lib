# 测试策略

[English](testing.md) | [中文](testing.zh.md)

## 测试金字塔概览

x-raft-lib 采用多层测试策略，从算法不变量到全栈分布式场景，全面验证 Raft 协议的正确性。

```
                    ┌─────────────┐
                    │   浸泡 /    │  每周：持续负载，
                    │   混沌      │  资源泄漏检测
                    ├─────────────┤
                  ┌─┤   集成      │  每次 PR：真实 gRPC + RocksDB，
                  │ │   测试      │  多节点场景
                  │ ├─────────────┤
                  │ │   模糊      │  每夜：Jazzer 覆盖率引导
                  │ │   测试      │  模糊测试 step() + parse
                  │ ├─────────────┤
                  │ │   属性      │  每次 PR：jqwik 属性测试
                  │ │   测试      │  验证 quorum / confchange
                ┌─┤ ├─────────────┤
                │ │ │  数据驱动   │  每次 PR：场景驱动
                │ │ │   测试      │  交互测试
                │ ├─┼─────────────┤
                │ │ │   单元      │  每次 PR：448+ 测试，
                │ │ │   测试      │  85%+ 覆盖率
                └─┴─┴─────────────┘
```

| 层级 | 数量 | 模块 | 运行时机 |
|------|------|------|----------|
| 单元 & 功能测试 | 448+ | raft-core | 每次 PR |
| 属性测试 | ~10 | raft-core | 每次 PR |
| 数据驱动测试 | 13 个场景 | raft-core | 每次 PR |
| 模糊测试 | 2 个 harness | raft-core | 每夜 |
| 集成测试 | 33+ | raft-tests | 每次 PR |
| 混沌压力 | 6 个测试类 × 5 次迭代 | raft-tests | 每周 |
| 浸泡测试 | 2 | raft-tests | 每周 |

---

## 单元测试（raft-core）

从 etcd-raft 移植的核心协议测试，覆盖状态机的每个分支：

| 测试类 | 覆盖内容 |
|--------|----------|
| `RaftTest` | Leader 选举、日志复制、提交推进、消息处理 |
| `RaftPaperTest` | Raft 论文 Figure 2 不变量 —— 选举安全性、Leader 只追加、日志匹配、Leader 完整性、状态机安全性 |
| `RaftSnapTest` | 快照创建、安装、恢复、与压缩的交互 |
| `RaftFlowControlTest` | MaxInflightMsgs、MaxSizePerMsg、Inflights 滑动窗口、背压 |
| `UnstableTest` | 未持久化日志缓冲：stableTo、truncateAndAppend、shrink |
| `RaftLogTest` | 跨 Unstable + Storage 边界的日志管理 |
| `ReadOnlyTest` | ReadIndex safe 和 lease-based 模式 |
| `RawNodeTest` | RawNode API：bootstrap、propose、conf change、ready、advance |
| `ConfChangeTest` | V1/V2 成员变更、联合共识进入/退出 |

### 运行方式

```bash
mvn -pl raft-core test                     # 全部单元测试 + JaCoCo 门禁
mvn -pl raft-core test -Dtest='RaftTest'   # 单个类
```

---

## 属性测试（jqwik）

随机化属性测试，探索手写场景覆盖不到的边界情况：

| 测试类 | 验证的属性 |
|--------|-----------|
| `ChangerPropertyTest` | ConfChange 操作在随机 voter/learner 配置下保持 quorum 不变量 |
| `InflightsPropertyTest` | Inflights 的 add/freeFirstOne/freeLE/reset 在随机操作序列下维持正确的计数和容量 |
| `MajorityConfigPropertyTest` | CommittedIndex 计算对任意 quorum 大小和 match-index 分布都正确 |

---

## 数据驱动测试

`InteractionTest` 框架（移植自 CockroachDB 的 datadriven 框架）在 `.txt` 文件中定义 Raft 场景：

```
# 示例：three_node_election.txt
add-nodes 3 voters=(1,2,3) ...
campaign 1
stabilize
----
> 1 becomes leader
```

当前场景：`single_node`、`three_node_election`、`partition_recovery`、`forget_leader`、`leader_transfer`、`confchange_v2_joint`、`prevote_no_term_bump`、`checkquorum_leader_steps_down`、`heartbeat_resp_recovers_from_probing`、`snapshot_install_after_compact`、`replicate_pause`、`lagging_commit`、`snapshot_succeed_via_app_resp`。

### Rewrite 模式

编写场景骨架（命令 + `----`），然后运行：

```bash
mvn -pl raft-core test -Dtest='InteractionTest' -Ddatadriven.rewrite=true
```

自动捕获实际输出并填入期望结果。

---

## 模糊测试（Jazzer）

在协议边界上进行覆盖率引导的模糊测试：

| Harness | 目标 | 目的 |
|---------|------|------|
| `EraftpbParseFuzzTest` | Protobuf 消息反序列化 | 捕获畸形输入导致的 panic、精心构造的载荷引发的 OOM |
| `RaftStepFuzzTest` | `Raft.step(Message)` 入口 | 发现任意消息序列下的不变量违反 |

每个 harness 每次运行 30 分钟，在 [fuzz-nightly](../.github/workflows/fuzz-nightly.yml) 工作流中每天执行两次。发现的语料库作为构件上传。

### 本地运行

```bash
mvn -P fuzz -pl raft-core test -Dtest='EraftpbParseFuzzTest'
mvn -P fuzz -pl raft-core test -Dtest='RaftStepFuzzTest'
```

---

## 集成测试（raft-tests）

使用真实 gRPC 套接字、真实 RocksDB 存储和临时端口的端到端测试：

| 测试类 | 验证内容 |
|--------|----------|
| `SingleNodeIntegrationTest` | 单节点启动、Leader 选举、提议、应用 |
| `ThreeNodeClusterIntegrationTest` | 3 节点集群选举、Leader 收敛、有序复制 |
| `RestartFromDiskIntegrationTest` | RocksDB 持久化：关闭、重新打开、恢复状态、不重复应用 |
| `LeaderFailoverIntegrationTest` | 杀死 Leader，存活节点重新选举，继续提交 |
| `SnapshotInstallIntegrationTest` | 分区 Follower 超过压缩点，愈合后安装 MsgSnapshot |
| `DynamicMembershipIntegrationTest` | ConfChangeV2：移除 voter、添加 learner、晋升为 voter |
| `PartitionIntegrationTest` | 少数分区无法提交；多数继续；愈合后收敛 |
| `ChaosFaultInjectionIntegrationTest` | 随机故障注入（丢失、分区、隔离），恢复验证 |
| `KvLinearizabilityIntegrationTest` | 并发 KV 操作 + Jepsen 风格线性一致性检查器 |
| `ZeroCopySnapshotStreamingTest` | 64 MiB 逐字节流式快照验证 |
| `MultiNodeRestartIntegrationTest` | 全节点重启 + 多数节点重启（少数存活） |
| `LeaderTransferIntegrationTest` | Leadership 转移至 Follower；活跃提议期间转移 |
| `ConfChangeCrashIntegrationTest` | 配置变更期间 Leader 崩溃；扩缩容（3->5->3） |

### 运行方式

```bash
mvn -pl raft-tests -am test                # 所有集成测试
mvn -pl raft-tests -am test -P fast        # 内循环跳过
```

---

## 混沌测试

### ChaosController / ChaosTransport

`ChaosTransport` 装饰器包装真实的 gRPC Transport，在发送和接收路径上查询共享的 `ChaosController` 注入故障。

**故障模型**（仅注入 Raft 设计上能容忍的故障）：

| 故障类型 | API | 效果 |
|----------|-----|------|
| 隔离 | `chaos.isolate(nodeId)` | 丢弃目标节点的所有出入消息 |
| 分区 | `chaos.partition(setA, setB)` | 阻断两组节点间的消息 |
| 链路阻断 | `chaos.blockLink(from, to)` | 阻断单一方向的链路 |
| 丢包概率 | `chaos.dropProbability(from, to, p)` | 以概率 p 随机丢弃消息 |
| 延迟 | `chaos.latency(from, to, ms)` | 在链路上增加延迟 |
| 重复 | `chaos.duplicate(from, to, n)` | 将消息重复 n 次 |
| 愈合 | `chaos.healAll()` | 清除所有故障 |

使用 `IntegrationTestSupport.chaosPeer(...)` 构建混沌包装的节点。

### ChaosFaultInjectionIntegrationTest

对运行中的集群施加随机故障组合：
1. 注入随机故障（隔离、分区、丢包）
2. 在混沌期间尝试提议
3. 愈合所有故障
4. 验证集群恢复并收敛

---

## 线性一致性检查

`KvLinearizabilityIntegrationTest` 对 3 节点集群执行并发 KV 操作，并使用线性一致性检查器（Jepsen 风格历史验证）校验结果。这能捕获仅在并发访问下才出现的一致性违反。

---

## 浸泡测试

标记为 `@Tag("soak")` 的长时间运行稳定性测试，排除在每次 PR CI 之外：

| 测试 | 时长 | 检查内容 |
|------|------|----------|
| `SoakStabilityTest` | 可配置（默认 60s，CI 30min） | 持续提议 + 周期性快照/压缩。断言持续的提交进展、无应用积压、无线程泄漏 |
| `ElectionCycleSoakIntegrationTest` | 可配置循环次数（默认 20） | 反复 Leader 崩溃 -> 重新选举 -> 重启循环。断言无已提交条目丢失、无提交回退、无线程泄漏 |

### 运行方式

```bash
# 默认时长
mvn -P soak -pl raft-tests test

# 30 分钟浸泡
mvn -P soak -pl raft-tests test -Dsoak.durationSeconds=1800

# 自定义选举循环次数
mvn -P soak -pl raft-tests test -Dsoak.electionCycles=50
```

---

## 覆盖率门禁

JaCoCo 覆盖率阈值，每次 PR 强制检查：

| 模块 | Instruction | Branch | Line | Method |
|------|:-----------:|:------:|:----:|:------:|
| raft-core | >= 85% | >= 80% | >= 88% | >= 85% |
| raft-transport-grpc | >= 75% | >= 60% | >= 75% | >= 80% |
| raft-storage-rocksdb | >= 80% | >= 70% | >= 80% | >= 95% |

项目级 Codecov：项目 >= 80%，补丁 >= 75%（1 个百分点容差）。
