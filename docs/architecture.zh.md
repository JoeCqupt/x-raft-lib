# 架构与设计

[English](architecture.md) | [中文](architecture.zh.md)

## 设计理念

x-raft-lib 遵循 etcd-io/raft 的核心设计原则：**纯状态机，零 I/O**。

Raft 核心不写磁盘、不发网络、不管定时器。所有 I/O 由上层应用通过明确定义的契约驱动：

1. 上层调用 `tick()` 推进逻辑时钟
2. 上层调用 `step(msg)` 投递入站消息
3. 上层读取 `Ready` 获取状态机输出（需持久化的日志条目、需发送的消息、需应用的已提交条目）
4. 上层调用 `advance()` 通知处理完成

这种设计使核心完全确定性、无需真实 I/O 即可测试，并可嵌入任何宿主架构 —— 同步或异步、单线程或多线程。

## 架构图

```
                ┌──────────────────────────────────────────┐
                │  应用状态机（由你实现）                    │
                │   apply(committedEntries)                │
                └──────────────▲────────────▲──────────────┘
                               │ 已提交     │ propose()
                               │            │
   tick()  ┌──────────────────────────────────────────────┐
   ──────▶ │  raft-core  (Node / RawNode)                  │
           │  选举、复制、配置变更、                         │
           │  ReadIndex、快照、Ready 循环                   │
           └────────▲──────────────────────────▲───────────┘
                    │ Ready: 消息 / 日志条目   │ step(msg)
                    │       HardState / 快照   │
                    │                          │
       ┌────────────┴───────────┐  ┌───────────┴──────────────┐
       │  Storage（接口）        │  │  Transport（接口）        │
       │  ─────────────────     │  │  ──────────────────      │
       │  raft-storage-rocksdb  │  │  raft-transport-grpc     │
       │  （或自定义实现）        │  │  （或自定义实现）          │
       └────────────────────────┘  └──────────────────────────┘
```

API 包标注了 `@NullMarked`（JSpecify）：每个公开引用默认非空，除非显式标注 `@Nullable`。内部 `Raft` 状态机的字段为 package-private，跨包调用必须通过文档化的访问器。

## 模块职责矩阵

| 模块 | 职责 | 依赖 |
|------|------|------|
| **raft-proto** | Protobuf 消息定义（`eraftpb.proto`，从 etcd-raft 移植） | protobuf-java |
| **raft-core** | 纯 Raft 状态机：选举、复制、配置变更、ReadIndex、快照、Ready 循环 | raft-proto |
| **raft-transport-grpc** | gRPC Transport：消息投递、快照流式传输、TLS/mTLS | raft-core, grpc-netty-shaded |
| **raft-storage-rocksdb** | RocksDB Storage：原子日志持久化、快照管理 | raft-core, rocksdbjni |
| **raft-examples** | 端到端 KV 示例：`RaftPeer` 胶水层 + `RocksKvStore` 状态机 | 以上全部 |
| **raft-tests** | 跨模块集成与混沌测试套件 | 以上全部 |

## 核心内部实现

### Raft.java —— 状态机

实现 Raft 共识算法的核心类，管理：

- **Leader 选举**：随机化超时、PreVote 协议、CheckQuorum
- **日志复制**：批量化（`MaxSizePerMsg`、`MaxInflightMsgs`）
- **提交推进**：跨 Quorum 的提交索引推进
- **心跳**与 Leader 存活检查

### RaftLog —— 日志管理

组合两层：

- **Unstable** —— 尚未持久化的内存缓冲。使用 in-place `removeRange` + shrink-on-empty 优化热路径性能
- **Storage** —— 上层提供的持久化日志（RocksDB 或任何自定义实现）

### DefaultNode —— 线程安全封装

用独立的事件循环线程封装 `RawNode`。多个生产者线程可安全调用 `propose()`、`step()`、`readIndex()` 等方法，通过有界输入队列（默认容量 1024，溢出时背压）。

### ReadOnly —— 线性一致性读

两种模式：

- **ReadOnlySafe** —— 广播心跳轮次确认 Leadership 后响应
- **ReadOnlyLeaseBased** —— 信任 Leader 租约，延迟更低但需要时钟同步

### ConfChange / Joint Consensus

通过 `Changer.java` 完整支持 V1 和 V2（联合共识）。原子多节点成员变更经过中间联合配置，在此期间旧配置和新配置必须同时达成多数。

### Progress / Inflights —— 流控

每个 Follower 有一个 `Progress` 跟踪器，管理：

- **Probe / Replicate / Snapshot** 状态
- **Inflights** —— 滑动窗口，限制每个 Follower 的在飞 MsgAppend 消息数和字节数

## Storage 接口

`Storage` 接口定义上层必须提供的能力：

```java
public interface Storage {
    RaftState initialState();                    // 恢复 HardState + ConfState
    List<Entry> entries(long lo, long hi, long maxSize);  // 日志范围查询
    long term(long i);                           // 条目 i 的任期
    long lastIndex();
    long firstIndex();
    Snapshot snapshot();
}
```

### RocksDB 实现

三个列族：

| 列族 | Key | Value |
|------|-----|-------|
| `log` | uint64 大端序索引 | 序列化的 `Eraftpb.Entry` |
| `state` | `hard_state`、`applied`、`conf_state`、`snapshot_file` | 序列化的 protobuf |
| `snapshot` | `snapshot` | 序列化的 `Eraftpb.Snapshot` |

核心特性：

- **原子 Ready 周期**：`writeBatched(entries, hardState, snapshot)` —— 一次 `WriteBatch`，一次 fsync
- **流式快照**：快照文件存放在 `<dbDir>/snapshots/` 下，适用于多 GB 状态机。`MsgSnapshot` 仅携带元数据；负载在 Storage 之间流式传输。写入是崩溃安全的（临时文件 -> fsync -> 原子重命名 -> 目录 fsync）
- **Apply 水位**：`setApplied(index)` 持久化上层的应用进度，重启时跳过已应用的条目

详见 [raft-storage-rocksdb README](../raft-storage-rocksdb/README.md)。

## Transport 接口

`Transport` 接口定义消息投递：

```java
public interface Transport {
    void send(long to, Message msg);
    void start();
    void close();
}
```

### gRPC 实现

`RaftTransportService` 中的两个 RPC：

| RPC | 类型 | 用途 |
|-----|------|------|
| `Send` | Unary | 热路径：心跳、日志追加、投票、ReadIndex |
| `InstallSnapshot` | Client-Streaming | 多 GB 快照流式传输。首个 chunk 携带信封元数据；后续 chunk 携带负载分片 |

核心特性：

- **TLS / mTLS**：通过 `TlsConfig.builder()` 配置单向 TLS 或双向 mTLS
- **回环消息优化**：发给自己的消息不经过网络
- **动态节点管理**：`addPeer(id, address)` / `removePeer(id)`

详见 [raft-transport-grpc README](../raft-transport-grpc/README.md)。

## 与 etcd-raft 的功能对比矩阵

| 能力 | etcd-raft | x-raft-lib |
|------|:---------:|:----------:|
| Leader 选举（随机化超时） | 是 | 是 |
| PreVote（防止破坏性选举） | 是 | 是 |
| CheckQuorum（Leader 存活检查） | 是 | 是 |
| 日志复制（MaxSizePerMsg / MaxInflightMsgs 批量化） | 是 | 是 |
| Joint Consensus / ConfChangeV2（原子多节点成员变更） | 是 | 是 |
| Learner 升级 / 降级 | 是 | 是 |
| 线性一致性读 —— ReadOnlySafe | 是 | 是 |
| 线性一致性读 —— ReadOnlyLeaseBased | 是 | 是 |
| Leadership 转移（MsgTransferLeader） | 是 | 是 |
| 被移除时主动下台 | 是 | 是 |
| 异步存储写入（MsgStorageAppend / MsgStorageApply） | 是 | 是 |
| 内联快照 | 是 | 是 |
| **带外流式快照** | 否 | 是 |
| **有界 pendingReadIndexMessages / readStates** | 否 | 是 |
| RaftMetrics 可插拔 Sink | 否 | 是 |
| TraceLogger 逐事件 Hook | 部分 | 是 |
| Storage 参考实现（RocksDB） | 否 | 是 |
| Transport 参考实现（gRPC + TLS/mTLS） | 否 | 是 |
| 覆盖率引导的模糊测试（Jazzer） | 否 | 是（每夜） |
| 线性一致性检查器 / 混沌测试框架 | 否 | 是 |
| 生产环境验证 | 是（多年） | 否（alpha） |
| API 稳定性承诺 | 是（1.0 后） | 否（1.0 前） |

**带外流式快照**是核心亮点：多 GB 的应用快照通过 Client-Streaming gRPC 通道在 Storage 之间流式传输，负载永远不会完整加载到堆中。

## 与 etcd-raft 的关键差异

这是一个移植项目，而非从零实现。状态机忠实于 etcd-raft 语义。Java 化做了以下适配：

| 领域 | 适配方式 |
|------|----------|
| **错误处理** | `RaftException` 携带 `Code` 枚举；`RaftInvariantException`（RuntimeException）用于协议不变量违反 |
| **随机源** | 使用 `ThreadLocalRandom` 替代 `math/rand` |
| **Ready 字段** | 防御性 ArrayList 拷贝，防止异步处理期间 subList 视图失效 |
| **Unstable.stableTo** | in-place `removeRange` + shrink-on-empty（热路径性能提升约 140%） |
| **DefaultNode** | try-finally 保证 done/drain/notify 在 Throwable 逃逸时也会执行 |
| **Changer.initProgress** | `Next = max(lastIndex, 1)` 而非 `lastIndex + 1`（详见 Changer.java 注释） |

无差异的核心：选举、日志复制、提交推进、ReadIndex（safe + lease-based）、Joint Consensus、Snapshot install/restore、PreVote、CheckQuorum、ForgetLeader、TransferLeader、Inflights 流控。
