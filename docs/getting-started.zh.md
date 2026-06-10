# 快速开始

[English](getting-started.md) | [中文](getting-started.zh.md)

## 环境准备

- **JDK 17+**（推荐 Temurin）
- **Maven 3.8+**
- RocksDB 原生库通过 `rocksdbjni` 自动捆绑，无需手动安装（支持 Linux x86_64/aarch64、macOS x86_64/aarch64；Windows 支持等待上游修复）

## Maven 坐标

```xml
<dependency>
  <groupId>io.github.x-infra-lab</groupId>
  <artifactId>raft-core</artifactId>
  <version>0.1.0-RC1</version>
</dependency>
<dependency>
  <groupId>io.github.x-infra-lab</groupId>
  <artifactId>raft-transport-grpc</artifactId>
  <version>0.1.0-RC1</version>
</dependency>
<dependency>
  <groupId>io.github.x-infra-lab</groupId>
  <artifactId>raft-storage-rocksdb</artifactId>
  <version>0.1.0-RC1</version>
</dependency>
```

## 1. 运行 KV 示例

体验所有模块协同工作最快的方式是运行内置的 3 节点 KV 集群：

```bash
mvn -f raft-examples/pom.xml compile exec:java \
    -Dexec.mainClass=io.github.xinfra.lab.raft.examples.KvClusterDemo
```

这会在单个 JVM 中启动三个节点（各自绑定独立的 gRPC 端口和 RocksDB 目录），选出 Leader，执行一组脚本化的写操作，等待复制完成，然后输出每个节点的最终 KV 视图。

详见 [raft-examples README](../raft-examples/README.md)。

## 2. 集成到你的应用

### 分步指南

```java
// 1. Storage —— 持久化日志 + 快照。
RocksDbStorage storage = new RocksDbStorage(Path.of("/var/lib/myapp/raft-1"));

// 2. Transport —— gRPC；绑定端口，注册节点。
GrpcTransport transport = new GrpcTransport(/*localId=*/ 1, /*localPort=*/ 9001);
transport.addPeer(2L, "peer-2.local:9001");
transport.addPeer(3L, "peer-3.local:9001");

// 3. Config —— 构建时校验，此后不可变。
Config cfg = Config.builder()
        .id(1)
        .electionTick(10)
        .heartbeatTick(1)
        .storage(storage)
        .maxSizePerMsg(1L << 20)             // 1 MiB
        .maxInflightMsgs(256)
        .maxUncommittedEntriesSize(64L << 20) // 64 MiB
        .preVote(true)
        .checkQuorum(true)
        .applied(storage.getApplied())        // 重启时从磁盘恢复
        .build();

// 4. 启动节点并关联 Transport 回调。
Node node = Node.startNode(cfg, List.of(new Peer(1), new Peer(2), new Peer(3)));
transport.setReceiver(msg -> {
    try { node.step(msg); }
    catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
    catch (RaftException re) { /* 记录日志并丢弃；raft 容忍消息丢失 */ }
});
transport.setUnreachableListener(node::reportUnreachable);
transport.start();

// 5. 驱动 tick（单线程调度器，每 tick 约 10ms）。
ScheduledExecutorService ticker = Executors.newSingleThreadScheduledExecutor();
ticker.scheduleAtFixedRate(node::tick, 0, 10, TimeUnit.MILLISECONDS);

// 6. 在专用线程上消费 Ready 循环。
new Thread(() -> {
    try {
        while (running) {
            Ready rd = node.ready();
            storage.writeBatched(rd.entries(), rd.hardState(), rd.snapshot());
            for (Eraftpb.Entry e : rd.committedEntries()) applyToStateMachine(e);
            for (Eraftpb.Message m : rd.messages()) transport.send(m.getTo(), m);
            node.advance();
        }
    } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
}, "raft-ready").start();

// 7. 提议应用数据。
node.propose("hello".getBytes(StandardCharsets.UTF_8));
```

[raft-examples](../raft-examples/src/main/java/io/github/xinfra/lab/raft/examples/RaftPeer.java) 中的 `RaftPeer` 类将这个循环封装为可复用的宿主 —— 建议直接复制并适配，而非从零开始。

## RawNode vs DefaultNode

| | RawNode | DefaultNode |
|---|---------|-------------|
| 线程安全 | 单线程，调用方管理同步 | 通过内部事件循环保证线程安全 |
| 使用场景 | 精细控制、测试用例 | 生产环境多线程宿主 |
| API | `tick()`、`step()`、`hasReady()`、`ready()`、`advance()` | 相同 + 有界输入队列（1024）、背压 |
| Tick 行为 | 调用方驱动 | 非阻塞，突发限制 128 |

### RawNode —— 单线程

```java
RawNode rn = RawNode.newRawNode(cfg);
rn.bootstrap(List.of(new Peer(1)));

while (running) {
    rn.tick();
    for (Message msg : received) rn.step(msg);
    if (rn.hasReady()) {
        Ready rd = rn.ready();
        persist(rd.entries, rd.hardState);
        send(rd.messages);
        apply(rd.committedEntries);
        rn.advance(rd);
    }
}
```

### DefaultNode —— 多线程

```java
Node n = DefaultNode.startNode(cfg, List.of(new Peer(1)));

// 任意线程可调用：
n.tick();
n.propose("payload".getBytes());
n.proposeConfChange(ccv2);
n.readIndex("ctx".getBytes());
n.transferLeadership(myId, targetId);

// 单一消费者消费 Ready：
while (running) {
    Ready rd = n.ready();
    persist(rd.entries, rd.hardState);
    send(rd.messages);
    apply(rd.committedEntries);
    n.advance(rd);
}

n.stop();
```

## 配置参考

| 字段 | 默认值 | 说明 |
|------|--------|------|
| `id` | （必填） | 节点 ID，不能为 0 |
| `electionTick` | （必填） | 选举超时（tick 数）；必须 > heartbeatTick |
| `heartbeatTick` | （必填） | 心跳间隔（tick 数）；必须 > 0 |
| `storage` | （必填） | 持久化存储实现 |
| `maxSizePerMsg` | 0 | MsgApp 单条最大字节数 |
| `maxCommittedSizePerReady` | maxSizePerMsg | 单个 Ready 中 committed entries 最大字节数 |
| `maxUncommittedEntriesSize` | NO_LIMIT | 未提交 entries 总字节上限 |
| `maxInflightMsgs` | （必填） | 单 Follower 在飞 MsgApp 数上限 |
| `maxInflightBytes` | NO_LIMIT | 单 Follower 在飞字节数上限 |
| `preVote` | false | 启用 PreVote 防止 term 膨胀 |
| `checkQuorum` | false | 启用 Leader 主动 Quorum 检查 |
| `readOnlyOption` | ReadOnlySafe | ReadIndex 实现方式 |
| `disableProposalForwarding` | false | 禁用 Follower 转发 propose |
| `stepDownOnRemoval` | false | Leader 被移除时主动下台 |
| `asyncStorageWrites` | false | 持久化走 MsgStorageAppend/Apply 异步通道 |

**生产环境需要关注的默认值：**

- `maxUncommittedEntriesSize` 默认 `NO_LIMIT`（无 propose 背压）
- `maxSizePerMsg` 默认 `0`（Leader 首次联系时会将整个日志打包到一条 MsgAppend 中）
- `electionTick` 建议 >= `10 * heartbeatTick`（不强制但推荐）

## 构建与测试

```bash
# 完整构建
mvn install

# 快速内循环 —— 跳过集成测试
mvn -P fast install

# 单模块构建
mvn -pl raft-core -am install
mvn -pl raft-transport-grpc -am install
mvn -pl raft-storage-rocksdb -am install
```
