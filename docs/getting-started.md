# Getting Started

[English](getting-started.md) | [中文](getting-started.zh.md)

## Prerequisites

- **JDK 17+** (Temurin recommended)
- **Maven 3.8+**
- RocksDB native binaries are bundled via `rocksdbjni` — no manual install needed (Linux x86_64/aarch64, macOS x86_64/aarch64; Windows support pending upstream fix)

## Maven Coordinates

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

## 1. Run the KV Demo

The fastest way to see all modules working together is the bundled 3-node KV cluster:

```bash
mvn -f raft-examples/pom.xml compile exec:java \
    -Dexec.mainClass=io.github.xinfra.lab.raft.examples.KvServerBootstrap
```

This brings up three peers in a single JVM (each on its own gRPC port, its own RocksDB directory), elects a leader, runs a scripted workload, waits for replication, and prints each peer's final KV view.

See [raft-examples README](../raft-examples/README.md) for details.

## 2. Embed in Your Application

### Step-by-step

```java
// 1. Storage — durable log + snapshots.
RocksDbStorage storage = new RocksDbStorage(Path.of("/var/lib/myapp/raft-1"));

// 2. Transport — gRPC; bind a port, register peers.
GrpcTransport transport = new GrpcTransport(/*localId=*/ 1, /*localPort=*/ 9001);
transport.addPeer(2L, "peer-2.local:9001");
transport.addPeer(3L, "peer-3.local:9001");

// 3. Config — validated at build time, immutable thereafter.
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
        .applied(storage.getApplied())        // recover from disk on restart
        .build();

// 4. Start the node and wire transport callbacks.
Node node = Node.startNode(cfg, List.of(new Peer(1), new Peer(2), new Peer(3)));
transport.setReceiver(msg -> {
    try { node.step(msg); }
    catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
    catch (RaftException re) { /* log + drop; raft tolerates message loss */ }
});
transport.setUnreachableListener(node::reportUnreachable);
transport.start();

// 5. Drive ticks (single-threaded scheduler, ~10ms per tick).
ScheduledExecutorService ticker = Executors.newSingleThreadScheduledExecutor();
ticker.scheduleAtFixedRate(node::tick, 0, 10, TimeUnit.MILLISECONDS);

// 6. Drain the Ready loop on a dedicated thread.
new Thread(() -> {
    try {
        while (running) {
            Ready rd = node.ready();
            // 1. Persist.
            storage.writeBatched(rd.entries(), rd.hardState(), rd.snapshot());
            // 2. Send messages.
            for (Eraftpb.Message m : rd.messages()) transport.send(m.getTo(), m);
            // 3. Apply snapshot (if any).
            if (rd.snapshot().getMetadata().getIndex() > 0) {
                restoreStateMachineFromSnapshot(rd.snapshot());
            }
            // 4. Apply committed entries.
            for (Eraftpb.Entry e : rd.committedEntries()) applyToStateMachine(e);
            // 5. Signal done.
            node.advance();
        }
    } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
}, "raft-ready").start();

// 7. Propose application data.
node.propose("hello".getBytes(StandardCharsets.UTF_8));
```

The `RaftKVNode` class in [raft-examples](../raft-examples/src/main/java/io/github/xinfra/lab/raft/examples/RaftKVNode.java) wraps this loop in a reusable host — copy and adapt rather than starting from scratch.

## RawNode vs DefaultNode

| | RawNode | DefaultNode |
|---|---------|-------------|
| Thread safety | Single-threaded, caller manages synchronization | Thread-safe via internal event loop |
| Usage | Direct control, test harnesses | Production multi-threaded hosts |
| API | `tick()`, `step()`, `hasReady()`, `ready()`, `advance()` | Same + bounded input queue (1024), backpressure |
| Tick behavior | Caller-driven | Non-blocking, burst limited to 128 |

### RawNode — Single-threaded

```java
RawNode rn = RawNode.newRawNode(cfg);
rn.bootstrap(List.of(new Peer(1)));

while (running) {
    rn.tick();
    for (Message msg : received) rn.step(msg);
    if (rn.hasReady()) {
        Ready rd = rn.ready();
        persist(rd.entries(), rd.hardState());           // 1. persist
        send(rd.messages());                             // 2. send
        if (!Util.isEmptySnap(rd.snapshot())) {
            restoreFromSnapshot(rd.snapshot());           // 3. apply snapshot
        }
        apply(rd.committedEntries());                    // 4. apply entries
        rn.advance(rd);                                  // 5. signal done
    }
}
```

### DefaultNode — Thread-safe

```java
Node n = DefaultNode.startNode(cfg, List.of(new Peer(1)));

// Any thread can call:
n.tick();
n.propose("payload".getBytes());
n.proposeConfChange(ccv2);
n.readIndex("ctx".getBytes());
n.transferLeadership(myId, targetId);

// Single consumer drains Ready:
while (running) {
    Ready rd = n.ready();
    persist(rd.entries(), rd.hardState());           // 1. persist
    send(rd.messages());                             // 2. send
    if (!Util.isEmptySnap(rd.snapshot())) {
        restoreFromSnapshot(rd.snapshot());           // 3. apply snapshot
    }
    apply(rd.committedEntries());                    // 4. apply entries
    n.advance(rd);                                   // 5. signal done
}

n.stop();
```

## Configuration Reference

| Field | Default | Description |
|-------|---------|-------------|
| `id` | (required) | Node ID, must not be 0 |
| `electionTick` | (required) | Election timeout in ticks; must be > heartbeatTick |
| `heartbeatTick` | (required) | Heartbeat interval in ticks; must be > 0 |
| `storage` | (required) | Persistent storage implementation |
| `maxSizePerMsg` | 0 | Max bytes per MsgApp |
| `maxCommittedSizePerReady` | maxSizePerMsg | Max bytes of committed entries per Ready |
| `maxUncommittedEntriesSize` | NO_LIMIT | Max total bytes of uncommitted entries |
| `maxInflightMsgs` | (required) | Max in-flight MsgApp per follower |
| `maxInflightBytes` | NO_LIMIT | Max in-flight bytes per follower |
| `preVote` | false | Enable PreVote to prevent term inflation |
| `checkQuorum` | false | Enable leader quorum check |
| `readOnlyOption` | ReadOnlySafe | ReadIndex implementation mode |
| `disableProposalForwarding` | false | Disable follower proposal forwarding |
| `stepDownOnRemoval` | false | Leader steps down when removed from config |
| `asyncStorageWrites` | false | Enable async storage via MsgStorageAppend/Apply |

**Important defaults to review for production:**

- `maxUncommittedEntriesSize` defaults to `NO_LIMIT` (no propose backpressure)
- `maxSizePerMsg` defaults to `0` (leader packs entire log into one MsgAppend on first contact)
- `electionTick` should be >= `10 * heartbeatTick` (not enforced but recommended)

## Build & Test

```bash
# full reactor build
mvn install

# fast inner loop — skip integration tests
mvn -P fast install

# per-module
mvn -pl raft-core -am install
mvn -pl raft-transport-grpc -am install
mvn -pl raft-storage-rocksdb -am install
```
