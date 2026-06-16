# Architecture & Design

[English](architecture.md) | [中文](architecture.zh.md)

## Design Philosophy

x-raft-lib follows etcd-io/raft's core design principle: **pure state machine, zero I/O**.

The Raft core never touches disk, network, or the system clock. All I/O is driven by the host application through a well-defined contract:

1. The host calls `tick()` to advance the logical clock
2. The host calls `step(msg)` to feed inbound messages
3. The host drains `Ready` to get the state machine's output (entries to persist, messages to send, committed entries to apply)
4. The host calls `advance()` to signal completion

This design makes the core fully deterministic, testable without real I/O, and embeddable in any host architecture — synchronous or asynchronous, single-threaded or multi-threaded.

## Architecture Diagram

```
                ┌──────────────────────────────────────────┐
                │  application state machine (yours)       │
                │   apply(committedEntries)                │
                └──────────────▲────────────▲──────────────┘
                               │ committed  │ propose()
                               │            │
   tick()  ┌──────────────────────────────────────────────┐
   ──────▶ │  raft-core  (Node / RawNode)                  │
           │  election, replication, conf change,          │
           │  read-index, snapshots, ready loop            │
           └────────▲──────────────────────────▲───────────┘
                    │ Ready: msgs / entries    │ step(msg)
                    │       hardState/snap     │
                    │                          │
       ┌────────────┴───────────┐  ┌───────────┴──────────────┐
       │  Storage (interface)   │  │  Transport (interface)   │
       │  ─────────────────     │  │  ──────────────────      │
       │  raft-storage-rocksdb  │  │  raft-transport-grpc     │
       │  (or your own impl)    │  │  (or your own impl)      │
       └────────────────────────┘  └──────────────────────────┘
```

The API package is `@NullMarked` (JSpecify): every public reference is non-null unless explicitly `@Nullable`. The internal `Raft` state machine's fields are package-private; cross-package callers go through documented accessors.

## Module Responsibility Matrix

| Module | Responsibility | Dependencies |
|--------|---------------|-------------|
| **raft-proto** | Protobuf message definitions (`eraftpb.proto`, vendored from etcd-raft) | protobuf-java |
| **raft-core** | Pure Raft state machine: election, replication, conf change, ReadIndex, snapshot, Ready loop | raft-proto |
| **raft-transport-grpc** | gRPC Transport: message delivery, snapshot streaming, TLS/mTLS | raft-core, grpc-netty-shaded |
| **raft-storage-rocksdb** | RocksDB Storage: atomic log persistence, snapshot management | raft-core, rocksdbjni |
| **raft-examples** | End-to-end KV demo: `RaftPeer` glue + `RocksKvStore` state machine | all above |
| **raft-tests** | Cross-module integration & chaos test suite | all above |

## Core Internals

### Raft.java — The State Machine

The central class that implements the Raft consensus algorithm. It manages:

- **Leader election** with randomized timeouts, PreVote protocol, and CheckQuorum
- **Log replication** with batching (`MaxSizePerMsg`, `MaxInflightMsgs`)
- **Commit advancement** across the quorum
- **Heartbeat** and leader liveness checking

### RaftLog — Log Management

Combines two layers:

- **Unstable** — in-memory buffer for entries not yet persisted. Uses in-place `removeRange` + shrink-on-empty for hot-path performance
- **Storage** — the host-supplied durable log (backed by RocksDB or any custom implementation)

### DefaultNode — Thread-Safe Wrapper

Wraps `RawNode` with a dedicated event-loop thread. Multiple producer threads can safely call `propose()`, `step()`, `readIndex()`, etc. through a bounded input queue (default capacity 1024, backpressure on overflow).

### ReadOnly — Linearizable Reads

Two modes:

- **ReadOnlySafe** — broadcasts a heartbeat round to confirm leadership before responding
- **ReadOnlyLeaseBased** — trusts the leader lease, lower latency but requires synchronized clocks

### ConfChange / Joint Consensus

Full V1 and V2 (joint consensus) support via `Changer.java`. Atomic multi-node membership changes go through an intermediate joint configuration where both the old and new voter sets must agree.

### Progress / Inflights — Flow Control

Each follower has a `Progress` tracker that manages:

- **Probe / Replicate / Snapshot** states
- **Inflights** — a sliding window bounding the number and bytes of in-flight MsgAppend messages per follower

## Storage Interface

The `Storage` interface defines what the host must provide:

```java
public interface Storage extends AutoCloseable {
    InitialStateResult initialState();           // recover HardState + ConfState
    List<Entry> entries(long lo, long hi, long maxSize);  // log range
    long term(long i);                           // term of entry i
    long lastIndex();
    long firstIndex();
    Snapshot snapshot();
}
```

### RocksDB Implementation

Three column families:

| CF | Key | Value |
|----|-----|-------|
| `log` | uint64 BE index | serialized `Eraftpb.Entry` |
| `state` | `hard_state`, `applied`, `conf_state`, `snapshot_file` | serialized protobuf |
| `snapshot` | `snapshot` | serialized `Eraftpb.Snapshot` |

Key features:

- **Atomic Ready cycle**: `writeBatched(entries, hardState, snapshot)` — one `WriteBatch`, one fsync per Ready
- **Streaming snapshots**: side-car file under `<dbDir>/snapshots/` for multi-GB state machines. The `MsgSnapshot` carries metadata only; the payload streams Storage-to-Storage. Writes are crash-safe (temp -> fsync -> atomic rename -> dir fsync)
- **Apply watermark**: `setApplied(index)` persists the host's apply progress so restarts skip already-applied entries

See [raft-storage-rocksdb README](../raft-storage-rocksdb/README.md) for full details.

## Transport Interface

The `Transport` interface defines message delivery:

```java
public interface Transport {
    void send(long to, Message msg);
    void start();
    void close();
}
```

### gRPC Implementation

Two RPCs in `RaftTransportService`:

| RPC | Type | Purpose |
|-----|------|---------|
| `Send` | Unary | Hot path: heartbeat, append, vote, read-index |
| `InstallSnapshot` | Client-streaming | Multi-GB snapshot streaming. First chunk carries envelope metadata; subsequent chunks carry payload slices |

Key features:

- **TLS / mTLS**: `TlsConfig.builder()` for one-way TLS or mutual TLS with per-node certificates
- **Loopback elision**: messages to self are delivered without going through the network
- **Peer management**: `addPeer(id, address)` / `removePeer(id)` for dynamic membership

See [raft-transport-grpc README](../raft-transport-grpc/README.md) for full details.

## Feature Matrix vs etcd-raft

| Capability | etcd-raft | x-raft-lib |
|---|:---:|:---:|
| Leader election (randomised timeout) | Yes | Yes |
| PreVote (avoid disruptive elections) | Yes | Yes |
| CheckQuorum (leader liveness) | Yes | Yes |
| Log replication (MaxSizePerMsg / MaxInflightMsgs batching) | Yes | Yes |
| Joint consensus / ConfChangeV2 (atomic multi-node membership) | Yes | Yes |
| Learner promotion / demotion | Yes | Yes |
| Linearizable reads — ReadOnlySafe | Yes | Yes |
| Linearizable reads — ReadOnlyLeaseBased | Yes | Yes |
| Leadership transfer (MsgTransferLeader) | Yes | Yes |
| Step-down on removal | Yes | Yes |
| Async storage writes (MsgStorageAppend / MsgStorageApply) | Yes | Yes |
| In-line snapshots | Yes | Yes |
| **Out-of-band streaming snapshots** | No | Yes |
| **Bounded pendingReadIndexMessages / readStates** | No | Yes |
| RaftMetrics pluggable sink | No | Yes |
| TraceLogger per-event hook | Partial | Yes |
| Storage reference impl (RocksDB) | No | Yes |
| Transport reference impl (gRPC + TLS/mTLS) | No | Yes |
| Coverage-guided fuzzing (Jazzer) | No | Yes (nightly) |
| Linearizability checker / chaos test framework | No | Yes |
| Production mileage | Yes (years) | No (alpha) |
| Battle-tested API stability | Yes (post-1.0) | No (until 1.0) |

The **out-of-band streaming snapshot** is the headline feature: a multi-GiB snapshot streams Storage-to-Storage through a client-streaming gRPC channel. The payload never fully materialises in heap.

## Key Differences from etcd-raft

This is a port, not a clean-room implementation. The state machine is faithful to etcd-raft semantics. Java-side adaptations:

| Area | Adaptation |
|------|-----------|
| **Error handling** | `RaftException` carries a `Code` enum; `RaftInvariantException` (RuntimeException) for protocol violations |
| **Random source** | `ThreadLocalRandom` instead of `math/rand` |
| **Ready fields** | Defensive ArrayList copy to prevent subList invalidation during async processing |
| **Unstable.stableTo** | In-place `removeRange` + shrink-on-empty (~140% hot-path improvement) |
| **DefaultNode** | try-finally ensures done/drain/notify execute even on Throwable escape |
| **Changer.initProgress** | `Next = max(lastIndex, 1)` instead of `lastIndex + 1` (see Changer.java comment) |

What's identical: leader election, log replication, commit advancement, ReadIndex (safe + lease-based), joint consensus, snapshot install/restore, PreVote, CheckQuorum, ForgetLeader, TransferLeader, inflights flow control.
