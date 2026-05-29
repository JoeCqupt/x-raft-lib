# x-raft-lib / raft-core

> ⚠️ **Alpha (`0.1.0-alpha`) — not yet production-validated.** Protocol
> correctness is on par with etcd-raft, and the surrounding pieces are in
> place: pluggable metrics, error categorisation, a gRPC transport
> ([raft-transport-grpc](../raft-transport-grpc)) and a RocksDB Storage
> ([raft-storage-rocksdb](../raft-storage-rocksdb)). Remaining gaps before
> 1.0 are tracked in [`TODO.md`](./TODO.md).

English | [中文](README.zh.md)

A Java port of [etcd-io/raft](https://github.com/etcd-io/raft): a pure Raft
consensus state machine with no I/O dependencies.

## Why another Raft

The Java ecosystem has two well-known Raft libraries:

| | x-raft-lib (this) | [Apache Ratis][ratis] | [hashicorp/raft][hcraft] |
|---|---|---|---|
| Language | Java 17 | Java 8+ | Go (not Java) |
| Lineage | port of etcd-io/raft | clean-room | clean-room |
| I/O model | host-driven `Ready`/`Advance` | gRPC + log built-in | net/RPC + log built-in |
| State machine purity | yes (no I/O in core) | bundled (transport + log inside) | bundled |
| ConfChange | V1 + V2 (joint) | V1 | V2 (joint, recent) |
| Async storage writes | yes | n/a | n/a |
| Lease-based reads | yes | yes | yes |
| Status | alpha | production (Hadoop Ozone) | production |

x-raft-lib's niche: **closely tracks the etcd-io/raft state machine** so users
familiar with etcd can apply the same mental model. It is a *building block*,
not a batteries-included framework — you provide the network transport and
persistent Storage; the library gives you the protocol.

[ratis]: https://ratis.apache.org/
[hcraft]: https://github.com/hashicorp/raft

## Design

- **Pure state machine, zero I/O.** The core never touches disk, network, or
  the system clock. The host application drives ticks, persists log entries
  and snapshots, sends messages over its own transport, and applies
  committed entries.
- **`Ready`/`Advance` host loop** (mirrors etcd-raft `node.Ready()`):
  every state-machine output (entries to persist, messages to send, snapshot
  to install, committed entries to apply) is bundled into a single `Ready`
  struct. The host processes it in order, then calls `advance()`.
- **Single-threaded core, optional thread-safe wrapper.** `RawNode` is the
  thread-unsafe state machine. `DefaultNode` wraps it with a dedicated
  event-loop thread and a bounded input queue so multiple producer threads
  can call `propose / step / readIndex / ...` safely.

## Module layout

```
io.github.xinfra.lab.raft              // PUBLIC API
├── Node.java                 // thread-safe API entry point (startNode/restartNode)
├── RawNode.java              // single-threaded API
├── Config.java               // raft config + validate()
├── Ready.java                // host output bundle
├── Storage.java              // host-supplied storage interface
├── Transport.java            // host-supplied transport interface
├── MemoryStorage.java        // in-memory reference impl (testing only)
├── Status.java, SoftState.java, ReadState.java, Peer.java, ...
│
└── internal/                          // NOT API — see package-info.java
    ├── Raft.java             // state machine: election, log replication, heartbeat
    ├── RaftLog.java          // log management (unstable + storage)
    ├── Unstable.java         // unstable buffer (in-place shrink)
    ├── DefaultNode.java      // thread-safe Node wrapper (event loop)
    ├── ReadOnly.java         // ReadIndex implementation
    ├── confchange/Changer.java   // V1/V2 joint consensus
    ├── quorum/MajorityConfig.java, JointConfig.java
    └── tracker/Progress.java, ProgressTracker.java, Inflights.java
```

Everything under `internal/` is an implementation detail. Some of those
classes are declared `public` only so the API package can reach them across
the package boundary — depend only on `io.github.xinfra.lab.raft`.

## Quickstart (single-node, in-process)

> The snippet below is illustrative. For a working multi-node demo see
> [`KVCluster.java`](./src/test/java/io/github/xinfra/lab/raft/examples/KVCluster.java)
> in tests.

### `RawNode` — single-threaded

```java
Config cfg = new Config();
cfg.id = 1;
cfg.electionTick = 10;
cfg.heartbeatTick = 1;
cfg.storage = new MemoryStorage();
cfg.maxSizePerMsg = 64L * 1024;        // 64 KB per MsgApp
cfg.maxInflightMsgs = 256;

RawNode rn = RawNode.newRawNode(cfg);
rn.bootstrap(List.of(new Peer(1)));

while (running) {
    rn.tick();                                              // host clock
    for (Eraftpb.Message msg : received) rn.step(msg);      // inbound from transport
    if (rn.hasReady()) {
        Ready rd = rn.ready();
        // 1. persist (HardState + entries) ATOMICALLY before sending
        cfg.storage.append(rd.entries);
        if (!Util.isEmptySnap(rd.snapshot)) {
            cfg.storage.applySnapshot(rd.snapshot);
        }
        // 2. send messages over your transport
        send(rd.messages);
        // 3. apply committed entries to the state machine (must be idempotent)
        apply(rd.committedEntries);
        // 4. signal raft we are done with this Ready
        rn.advance(rd);
    }
}
```

### `DefaultNode` — thread-safe

```java
Node n = DefaultNode.startNode(cfg, List.of(new Peer(1)));

// Any thread can call:
n.tick();                                  // non-blocking, burst-limited to 128
n.propose("payload".getBytes());           // blocks until proposal accepted/rejected
n.proposeConfChange(ccv2);
n.readIndex("ctx".getBytes());
n.transferLeadership(myId, targetId);

// Single consumer drains Ready:
while (running) {
    Ready rd = n.ready();
    persist(rd.entries, rd.hardState);
    send(rd.messages);
    apply(rd.committedEntries);
    n.advance(rd);
}

n.stop();   // blocks until the event loop exits and pending futures complete
```

## Build & test

```bash
mvn test                       # full unit + functional suite + jacoco gates
mvn test -Dtest='RaftTest'     # one class
mvn test -Dtest='InteractionTest' -Ddatadriven.rewrite=true   # regenerate testdata
```

JDK 17+ required.

## Maven coordinates

```xml
<dependency>
    <groupId>io.github.x-infra-lab</groupId>
    <artifactId>raft-core</artifactId>
    <version>0.1.0-alpha</version>
</dependency>
```

> Not yet published to Maven Central. Build from source for now.
> The `protobuf-java` runtime (3.25.x) leaks into your classpath via this
> artifact — pinning will matter once we ship a stable line.

## Configuration cheatsheet

See [`Config.java`](./src/main/java/io/github/xinfra/lab/raft/Config.java) and
[Chinese README](./README.zh.md) for the full table. **Defaults are not
production-safe** — pay attention to:

- `maxUncommittedEntriesSize` defaults to `NO_LIMIT` (no propose backpressure).
- `maxSizePerMsg` defaults to `0` (validate accepts → leader will pack the
  entire log into one MsgAppend on first contact).
- `electionTick` should be ≥ `10 * heartbeatTick` (not enforced).

These are tracked in [`TODO.md`](./TODO.md) Step 2.1.

## Differences from etcd-io/raft

This is a port, not a clean-room implementation. The state machine is
faithful to etcd-raft semantics. Java-side adaptations:

- **Errors:** `RaftException` carries a `Code` enum (compare with
  `e.is(Code.X)`); invariant violations throw `RaftInvariantException`
  (a `RuntimeException`).
- **Random:** `ThreadLocalRandom` instead of `math/rand`.
- **Defensive copies in `Ready`:** `Ready.entries` / `committedEntries`
  are copied so `subList` views can't be invalidated mid-step.
- **`Unstable.stableTo`:** in-place `removeRange` + shrink-on-empty
  (≈ +140% on the hot path vs. fresh ArrayList).
- **`DefaultNode`:** event loop's `done/drain/notify` runs in a
  `try-finally` so an unchecked Throwable can never strand a producer
  blocked on a full queue.
- **`Changer.initProgress`:** `Next = max(lastIndex, 1)` instead of
  etcd-raft's `lastIndex + 1` — see comment in `Changer.java` for the
  rationale.

What's identical: leader election, log replication, commit advancement,
ReadIndex (safe + lease-based), joint consensus, snapshot install/restore,
PreVote, CheckQuorum, ForgetLeader, TransferLeader, inflights flow control.

## Sibling modules

`raft-core` is intentionally I/O-free. The reference Transport and
Storage implementations live in sibling Maven modules so users can pick
and choose without dragging gRPC + RocksDB into a vanilla raft-core
dependency:

- [**raft-transport-grpc**](../raft-transport-grpc) — gRPC implementation
  of `Transport`. Unary RPC for the hot path; client-streaming RPC for
  snapshots so multi-GB blobs don't hit protobuf's single-message
  ceiling.
- [**raft-storage-rocksdb**](../raft-storage-rocksdb) — RocksDB
  implementation of `Storage`. Atomic Ready-cycle persistence via
  `WriteBatch` (`writeBatched(entries, hardState, snapshot)`).
- [**raft-examples**](../raft-examples) — `RaftPeer` scaffold wiring
  the three together (transport + storage + Ready loop + persistent
  apply / confState watermarks).
- [**raft-tests**](../raft-tests) — cross-module integration tests
  against real gRPC sockets + real RocksDB stores: single-node,
  3-node cluster, restart-from-disk, leader failover.

## Roadmap

See [`TODO.md`](./TODO.md). At a glance:

- **Done** — open-source compliance (license headers, NOTICE,
  Sonatype-ready pom); production hardening (bounded queues, pluggable
  metrics, error categorisation, stop-with-timeout, leader observer,
  Storage contract docs); integration completeness (Transport interface,
  gRPC transport with TLS/mTLS + chunked snapshots, RocksDB storage,
  runnable multi-node KV demo, GitHub Actions CI); the `Storage` streaming
  snapshot API (sidecar payload, never fully in heap); structured
  logging/MDC (`RaftMdc`); a Spotless format gate plus CodeQL/Dependabot.
- **Remaining before 1.0** — end-to-end zero-copy snapshot transmission
  (core's `MsgSnapshot` and the gRPC transport still materialize the payload
  per-end), dropping the `Eraftpb` protobuf leak / `Status` view types from
  the public surface, broader chaos/soak coverage, and the first Maven
  Central release. The public-vs-internal package boundary is now in place:
  implementation classes live under `io.github.xinfra.lab.raft.internal`.

## Contributing

See [`CONTRIBUTING.md`](../CONTRIBUTING.md). Issue reports and PRs are
welcome — for non-trivial changes, please open an issue first to discuss.

## Security

For security issues, please follow [`SECURITY.md`](../SECURITY.md).

## License

[Apache License 2.0](./LICENSE). This project is a derivative work of
[etcd-io/raft](https://github.com/etcd-io/raft) (Apache-2.0); see
[`NOTICE`](./NOTICE) for attribution.
