# x-raft-lib

[![Maven Central](https://img.shields.io/maven-central/v/io.github.x-infra-lab/raft-core?label=Maven%20Central)](https://central.sonatype.com/artifact/io.github.x-infra-lab/raft-core)
[![CI](https://github.com/x-infra-lab/x-raft-lib/actions/workflows/ci.yml/badge.svg)](https://github.com/x-infra-lab/x-raft-lib/actions/workflows/ci.yml)
[![Chaos / Soak](https://github.com/x-infra-lab/x-raft-lib/actions/workflows/chaos-soak-weekly.yml/badge.svg)](https://github.com/x-infra-lab/x-raft-lib/actions/workflows/chaos-soak-weekly.yml)
[![Fuzz](https://github.com/x-infra-lab/x-raft-lib/actions/workflows/fuzz-nightly.yml/badge.svg)](https://github.com/x-infra-lab/x-raft-lib/actions/workflows/fuzz-nightly.yml)
[![CodeQL](https://github.com/x-infra-lab/x-raft-lib/actions/workflows/codeql.yml/badge.svg)](https://github.com/x-infra-lab/x-raft-lib/actions/workflows/codeql.yml)
[![codecov](https://codecov.io/gh/x-infra-lab/x-raft-lib/branch/main/graph/badge.svg)](https://codecov.io/gh/x-infra-lab/x-raft-lib)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)
![Java](https://img.shields.io/badge/Java-17%2B-orange.svg)

A faithful Java port of [etcd-io/raft](https://github.com/etcd-io/raft),
split into a pure-protocol core and pluggable Transport / Storage
implementations. The core has zero I/O and zero network dependencies;
production-grade gRPC and RocksDB modules ship alongside.

> ⚠️ **Latest release: `0.1.0-RC1`** (on Maven Central). **`main` is
> `0.1.0-SNAPSHOT`** — unreleased work toward `0.1.0` GA. Protocol
> behaviour matches etcd-raft 1:1; the public API has been hardened
> (immutable `Status` / `Ready` records, `Config.Builder`, JSpecify
> nullability, internal `Raft` fields locked to package-private). RC
> means the `0.1.0` API surface is frozen, but production mileage /
> external benchmarks are still ahead of `1.0`. Pin to an exact version
> until `1.0`.

## Modules

| Module | Purpose |
|---|---|
| [**raft-core**](raft-core) | Pure Raft state machine. Zero I/O, zero network, zero clock. The host drives ticks and the Ready/Advance loop, supplies Storage, supplies Transport. |
| [**raft-transport-grpc**](raft-transport-grpc) | gRPC `Transport` implementation. Unary RPC for the hot path, client-streaming for snapshots, TLS / mTLS support. |
| [**raft-storage-rocksdb**](raft-storage-rocksdb) | RocksDB `Storage` implementation. Atomic per-Ready-cycle persistence via `WriteBatch`; out-of-band side-car file for streaming snapshots. |
| [**raft-examples**](raft-examples) | Production-quality distributed KV server (`KvServerBootstrap`) wiring the three together: a RocksDB-backed state machine replicated over gRPC, with gRPC client API and admin service. |
| [**raft-tests**](raft-tests) | Cross-module integration suite — real gRPC sockets, real RocksDB stores, partition / chaos / linearizability / restart-from-disk / snapshot-install scenarios. |

The split keeps raft-core's invariant intact: no I/O, no native libs,
runs anywhere a JVM does. Hosts that don't want gRPC or RocksDB plug in
their own implementations of the two interfaces.

## Quick start

### 1. Run the in-process KV demo

The fastest way to see all five modules cooperating is the bundled
3-node KV cluster:

```bash
mvn -f raft-examples/pom.xml compile exec:java \
    -Dexec.mainClass=io.github.xinfra.lab.raft.examples.KvServerBootstrap
```

This brings up three peers in a single JVM (each on its own gRPC port,
its own RocksDB directory), elects a leader, runs a small scripted
workload through the leader, waits for replication, and prints each
peer's final KV view. The wiring is the same one you'd use across real
hosts — only the addresses in the peer map change.

### 2. Embed in your own application

Add the dependencies (replace the version with the latest tag):

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

Wire up a node:

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

// 4. Start the node and wire transport callbacks. Node.step / ready / advance
//    declare checked exceptions, so the receiver lambda has to catch them
//    rather than use a method reference.
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
            // 1. Persist hardState + entries + snapshot atomically.
            storage.writeBatched(rd.entries(), rd.hardState(), rd.snapshot());
            // 2. Send outbound messages (before applying — raft correctness
            //    requires persisted entries to be sent before applying).
            for (Eraftpb.Message m : rd.messages()) transport.send(m.getTo(), m);
            // 3. Apply snapshot to the application state machine (if any).
            if (rd.snapshot().getMetadata().getIndex() > 0) {
                restoreStateMachineFromSnapshot(rd.snapshot());
            }
            // 4. Apply committed entries to the application state machine.
            for (Eraftpb.Entry e : rd.committedEntries()) applyToStateMachine(e);
            // 5. Signal done.
            node.advance();
        }
    } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
}, "raft-ready").start();

// 7. Propose application data.
node.propose("hello".getBytes(StandardCharsets.UTF_8));
```

The `RaftKVNode` class in
[`raft-examples`](raft-examples/src/main/java/io/github/xinfra/lab/raft/examples/RaftKVNode.java)
wraps this loop in a reusable host; copy and adapt rather than starting
from scratch.

## Architecture

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

The contract is enforced by the API package being `@NullMarked`
(JSpecify): every public reference is non-null unless explicitly
`@Nullable`. The internal `Raft` state machine's fields are
package-private; cross-package callers go through documented
accessors, so a host can't accidentally mutate raft state.

## Feature matrix vs etcd-raft

Behaviour matches etcd-raft 1:1 unless flagged otherwise. Wire format
is `eraftpb.proto` (the etcd-raft proto file, vendored as-is in
`raft-proto`).

| Capability | etcd-raft | x-raft-lib |
|---|:---:|:---:|
| Leader election (randomised timeout) | ✅ | ✅ |
| PreVote (avoid disruptive elections) | ✅ | ✅ |
| CheckQuorum (leader liveness) | ✅ | ✅ |
| Log replication (`MaxSizePerMsg` / `MaxInflightMsgs` batching) | ✅ | ✅ |
| Joint consensus / `ConfChangeV2` (atomic multi-node membership) | ✅ | ✅ |
| Learner promotion / demotion | ✅ | ✅ |
| Linearizable reads — `ReadOnlySafe` | ✅ | ✅ |
| Linearizable reads — `ReadOnlyLeaseBased` | ✅ | ✅ |
| Leadership transfer (`MsgTransferLeader`) | ✅ | ✅ |
| Step-down on removal | ✅ | ✅ |
| Async storage writes (`MsgStorageAppend` / `MsgStorageApply`) | ✅ | ✅ |
| In-line snapshots (payload inside `MsgSnapshot`) | ✅ | ✅ |
| **Out-of-band streaming snapshots** (Storage→Storage side-car, `MsgSnapshot` carries metadata only) | ❌ | ✅ |
| **Bounded `pendingReadIndexMessages` / `readStates`** (drop-oldest + warn) | ❌ | ✅ |
| `RaftMetrics` pluggable sink (Micrometer / Prom / OTel friendly) | ❌ | ✅ |
| `TraceLogger` per-event hook | partial (Go `Tracer`) | ✅ |
| `Storage` reference impl (RocksDB, atomic Ready cycle) | ❌ (host writes own) | ✅ |
| `Transport` reference impl (gRPC + TLS / mTLS) | ❌ (host writes own) | ✅ |
| Coverage-guided fuzzing (Jazzer) on `step()` + parse boundary | ❌ | ✅ (nightly) |
| Linearizability checker / chaos test framework | ❌ | ✅ |
| Production mileage / large-cluster operator experience | ✅ (years) | ❌ (alpha) |
| Battle-tested API stability commitment | ✅ (post-1.0) | ❌ (until 1.0) |

The **out-of-band streaming snapshot** path is the headline feature.
A multi-GiB application snapshot streams Storage→Storage through a
client-streaming gRPC channel; the `MsgSnapshot` that crosses the wire
and the snapshots persisted on both ends carry metadata only — the
payload never fully materialises in heap. etcd-raft inlines the entire
payload into a single message and leaves transport / persistence to
the host.

## Benchmarks

⚠️ **No comparative numbers published yet.** JMH benchmarks for
hot-path raft operations live in the
[`raft-benchmark`](raft-benchmark/src/main/java/io/github/xinfra/lab/raft/benchmark/) module;
fair head-to-head numbers against etcd-raft and sofa-jraft require an
isolated benchmark host (no shared CI runner) and are tracked in the
roadmap.

Run the JMH suite locally:

```bash
mvn -B -ntp package -pl raft-benchmark -am -DskipTests
# core micro-benchmarks:
java -jar raft-benchmark/target/raft-benchmark.jar RaftCoreBenchmarks
# specific benchmark:
java -jar raft-benchmark/target/raft-benchmark.jar proposeAndDrain
```

For an end-to-end soak (3-node cluster, sustained propose load,
periodic snapshot+compact, thread-leak / apply-backlog assertions):

```bash
# default 60s; -Dsoak.durationSeconds=1800 for 30 min
mvn -P soak -pl raft-tests test
```

The same soak runs weekly in CI ([chaos-soak-weekly](.github/workflows/chaos-soak-weekly.yml))
at 30 min, plus a 5× chaos / partition / linearizability stress loop.

## Build & test

```bash
# full reactor (raft-core unit + functional + property + jacoco gate,
# transport / storage round-trip, integration suite)
mvn install

# fast inner loop — skip the integration suite
mvn -P fast install
```

Per-module:

```bash
mvn -pl raft-core -am install
mvn -pl raft-transport-grpc -am install
mvn -pl raft-storage-rocksdb -am install
```

CI runs every push and PR across **ubuntu-latest / macos-latest /
windows-latest** × **JDK 17 / 21**. RocksDB-tied modules are skipped
on Windows pending an upstream rocksdbjni native-binary fix.

## Quality gates

| Gate | Where | Threshold |
|---|---|---|
| Unit + functional + property tests | `mvn install` | must pass |
| JaCoCo coverage on `raft-core` | per-PR CI | 85% inst / 80% branch / 88% line / 85% method |
| JaCoCo coverage on `raft-transport-grpc` | per-PR CI | 75% inst / 60% branch / 75% line / 80% method |
| JaCoCo coverage on `raft-storage-rocksdb` | per-PR CI | 80% inst / 70% branch / 80% line / 95% method |
| [Codecov](https://codecov.io/gh/x-infra-lab/x-raft-lib) project + patch | per-PR CI | project ≥80% / patch ≥75% (1pp tolerance) |
| Cross-platform smoke | per-PR CI | linux + macOS + windows × JDK 17/21 |
| Integration suite (gRPC + RocksDB) | per-PR CI | must pass |
| Coverage-guided fuzz on `step()` + parse | [`fuzz-nightly`](.github/workflows/fuzz-nightly.yml) | no findings |
| Soak + chaos stress | [`chaos-soak-weekly`](.github/workflows/chaos-soak-weekly.yml) | 30 min soak + 5× chaos must pass |
| Spotless format / unused-import gate | `mvn verify` | no diff |
| CodeQL static analysis | [`codeql`](.github/workflows/codeql.yml) | no high-severity findings |

## Documentation

Comprehensive bilingual (English / Chinese) documentation lives in
[`docs/`](docs/index.md):

- [Architecture & Design](docs/architecture.md) — design philosophy, core internals, Storage & Transport interfaces
- [Getting Started](docs/getting-started.md) — prerequisites, KV demo, embedding guide, configuration reference
- [Testing Strategy](docs/testing.md) — test pyramid, unit / property / fuzz / integration / chaos / soak
- [CI/CD & Quality Gates](docs/ci.md) — CI pipeline, fuzz nightly, chaos soak weekly, release process

Chinese versions: [文档中心](docs/index.zh.md) |
[架构与设计](docs/architecture.zh.md) |
[快速开始](docs/getting-started.zh.md) |
[测试策略](docs/testing.zh.md) |
[CI/CD 与质量门禁](docs/ci.zh.md)

## Contributing & security

See [CONTRIBUTING.md](CONTRIBUTING.md) and [SECURITY.md](SECURITY.md).
Maintainer list and how to become one: [MAINTAINERS.md](MAINTAINERS.md).
How releases are cut: [RELEASING.md](RELEASING.md).
Participation is governed by the [Code of Conduct](CODE_OF_CONDUCT.md).

## License

Apache-2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE) for attribution
to etcd-io/raft.
