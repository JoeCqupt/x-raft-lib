# Roadmap

What's done, and what's left before a `1.0`. This is a port of
[etcd-io/raft](https://github.com/etcd-io/raft) plus pluggable Transport /
Storage modules; the protocol core is mature, the surrounding production and
release machinery is mostly in place, and the remaining work is API hardening
and broader test coverage.

## Done

### Open-source compliance
- Apache-2.0 `LICENSE` + `NOTICE` (etcd-io/raft attribution) at the repo root
  and in `raft-core`; license headers on all hand-written source files.
- English `README.md` + Chinese `README.zh.md`; per-module READMEs.
- `CONTRIBUTING`, `CODE_OF_CONDUCT`, `SECURITY`, `CHANGELOG` at the repo root.
- Sonatype-ready pom: `<licenses>`, `<developers>`, `<scm>`,
  `<issueManagement>`, and a `release` profile (sources + javadoc + GPG +
  `central-publishing-maven-plugin`). Tag-driven `release.yml` GitHub
  Actions job runs the signed deploy on every `v*` tag push.

### Production hardening
- Bounded queues for `pendingReadIndexMessages` and `readStates`
  (drop-oldest + warn + metric); `Config.maxPendingReadIndexMessages` /
  `maxReadStates`.
- Pluggable `RaftMetrics` (zero external deps) wired across the common events.
- Error categorisation: `RaftException` carries a `Code`; invariant
  violations throw `RaftInvariantException`.
- Lifecycle: `Node.stop(timeout, TimeUnit)`, lock-free `Node.basicStatus()`,
  `Node.registerLeaderObserver`, `Config.daemonEventLoop`.
- `Config.validate()` rejects unsafe defaults; `electionElapsed` /
  `heartbeatElapsed` widened to `long`.
- `Storage` interface extended (`append` / `setHardState` / `applySnapshot` /
  `createSnapshot` / `compact` / `close`) with full atomicity / durability /
  concurrency / async-path javadoc.

### Integration
- `Transport` interface in raft-core (core stays I/O-free).
- **raft-transport-grpc** — unary RPC hot path, client-streaming + chunked
  RPC for snapshots (bypasses protobuf's 2 GiB single-message ceiling),
  TLS / mTLS support.
- **raft-storage-rocksdb** — three column families, atomic Ready-cycle
  persistence via `writeBatched`, streaming snapshot sidecar, reopen-from-disk
  verified.
- **End-to-end zero-copy snapshot transmission.** The payload travels
  Storage→Storage fully out-of-band: the leader streams its sidecar over a
  client-streaming gRPC channel into the follower's sidecar (reused chunk
  buffer, ~1 MiB back-pressured pipe), while the `MsgSnapshot` that crosses the
  wire and the snapshots persisted on both ends carry metadata only — never the
  bytes. The follower stages the payload durably *before* the core restores
  (stage-then-finalize), so the restore-ordering and sidecar double-persist
  hazards are both closed. Covered by `SnapshotInstallIntegrationTest` and a
  64 MiB byte-for-byte `ZeroCopySnapshotStreamingTest`; non-streaming
  Storage/Transport fall back to the inline path automatically.
- **raft-examples** — runnable distributed KV demo (`KvClusterDemo#main`).
- **raft-tests** — cross-module integration (single-node, 3-node cluster,
  restart-from-disk, leader failover, dynamic membership, snapshot install,
  partition, chaos, soak).
- GitHub Actions CI (`.github/workflows/ci.yml`): JDK 17/21 matrix, full
  reactor `mvn install`, jacoco summary.
- Datadriven corpus at 28 scenarios (matches the etcd-raft target):
  elections, replication, conf-change V1/V2 + learner lifecycle, snapshot
  install/compact, prevote, checkquorum, forget-leader, leader-transfer,
  partition recovery (`raft-core/src/test/resources/testdata`).

### Public API boundary
- Public vs. internal split via the `internal` package convention: the Raft
  state machine and its helpers (`Raft`, `RaftLog`, `Unstable`, `ReadOnly`,
  `DefaultNode`, `confchange/`, `quorum/`, `tracker/`, `Util`, tracing) now
  live under `io.github.xinfra.lab.raft.internal`; only the host-facing API
  (`Node`, `RawNode`, `Config`, `Ready`, `Storage`, `Transport`, `Status`,
  …) stays in the root package. White-box tests moved alongside the internal
  classes; `internal/package-info.java` documents the boundary.
- `Status` no longer leaks `tracker` / `quorum` types: it exposes the dedicated
  public view types `PeerProgress`, `TrackerConfig`, and `ProgressState`.

### Observability & tooling
- Structured logging / MDC: `RaftMdc` populates node-id / term / role /
  leader on the event-loop thread (see `DefaultNode#run`).
- Spotless format/lint gate (`spotless:check` in `verify`); CodeQL,
  Dependabot, maven-enforcer build guard, `.editorconfig`.
- Per-module jacoco coverage gates: raft-core 85/80/88/85 (instruction/
  branch/line/method); raft-examples, raft-storage-rocksdb, and
  raft-transport-grpc each enforce a regression-guard floor set just below
  their own-test coverage. raft-tests has no main classes, so it carries no
  gate. (Per-module reports under-credit the cross-module coverage that
  raft-tests' integration suite contributes to the transport/storage modules.)

## Remaining before 1.0

### Public API boundary
- Stop leaking generated `Eraftpb` protobuf types through the public API.
- Decide on `Config` → Builder and `Node.propose` → exception/Result before
  freezing the API.

### Storage / transport
- Tunable RocksDB options (block cache, compaction style).
- Protocol version field + capability negotiation for safe rolling upgrades.

### Observability & ops
- Rate-limited proposal-rejection logging.


### Release & tooling
- Cut the first Maven Central release and add the version badge.
- Checkstyle/PMD beyond the Spotless hygiene gate; tag → release automation.

## Deliberate non-changes

- POJO → record/Builder for `Config` / `Ready` / `SoftState` — large blast
  radius across tests, limited benefit.
- `(value, error)` records → exceptions — current pattern works.
- Splitting the `Raft` god-class — architectural churn, no behaviour change.
- `Changer.initProgress` keeps `Next = max(lastIndex, 1)` instead of
  etcd-raft's `lastIndex + 1` (the +1 form worsens fresh-peer convergence and
  breaks the `snapshot_install_after_compact` scenario; see `Changer.java`).
