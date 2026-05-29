# x-raft-lib

[![CI](https://github.com/x-infra-lab/x-raft-lib/actions/workflows/ci.yml/badge.svg)](https://github.com/x-infra-lab/x-raft-lib/actions/workflows/ci.yml)
[![CodeQL](https://github.com/x-infra-lab/x-raft-lib/actions/workflows/codeql.yml/badge.svg)](https://github.com/x-infra-lab/x-raft-lib/actions/workflows/codeql.yml)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)
![Java](https://img.shields.io/badge/Java-17%2B-orange.svg)

Java port of [etcd-io/raft](https://github.com/etcd-io/raft), split into
a pure-protocol core and pluggable Transport / Storage implementations.

> ⚠️ **Alpha (`0.1.0-alpha`) — not yet production-validated.** Protocol
> correctness is on par with etcd-raft, and the surrounding pieces
> (TLS/mTLS for the gRPC transport, chunked snapshots, error
> categorisation, a runnable multi-node KV demo) are in place. The
> remaining gaps before 1.0 are tracked in
> [`raft-core/TODO.md`](raft-core/TODO.md): a hardened public-API boundary,
> broader chaos/soak coverage, and the first Maven Central release. Pin to
> an exact version until then.

## Modules

| Module | Purpose |
|---|---|
| [**raft-core**](raft-core) | Pure Raft state machine. Zero I/O, zero network, zero clock. The host drives ticks and the Ready/Advance loop, supplies Storage, supplies Transport. |
| [**raft-transport-grpc**](raft-transport-grpc) | gRPC implementation of the `Transport` interface defined in raft-core. Unary RPC for hot path, streaming RPC for snapshots. |
| [**raft-storage-rocksdb**](raft-storage-rocksdb) | RocksDB implementation of the `Storage` interface. Atomic per-Ready-cycle persistence via `WriteBatch`. |
| [**raft-examples**](raft-examples) | Runnable distributed KV demo wiring the three together: a RocksDB-backed state machine replicated over gRPC. `KvClusterDemo` brings up an in-process cluster you can run from `main`. |
| [**raft-tests**](raft-tests) | Cross-module integration tests: real gRPC sockets + real RocksDB stores, single-node, 3-node cluster, restart-from-disk, leader failover. |

The split keeps raft-core's design invariant intact — no I/O dependencies,
no native libraries, runs anywhere a JVM does. Hosts that don't want gRPC
or RocksDB can supply their own implementations of the two interfaces.

## Build & test

```bash
# multi-module from the repo root:
mvn install
```

Each module can also be built individually with `mvn -pl <module> -am`.

## Status

- raft-core: unit + functional + property + data-driven suites; jacoco gate at 85% instruction / 80% branch / 88% line / 85% method.
- raft-transport-grpc: round-trip tests (unary + multi-MiB snapshot chunking).
- raft-storage-rocksdb: persistence-across-reopen and snapshot-truncates-log.
- raft-tests: cross-module integration (single-node, 3-node cluster, restart-from-disk, leader failover).

## Contributing & security

See [CONTRIBUTING.md](CONTRIBUTING.md) and [SECURITY.md](SECURITY.md).
Participation is governed by the [Code of Conduct](CODE_OF_CONDUCT.md).

## License

Apache-2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE) for attribution to
etcd-io/raft.
