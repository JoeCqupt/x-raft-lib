# x-raft-lib Documentation

[![Maven Central](https://img.shields.io/maven-central/v/io.github.x-infra-lab/raft-core?label=Maven%20Central)](https://central.sonatype.com/artifact/io.github.x-infra-lab/raft-core)
[![CI](https://github.com/x-infra-lab/x-raft-lib/actions/workflows/ci.yml/badge.svg)](https://github.com/x-infra-lab/x-raft-lib/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/x-infra-lab/x-raft-lib/branch/main/graph/badge.svg)](https://codecov.io/gh/x-infra-lab/x-raft-lib)

[English](index.md) | [中文](index.zh.md)

A faithful Java port of [etcd-io/raft](https://github.com/etcd-io/raft) — a pure-protocol Raft consensus core with pluggable Transport and Storage implementations. The core has zero I/O and zero network dependencies; production-grade gRPC and RocksDB modules ship alongside.

---

## Table of Contents

| Document | Description |
|----------|-------------|
| [Architecture & Design](architecture.md) | Design philosophy, module structure, core internals, Storage & Transport interfaces |
| [Getting Started](getting-started.md) | Prerequisites, run the KV demo, embed in your application, configuration reference |
| [Testing Strategy](testing.md) | Test pyramid, unit / property / fuzz / integration / chaos / soak / linearizability |
| [CI/CD & Quality Gates](ci.md) | CI pipeline, fuzz nightly, chaos soak weekly, release process |

---

## Module Overview

| Module | Purpose | Published |
|--------|---------|:---------:|
| [**raft-core**](../raft-core) | Pure Raft state machine. Zero I/O, zero network, zero clock. The host drives ticks and the Ready/Advance loop. | Yes |
| [**raft-transport-grpc**](../raft-transport-grpc) | gRPC `Transport` implementation. Unary RPC for hot path, client-streaming for snapshots, TLS/mTLS. | Yes |
| [**raft-storage-rocksdb**](../raft-storage-rocksdb) | RocksDB `Storage` implementation. Atomic per-Ready-cycle persistence via `WriteBatch`; side-car file for streaming snapshots. | Yes |
| [**raft-examples**](../raft-examples) | Runnable distributed KV demo wiring the three modules together. | No |
| [**raft-tests**](../raft-tests) | Cross-module integration suite — real gRPC, real RocksDB, chaos / linearizability / restart scenarios. | No |

---

## Where to Start

**If you want to use x-raft-lib in your application:**
1. Read [Getting Started](getting-started.md) for prerequisites and embedding guide
2. Refer to [Architecture & Design](architecture.md) for the Ready/Advance loop contract

**If you want to understand the design:**
1. Start with [Architecture & Design](architecture.md) for the full picture
2. Check the [feature matrix vs etcd-raft](architecture.md#feature-matrix-vs-etcd-raft)

**If you want to contribute or review:**
1. Read [Testing Strategy](testing.md) to understand the test layers
2. Review [CI/CD & Quality Gates](ci.md) for the quality bar
3. See [CONTRIBUTING.md](../CONTRIBUTING.md) for the contribution workflow

---

## Links

- [GitHub Repository](https://github.com/x-infra-lab/x-raft-lib)
- [Maven Central](https://central.sonatype.com/artifact/io.github.x-infra-lab/raft-core)
- [CHANGELOG](../CHANGELOG.md)
- [CONTRIBUTING](../CONTRIBUTING.md)
- [RELEASING](../RELEASING.md)
- [SECURITY](../SECURITY.md)
