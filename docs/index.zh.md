# x-raft-lib 文档中心

[![Maven Central](https://img.shields.io/maven-central/v/io.github.x-infra-lab/raft-core?label=Maven%20Central)](https://central.sonatype.com/artifact/io.github.x-infra-lab/raft-core)
[![CI](https://github.com/x-infra-lab/x-raft-lib/actions/workflows/ci.yml/badge.svg)](https://github.com/x-infra-lab/x-raft-lib/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/x-infra-lab/x-raft-lib/branch/main/graph/badge.svg)](https://codecov.io/gh/x-infra-lab/x-raft-lib)

[English](index.md) | [中文](index.zh.md)

[etcd-io/raft](https://github.com/etcd-io/raft) 的 Java 忠实移植 —— 纯协议 Raft 共识核心，搭配可插拔的 Transport 和 Storage 实现。核心模块零 I/O、零网络依赖；同时提供生产级的 gRPC 和 RocksDB 实现。

---

## 目录

| 文档 | 说明 |
|------|------|
| [架构与设计](architecture.zh.md) | 设计理念、模块结构、核心内部实现、Storage 与 Transport 接口 |
| [Raft / RawNode / Node 三层架构](raft-node-layers.zh.md) | 三层 API 分层、Ready 生命周期、事件循环内部机制、使用模式 |
| [快速开始](getting-started.zh.md) | 环境准备、运行 KV 示例、集成到你的应用、配置参考 |
| [测试策略](testing.zh.md) | 测试金字塔：单元 / 属性 / 模糊 / 集成 / 混沌 / 浸泡 / 线性一致性 |
| [CI/CD 与质量门禁](ci.zh.md) | CI 流水线、每夜模糊测试、每周混沌浸泡、发布流程 |

---

## 模块总览

| 模块 | 职责 | 是否发布 |
|------|------|:--------:|
| [**raft-core**](../raft-core) | 纯 Raft 状态机。零 I/O、零网络、零时钟。上层驱动 tick 和 Ready/Advance 循环。 | 是 |
| [**raft-transport-grpc**](../raft-transport-grpc) | gRPC `Transport` 实现。热路径使用 Unary RPC，快照使用 Client-Streaming，支持 TLS/mTLS。 | 是 |
| [**raft-storage-rocksdb**](../raft-storage-rocksdb) | RocksDB `Storage` 实现。通过 `WriteBatch` 实现 Ready 周期的原子持久化；快照走 Side-car 文件流式传输。 | 是 |
| [**raft-examples**](../raft-examples) | 可运行的分布式 KV 示例，将三个模块完整集成。 | 否 |
| [**raft-tests**](../raft-tests) | 跨模块集成测试 —— 真实 gRPC、真实 RocksDB、混沌 / 线性一致性 / 重启恢复场景。 | 否 |

---

## 从哪里开始

**想在你的应用中使用 x-raft-lib：**
1. 阅读 [快速开始](getting-started.zh.md) 了解环境准备和集成指南
2. 参考 [架构与设计](architecture.zh.md) 理解 Ready/Advance 循环契约

**想了解设计原理：**
1. 从 [架构与设计](architecture.zh.md) 开始，了解全貌
2. 查看 [与 etcd-raft 的功能对比矩阵](architecture.zh.md#与-etcd-raft-的功能对比矩阵)

**想贡献代码或 Review：**
1. 阅读 [测试策略](testing.zh.md) 了解测试分层
2. 查看 [CI/CD 与质量门禁](ci.zh.md) 了解质量标准
3. 参阅 [CONTRIBUTING.md](../CONTRIBUTING.md) 了解贡献流程

---

## 链接

- [GitHub 仓库](https://github.com/x-infra-lab/x-raft-lib)
- [Maven Central](https://central.sonatype.com/artifact/io.github.x-infra-lab/raft-core)
- [更新日志](../CHANGELOG.md)
- [贡献指南](../CONTRIBUTING.md)
- [发布流程](../RELEASING.md)
- [安全政策](../SECURITY.md)
