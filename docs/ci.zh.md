# CI/CD 与质量门禁

[English](ci.md) | [中文](ci.zh.md)

## 流水线概览

x-raft-lib 运行四个 GitHub Actions 工作流，构成分层的质量保障网：

```
  每次 PR / push 到 main          每夜（2 次/天）           每周（周日）
  ─────────────────────           ──────────────           ──────────────
  ┌─────────────────┐             ┌─────────────┐          ┌──────────────┐
  │   ci.yml        │             │ fuzz-nightly │          │ chaos-soak-  │
  │                 │             │              │          │ weekly       │
  │ 3 OS × 2 JDK   │             │ 2 个 Jazzer  │          │              │
  │ 完整 reactor    │             │ harness      │          │ 30 分钟浸泡  │
  │ JaCoCo + Codecov│             │ 各 30 分钟   │          │ 5 轮混沌     │
  │ Spotless        │             └─────────────┘          │ 压力循环     │
  └─────────────────┘                                      └──────────────┘

  推送标签时（vX.Y.Z）
  ─────────────────────
  ┌─────────────────┐
  │  release.yml    │
  │                 │
  │ GPG 签名        │
  │ Maven Central   │
  │ GitHub Release  │
  └─────────────────┘
```

---

## CI 流水线（`ci.yml`）

**触发条件：** 每次推送到 `main` 和每次 Pull Request。

### 构建矩阵

| 操作系统 | JDK | JaCoCo | 说明 |
|----------|-----|:------:|------|
| ubuntu-latest | 17 | 是 | 基准环境 —— 覆盖率报告、Codecov 上传 |
| ubuntu-latest | 21 | 否 | JDK 前向兼容性检查 |
| macos-latest (arm64) | 17 | 否 | macOS 平台验证 |
| macos-latest (arm64) | 21 | 否 | macOS + JDK 21 |
| windows-latest | 17 | 否 | Windows 冒烟测试 —— **跳过 RocksDB 模块**（上游 JNI 问题） |
| windows-latest | 21 | 否 | Windows + JDK 21 |

### 每个环境执行的内容

1. **构建 & 测试** —— `mvn install` 运行完整 reactor：单元测试、属性测试、数据驱动测试、集成测试、Spotless 格式检查
2. **JaCoCo 门禁**（仅 ubuntu/JDK17）—— 强制执行各模块的覆盖率阈值
3. **Codecov 上传** —— 聚合 + 各模块 XML 报告，提供跨模块覆盖率视图
4. **Surefire 上传** —— 失败时将测试报告作为构件上传，便于诊断

### 并发控制

同一 ref 上的进行中运行会在新提交到达时被取消（`cancel-in-progress: true`），避免 force-push 到 PR 分支时堆积过时的运行。

---

## 每夜模糊测试（`fuzz-nightly.yml`）

**调度：** 每天两次（约 07:23 UTC 和 19:47 UTC），支持手动 `workflow_dispatch`。

两个 Jazzer harness 并行运行，每个 30 分钟（可通过 dispatch 配置）：

| Harness | 目标 |
|---------|------|
| `EraftpbParseFuzzTest` | Protobuf 反序列化边界 |
| `RaftStepFuzzTest` | `Raft.step(Message)` 入口 |

发现问题时：语料库作为构件上传，可本地重放：

```bash
mvn -P fuzz -pl raft-core test -Dtest=EraftpbParseFuzzTest
```

---

## 每周混沌浸泡（`chaos-soak-weekly.yml`）

**调度：** 每周日约 04:23 UTC，支持手动 `workflow_dispatch`（可配置时长/迭代次数）。

### 任务 1：浸泡

3 节点集群在持续提议负载下运行 30 分钟（默认）。执行 `@Tag("soak")` 测试：

- 持续提交进展断言
- 无应用积压累积
- 无线程泄漏（线程数差值检查）
- 周期性快照 + 压缩

失败时：上传 surefire 报告 + 堆/线程诊断信息（JPS、hs_err 日志）。

### 任务 2：混沌压力

重复执行混沌 / 分区 / 线性一致性测试套件 **5 次**（默认）。参与的测试：

- `ChaosFaultInjectionIntegrationTest`
- `PartitionIntegrationTest`
- `KvLinearizabilityIntegrationTest`
- `DynamicMembershipIntegrationTest`
- `SnapshotInstallIntegrationTest`
- `ZeroCopySnapshotStreamingTest`

每次迭代的 surefire 报告独立归档，确保失败的迭代不会被覆盖。

---

## 静态分析

### CodeQL（`codeql.yml`）

GitHub CodeQL 在每次 push/PR 时运行，扫描安全漏洞和代码质量问题。阈值：无高危发现。

### Spotless

格式检查集成到 `mvn verify`。检查导入排序、未使用的导入和代码格式化。不允许任何差异。

---

## 质量门禁汇总

| 门禁 | 位置 | 阈值 |
|------|------|------|
| 单元 + 功能 + 属性测试 | `mvn install` | 必须通过 |
| raft-core JaCoCo | 每次 PR CI | 85% inst / 80% branch / 88% line / 85% method |
| raft-transport-grpc JaCoCo | 每次 PR CI | 75% inst / 60% branch / 75% line / 80% method |
| raft-storage-rocksdb JaCoCo | 每次 PR CI | 80% inst / 70% branch / 80% line / 95% method |
| Codecov 项目 + 补丁 | 每次 PR CI | 项目 >= 80% / 补丁 >= 75%（1pp 容差） |
| 跨平台冒烟 | 每次 PR CI | Linux + macOS + Windows x JDK 17/21 |
| 集成测试套件 | 每次 PR CI | 必须通过 |
| 覆盖率引导模糊测试 | [fuzz-nightly](../.github/workflows/fuzz-nightly.yml) | 无发现 |
| 浸泡 + 混沌压力 | [chaos-soak-weekly](../.github/workflows/chaos-soak-weekly.yml) | 30 分钟浸泡 + 5 轮混沌必须通过 |
| Spotless 格式 | `mvn verify` | 无差异 |
| CodeQL 静态分析 | [codeql](../.github/workflows/codeql.yml) | 无高危发现 |

---

## 发布流水线（`release.yml`）

**触发条件：** 推送匹配 `v[0-9]+.[0-9]+.[0-9]+`（或带预发布后缀）的注解标签。

### 发布步骤

1. **版本校验** —— 验证标签字面值与 pom `<version>` 匹配，防止标签/pom 不一致
2. **构建 & 测试** —— 完整 reactor 验证发布候选
3. **GPG 签名** —— 所有构件使用仓库的 GPG 密钥签名（存储在 GitHub Secrets 中）
4. **Maven Central 部署** —— 通过 `central-publishing-maven-plugin`，`autoPublish=true`
5. **GitHub Release** —— 创建附加到标签的 Release 草稿，含自动生成的发布说明

发布的模块：`raft-core`、`raft-transport-grpc`、`raft-storage-rocksdb`、`raft-proto`。`raft-examples` 和 `raft-tests` 模块不发布。

### 版本规范

- `0.x.0-alphaN` —— 探索性版本，无 API 稳定性承诺
- `0.x.0-RCN` —— 发布候选，API 冻结
- `0.x.0` —— 0.x 线上的稳定版本
- `0.x.y` —— 补丁版本，无破坏性变更

详见 [RELEASING.md](../RELEASING.md) 了解完整发布清单。
