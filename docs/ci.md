# CI/CD & Quality Gates

[English](ci.md) | [中文](ci.zh.md)

## Pipeline Overview

x-raft-lib runs four GitHub Actions workflows that form a layered quality net:

```
  Every PR / push to main          Nightly (2x/day)         Weekly (Sunday)
  ─────────────────────           ───────────────           ───────────────
  ┌─────────────────┐             ┌─────────────┐          ┌──────────────┐
  │   ci.yml        │             │ fuzz-nightly │          │ chaos-soak-  │
  │                 │             │              │          │ weekly       │
  │ 3 OS × 2 JDK   │             │ 2 Jazzer     │          │              │
  │ full reactor    │             │ harnesses    │          │ 30-min soak  │
  │ JaCoCo + Codecov│             │ 30 min each  │          │ 5× chaos     │
  │ Spotless        │             └─────────────┘          │ stress loop  │
  └─────────────────┘                                      └──────────────┘

  On tag push (vX.Y.Z)
  ─────────────────────
  ┌─────────────────┐
  │  release.yml    │
  │                 │
  │ GPG sign        │
  │ Maven Central   │
  │ GitHub Release  │
  └─────────────────┘
```

---

## CI Pipeline (`ci.yml`)

**Triggers:** Every push to `main` and every pull request.

### Build Matrix

| OS | JDK | JaCoCo | Notes |
|----|-----|:------:|-------|
| ubuntu-latest | 17 | Yes | Canonical leg — coverage reports, Codecov upload |
| ubuntu-latest | 21 | No | JDK forward-compatibility check |
| macos-latest (arm64) | 17 | No | macOS platform validation |
| macos-latest (arm64) | 21 | No | macOS + JDK 21 |
| windows-latest | 17 | No | Windows smoke — **skips RocksDB modules** (upstream JNI issue) |
| windows-latest | 21 | No | Windows + JDK 21 |

### What Each Leg Does

1. **Build & test** — `mvn install` runs the full reactor: unit tests, property tests, datadriven tests, integration tests, Spotless format check
2. **JaCoCo gate** (ubuntu/JDK17 only) — enforces per-module coverage thresholds
3. **Codecov upload** — aggregate + per-module XML reports for cross-cutting coverage view
4. **Surefire upload** — on failure, test reports are uploaded as artifacts for diagnosis

### Concurrency

In-progress runs on the same ref are cancelled when a new commit lands (`cancel-in-progress: true`), so force-pushing to a PR branch doesn't queue stale runs.

---

## Fuzz Nightly (`fuzz-nightly.yml`)

**Schedule:** Twice daily (~07:23 UTC and ~19:47 UTC), plus manual `workflow_dispatch`.

Two Jazzer harnesses run in parallel, each for 30 minutes (configurable via dispatch):

| Harness | Target |
|---------|--------|
| `EraftpbParseFuzzTest` | Protobuf deserialization boundary |
| `RaftStepFuzzTest` | `Raft.step(Message)` entry point |

On finding: the corpus is uploaded as an artifact so the failing input can be replayed locally:

```bash
mvn -P fuzz -pl raft-core test -Dtest=EraftpbParseFuzzTest
```

---

## Chaos Soak Weekly (`chaos-soak-weekly.yml`)

**Schedule:** Sunday ~04:23 UTC, plus manual `workflow_dispatch` with configurable duration/iterations.

### Job 1: Soak

A 3-node cluster under sustained proposal load for 30 minutes (default). Runs `@Tag("soak")` tests:

- Continuous commit progress assertion
- No apply backlog accumulation
- No thread leak (thread count delta check)
- Periodic snapshot + compaction

On failure: surefire reports + heap/thread diagnostics (JPS, hs_err logs) are uploaded.

### Job 2: Chaos Stress

Repeats the chaos / partition / linearizability test suite **5 times** (default). Tests that participate:

- `ChaosFaultInjectionIntegrationTest`
- `PartitionIntegrationTest`
- `KvLinearizabilityIntegrationTest`
- `DynamicMembershipIntegrationTest`
- `SnapshotInstallIntegrationTest`
- `ZeroCopySnapshotStreamingTest`

Each iteration's surefire reports are archived independently so a failing iteration isn't overwritten.

---

## Static Analysis

### CodeQL (`codeql.yml`)

GitHub's CodeQL runs on every push/PR, scanning for security vulnerabilities and code quality issues. Threshold: no high-severity findings.

### Spotless

Format enforcement integrated into `mvn verify`. Checks import ordering, unused imports, and code formatting. No diff allowed.

---

## Quality Gate Summary

| Gate | Where | Threshold |
|------|-------|-----------|
| Unit + functional + property tests | `mvn install` | Must pass |
| JaCoCo on raft-core | Per-PR CI | 85% inst / 80% branch / 88% line / 85% method |
| JaCoCo on raft-transport-grpc | Per-PR CI | 75% inst / 60% branch / 75% line / 80% method |
| JaCoCo on raft-storage-rocksdb | Per-PR CI | 80% inst / 70% branch / 80% line / 95% method |
| Codecov project + patch | Per-PR CI | Project >= 80% / Patch >= 75% (1pp tolerance) |
| Cross-platform smoke | Per-PR CI | Linux + macOS + Windows x JDK 17/21 |
| Integration suite | Per-PR CI | Must pass |
| Coverage-guided fuzz | [fuzz-nightly](../.github/workflows/fuzz-nightly.yml) | No findings |
| Soak + chaos stress | [chaos-soak-weekly](../.github/workflows/chaos-soak-weekly.yml) | 30 min soak + 5x chaos must pass |
| Spotless format | `mvn verify` | No diff |
| CodeQL static analysis | [codeql](../.github/workflows/codeql.yml) | No high-severity findings |

---

## Release Pipeline (`release.yml`)

**Trigger:** Pushing an annotated tag matching `v[0-9]+.[0-9]+.[0-9]+` (or with pre-release suffix).

### Release Steps

1. **Version check** — verifies tag literal matches pom `<version>` to catch tag/pom mismatch
2. **Build & test** — full reactor to validate the release candidate
3. **GPG sign** — all artifacts signed with the repo's GPG key (stored in GitHub Secrets)
4. **Maven Central deploy** — via `central-publishing-maven-plugin` with `autoPublish=true`
5. **GitHub Release** — drafts a Release attached to the tag with auto-generated release notes

Published modules: `raft-core`, `raft-transport-grpc`, `raft-storage-rocksdb`, `raft-proto`. The `raft-examples` and `raft-tests` modules are not published.

### Versioning

- `0.x.0-alphaN` — exploratory, no API stability
- `0.x.0-RCN` — release candidate, API frozen
- `0.x.0` — stable on the 0.x line
- `0.x.y` — patch release, no breaking changes

See [RELEASING.md](../RELEASING.md) for the full release checklist.
