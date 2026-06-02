# Contributing to x-raft-lib

Thanks for your interest. This project is alpha; the surface is moving.

## Before you start

For non-trivial changes, **please open an issue first** so we can agree on
scope and approach. The maintainer's time is the bottleneck — drive-by PRs
that need a redesign are likely to bounce.

Bug reports are always welcome and don't need pre-discussion.

## Development setup

```bash
git clone https://github.com/x-infra-lab/x-raft-lib.git
cd x-raft-lib
mvn install               # full reactor: core + transport + storage + tests
cd raft-core && mvn test  # core suite + jacoco gates, JDK 17+
```

## Code expectations

- **Match etcd-io/raft semantics by default.** This is a port. If you find
  a divergence, the burden of proof is on the change to either align with
  etcd or document the deliberate difference (see `Changer.initProgress`
  for the established pattern).
- **Add tests.** Look at the test pyramid in [`raft-core/README.zh.md`](./raft-core/README.zh.md):
  unit / property (jqwik) / datadriven / system / coverage gaps. Pick the
  level that matches your change.
- **Don't break the jacoco gates.** Defaults: instruction ≥85%, branch
  ≥80%, line ≥88%, method ≥85%. Raise the floor in `pom.xml` after
  improving coverage; don't lower it.
- **Keep public API stable.** The `internal-vs-public` boundary is being
  designed (see `raft-core/TODO.md`); until that lands, treat anything in
  `tracker/`, `quorum/`, `confchange/`, `Util` as internal even though
  they're nominally `public`.
- **Source headers.** Every new `.java` file gets the Apache-2.0 header
  from existing files (see e.g. `Raft.java:1-15`).

## Commit messages

Conventional commits encouraged but not enforced. Past commits in this
repo use plain imperative subject lines + multi-paragraph body explaining
the *why*. Mirror that style.

## What's in scope

- Bug fixes against the etcd-raft reference behaviour.
- Roadmap items from [`raft-core/TODO.md`](./raft-core/TODO.md). Pick a task and
  comment on the related issue (or open one).
- Documentation, examples, integration recipes (RocksDB / WAL Storage /
  Netty transport are all wanted).

## What's out of scope (for now)

- Major API redesigns without a discussion issue.
- New consensus protocols or non-Raft variants.
- Performance optimisations without a benchmark.

## Maintainers and releases

Current maintainers, their areas, and how to become one are listed in
[`MAINTAINERS.md`](./MAINTAINERS.md). The exact recipe for cutting a
release (version bump, dry-run, Maven Central publish, GitHub Release)
lives in [`RELEASING.md`](./RELEASING.md) — a contributor doesn't need
to read it day-to-day, but a new maintainer should.

## Code of Conduct

By participating you agree to abide by the [Code of Conduct](./CODE_OF_CONDUCT.md).
