---
name: Bug report
about: Report incorrect behaviour (consensus safety, liveness, crashes)
title: "[bug] "
labels: bug
---

## What happened

A clear description of the bug.

## Expected behaviour

What you expected instead. If it diverges from etcd-io/raft, link the etcd
behaviour you're comparing against.

## Reproduction

Smallest setup that triggers it — ideally a failing test or a minimal cluster
config (number of voters, `Config` fields, the message sequence).

```java
// minimal repro
```

## Environment

- x-raft-lib version / commit:
- Module(s): raft-core / raft-transport-grpc / raft-storage-rocksdb / examples
- JDK version:
- OS:

## Logs / stack trace

```
paste here
```
