# raft-storage-rocksdb

RocksDB implementation of the [`raft-core`](../raft-core) `Storage` interface.

## Layout

Three column families:

- `log` — log entries. Key = uint64 BE index, value = serialized
  `Eraftpb.Entry`.
- `state` — singletons. `hard_state` → serialized `Eraftpb.HardState`.
- `snapshot` — singleton `snapshot` → serialized `Eraftpb.Snapshot`.

Snapshots support two persistence paths:

- **Inline** — `createSnapshot(i, cs, byte[])` / `applySnapshot(Snapshot)`
  store the payload directly in the `snapshot` column family. Simple, but
  the whole payload must fit in heap.
- **Streaming (side-car file)** — `createSnapshot(i, cs, SnapshotDataWriter)`,
  `applySnapshot(Snapshot, InputStream)`, and `openSnapshotData(Snapshot)`
  stream the payload to/from a file under `<dbDir>/snapshots/` and keep only
  the small `SnapshotMetadata` in RocksDB. Use this for large / PB-scale
  state machines to avoid OOM. Writes are crash-safe (temp → fsync → atomic
  rename → dir fsync); stale `.tmp` files are reaped on open. The
  `snapshot_file` key in the `state` CF records whether the current
  snapshot is streamed or inline, so the two paths mix safely.

## Atomicity

Single-method calls (`append`, `setHardState`, `applySnapshot`) each use
their own `WriteBatch` so they're atomic on their own. For the typical
"persist a Ready cycle" pattern (which combines entries + hardState +
optional snapshot), use the helper:

```java
storage.writeBatched(rd.entries, rd.hardState, rd.snapshot);
```

This is the recommended host integration — one fsync per Ready cycle.

## fsync

Constructor takes a `boolean fsync` flag. Defaults to `true`
(`WriteOptions.setSync(true)`); set `false` for tests / non-critical
paths where you'll fsync separately. Note: with `fsync=false`, raft's
durability invariants are no longer met and a crash can violate Raft
safety unless the host syncs before sending outbound messages.

## Usage

```java
try (RocksDbStorage storage = new RocksDbStorage(Paths.get("/var/lib/raft/node-1"))) {
    Config cfg = new Config();
    cfg.id = 1;
    cfg.storage = storage;
    cfg.maxSizePerMsg = 1L << 20;
    cfg.maxInflightMsgs = 256;
    // Recover the apply-index watermark so a restart skips
    // already-applied entries.
    cfg.applied = storage.getApplied();
    // ... start node, drive Ready cycle:
    storage.writeBatched(rd.entries, rd.hardState, rd.snapshot);
    // ... after applying each batch of committed entries:
    storage.setApplied(highestAppliedIndex);
    // ... after applying a ConfChange entry:
    storage.setConfState(currentConfState);
}
```

See [`raft-examples`](../raft-examples) for a full single-node demo and
[`raft-tests`](../raft-tests) for end-to-end restart and 3-node cluster
tests.

## Persistent state outside the log

In addition to log entries, this storage keeps three host-managed
watermarks in the `state` column family:

| Key | Purpose |
|---|---|
| `hard_state` | Raft's persistent term/vote/commit (written by raft via `setHardState`). |
| `applied` | The host's apply-index watermark. Recovered as `Config.applied` on restart so raft doesn't re-deliver previously-applied entries. |
| `conf_state` | The current cluster `ConfState`. Recovered by `initialState()` so raft knows the voter set without replaying the log. |

## Status

Alpha. Production hardening still missing:

- Configurable RocksDB options (block cache, compaction style,
  WAL settings) — currently uses defaults.
- Per-CF metrics surfaced via `RaftMetrics`.
- Periodic checkpoint / backup helpers.

## Test

```bash
mvn -pl raft-storage-rocksdb -am test
```
