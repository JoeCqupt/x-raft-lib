# Async Storage Writes

[English](async-storage-writes.md) | [中文](async-storage-writes.zh.md)

This document explains how async storage writes work in x-raft-lib, using the `RaftKVNode` in `raft-examples` as a concrete reference. It covers usage, the message protocol, and why the approach is safe and correct.

## 1. Background: The Sync Mode Bottleneck

In the default sync mode, Ready processing is strictly sequential:

```
persist entries/hardState → send messages → apply committedEntries → advance()
```

The key constraint is that **`advance()` forms a barrier**: before `advance()` returns, the DefaultNode event loop will not emit the next Ready (`waitingAdvance = true`). This means:

```
Ready₁ persist ──┬── Ready₁ apply ──── advance₁ ──── Ready₂ persist ──┬── Ready₂ apply ...
                 │                                                    │
                 └── persist and apply are serial, cannot pipeline ────┘
```

When persistence is a high-latency operation (e.g., fsync to SSD), the state machine must wait for the previous advance before processing the next batch, creating a **throughput bottleneck**.

## 2. Core Idea of Async Mode

AsyncStorageWrites decouples the "persistence complete" and "apply complete" notifications from `advance()`, replacing them with a message-driven protocol:

```
Ready.messages() contains:
├── MsgStorageAppend  → "please persist these entries/hardState/snapshot"
│   └── Responses: [MsgStorageAppendResp(self), MsgAppResp(to peers), ...]
├── MsgStorageApply   → "please apply these committedEntries"
│   └── Responses: [MsgStorageApplyResp(self)]
└── Other messages    → MsgAppend, MsgHeartbeat, etc.
```

**`advance()` is never called.** Instead, after the application finishes persistence/apply, it delivers the `Responses` back to raft (self-addressed ones via `node.step()`, peer-addressed ones via `transport.send()`). Raft advances its state upon receiving these responses.

This enables true pipelining:

```
                  ┌── persist thread ──┐     ┌── persist thread ──┐
Ready₁ ──┤                            ├──── Ready₂ ──┤                            ├── ...
                  └── apply thread ────┘     └── apply thread ────┘
                  (can run in parallel)        (can run in parallel)
```

## 3. Usage in raft-examples

### 3.1 Enabling

Enable via `Config.builder().asyncStorageWrites(true)`:

```java
// RaftKVNode constructor
Config cfg = Config.builder()
        .id(id)
        .electionTick(10)
        .heartbeatTick(1)
        .storage(storage)
        .asyncStorageWrites(asyncStorageWrites)  // ← toggle
        .applied(storage.getApplied())
        .build();
```

When enabled, the DefaultNode event loop skips setting `waitingAdvance = true` (`DefaultNode.java:206`), allowing consecutive Ready emissions without waiting for the previous one to complete.

### 3.2 Three-Thread Architecture

`RaftKVNode` in async mode uses three threads:

| Thread | Responsibility | Trigger |
|--------|---------------|---------|
| readyLoop | Get Ready, dispatch `MsgStorageAppend`/`MsgStorageApply` to workers, send other peer messages immediately | `node.ready()` |
| persistExecutor | Persist entries/hardState/snapshot **from MsgStorageAppend**, deliver its Responses | `MsgStorageAppend` dispatched from readyLoop |
| applyExecutor | Apply committedEntries **from MsgStorageApply** to state machine, deliver its Responses | `MsgStorageApply` dispatched from readyLoop |

The readyLoop thread **does no persistence and no application** — it only dispatches work and returns immediately to wait for the next Ready. This is what enables pipelining: while persistExecutor is persisting Ready₁, readyLoop can already fetch and dispatch Ready₂.

### 3.3 processReadyAsync: Dispatch Logic

```java
private void processReadyAsync(Ready rd) {
    boolean hasSnapshot = rd.snapshot().getMetadata().getIndex() > 0;
    CompletableFuture<Void> persistDone = CompletableFuture.completedFuture(null);

    // ReadStates processed eagerly on readyLoop thread (uses best-known applied index)
    processReadStates(rd.readStates(), 0);

    for (Eraftpb.Message m : rd.messages()) {
        switch (m.getMsgType()) {
            case MsgStorageAppend -> {
                // ① Submit to persist thread
                persistDone = CompletableFuture.runAsync(
                        () -> handleStorageAppend(m), persistExecutor);
            }
            case MsgStorageApply -> {
                // ② Submit to apply thread
                Runnable applyTask = () -> handleStorageApply(m);
                if (hasSnapshot) {
                    // With snapshot: must wait for persist to complete first
                    persistDone.thenRunAsync(applyTask, applyExecutor);
                } else {
                    // Without snapshot: persist and apply can run in parallel
                    CompletableFuture.runAsync(applyTask, applyExecutor);
                }
            }
            default -> sendPeerMessage(m); // ③ peer messages sent immediately
        }
    }
    // readyLoop thread returns immediately — no advance(), no waiting
}
```

Key point: readyLoop only **dispatches** — it submits `MsgStorageAppend` to the persist thread and `MsgStorageApply` to the apply thread. All I/O happens on the worker threads.

### 3.4 handleStorageAppend: Persistence on Dedicated Thread

The persist thread extracts entries, HardState, and snapshot **from the MsgStorageAppend message itself**, persists them, and delivers attached Responses:

```java
private void handleStorageAppend(Eraftpb.Message m) {
    // Reconstruct HardState from MsgStorageAppend fields (term/vote/commit)
    Eraftpb.HardState hs = (m.getTerm() != 0 || m.getVote() != 0 || m.getCommit() != 0)
            ? Eraftpb.HardState.newBuilder()
                    .setTerm(m.getTerm()).setVote(m.getVote()).setCommit(m.getCommit()).build()
            : Eraftpb.HardState.getDefaultInstance();

    // Persist entries + hardState + snapshot
    storage.writeBatched(m.getEntriesList(), hs, m.getSnapshot());

    // Restore snapshot to state machine if present
    applySnapshotToStateMachine(m.getSnapshot());

    // After persistence, deliver attached responses
    for (Eraftpb.Message resp : m.getResponsesList())
        deliverOrSend(resp);
}
```

**HardState encoding:** The protobuf `Message` type has `term`, `vote`, and `commit` fields. RawNode writes the HardState into these fields when constructing `MsgStorageAppend` (see `RawNode.newStorageAppendMsg()`), so the application reconstructs HardState from them.

### 3.5 handleStorageApply: Application on Dedicated Thread

The apply thread applies committed entries **from the MsgStorageApply message**, then delivers `MsgStorageApplyResp`:

```java
private void handleStorageApply(Eraftpb.Message m) {
    // Apply committedEntries from MsgStorageApply to state machine
    long highestApplied = applyEntries(m.getEntriesList(), 0);

    // Drain reads waiting for apply to catch up
    drainWaitingReads(highestApplied);

    // Maybe trigger snapshot creation
    maybeSnapshot(highestApplied);

    // After apply, deliver MsgStorageApplyResp
    for (Eraftpb.Message resp : m.getResponsesList())
        deliverOrSend(resp);
}
```

### 3.6 deliverOrSend: Response Routing

`MsgStorageAppend.Responses` contains two kinds of messages:
- **Self-addressed** (`resp.getTo() == self`): e.g., `MsgStorageAppendResp`, delivered via `node.step()` back to raft core
- **Peer-addressed** (`resp.getTo() != self`): e.g., `MsgAppResp`, sent via `transport.send()` to the peer

```java
private void deliverOrSend(Eraftpb.Message resp) throws InterruptedException {
    if (resp.getTo() == id) {
        node.step(resp);      // self-loop: tell raft persist/apply is done
    } else {
        sendPeerMessage(resp); // send to peer
    }
}
```

### 3.7 Snapshot Ordering Guarantee

When a Ready contains a snapshot (`hasSnapshot = true`), persist and apply **cannot** run in parallel — the snapshot must be persisted and restored to the state machine before subsequent committed entries are applied.

This is enforced by `CompletableFuture` chaining:

```java
if (hasSnapshot) {
    persistDone.thenRunAsync(applyTask, applyExecutor);
} else {
    CompletableFuture.runAsync(applyTask, applyExecutor);
}
```

In the normal case (no snapshot), persist and apply run fully in parallel for maximum throughput.

### 3.8 CLI and Tests

Enable via `--async-storage-writes` on the command line:

```bash
java -cp ... KvServerBootstrap \
    --id=1 --raft-port=8081 --kv-port=9001 \
    --data-dir=/tmp/node1 \
    --peers=1=localhost:8081,2=localhost:8082,3=localhost:8083 \
    --bootstrap --async-storage-writes
```

The integration test uses `@MethodSource` parameterization covering three combinations:

| snapshotStreaming | asyncStorageWrites | Description |
|---|---|---|
| false | false | Sync + inline snapshot (baseline) |
| true | false | Sync + streaming snapshot |
| false | true | Async + inline snapshot |

## 4. Why Async Mode Is Safe and Correct

### 4.1 Core Invariant: Responses Encode Causal Dependencies

The safety of async mode rests on a key design: **RawNode encodes all causal dependencies in the Responses list**.

In sync mode, `advance()` internally replays `stepsOnAdvance` (`MsgStorageAppendResp` + `MsgStorageApplyResp`). These self-loop messages tell raft core "persistence/apply is done", and raft advances commit/applied accordingly.

Async mode does **exactly the same thing**, just with a different delivery mechanism:

| Sync mode | Async mode |
|-----------|------------|
| `acceptReady()` collects stepsOnAdvance | `readyWithoutAccept()` generates MsgStorageAppend/Apply |
| `advance()` replays `raft.step(appendResp)` | App persists, then delivers `node.step(MsgStorageAppendResp)` |
| `advance()` replays `raft.step(applyResp)` | App applies, then delivers `node.step(MsgStorageApplyResp)` |

**Raft core receives identical messages and makes identical state transitions.** The only difference is the delivery timing: implicit via `advance()` vs. explicit via application.

### 4.2 Persistence Before Peer Responses: No Risk of Losing Promised Data

A critical Raft safety constraint: **a node must persist log entries before telling other nodes "I have persisted them".**

In async mode, this is guaranteed by the structure of `MsgStorageAppend.Responses`:

```java
// RawNode.newStorageAppendMsg() construction
mb.addAllEntries(rd.entries());                    // entries to persist
mb.addAllResponses(r.msgsAfterAppend());           // MsgAppResp etc. (peer messages)
mb.addResponses(newStorageAppendRespMsg(r, rd));    // self-loop appendResp
```

`msgsAfterAppend` (e.g., `MsgAppendResponse`, `MsgVoteResponse`) are bundled inside `MsgStorageAppend.Responses`. The `handleStorageAppend` method processes them in order:

1. **First** `storage.writeBatched()` persists entries
2. **Then** iterates `m.getResponsesList()` to deliver

Therefore `MsgAppResp` ("I have persisted up to index N") is **always sent after local persistence**. This is semantically identical to sync mode's "persist → send messages (including msgsAfterAppend) → advance".

### 4.3 DefaultNode Event Loop Guarantees

The special handling for async mode in `DefaultNode.run()` (`DefaultNode.java:206-208`):

```java
if (!rn.asyncStorageWrites) {
    waitingAdvance = true;    // Sync: block next Ready
}
// Async: don't set waitingAdvance, allow consecutive Ready emissions
```

This is safe because:

- **Raft core maintains correct state internally**: `acceptReady()` calls `raftLog.acceptUnstable()` and `raftLog.acceptApplying()` to mark which entries are in progress; `hasReady()` will not re-emit the same entries
- **MsgStorageAppendResp carries lastIndex/lastTerm**: raft core uses this to execute `stableTo()`, moving entries from unstable to stable, preventing duplicate persistence
- **MsgStorageApplyResp carries the applied entries**: raft core uses this to advance `raftLog.applied`, preventing duplicate application

### 4.4 Thread Safety in the Three-Thread Model

The three threads share several resources, all of which are concurrent-safe:

| Resource | Safety mechanism |
|----------|-----------------|
| `pendingProposals`, `pendingConfChanges`, `peerAddresses` | `ConcurrentHashMap` |
| `waitingForApply` | `ConcurrentLinkedQueue` |
| `node.step()` | Serialized through DefaultNode's internal event queue |
| `transport.send()` | `GrpcTransport` is thread-safe |
| `RocksDbStorage` | RocksDB is internally thread-safe |
| `lastSnapshotIndex` | `volatile` for cross-thread visibility |

The single-thread `persistExecutor` and single-thread `applyExecutor` each guarantee that their respective work items execute sequentially, preventing concurrent modifications to the storage and state machine.

## 5. Comparison with Sync Mode

### 5.1 Data Flow Comparison

**Sync mode:**

```
                    RawNode                          Application
                    ───────                          ───────────
readyWithoutAccept ─── harvest entries/committed/msgs
                        build Ready
acceptReady ─────────── update prev state
                        collect stepsOnAdvance           ← replayed in advance
                        (appendResp, applyResp)

                                                     rd = node.ready()
                                                     storage.writeBatched(entries, hs, snap)
                                                     transport.send(messages)   ← includes msgsAfterAppend
                                                     apply(committedEntries)
                                                     node.advance()
                                                       ↓
                    advance() ──── raft.step(appendResp)
                                   raft.step(applyResp)
                                   → advance stable/applied
```

**Async mode:**

```
                    RawNode                          readyLoop thread
                    ───────                          ────────────────
readyWithoutAccept ─── harvest entries/committed/msgs
                        build Ready
                        generate MsgStorageAppend
                          entries + hardState + snapshot
                          responses: [msgsAfterAppend, appendResp]
                        generate MsgStorageApply
                          entries: committedEntries
                          responses: [applyResp]
acceptReady ─────────── update prev state
                        do NOT collect stepsOnAdvance

                                                     rd = node.ready()
                                                     // dispatch only — no I/O on this thread
                                                     for m in messages:
                                                       MsgStorageAppend → submit to persistExecutor
                                                       MsgStorageApply  → submit to applyExecutor
                                                       Other messages   → transport.send(m)
                                                     // return immediately, get next Ready

                    persistExecutor thread           applyExecutor thread
                    ──────────────────────           ────────────────────
                    storage.writeBatched(            apply(m.entries)
                      m.entries, hs, snap)           drainWaitingReads()
                    applySnapshot(snap)              maybeSnapshot()
                    for resp in responses:           for resp in responses:
                      deliverOrSend(resp)              deliverOrSend(resp)
                        ↓                                ↓
node.step(appendResp) ─── advance stable     node.step(applyResp) ─── advance applied
transport.send(MsgAppResp) → peer
```

### 5.2 Summary

| Dimension | Sync mode | Async mode |
|-----------|-----------|------------|
| Enable | Default | `Config.asyncStorageWrites(true)` |
| Completion notification | `advance()` implicitly delivers | Application explicitly delivers Responses |
| DefaultNode backpressure | `waitingAdvance = true` blocks next Ready | No blocking, allows pipelining |
| Persist source | `rd.entries()` + `rd.hardState()` | `MsgStorageAppend.getEntriesList()` + message fields |
| Apply source | `rd.committedEntries()` | `MsgStorageApply.getEntriesList()` |
| Peer responses (e.g. MsgAppResp) | Sent directly in `rd.messages()` | In `MsgStorageAppend.Responses`, sent after persistence |
| `advance()` call | Required | **Forbidden** (throws exception) |
| Threads | Single readyLoop thread | readyLoop + persistExecutor + applyExecutor |
| Parallelism | None (strictly serial) | Persist and apply can be parallel |

## 6. Common Pitfalls

### Pitfall 1: Doing persistence on the readyLoop thread

**Wrong approach.** If you call `storage.writeBatched()` on the readyLoop thread and then only deliver responses from `MsgStorageAppend` on a worker, you are **not truly async** — persistence still blocks the next Ready. The key is: **persistence must happen on the persistExecutor thread**, driven by the MsgStorageAppend message.

### Pitfall 2: Can call advance() in async mode

**Forbidden.** `RawNode.advance()` throws `RaftInvariantException("Advance must not be called when using AsyncStorageWrites")` in async mode. The responsibilities of advance have been replaced by Responses messages; calling it would corrupt state.

### Pitfall 3: Skipping MsgStorageAppend Responses delivery

**No.** Responses contain:
- `MsgStorageAppendResp` (self-loop): raft core needs this to execute `stableTo()`, otherwise unstable entries are never marked stable
- `MsgAppResp` / `MsgVoteResp` etc. (to peers): these must be sent **after** persistence — not delivering them means the Leader cannot advance commitIndex

### Pitfall 4: Sending peer messages from rd.messages() directly

**No.** Peer messages in `msgsAfterAppend` (e.g., Follower's `MsgAppendResponse`) are **intentionally** bundled inside `MsgStorageAppend.Responses`. They must be sent after persistence — this is precisely why `msgsAfterAppend` exists. Sending them directly from `rd.messages()` (as in sync mode) would bypass the persistence guarantee.

### Pitfall 5: Running persist and apply in parallel when there is a snapshot

**Dangerous.** When a Ready contains a snapshot, the snapshot must be persisted and restored to the state machine **before** committed entries are applied. Use `persistDone.thenRunAsync(applyTask, applyExecutor)` to enforce ordering. Only in the no-snapshot case can persist and apply run fully in parallel.
