# Raft / RawNode / Node — Three-Layer Architecture

[English](raft-node-layers.md) | [中文](raft-node-layers.zh.md)

The core API of x-raft-lib is organized into three layers, from bottom to top: **Raft** (pure state machine) → **RawNode** (single-threaded Ready management) → **Node / DefaultNode** (thread-safe wrapper). Applications choose which layer to integrate based on their threading model.

## Overview

```
┌─────────────────────────────────────────────────────────┐
│  Application (RaftKVNode / your app)                    │
│  Periodic tick() · submit propose() / step()            │
│  Consume ready() → persist → send → apply → advance()  │
└───────────────────────┬─────────────────────────────────┘
                        │  Node interface (thread-safe)
                        ▼
┌─────────────────────────────────────────────────────────┐
│  DefaultNode                                            │
│  Single-thread event loop, bounded queue (1024)         │
│  Tick burst limit (128), Drain model                    │
│  Leader change notifications, lock-free liveness mirror │
└───────────────────────┬─────────────────────────────────┘
                        │  Exclusive access (event-loop thread)
                        ▼
┌─────────────────────────────────────────────────────────┐
│  RawNode                                                │
│  Ready lifecycle: hasReady → ready → advance            │
│  Maintains prevSoftSt / prevHardSt for incremental Ready│
│  AsyncStorageWrites synthetic message generation        │
└───────────────────────┬─────────────────────────────────┘
                        │  raft.step() / raft.tick()
                        ▼
┌─────────────────────────────────────────────────────────┐
│  Raft (internal)                                        │
│  Pure state machine: election · replication · commit    │
│  advancement · heartbeat · ReadIndex                    │
│  Zero I/O, zero networking, zero timers                 │
│  Output accumulates in msgs / msgsAfterAppend /         │
│  readStates                                             │
└─────────────────────────────────────────────────────────┘
```

## Layer Responsibilities

### Raft — Pure Consensus State Machine

**Location:** `raft-core/.../internal/Raft.java` (package-private fields, cross-package access via accessors)

The Raft class implements the complete Raft consensus algorithm and is the computational core of the library. It performs no I/O — no disk writes, no network calls, no timer management.

**Key state:**

| Field | Description |
|-------|-------------|
| `term` / `vote` / `lead` | Current term, vote cast, known leader |
| `state` | Role: Leader / Follower / Candidate / PreCandidate |
| `raftLog` | Log manager (Unstable + Storage, two layers) |
| `trk` (ProgressTracker) | Per-follower replication progress, Inflights flow control |
| `msgs` / `msgsAfterAppend` | Accumulated outbound messages |
| `readStates` | ReadIndex result queue |

**Role switching** uses function pointers `tickFn` / `stepFn` for zero if-else dispatch:

```
becomeFollower()  → tickFn = tickElection,  stepFn = stepFollower
becomeCandidate() → tickFn = tickElection,  stepFn = stepCandidate
becomeLeader()    → tickFn = tickHeartbeat, stepFn = stepLeader
```

**I/O model:**

- **Input:** `tick()` advances the logical clock; `step(Message)` delivers messages (local or remote)
- **Output:** Never pushed. Output accumulates in internal lists, waiting for RawNode to harvest during the Ready cycle

### RawNode — Single-Threaded Ready Manager

**Location:** `raft-core/.../RawNode.java` (public API)

RawNode is a thin wrapper around Raft that introduces no concurrency. Its core responsibility is managing the **Ready lifecycle** — packaging Raft's internal state changes into an immutable `Ready` snapshot for the application to process.

**Key methods:**

| Method | Responsibility |
|--------|---------------|
| `tick()` | Delegates directly to `raft.tick()` |
| `propose(data)` | Constructs MsgPropose, calls `raft.step()` |
| `step(msg)` | Validates message source, delegates to `raft.step()` |
| `hasReady()` | Checks whether there is new output to process |
| `readyWithoutAccept()` | Harvests all output from Raft, builds Ready |
| `acceptReady(rd)` | Updates prev state, clears Raft buffers |
| `advance(rd)` | Replays stepsOnAdvance, notifies Raft persistence is complete |
| `ready()` | Combines `readyWithoutAccept()` + `acceptReady()` |

**Incremental detection:** RawNode maintains `prevSoftSt` and `prevHardSt`. Only when Raft's SoftState/HardState differs from the previous values are they included in Ready, avoiding redundant processing by the application.

**Defensive copies:** `readyWithoutAccept()` creates `new ArrayList<>(...)` copies of `unstableEnts` and `committedEnts`. The Raft-internal unstable buffer is a subList view; subsequent `step()` calls may trigger `stableTo()` which modifies the underlying list, causing `ConcurrentModificationException` without the copy.

### Node / DefaultNode — Thread-Safe Wrapper

**Location:** `Node.java` (public interface) + `internal/DefaultNode.java` (implementation)

DefaultNode wraps RawNode with a dedicated event-loop thread, allowing multiple producer threads to safely call `propose()`, `step()`, `tick()`, etc. concurrently.

**Factory methods:**

```java
Node n = Node.startNode(config, peers);    // First-time bootstrap
Node n = Node.restartNode(config);          // Restore from Storage
```

Both internally create a `RawNode`, optionally call `bootstrap()` (startNode only), then start the event-loop thread.

**Event queue:**

All external API calls are converted to Event objects and placed into a bounded `LinkedBlockingQueue<Event>` (default capacity 1024). The event-loop thread is the sole consumer.

| Event Type | Source | Enqueue Method | Result Notification |
|------------|--------|----------------|---------------------|
| `TickEvent` | `tick()` | `offer()` (non-blocking, dropped on overflow) | None |
| `ProposeEvent` | `propose()` | `put()` (blocking) | `CompletableFuture<Void>` |
| `RecvEvent` | `step()` / `campaign()` | `put()` (blocking) | None (fire-and-forget) |
| `ConfChangeEvent` | `applyConfChange()` | `put()` (blocking) | `CompletableFuture<ConfState>` |
| `StatusEvent` | `status()` | `put()` (blocking) | `CompletableFuture<Status>` |
| `WakeEvent` | `advance()` | `offer()` (non-blocking) | None (wake-up signal) |

**Tick handling:** Ticks use `offer()` instead of `put()` to prevent the timer thread from blocking. An `AtomicInteger pendingTicks` enforces a burst limit of 128; excess ticks are dropped with a warning. This mirrors etcd-raft's buffered `tickc` channel semantics.

**Drain model:**

```
while (!stopped) {
    ① If rn.hasReady() and not waiting for advance → emit Ready to readyc
    ② Detect leader changes → notify observers + update liveness mirror
    ③ events.take() blocks for the first event
    ④ events.poll() non-blocking batch of up to 128 more events
    ⑤ After each event, check advancePending flag
}
```

The key insight: Ready is emitted before processing events, so multiple proposals accumulated during the previous drain batch are combined into a single Ready — achieving group commit with one fsync for many log entries.

**Advance mechanism:** When the application calls `advance()`, it doesn't enqueue directly (avoiding competition with propose/step for queue capacity). Instead, it sets the `AtomicBoolean advancePending` flag and offers a `WakeEvent` to wake the event loop. The loop checks this flag after every take/poll.

**Lock-free liveness mirror:** `basicStatus()` reads from `AtomicLong` fields (`liveTerm`, `liveLead`, `liveCommit`, etc.) without going through the event queue. Used for health endpoints and liveness probes — responds even when the event loop is wedged.

## Ready Data Flow

`Ready` is a point-in-time output snapshot from the Raft state machine, containing everything the application needs to process:

| Field | Description | Processing Order |
|-------|-------------|-----------------|
| `softState` | Leader and role changes (not persisted) | Notification only |
| `hardState` | term / vote / commit (must persist) | ① Persist |
| `entries` | Unpersisted log entries | ① Persist |
| `snapshot` | Snapshot (must persist) | ① Persist |
| `messages` | Outbound messages | ② Send |
| `readStates` | ReadIndex results | ③ Notify waiting readers |
| `committedEntries` | Committed entries to apply | ④ Apply to state machine |
| `mustSync` | Whether fsync is required | ① Reference during persist |

**Processing order matters:** Entries and hardState must be persisted before messages are sent. Otherwise, a crash could lose log entries that other nodes have been told "I have persisted," violating Raft safety.

### Ready Lifecycle (Sync Mode)

```
Application                 RawNode                          Raft
───────────                 ───────                          ────
                 ┌─── hasReady() ────────── Compare softState/hardState
                 │                          Check msgs, readStates,
                 │                          unstableEnts, committedEnts
                 │
                 │    readyWithoutAccept() ─── Defensive-copy all output
                 │                             Build Ready record
                 │
rd = ready() ◄───┤    acceptReady(rd) ─────── Update prevSoftSt/prevHardSt
                 │                             drainMsgs()
                 │                             raftLog.acceptUnstable()
                 └                             Collect stepsOnAdvance

persist(rd)
send(rd.messages)
apply(rd.committedEntries)

advance(rd) ───────── advance(rd) ─────────── Replay stepsOnAdvance
                                               raft.step(appendResp)
                                               raft.step(applyResp)
```

### Ready Lifecycle (Async Mode)

With AsyncStorageWrites enabled, `advance()` is no longer called explicitly. RawNode automatically generates synthetic messages in `readyWithoutAccept()`:

- `MsgStorageAppend` → tells the application to persist entries + hardState + snapshot
- `MsgStorageApply` → tells the application to apply committedEntries

Once complete, the application delivers response messages via `step()`:

- `MsgStorageAppendResp` → informs Raft that persistence is done
- `MsgStorageApplyResp` → informs Raft that application is done

This allows persistence and application to run in parallel on different threads, improving throughput.

## Two Usage Modes

### Mode 1: Direct RawNode

Best for applications with an existing event loop or actor framework. The application manages threading and drives RawNode in its own loop.

```java
RawNode rn = RawNode.newRawNode(config);
rn.bootstrap(List.of(new Peer(1), new Peer(2), new Peer(3)));

while (running) {
    rn.tick();
    for (Message msg : received) rn.step(msg);

    if (rn.hasReady()) {
        Ready rd = rn.ready();
        storage.writeBatched(rd.entries(), rd.hardState(), rd.snapshot());
        transport.send(rd.messages());
        stateMachine.apply(rd.committedEntries());
        rn.advance(rd);
    }
}
```

### Mode 2: Node / DefaultNode (Recommended)

Recommended for production. DefaultNode provides a thread-safe API with a built-in event loop.

```java
Node node = Node.startNode(config, List.of(new Peer(1), new Peer(2), new Peer(3)));

// Timer thread: drive the logical clock
scheduler.scheduleAtFixedRate(node::tick, 100, 100, TimeUnit.MILLISECONDS);

// Network thread: deliver inbound messages
transport.setReceiver(msg -> node.step(msg));

// Application thread: consume Ready
while (running) {
    Ready rd = node.ready();                           // Blocks until available
    storage.writeBatched(rd.entries(), rd.hardState(), rd.snapshot());
    transport.send(rd.messages());
    stateMachine.apply(rd.committedEntries());
    node.advance();                                    // Notify completion
}

node.stop();
```

`RaftKVNode` in `raft-examples` is a complete Mode 2 implementation, including proposal tracking, ReadIndex, ConfChange, and snapshot management.

## Key Design Decisions

### Why three layers instead of two?

The Raft state machine must remain pure — zero I/O, deterministic, testable. But exposing Raft directly would require users to understand the `msgs` / `msgsAfterAppend` split, unstable log management, and other internal details. RawNode as a middle layer encapsulates this complexity behind the clean `Ready` abstraction.

DefaultNode adds another layer because most applications need multi-threaded safety — network callback threads need to call `step()`, timer threads need to call `tick()`, and Ready consumption runs on yet another thread. The event loop serializes all of this onto a single thread, making it both safe and efficient.

### Why are msgs and msgsAfterAppend separate?

`msgs` contains MsgAppend, MsgHeartbeat, etc. — messages that can be sent before local persistence. `msgsAfterAppend` contains MsgAppendResponse, MsgVoteResponse, etc. — messages that must only be sent after local log persistence. RawNode merges both into Ready's `messages` field, but the internal ordering guarantees correctness — the application simply sends them in order.

### Why does advance() replay stepsOnAdvance?

In sync mode, Raft needs to know that persistence and application are complete before advancing commitIndex and appliedIndex. `acceptReady()` collects self-addressed messages (`MsgStorageAppendResp`, `MsgStorageApplyResp`) that must be delivered on advance. `advance()` feeds them back into the state machine via `raft.step()`, completing the feedback loop.
