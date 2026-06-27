# raft-examples

A production-quality distributed key-value server that wires `raft-core` +
`raft-transport-grpc` + `raft-storage-rocksdb` into a runnable replicated
system. **Not** published to Maven Central — this module demonstrates what
a real integration looks like.

## What it is

A replicated KV server with gRPC client API and admin service. An N-node
cluster (default 3) can run entirely in one JVM or across separate
processes: each node binds its own gRPC port and owns its own RocksDB
directories. Commands proposed through the leader are replicated by raft
and applied on every node, so all replicas converge on the same keys.

Every layer of the stack is exercised for real:

- **raft-core** drives consensus — election, replication, commit.
- **raft-transport-grpc** carries messages between nodes over real gRPC
  sockets on `localhost`.
- **raft-storage-rocksdb** persists each node's raft log and hard-state
  to RocksDB.
- The application state machine (`KvStateMachine`) is itself a
  RocksDB-backed KV store — committed commands replicate to every node.

## Pieces

| Class | Role |
|-------|------|
| `KvCommand` | A single KV mutation (PUT / DELETE), serialized as the `data` of a raft `EntryNormal`. |
| `KvStateMachine` | The replicated state machine: a RocksDB-backed KV store. Each node owns one and feeds it committed commands. |
| `RaftKVNode` | Glue wiring a `Node` (raft-core) + `GrpcTransport` + `RocksDbStorage`, running the canonical Ready loop with pending proposal / read-index / conf-change tracking. |
| `KvServer` | Application layer: wraps `RaftKVNode` + `KvStateMachine`, exposes `proposeCommand()` / `linearizableGet()` / `proposeConfChange()`. |
| `KvServerBootstrap` | `main()` entry point: brings up a multi-node cluster, runs a scripted workload, prints each node's converged KV snapshot. |

### The Ready loop (`RaftKVNode`)

Each peer runs the canonical loop on a dedicated applier thread:

```
while (running) {
    Ready rd = node.ready();
    storage.writeBatched(rd.entries, rd.hardState, rd.snapshot);  // 1. persist
    for (m : rd.messages) transport.send(m.getTo(), m);            // 2. send
    if (snapshot present) restoreStateMachine(snapshot);            // 3. apply snapshot
    for (e : rd.committedEntries) applyToStateMachine(e);          // 4. apply entries
    node.advance();                                                 // 5. ack
}
```

The host supplies an apply callback `(index, bytes) -> ()` for committed
`EntryNormal` entries; the server deserializes each into a `KvCommand` and
applies it to that node's `KvStateMachine`.

## Run

Run the demo (3-node cluster by default; pass a node count as an arg):

```bash
mvn -pl raft-examples -am compile exec:java \
    -Dexec.mainClass=io.github.xinfra.lab.raft.examples.KvServerBootstrap
```

It prints the elected leader, whether all nodes converged, and each
node's final KV view.

Run the smoke test (brings up a real 3-node cluster and asserts
convergence):

```bash
mvn -pl raft-examples -am test
```

## What's not covered here

Failure injection, leader failover, snapshot install, restart-from-disk,
linearizable reads, and chaos testing are all covered in
[`raft-tests`](../raft-tests). This module is a production-quality example
showing how raft-core, the transport, and the storage plug together into a
working replicated KV server.
