# raft-examples

End-to-end demos that wire `raft-core` + `raft-transport-grpc` +
`raft-storage-rocksdb` into a runnable distributed key-value system.
**Not** published to Maven Central — this module is here to show what a
real integration looks like.

## What it is

A small replicated KV store. An N-node cluster (default 3) runs entirely
in one JVM: each node binds its own gRPC port and owns its own RocksDB
directories. Commands proposed through the leader are replicated by raft
and applied on every node, so all replicas converge on the same keys.

Every layer of the stack is exercised for real:

- **raft-core** drives consensus — election, replication, commit.
- **raft-transport-grpc** carries messages between nodes over real gRPC
  sockets on `localhost`.
- **raft-storage-rocksdb** persists each node's raft log and hard-state
  to RocksDB.
- The application state machine (`RocksKvStore`) is itself a
  RocksDB-backed KV store — committed commands replicate to every node.

## Pieces

| Class | Role |
|-------|------|
| `KvCommand` | A single KV mutation (PUT / DELETE), serialized as the `data` of a raft `EntryNormal`. |
| `RocksKvStore` | The replicated state machine: a RocksDB-backed KV store. Each node owns one and feeds it committed commands. |
| `RaftPeer` | Glue wiring a `Node` (raft-core) + `GrpcTransport` + `RocksDbStorage`, running the canonical Ready loop. |
| `KvClusterDemo` | `main()` entry point: brings up the cluster, runs a scripted workload, prints each node's converged KV snapshot. |

### The Ready loop (`RaftPeer`)

Each peer runs the canonical loop on a dedicated applier thread:

```
while (running) {
    Ready rd = node.ready();
    storage.writeBatched(rd.entries, rd.hardState, rd.snapshot);  // 1. persist
    for (m : rd.messages) transport.send(m.getTo(), m);            // 2. send
    for (e : rd.committedEntries) applyToStateMachine(e);          // 3. apply
    node.advance();                                                 // 4. ack
}
```

The host supplies an apply callback `(index, bytes) -> ()` for committed
`EntryNormal` entries; the demo deserializes each into a `KvCommand` and
applies it to that node's `RocksKvStore`.

## Run

Run the demo (3-node cluster by default; pass a node count as an arg):

```bash
mvn -pl raft-examples -am compile exec:java \
    -Dexec.mainClass=io.github.xinfra.lab.raft.examples.KvClusterDemo
```

It prints the elected leader, whether all nodes converged, and each
node's final KV view.

Run the smoke test (brings up a real 3-node cluster and asserts
convergence):

```bash
mvn -pl raft-examples -am test
```

## What's not covered yet

- Failure-injection harness (kill the leader mid-workload, partition
  nodes).
- Read-index / linearizable read path.
- Snapshot install demo across nodes.
- Restart-from-disk demo (proves `RocksDbStorage` survives a crash).
- Client RPC layer (a network-facing KV API rather than in-process
  `propose`).

These belong in a future demo expansion; the existing demo is the
minimum to prove that raft-core, the transport, and the storage plug
together into a working replicated KV system.
