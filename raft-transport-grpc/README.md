# raft-transport-grpc

gRPC implementation of the [`raft-core`](../raft-core) `Transport` interface.

## Wire format

A single gRPC service `RaftTransportService` with two RPCs:

- `Send(RaftMessage) -> Ack` — unary RPC for the hot path (heartbeat,
  append, vote, read-index). The `RaftMessage` carries serialized
  `Eraftpb.Message` bytes as opaque payload, so a `raft-core` proto bump
  doesn't force a transport rebuild.
- `InstallSnapshot(stream SnapshotChunk) -> Ack` — client-streaming RPC
  for snapshot install. Required because protobuf's single-message ceiling
  is 2 GiB. The first chunk carries `[4-byte BE envelope length][envelope
  bytes][slice of snapshot.data]`; subsequent chunks carry only further
  slices of `snapshot.data`. The "envelope" is a copy of the raft
  `MsgSnapshot` with `snapshot.data` cleared, so it stays small even
  when the actual snapshot is multi-GB.

## Usage

```java
GrpcTransport t = new GrpcTransport(/*localId=*/1L, /*port=*/8080);
t.setReceiver(msg -> node.step(msg));     // wire to raft-core Node.step
t.addPeer(2L, "peer-2.example.com:8080");
t.addPeer(3L, "peer-3.example.com:8080");
t.start();
// for each Ready, forward outbound messages:
for (Eraftpb.Message m : ready.messages) {
    t.send(m.getTo(), m);
}
// shutdown:
t.close();
```

## Transport security (TLS / mTLS)

By default the transport runs in **plaintext (h2c)** — fine for local tests
or a trusted network, but inter-node raft traffic (votes, log entries,
snapshots) is consensus-critical and must never cross an untrusted network
in the clear. Production deployments pass a `TlsConfig` to the TLS-enabled
constructor:

```java
// One-way TLS: peers verify this node's server certificate.
TlsConfig tls = TlsConfig.builder()
        .certChain(new File("node.crt"))          // server cert (PEM)
        .privateKey(new File("node.key"))         // server key (PEM, PKCS#8)
        .trustCertCollection(new File("ca.crt"))  // CA used to verify peers
        .build();

GrpcTransport t = new GrpcTransport(1L, 8080, tls);
```

For **mutual TLS** — each node verifies the other's certificate — set
`requireClientAuth(true)`. The client identity defaults to the server
cert/key, so one per-node certificate covers both directions; override with
`clientCertChain` / `clientPrivateKey` only if you issue separate client
certs:

```java
TlsConfig mtls = TlsConfig.builder()
        .certChain(new File("node.crt"))
        .privateKey(new File("node.key"))
        .trustCertCollection(new File("ca.crt"))
        .requireClientAuth(true)
        .build();
```

The certificate subject (CN/SAN) must be valid for the host peers dial in
`addPeer(id, "host:port")`. When they differ (tests, split-horizon DNS),
set `authorityOverride(...)`. TLS support is provided by the BoringSSL
runtime bundled in `grpc-netty-shaded`, so no extra dependency is needed.

## Status

Alpha. Production hardening still missing:

- Per-peer back-pressure beyond the per-thread send queue.
- Reconnect / channel reuse on transient failure (gRPC handles
  reconnect automatically; we don't yet expose tuning).
- Snapshot send ack is awaited synchronously (60s default); a wedged
  peer blocks the per-peer send thread until timeout. A future
  iteration should pipeline via async send + report-snapshot callback
  so raft can pause replication promptly.

## Test

```bash
mvn -pl raft-transport-grpc -am test
```
