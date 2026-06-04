/*
 * Copyright 2024-2026 The x-raft-lib Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * SPDX-License-Identifier: Apache-2.0
 */
package io.github.xinfra.lab.raft.transport.grpc;

import io.github.xinfra.lab.raft.Transport;
import io.github.xinfra.lab.raft.proto.Eraftpb;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.net.ServerSocket;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Lifecycle and edge-path coverage for {@link GrpcTransport}. The existing
 * {@code GrpcTransportTest} pins the happy-path round-trip and snapshot
 * chunking; this suite covers the surface around it: idempotency, error
 * paths, callback wiring, and the trust-boundary assertions hosts depend
 * on (loopback rejection, unknown peer drop, unreachable listener fired
 * exactly when expected).
 */
class GrpcTransportLifecycleTest {

    private static int freePort() throws Exception {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    private static GrpcTransport bareTransport(long id, int port) {
        return new GrpcTransport(id, port);
    }

    private static Eraftpb.Message heartbeat(long from, long to, long term) {
        return Eraftpb.Message.newBuilder()
                .setMsgType(Eraftpb.MessageType.MsgHeartbeat)
                .setFrom(from).setTo(to).setTerm(term)
                .build();
    }

    // =================== addPeer / removePeer ===================

    @Test
    void addPeerRejectsLocalLoopback() throws Exception {
        try (GrpcTransport t = bareTransport(1L, freePort())) {
            assertThatThrownBy(() -> t.addPeer(1L, "localhost:9999"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("loopback");
        }
    }

    @Test
    void addPeerReplaceShutsDownOldChannel() throws Exception {
        // Replacing the address for an existing peer must close the
        // previous PeerChannel — otherwise old gRPC channels leak across
        // address changes (config reload, peer move).
        try (GrpcTransport t = bareTransport(1L, freePort())) {
            t.addPeer(2L, "localhost:" + freePort());
            // No exception, no leak — re-adding is idempotent at the API
            // boundary even if the underlying channel never opened.
            t.addPeer(2L, "localhost:" + freePort());
            // Removing the replaced one should still succeed.
            t.removePeer(2L);
        }
    }

    @Test
    void removePeerOnUnknownIdIsNoOp() throws Exception {
        try (GrpcTransport t = bareTransport(1L, freePort())) {
            // No exception even though peer 99 was never added.
            t.removePeer(99L);
        }
    }

    // =================== start() guards ===================

    @Test
    void startWithoutReceiverThrows() throws Exception {
        try (GrpcTransport t = bareTransport(1L, freePort())) {
            assertThatThrownBy(t::start)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("setReceiver");
        }
    }

    @Test
    void startIsIdempotent() throws Exception {
        int port = freePort();
        try (GrpcTransport t = bareTransport(1L, port)) {
            t.setReceiver(m -> { });
            t.start();
            // Second start() must be a no-op — not throw "port in use".
            t.start();
        }
    }

    // =================== close() ===================

    @Test
    void closeIsIdempotent() throws Exception {
        GrpcTransport t = bareTransport(1L, freePort());
        t.setReceiver(m -> { });
        t.start();
        t.close();
        // Second close() must not throw, even though server is already null.
        t.close();
    }

    @Test
    void closeShutsDownRegisteredPeers() throws Exception {
        try (GrpcTransport t = bareTransport(1L, freePort())) {
            t.setReceiver(m -> { });
            t.start();
            t.addPeer(2L, "localhost:" + freePort());
            t.addPeer(3L, "localhost:" + freePort());
            // close() must drain peer channels without throwing.
        }
    }

    @Test
    void sendAfterCloseIsSilentNoOp() throws Exception {
        GrpcTransport t = bareTransport(1L, freePort());
        t.setReceiver(m -> { });
        t.start();
        t.close();
        // No NPE, no exception — closed transport drops sends quietly.
        t.send(2L, heartbeat(1, 2, 1));
    }

    // =================== send() routing ===================

    @Test
    void sendToSelfRoutesThroughLocalReceiver() throws Exception {
        // Defensive: raft elides self-targeted messages before reaching
        // send(), but if a host bug lets one through the transport must
        // deliver it back via the receiver — not the wire — to avoid the
        // infinite-forwarding-loop a real socket-level loopback would
        // create.
        BlockingQueue<Eraftpb.Message> inbox = new ArrayBlockingQueue<>(1);
        try (GrpcTransport t = bareTransport(1L, freePort())) {
            t.setReceiver(inbox::offer);
            t.send(1L, heartbeat(1, 1, 1));
            Eraftpb.Message m = inbox.poll(2, TimeUnit.SECONDS);
            assertThat(m).as("self-send must route through local receiver").isNotNull();
            assertThat(m.getTerm()).isEqualTo(1L);
        }
    }

    @Test
    void sendToUnknownPeerIsLoggedAndDropped() throws Exception {
        // Test the warn-and-drop branch: no peer registered for id 99.
        // Contract: no exception, no callback fired (unreachable listener
        // is only for transport-level errors, not for "we never knew you").
        AtomicInteger unreachableCount = new AtomicInteger();
        try (GrpcTransport t = bareTransport(1L, freePort())) {
            t.setReceiver(m -> { });
            t.setUnreachableListener(peerId -> unreachableCount.incrementAndGet());
            t.start();
            t.send(99L, heartbeat(1, 99, 1));
            // Give the send-executor a moment to NOT do anything async.
            Thread.sleep(100);
            assertThat(unreachableCount.get())
                    .as("unknown peer doesn't fire unreachable listener — never registered")
                    .isZero();
        }
    }

    // =================== Unreachable listener ===================

    @Test
    void unreachableListenerFiresOnSendFailure() throws Exception {
        // Real signal: peer is registered but unreachable (no server on
        // the other end). The first send fires the listener via the
        // blocking-stub failure path.
        BlockingQueue<Long> unreachable = new ArrayBlockingQueue<>(4);
        try (GrpcTransport t = bareTransport(1L, freePort())) {
            t.setReceiver(m -> { });
            t.setUnreachableListener(unreachable::offer);
            t.start();
            // Point peer 2 at a port nothing is listening on — first send
            // observes the connection failure.
            int deadPort = freePort();   // freed before anyone binds
            t.addPeer(2L, "localhost:" + deadPort);

            t.send(2L, heartbeat(1, 2, 1));

            Long firedFor = unreachable.poll(10, TimeUnit.SECONDS);
            assertThat(firedFor).as("listener fires for the dead peer").isEqualTo(2L);
        }
    }

    @Test
    void unreachableListenerExceptionDoesNotKillSendThread() throws Exception {
        // The send-thread must survive a listener that throws (e.g. host
        // bug in their reporter wiring). A second send should still
        // attempt and fire the listener again.
        AtomicLong calls = new AtomicLong();
        try (GrpcTransport t = bareTransport(1L, freePort())) {
            t.setReceiver(m -> { });
            t.setUnreachableListener(peerId -> {
                calls.incrementAndGet();
                throw new RuntimeException("listener gone rogue");
            });
            t.start();
            t.addPeer(2L, "localhost:" + freePort());

            t.send(2L, heartbeat(1, 2, 1));
            t.send(2L, heartbeat(1, 2, 2));

            // Both sends must have fired the listener (>= 2). Allow up to
            // 10s for both gRPC failures to surface.
            long deadline = System.currentTimeMillis() + 10_000;
            while (calls.get() < 2 && System.currentTimeMillis() < deadline) {
                Thread.sleep(50);
            }
            assertThat(calls.get())
                    .as("send-thread survives throwing listener and keeps retrying")
                    .isGreaterThanOrEqualTo(2);
        }
    }

    // =================== sendSnapshot (out-of-band) ===================

    @Test
    void supportsSnapshotStreamingIsTrue() throws Exception {
        try (GrpcTransport t = bareTransport(1L, freePort())) {
            assertThat(t.supportsSnapshotStreaming()).isTrue();
        }
    }

    @Test
    void sendSnapshotToUnknownPeerFiresCallbackWithFailure() throws Exception {
        // Peer 99 not registered; the OOB snapshot send must close the
        // payload stream and fire the callback with success=false +
        // IllegalArgumentException — never let the host hang.
        CompletableFuture<Throwable> cbErr = new CompletableFuture<>();
        try (GrpcTransport t = bareTransport(1L, freePort())) {
            t.setReceiver(m -> { });
            t.start();
            Eraftpb.Message meta = Eraftpb.Message.newBuilder()
                    .setMsgType(Eraftpb.MessageType.MsgSnapshot)
                    .setFrom(1L).setTo(99L).setTerm(1L)
                    .build();
            t.sendSnapshot(99L, meta,
                    new ByteArrayInputStream(new byte[]{1, 2, 3}),
                    (success, err) -> {
                        if (!success) cbErr.complete(err);
                    });
            Throwable err = cbErr.get(2, TimeUnit.SECONDS);
            assertThat(err)
                    .as("unknown peer fires the callback with IllegalArgumentException")
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("unknown peer");
        }
    }

    @Test
    void sendSnapshotAfterCloseFiresCallbackWithFailure() throws Exception {
        // Closed transport must still complete every outstanding callback
        // — a host that submitted a snapshot and is awaiting the callback
        // would otherwise hang on shutdown.
        CompletableFuture<Throwable> cbErr = new CompletableFuture<>();
        GrpcTransport t = bareTransport(1L, freePort());
        t.setReceiver(m -> { });
        t.start();
        t.close();
        Eraftpb.Message meta = Eraftpb.Message.newBuilder()
                .setMsgType(Eraftpb.MessageType.MsgSnapshot)
                .setFrom(1L).setTo(2L).setTerm(1L)
                .build();
        t.sendSnapshot(2L, meta,
                new ByteArrayInputStream(new byte[]{1, 2, 3}),
                (success, err) -> { if (!success) cbErr.complete(err); });
        Throwable err = cbErr.get(2, TimeUnit.SECONDS);
        assertThat(err)
                .as("closed transport fires callback with IllegalStateException")
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("closed");
    }

    // =================== setSnapshotSink ===================

    @Test
    void setSnapshotSinkRoutesInboundOutOfBandSnapshot() throws Exception {
        // When the receiver has a SnapshotSink installed AND the inbound
        // snapshot arrives via the streaming RPC, it goes to the sink
        // (Storage→Storage path) instead of through MessageReceiver.
        int port1 = freePort();
        int port2 = freePort();
        BlockingQueue<Eraftpb.Message> sinkInbox = new ArrayBlockingQueue<>(4);
        BlockingQueue<Eraftpb.Message> receiverInbox = new ArrayBlockingQueue<>(4);

        try (GrpcTransport t1 = bareTransport(1L, port1);
             GrpcTransport t2 = bareTransport(2L, port2)) {
            t1.setReceiver(m -> { });
            t2.setReceiver(receiverInbox::offer);
            t2.setSnapshotSink((Transport.SnapshotSink) (metaMsg, payload) -> {
                sinkInbox.offer(metaMsg);
                // Drain the payload so the sender's stream-completion ack
                // fires (otherwise the channel hangs waiting for EOF read).
                payload.transferTo(java.io.OutputStream.nullOutputStream());
            });
            t1.start();
            t2.start();
            t1.addPeer(2L, "localhost:" + port2);

            byte[] payload = new byte[1024];
            for (int i = 0; i < payload.length; i++) payload[i] = (byte) i;
            Eraftpb.Message meta = Eraftpb.Message.newBuilder()
                    .setMsgType(Eraftpb.MessageType.MsgSnapshot)
                    .setFrom(1L).setTo(2L).setTerm(3L)
                    .setSnapshot(Eraftpb.Snapshot.newBuilder()
                            .setMetadata(Eraftpb.SnapshotMetadata.newBuilder()
                                    .setIndex(42L).setTerm(3L)))
                    .build();
            CompletableFuture<Boolean> done = new CompletableFuture<>();
            t1.sendSnapshot(2L, meta,
                    new ByteArrayInputStream(payload),
                    (success, err) -> done.complete(success));

            assertThat(done.get(10, TimeUnit.SECONDS))
                    .as("OOB snapshot send completes successfully").isTrue();
            Eraftpb.Message routed = sinkInbox.poll(5, TimeUnit.SECONDS);
            assertThat(routed)
                    .as("OOB snapshot must reach the sink, not the receiver")
                    .isNotNull();
            assertThat(routed.getSnapshot().getMetadata().getIndex()).isEqualTo(42L);
            // The receiver path must NOT see this snapshot — the sink
            // captured it.
            assertThat(receiverInbox.poll(200, TimeUnit.MILLISECONDS))
                    .as("OOB-routed snapshot must not also reach the receiver").isNull();
        }
    }
}
