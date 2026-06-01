/*
 * Copyright 2024-2026 The x-raft-lib Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.github.xinfra.lab.raft.transport.grpc;

import com.google.protobuf.ByteString;
import io.github.xinfra.lab.raft.Transport;
import io.github.xinfra.lab.raft.proto.Eraftpb;
import io.github.xinfra.lab.raft.transport.grpc.proto.Ack;
import io.github.xinfra.lab.raft.transport.grpc.proto.RaftMessage;
import io.github.xinfra.lab.raft.transport.grpc.proto.RaftTransportServiceGrpc;
import io.github.xinfra.lab.raft.transport.grpc.proto.SnapshotChunk;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NegotiationType;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * gRPC implementation of {@link Transport}. One server bound to a local
 * port + lazily-created client channel per peer. Sends are async (executor
 * thread); inbound dispatch happens on the gRPC executor and forwards
 * directly to {@link Transport.MessageReceiver#receive}.
 *
 * <p>Snapshot install uses a server-streaming RPC (chunked) so multi-GB
 * snapshots don't bump into protobuf's 2 GB single-message limit.
 *
 * <p>Wire format: the {@code Eraftpb.Message} is serialized once on the
 * sender and carried as opaque {@code bytes} in {@link RaftMessage}, so
 * upgrading raft-core's proto doesn't force a transport rebuild.
 *
 * <p><b>Transport security.</b> By default the transport runs in plaintext
 * (h2c) — acceptable only for a trusted network or local tests. Inter-node
 * raft traffic (votes, log entries, snapshots) is consensus-critical, so
 * production deployments MUST pass a {@link TlsConfig} to one of the
 * TLS-enabled constructors. When a {@code TlsConfig} is present the server
 * is wrapped with its server {@code SslContext} and every peer channel
 * negotiates TLS; under mTLS ({@code requireClientAuth}) each side also
 * verifies the other's certificate.
 */
public class GrpcTransport implements Transport {

    private static final Logger LOG = LoggerFactory.getLogger(GrpcTransport.class);

    /** Snapshot chunk payload size. 1 MiB is a good default for typical MTUs. */
    static final int DEFAULT_SNAPSHOT_CHUNK_BYTES = 1 << 20;

    private final long localId;
    private final int localPort;
    private final int snapshotChunkBytes;

    /** TLS/mTLS settings, or {@code null} for plaintext (h2c). */
    private final TlsConfig tls;
    /**
     * Client-side {@link SslContext}, built once from {@link #tls} and shared
     * across all peer channels. {@code null} when running plaintext. Building
     * it eagerly in the constructor surfaces a bad cert/key as a clear
     * construction-time failure instead of a per-peer connect error.
     */
    private final SslContext clientSslContext;

    private final Map<Long, PeerChannel> peers = new ConcurrentHashMap<>();
    private final ExecutorService sendExecutor;

    private volatile MessageReceiver receiver;
    private volatile SnapshotSink snapshotSink;
    private volatile Server server;
    private volatile boolean closed = false;

    public GrpcTransport(long localId, int localPort) {
        this(localId, localPort, DEFAULT_SNAPSHOT_CHUNK_BYTES, null);
    }

    public GrpcTransport(long localId, int localPort, int snapshotChunkBytes) {
        this(localId, localPort, snapshotChunkBytes, null);
    }

    /**
     * Creates a TLS-enabled transport with the default snapshot chunk size.
     *
     * @param tls TLS/mTLS configuration; {@code null} runs plaintext.
     */
    public GrpcTransport(long localId, int localPort, TlsConfig tls) {
        this(localId, localPort, DEFAULT_SNAPSHOT_CHUNK_BYTES, tls);
    }

    /**
     * Full constructor.
     *
     * @param tls TLS/mTLS configuration; {@code null} runs plaintext (h2c).
     */
    public GrpcTransport(long localId, int localPort, int snapshotChunkBytes, TlsConfig tls) {
        if (localId == 0) {
            throw new IllegalArgumentException("localId must not be 0 (raft NONE)");
        }
        this.localId = localId;
        this.localPort = localPort;
        this.snapshotChunkBytes = snapshotChunkBytes;
        this.tls = tls;
        if (tls != null) {
            try {
                this.clientSslContext = tls.buildClientContext();
            } catch (SSLException e) {
                throw new IllegalArgumentException("failed to build client TLS context", e);
            }
        } else {
            this.clientSslContext = null;
        }
        // One thread per peer is overkill; a small pool is enough for the
        // hot path (heartbeat fan-out + propose batch). Snapshot streams
        // are blocking and run on this pool too.
        this.sendExecutor = Executors.newFixedThreadPool(
                Math.max(2, Runtime.getRuntime().availableProcessors() / 2),
                r -> {
                    Thread t = new Thread(r, "raft-grpc-send-" + localId);
                    t.setDaemon(false);
                    return t;
                });
    }

    @Override
    public void setReceiver(MessageReceiver receiver) {
        this.receiver = receiver;
    }

    @Override
    public boolean supportsSnapshotStreaming() {
        return true;
    }

    @Override
    public void setSnapshotSink(SnapshotSink sink) {
        this.snapshotSink = sink;
    }

    @Override
    public void addPeer(long peerId, String address) {
        if (peerId == localId) {
            // Loopback would create an infinite forwarding loop. Raft already
            // delivers self-messages via msgsAfterAppend, not the wire.
            throw new IllegalArgumentException("addPeer called with the local id; loopback is handled by raft itself");
        }
        PeerChannel old = peers.put(peerId, new PeerChannel(peerId, address));
        if (old != null) {
            old.shutdown();
        }
    }

    @Override
    public void removePeer(long peerId) {
        PeerChannel pc = peers.remove(peerId);
        if (pc != null) pc.shutdown();
    }

    @Override
    public void start() {
        if (server != null) return;
        if (receiver == null) {
            throw new IllegalStateException("setReceiver(...) must be called before start()");
        }
        try {
            NettyServerBuilder builder = NettyServerBuilder.forPort(localPort)
                    .addService(new RaftServiceImpl(localId, () -> receiver, () -> snapshotSink));
            if (tls != null) {
                builder.sslContext(tls.buildServerContext());
            }
            this.server = builder.build().start();
        } catch (IOException e) {
            throw new RaftTransportException("failed to bind grpc server on port " + localPort, e);
        }
        LOG.info("grpc transport for node {} listening on port {} ({})",
                localId, localPort, tls != null ? "TLS" : "plaintext");
    }

    @Override
    public void send(long peerId, Eraftpb.Message msg) {
        if (closed) return;
        if (peerId == localId) {
            // Should not happen — raft elides self-targeted messages — but
            // route safely if a host bug lets one through.
            MessageReceiver r = receiver;
            if (r != null) r.receive(msg);
            return;
        }
        PeerChannel pc = peers.get(peerId);
        if (pc == null) {
            LOG.warn("dropping message to unknown peer {} (msgType={})", peerId, msg.getMsgType());
            return;
        }
        sendExecutor.execute(() -> pc.send(msg));
    }

    @Override
    public void sendSnapshot(long peerId, Eraftpb.Message metaMsg, InputStream payload, SnapshotSendCallback cb) {
        if (closed) {
            closeQuietly(payload);
            cb.onComplete(false, new IllegalStateException("transport closed"));
            return;
        }
        PeerChannel pc = peers.get(peerId);
        if (pc == null) {
            closeQuietly(payload);
            cb.onComplete(false, new IllegalArgumentException("unknown peer " + peerId));
            return;
        }
        sendExecutor.execute(() -> pc.sendSnapshotStreaming(metaMsg, payload, cb));
    }

    /**
     * Reads up to {@code cap} bytes into {@code buf} starting at 0, looping over
     * partial reads. Returns the number read; a return value {@code < cap}
     * means EOF was reached (the only reason a full read can't complete).
     */
    private static int fill(InputStream in, byte[] buf, int cap) throws IOException {
        int total = 0;
        while (total < cap) {
            int n = in.read(buf, total, cap - total);
            if (n < 0) break;
            total += n;
        }
        return total;
    }

    private static void closeQuietly(java.io.Closeable c) {
        if (c == null) return;
        try {
            c.close();
        } catch (IOException ignored) {
            // best effort
        }
    }

    @Override
    public void close() {
        if (closed) return;
        closed = true;
        Server s = server;
        if (s != null) {
            try {
                s.shutdown().awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        for (PeerChannel pc : peers.values()) pc.shutdown();
        peers.clear();
        sendExecutor.shutdown();
        try {
            if (!sendExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                sendExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            sendExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    /** Per-peer channel + stub. Lazily creates the channel; recreates on shutdown. */
    private final class PeerChannel {
        final long peerId;
        final String address;
        volatile ManagedChannel channel;
        volatile RaftTransportServiceGrpc.RaftTransportServiceBlockingStub blockingStub;
        volatile RaftTransportServiceGrpc.RaftTransportServiceStub asyncStub;

        PeerChannel(long peerId, String address) {
            this.peerId = peerId;
            this.address = address;
        }

        synchronized void ensureOpen() {
            if (channel != null && !channel.isShutdown()) return;
            NettyChannelBuilder builder = NettyChannelBuilder.forTarget(address);
            if (tls != null) {
                builder.negotiationType(NegotiationType.TLS)
                        .sslContext(clientSslContext);
                String authority = tls.authorityOverride();
                if (authority != null) {
                    builder.overrideAuthority(authority);
                }
            } else {
                builder.usePlaintext();
            }
            this.channel = builder.build();
            this.blockingStub = RaftTransportServiceGrpc.newBlockingStub(channel);
            this.asyncStub = RaftTransportServiceGrpc.newStub(channel);
        }

        void send(Eraftpb.Message msg) {
            try {
                ensureOpen();
                if (msg.getMsgType() == Eraftpb.MessageType.MsgSnapshot) {
                    sendSnapshot(msg);
                } else {
                    sendUnary(msg);
                }
            } catch (Throwable t) {
                // WARN (not debug) so silent network death is visible without
                // having to drop the logger threshold across the whole tree.
                // Hosts should additionally call Node.reportUnreachable(peerId)
                // when they observe persistent send failures — wiring that
                // through a Transport-level callback is intentionally left to
                // a follow-up so this PR stays focused on the core API.
                LOG.warn("send to peer {} failed: {}", peerId, t.toString());
            }
        }

        void sendUnary(Eraftpb.Message msg) {
            RaftMessage wire = RaftMessage.newBuilder()
                    .setFrom(localId)
                    .setTo(peerId)
                    .setPayload(msg.toByteString())
                    .build();
            // Use blocking stub for back-pressure: a slow peer slows down
            // this peer's send-thread but doesn't pile up unbounded.
            blockingStub.send(wire);
        }

        void sendSnapshot(Eraftpb.Message msg) {
            // Encoding: chunk 0 starts with [4-byte BE envelope length][envelope bytes][slice of snapData].
            // Subsequent chunks carry only snapData slices. Receiver
            // reassembles by reading the 4-byte length, taking the
            // envelope, and appending the remainder to snapData.
            //
            // The "envelope" is a copy of msg with snapshot.data CLEARED, so
            // it stays small even when the snapshot is multi-GB. The actual
            // data flows through the chunk stream as raw bytes.
            byte[] snapData = msg.getSnapshot().getData().toByteArray();
            Eraftpb.Message envelope = msg.toBuilder()
                    .setSnapshot(msg.getSnapshot().toBuilder()
                            .clearData()
                            .build())
                    .build();
            byte[] envelopeBytes = envelope.toByteArray();

            byte[] header = new byte[4 + envelopeBytes.length];
            header[0] = (byte) (envelopeBytes.length >>> 24);
            header[1] = (byte) (envelopeBytes.length >>> 16);
            header[2] = (byte) (envelopeBytes.length >>> 8);
            header[3] = (byte) envelopeBytes.length;
            System.arraycopy(envelopeBytes, 0, header, 4, envelopeBytes.length);

            long total = (long) header.length + snapData.length;

            // Client-streaming RPC: server sends ONE Ack at end. Wait on it.
            java.util.concurrent.CompletableFuture<Ack> ackFuture = new java.util.concurrent.CompletableFuture<>();
            StreamObserver<Ack> ackObserver = new StreamObserver<>() {
                @Override public void onNext(Ack value) { ackFuture.complete(value); }
                @Override public void onError(Throwable t) { ackFuture.completeExceptionally(t); }
                @Override public void onCompleted() { /* ack already delivered via onNext */ }
            };
            StreamObserver<SnapshotChunk> req = asyncStub.installSnapshot(ackObserver);

            try {
                long offset = 0;
                // First chunk: header + slice of snapData.
                int firstSliceCap = Math.max(0, snapshotChunkBytes - header.length);
                int firstSlice = Math.min(firstSliceCap, snapData.length);
                byte[] first = new byte[header.length + firstSlice];
                System.arraycopy(header, 0, first, 0, header.length);
                if (firstSlice > 0) System.arraycopy(snapData, 0, first, header.length, firstSlice);
                req.onNext(SnapshotChunk.newBuilder()
                        .setFrom(localId).setTo(peerId)
                        .setTotalBytes(total)
                        .setOffset(offset)
                        .setPayload(ByteString.copyFrom(first))
                        .setLast(firstSlice == snapData.length)
                        .build());
                offset += first.length;

                int remaining = snapData.length - firstSlice;
                int dataPos = firstSlice;
                while (remaining > 0) {
                    int n = Math.min(snapshotChunkBytes, remaining);
                    byte[] slice = new byte[n];
                    System.arraycopy(snapData, dataPos, slice, 0, n);
                    boolean last = (remaining - n) == 0;
                    req.onNext(SnapshotChunk.newBuilder()
                            .setFrom(localId).setTo(peerId)
                            .setOffset(offset)
                            .setPayload(ByteString.copyFrom(slice))
                            .setLast(last)
                            .build());
                    offset += n;
                    dataPos += n;
                    remaining -= n;
                }
                req.onCompleted();
                // Block until server acks (or fails). Per-peer send-thread
                // is naturally serialised so this is OK.
                ackFuture.get(60, java.util.concurrent.TimeUnit.SECONDS);
            } catch (java.util.concurrent.TimeoutException te) {
                LOG.warn("snapshot send to peer {} timed out", peerId);
                req.onError(te);
            } catch (Throwable t) {
                LOG.warn("snapshot send to peer {} failed: {}", peerId, t.toString());
            }
        }

        /**
         * Zero-copy snapshot send: streams {@code payload} in
         * {@code snapshotChunkBytes}-sized slices through a reused buffer,
         * never materializing the whole payload. {@code metaMsg} is already
         * metadata-only (empty snapshot data). Closes {@code payload}.
         */
        void sendSnapshotStreaming(Eraftpb.Message metaMsg, InputStream payload, SnapshotSendCallback cb) {
            StreamObserver<SnapshotChunk> req = null;
            try (InputStream in = payload) {
                ensureOpen();
                byte[] envelopeBytes = metaMsg.toByteArray();
                byte[] header = new byte[4 + envelopeBytes.length];
                header[0] = (byte) (envelopeBytes.length >>> 24);
                header[1] = (byte) (envelopeBytes.length >>> 16);
                header[2] = (byte) (envelopeBytes.length >>> 8);
                header[3] = (byte) envelopeBytes.length;
                System.arraycopy(envelopeBytes, 0, header, 4, envelopeBytes.length);

                java.util.concurrent.CompletableFuture<Ack> ackFuture = new java.util.concurrent.CompletableFuture<>();
                StreamObserver<Ack> ackObserver = new StreamObserver<>() {
                    @Override public void onNext(Ack value) { ackFuture.complete(value); }
                    @Override public void onError(Throwable t) { ackFuture.completeExceptionally(t); }
                    @Override public void onCompleted() { /* ack delivered via onNext */ }
                };
                req = asyncStub.installSnapshot(ackObserver);

                long offset = 0;
                byte[] buf = new byte[snapshotChunkBytes];
                // First chunk: header + first data slice (kept within the chunk budget).
                int firstCap = Math.max(0, snapshotChunkBytes - header.length);
                int firstN = fill(in, buf, firstCap);
                boolean done = firstN < firstCap; // fill returns < cap only at EOF
                byte[] first = new byte[header.length + firstN];
                System.arraycopy(header, 0, first, 0, header.length);
                System.arraycopy(buf, 0, first, header.length, firstN);
                req.onNext(SnapshotChunk.newBuilder()
                        .setFrom(localId).setTo(peerId)
                        .setOffset(offset)
                        .setPayload(ByteString.copyFrom(first))
                        .setLast(done)
                        .build());
                offset += first.length;

                while (!done) {
                    int n = fill(in, buf, snapshotChunkBytes);
                    done = n < snapshotChunkBytes;
                    req.onNext(SnapshotChunk.newBuilder()
                            .setFrom(localId).setTo(peerId)
                            .setOffset(offset)
                            .setPayload(ByteString.copyFrom(buf, 0, n))
                            .setLast(done)
                            .build());
                    offset += n;
                }
                req.onCompleted();

                Ack ack = ackFuture.get(60, TimeUnit.SECONDS);
                if (ack.getOk()) {
                    cb.onComplete(true, null);
                } else {
                    cb.onComplete(false, new RaftTransportException("snapshot rejected by peer " + peerId + ": " + ack.getError()));
                }
            } catch (Throwable t) {
                LOG.warn("snapshot stream to peer {} failed: {}", peerId, t.toString());
                if (req != null) {
                    try { req.onError(t); } catch (Throwable ignored) { /* stream may be closed */ }
                }
                cb.onComplete(false, t);
            }
        }

        synchronized void shutdown() {
            ManagedChannel ch = channel;
            if (ch != null) {
                try {
                    ch.shutdown().awaitTermination(2, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }
}
