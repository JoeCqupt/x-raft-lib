/*
 * Copyright 2024-2026 The x-raft-lib Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * SPDX-License-Identifier: Apache-2.0
 */
package io.github.xinfra.lab.raft.examples;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.github.xinfra.lab.raft.Config;
import io.github.xinfra.lab.raft.Node;
import io.github.xinfra.lab.raft.Peer;
import io.github.xinfra.lab.raft.RaftException;
import io.github.xinfra.lab.raft.ReadState;
import io.github.xinfra.lab.raft.Ready;
import io.github.xinfra.lab.raft.SnapshotStatus;
import io.github.xinfra.lab.raft.Transport;
import io.github.xinfra.lab.raft.proto.Eraftpb;
import io.github.xinfra.lab.raft.storage.rocksdb.RocksDbStorage;
import io.github.xinfra.lab.raft.transport.grpc.GrpcTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * End-to-end glue: {@link Node} (raft-core) + {@link RocksDbStorage}
 * (raft-storage-rocksdb) + {@link GrpcTransport} (raft-transport-grpc).
 *
 * <p>Manages the Ready→persist→send→apply→advance loop with callback-driven
 * pending proposal/read/conf-change tracking. Upper layers pass pure data;
 * proposalId and confChangeId are managed internally.
 */
public class RaftKVNode implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(RaftKVNode.class);

    public final long id;
    public final RocksDbStorage storage;
    public final Transport transport;
    public final Node node;
    private final Thread applier;
    private final boolean snapshotStreaming;
    private volatile boolean running = true;

    private final BiConsumer<Long, byte[]> applier_cb;
    private final Consumer<byte[]> snapshotRestore_cb;

    private final ConcurrentHashMap<Long, CompletableFuture<Void>> pendingProposals = new ConcurrentHashMap<>();
    private final AtomicLong proposalIdGen = new AtomicLong(0);

    private final ConcurrentHashMap<ByteBuffer, CompletableFuture<Void>> pendingReads = new ConcurrentHashMap<>();
    private final ConcurrentLinkedQueue<PendingRead> waitingForApply = new ConcurrentLinkedQueue<>();

    private record PendingRead(long safeIndex, CompletableFuture<Void> future) {}


    private final ConcurrentHashMap<Long, CompletableFuture<Eraftpb.ConfState>> pendingConfChanges = new ConcurrentHashMap<>();
    private final AtomicLong confChangeIdGen = new AtomicLong(0);

    public RaftKVNode(long id,
                      int grpcPort,
                      Path storageDir,
                      Map<Long, String> peerAddresses,
                      boolean bootstrap,
                      BiConsumer<Long, byte[]> applyCallback) throws Exception {
        this(id, storageDir, peerAddresses, bootstrap, applyCallback, data -> {},
                new GrpcTransport(id, grpcPort));
    }

    public RaftKVNode(long id,
                      Path storageDir,
                      Map<Long, String> peerAddresses,
                      boolean bootstrap,
                      BiConsumer<Long, byte[]> applyCallback,
                      Transport transport) throws Exception {
        this(id, storageDir, peerAddresses, bootstrap, applyCallback, data -> {}, transport);
    }

    public RaftKVNode(long id,
                      Path storageDir,
                      Map<Long, String> peerAddresses,
                      boolean bootstrap,
                      BiConsumer<Long, byte[]> applyCallback,
                      Consumer<byte[]> snapshotRestoreCallback,
                      Transport transport) throws Exception {
        this.id = id;
        this.applier_cb = applyCallback;
        this.snapshotRestore_cb = snapshotRestoreCallback;
        this.storage = new RocksDbStorage(storageDir);
        this.transport = transport;

        Config cfg = Config.builder()
                .id(id)
                .electionTick(10)
                .heartbeatTick(1)
                .storage(storage)
                .maxSizePerMsg(1L << 20)
                .maxInflightMsgs(256)
                .maxUncommittedEntriesSize(64L << 20)
                .preVote(true)
                .checkQuorum(true)
                .applied(storage.getApplied())
                .build();

        if (bootstrap) {
            this.node = Node.startNode(cfg,
                    peerAddresses.keySet().stream()
                            .filter(p -> p == id || peerAddresses.containsKey(p))
                            .map(Peer::new)
                            .toList());
        } else {
            this.node = Node.restartNode(cfg);
        }

        transport.setReceiver(msg -> {
            try { node.step(msg); }
            catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            catch (RaftException e) { /* best-effort */ }
        });

        this.snapshotStreaming =
                storage.supportsStreamingSnapshot() && transport.supportsSnapshotStreaming();
        if (snapshotStreaming) {
            transport.setSnapshotSink((metaMsg, payload) -> {
                storage.stageSnapshotData(metaMsg.getSnapshot(), payload);
                try { node.step(metaMsg); }
                catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            });
        }
        peerAddresses.forEach((peerId, addr) -> {
            if (peerId != id) transport.addPeer(peerId, addr);
        });
        transport.start();

        this.applier = new Thread(this::readyLoop, "raft-kv-node-" + id + "-applier");
        this.applier.setDaemon(false);
        this.applier.start();

        java.util.concurrent.ScheduledExecutorService ticker =
                java.util.concurrent.Executors.newSingleThreadScheduledExecutor(r -> {
                    Thread t = new Thread(r, "raft-kv-node-" + id + "-tick");
                    t.setDaemon(true);
                    return t;
                });
        ticker.scheduleAtFixedRate(node::tick, 100, 100, TimeUnit.MILLISECONDS);
    }

    // ==================== Public API ====================

    private static final byte TRACKED_PROPOSAL_TAG = (byte) 0xFF;
    private static final int TRACKED_HEADER_LEN = 1 + 8; // tag + proposalId

    public CompletableFuture<Void> proposeWithFuture(byte[] cmdBytes) {
        long proposalId = proposalIdGen.incrementAndGet();
        CompletableFuture<Void> future = new CompletableFuture<>();
        pendingProposals.put(proposalId, future);

        byte[] data = new byte[TRACKED_HEADER_LEN + cmdBytes.length];
        data[0] = TRACKED_PROPOSAL_TAG;
        ByteBuffer.wrap(data, 1, 8).putLong(proposalId);
        System.arraycopy(cmdBytes, 0, data, TRACKED_HEADER_LEN, cmdBytes.length);

        try {
            node.propose(data);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            pendingProposals.remove(proposalId);
            future.completeExceptionally(e);
        } catch (RaftException e) {
            pendingProposals.remove(proposalId);
            future.completeExceptionally(e);
        }
        return future;
    }

    public CompletableFuture<Void> readIndexWithFuture() {
        long readId = proposalIdGen.incrementAndGet();
        byte[] ctx = new byte[8];
        ByteBuffer.wrap(ctx).putLong(readId);
        ByteBuffer ctxKey = ByteBuffer.wrap(ctx);

        CompletableFuture<Void> future = new CompletableFuture<>();
        pendingReads.put(ctxKey, future);

        try {
            node.readIndex(ctx);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            pendingReads.remove(ctxKey);
            future.completeExceptionally(e);
        } catch (RaftException e) {
            pendingReads.remove(ctxKey);
            future.completeExceptionally(e);
        }
        return future;
    }

    public CompletableFuture<Eraftpb.ConfState> proposeConfChangeWithFuture(Eraftpb.ConfChangeV2 cc) {
        long confChangeId = confChangeIdGen.incrementAndGet();
        CompletableFuture<Eraftpb.ConfState> future = new CompletableFuture<>();
        pendingConfChanges.put(confChangeId, future);

        byte[] ctx = new byte[8];
        ByteBuffer.wrap(ctx).putLong(confChangeId);
        Eraftpb.ConfChangeV2 ccWithCtx = cc.toBuilder()
                .setContext(ByteString.copyFrom(ctx))
                .build();

        try {
            node.proposeConfChange(ccWithCtx);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            pendingConfChanges.remove(confChangeId);
            future.completeExceptionally(e);
        } catch (RaftException e) {
            pendingConfChanges.remove(confChangeId);
            future.completeExceptionally(e);
        }
        return future;
    }

    public void propose(byte[] data) throws InterruptedException, RaftException {
        node.propose(data);
    }

    public void proposeConfChange(Eraftpb.ConfChangeV2 cc) throws InterruptedException, RaftException {
        node.proposeConfChange(cc);
    }

    public void transferLeader(long lead, long transferee) throws InterruptedException {
        node.transferLeadership(lead, transferee);
    }

    public Eraftpb.Snapshot forceSnapshotAndCompact(byte[] snapshotData) throws RaftException {
        long applied = node.basicStatus().applied;
        if (applied == 0) {
            return null;
        }
        Eraftpb.ConfState cs = storage.initialState().confState();
        final byte[] data = snapshotData == null ? new byte[0] : snapshotData;
        Eraftpb.Snapshot snap;
        if (snapshotStreaming) {
            try {
                snap = storage.createSnapshotStreaming(applied, cs, out -> out.write(data));
            } catch (IOException e) {
                throw new RaftException(RaftException.Code.UNAVAILABLE, "streaming snapshot create failed", e);
            }
        } else {
            snap = storage.createSnapshot(applied, cs, data);
        }
        try {
            storage.compact(applied);
        } catch (RaftException e) {
            if (e.code() != RaftException.Code.COMPACTED) {
                throw e;
            }
        }
        return snap;
    }

    public Node.BasicStatus basicStatus() {
        return node.basicStatus();
    }

    // ==================== Ready Loop ====================

    private void readyLoop() {
        try {
            while (running) {
                Ready rd = node.ready();
                // 1. Persist.
                storage.writeBatched(rd.entries(), rd.hardState(), rd.snapshot());

                // 2. Send messages.
                if (rd.messages() != null) {
                    for (Eraftpb.Message m : rd.messages()) {
                        if (m.getTo() == id) continue;
                        if (snapshotStreaming && m.getMsgType() == Eraftpb.MessageType.MsgSnapshot) {
                            sendSnapshotOutOfBand(m);
                        } else {
                            transport.send(m.getTo(), m);
                        }
                    }
                }

                // 3. Apply snapshot to state machine.
                if (rd.snapshot().getMetadata().getIndex() > 0) {
                    byte[] appData;
                    if (snapshotStreaming) {
                        try (InputStream sin = storage.openSnapshotData(rd.snapshot())) {
                            appData = sin.readAllBytes();
                        } catch (IOException | RaftException e) {
                            LOG.error("failed to read snapshot side-car data", e);
                            appData = ByteString.EMPTY.toByteArray();
                        }
                    } else {
                        appData = rd.snapshot().getData().toByteArray();
                    }
                    if (appData.length > 0) {
                        snapshotRestore_cb.accept(appData);
                    }
                }

                // 4. Apply committed entries.
                long highestApplied = 0;
                if (rd.committedEntries() != null) {
                    for (Eraftpb.Entry e : rd.committedEntries()) {
                        if (e.getEntryType() == Eraftpb.EntryType.EntryNormal && e.getData().size() > 0) {
                            applyNormalEntry(e);
                        } else if (e.getEntryType() == Eraftpb.EntryType.EntryConfChange) {
                            applyConfChangeV1(e);
                        } else if (e.getEntryType() == Eraftpb.EntryType.EntryConfChangeV2) {
                            applyConfChangeV2(e);
                        }
                        highestApplied = e.getIndex();
                    }
                    if (highestApplied > 0) {
                        storage.setApplied(highestApplied);
                    }
                }

                // 5. Process ReadStates: if applied already >= safeIndex,
                //    complete immediately; otherwise enqueue for later.
                if (rd.readStates() != null) {
                    long currentApplied = highestApplied > 0
                            ? highestApplied : node.basicStatus().applied;
                    for (ReadState rs : rd.readStates()) {
                        ByteBuffer ctxKey = ByteBuffer.wrap(rs.requestCtx());
                        CompletableFuture<Void> future = pendingReads.remove(ctxKey);
                        if (future != null) {
                            if (currentApplied >= rs.index()) {
                                future.complete(null);
                            } else {
                                waitingForApply.add(new PendingRead(rs.index(), future));
                            }
                        }
                    }
                }

                // 5b. Drain reads waiting for apply to catch up.
                if (highestApplied > 0 && !waitingForApply.isEmpty()) {
                    drainWaitingReads(highestApplied);
                }

                // 6. Advance.
                node.advance();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (InvalidProtocolBufferException e) {
            LOG.error("malformed conf-change entry", e);
        }
    }

    private void applyNormalEntry(Eraftpb.Entry e) {
        byte[] raw = e.getData().toByteArray();
        if (raw.length > TRACKED_HEADER_LEN && raw[0] == TRACKED_PROPOSAL_TAG) {
            long proposalId = ByteBuffer.wrap(raw, 1, 8).getLong();
            byte[] cmdBytes = new byte[raw.length - TRACKED_HEADER_LEN];
            System.arraycopy(raw, TRACKED_HEADER_LEN, cmdBytes, 0, cmdBytes.length);
            applier_cb.accept(e.getIndex(), cmdBytes);
            CompletableFuture<Void> future = pendingProposals.remove(proposalId);
            if (future != null) {
                future.complete(null);
            }
            return;
        }
        applier_cb.accept(e.getIndex(), raw);
    }

    private void applyConfChangeV1(Eraftpb.Entry e) throws InvalidProtocolBufferException, InterruptedException {
        Eraftpb.ConfChange cc = Eraftpb.ConfChange.parseFrom(e.getData());
        Eraftpb.ConfChangeV2 v2 = Eraftpb.ConfChangeV2.newBuilder()
                .addChanges(Eraftpb.ConfChangeSingle.newBuilder()
                        .setType(cc.getChangeType())
                        .setNodeId(cc.getNodeId()))
                .setContext(cc.getContext())
                .build();
        Eraftpb.ConfState cs = node.applyConfChange(v2);
        storage.setConfState(cs);
        completeConfChangeFuture(cc.getContext(), cs);
    }

    private void applyConfChangeV2(Eraftpb.Entry e) throws InvalidProtocolBufferException, InterruptedException {
        Eraftpb.ConfChangeV2 cc = Eraftpb.ConfChangeV2.parseFrom(e.getData());
        Eraftpb.ConfState cs = node.applyConfChange(cc);
        storage.setConfState(cs);
        completeConfChangeFuture(cc.getContext(), cs);
    }

    private void drainWaitingReads(long applied) {
        Iterator<PendingRead> it = waitingForApply.iterator();
        while (it.hasNext()) {
            PendingRead pr = it.next();
            if (applied >= pr.safeIndex()) {
                pr.future().complete(null);
                it.remove();
            }
        }
    }

    private void completeConfChangeFuture(ByteString context, Eraftpb.ConfState cs) {
        if (context == null || context.isEmpty() || context.size() < 8) return;
        long confChangeId = ByteBuffer.wrap(context.toByteArray(), 0, 8).getLong();
        CompletableFuture<Eraftpb.ConfState> future = pendingConfChanges.remove(confChangeId);
        if (future != null) {
            future.complete(cs);
        }
    }

    private void sendSnapshotOutOfBand(Eraftpb.Message m) {
        long to = m.getTo();
        InputStream in;
        try {
            in = storage.openSnapshotData(m.getSnapshot());
        } catch (Exception e) {
            LOG.warn("openSnapshotData failed for snapshot to {} at index {}: {}",
                    to, m.getSnapshot().getMetadata().getIndex(), e.toString());
            node.reportSnapshot(to, SnapshotStatus.SnapshotFailure);
            return;
        }
        transport.sendSnapshot(to, m, in, (ok, err) -> {
            if (!ok) {
                LOG.warn("out-of-band snapshot send to {} failed: {}", to,
                        err == null ? "unknown" : err.toString());
            }
            node.reportSnapshot(to, ok ? SnapshotStatus.SnapshotFinish : SnapshotStatus.SnapshotFailure);
        });
    }

    @Override
    public void close() {
        running = false;
        try {
            applier.interrupt();
            applier.join(2000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        try { node.stop(5, TimeUnit.SECONDS); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        transport.close();
        storage.close();

        RaftException shutdownEx = new RaftException(RaftException.Code.UNAVAILABLE, "node shutting down");
        pendingProposals.forEach((id, f) -> f.completeExceptionally(shutdownEx));
        pendingProposals.clear();
        pendingReads.forEach((id, f) -> f.completeExceptionally(shutdownEx));
        pendingReads.clear();
        PendingRead pr;
        while ((pr = waitingForApply.poll()) != null) {
            pr.future().completeExceptionally(shutdownEx);
        }
        pendingConfChanges.forEach((id, f) -> f.completeExceptionally(shutdownEx));
        pendingConfChanges.clear();
    }
}
