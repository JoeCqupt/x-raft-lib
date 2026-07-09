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
import io.github.xinfra.lab.raft.examples.proto.KvCommand;
import io.github.xinfra.lab.raft.proto.Eraftpb;
import io.github.xinfra.lab.raft.storage.rocksdb.RocksDbStorage;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class RaftKVNode implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(RaftKVNode.class);

    private static class ApplyFailureException extends RuntimeException {
        ApplyFailureException(String msg, Throwable cause) { super(msg, cause); }
    }

    private static final long SNAPSHOT_ENTRIES_THRESHOLD = 10_000;
    private static final int MAX_CONSECUTIVE_ERRORS = 5;

    public final long id;
    public final RocksDbStorage storage;
    public final Transport transport;
    public final Node node;
    private final KvStateMachine stateMachine;
    private final Thread applier;
    private final ScheduledExecutorService ticker;
    private final boolean snapshotStreaming;
    private final boolean asyncStorageWrites;
    private final ExecutorService persistExecutor;
    private final ExecutorService applyExecutor;
    private volatile boolean running = true;

    private volatile long lastSnapshotIndex;

    private final ConcurrentHashMap<Long, String> peerAddresses = new ConcurrentHashMap<>();

    private static final int CONF_CHANGE_CTX_HEADER_LEN = 8;

    private final ConcurrentHashMap<Long, CompletableFuture<Void>> pendingProposals = new ConcurrentHashMap<>();
    private final AtomicLong proposalIdGen = new AtomicLong(0);

    private final ConcurrentHashMap<ByteBuffer, CompletableFuture<Void>> pendingReads = new ConcurrentHashMap<>();
    private final ConcurrentLinkedQueue<PendingRead> waitingForApply = new ConcurrentLinkedQueue<>();

    private record PendingRead(long safeIndex, CompletableFuture<Void> future) {}

    private final ConcurrentHashMap<Long, CompletableFuture<Eraftpb.ConfState>> pendingConfChanges = new ConcurrentHashMap<>();
    private final AtomicLong confChangeIdGen = new AtomicLong(0);

    private final CompletableFuture<Void> removedFuture = new CompletableFuture<>();

    public RaftKVNode(long id,
                      Path storageDir,
                      Path kvDataDir,
                      Map<Long, String> peerAddresses,
                      boolean bootstrap,
                      Transport transport,
                      boolean snapshotStreaming,
                      boolean asyncStorageWrites) throws Exception {
        this.id = id;
        this.stateMachine = new KvStateMachine(kvDataDir);
        this.storage = new RocksDbStorage(storageDir);
        this.transport = transport;
        this.lastSnapshotIndex = storage.getApplied();
        this.asyncStorageWrites = asyncStorageWrites;

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
                .asyncStorageWrites(asyncStorageWrites)
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
        transport.setUnreachableListener(node::reportUnreachable);


        if (snapshotStreaming
                && (!storage.supportsStreamingSnapshot() || !transport.supportsSnapshotStreaming())) {
            LOG.warn("snapshot streaming requested but storage/transport do not both support it, falling back to inline");
            snapshotStreaming = false;
        }
        this.snapshotStreaming = snapshotStreaming;
        LOG.info("node {} snapshot mode: {}", id, snapshotStreaming ? "streaming" : "inline");
        LOG.info("node {} storage writes: {}", id, asyncStorageWrites ? "async" : "sync");
        if (asyncStorageWrites) {
            this.persistExecutor = Executors.newSingleThreadExecutor(
                    r -> { Thread t = new Thread(r, "raft-kv-node-" + id + "-persist"); t.setDaemon(true); return t; });
            this.applyExecutor = Executors.newSingleThreadExecutor(
                    r -> { Thread t = new Thread(r, "raft-kv-node-" + id + "-apply"); t.setDaemon(true); return t; });
        } else {
            this.persistExecutor = null;
            this.applyExecutor = null;
        }
        if (snapshotStreaming) {
            transport.setSnapshotSink((metaMsg, payload) -> {
                storage.stageSnapshotData(metaMsg.getSnapshot(), payload);
                try { node.step(metaMsg); }
                catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            });
        }
        this.peerAddresses.putAll(peerAddresses);
        this.peerAddresses.forEach((peerId, addr) -> {
            if (peerId != id) transport.addPeer(peerId, addr);
        });
        transport.start();

        this.applier = new Thread(this::readyLoop, "raft-kv-node-" + id + "-applier");
        this.applier.setDaemon(false);
        this.applier.start();

        this.ticker = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "raft-kv-node-" + id + "-tick");
            t.setDaemon(true);
            return t;
        });
        ticker.scheduleAtFixedRate(node::tick, 100, 100, TimeUnit.MILLISECONDS);
    }

    // ==================== Public API ====================

    public KvStateMachine stateMachine() {
        return stateMachine;
    }

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

    public void registerPeerAddress(long nodeId, String address) {
        peerAddresses.put(nodeId, address);
        if (nodeId != id) {
            transport.addPeer(nodeId, address);
        }
    }

    public String getPeerAddress(long nodeId) {
        return peerAddresses.get(nodeId);
    }

    public CompletableFuture<Eraftpb.ConfState> proposeConfChangeWithFuture(
            Eraftpb.ConfChangeV2 cc, String peerAddress) {
        long confChangeId = confChangeIdGen.incrementAndGet();
        CompletableFuture<Eraftpb.ConfState> future = new CompletableFuture<>();
        pendingConfChanges.put(confChangeId, future);

        byte[] addrBytes = (peerAddress != null)
                ? peerAddress.getBytes(java.nio.charset.StandardCharsets.UTF_8)
                : new byte[0];
        byte[] ctx = new byte[CONF_CHANGE_CTX_HEADER_LEN + addrBytes.length];
        ByteBuffer.wrap(ctx, 0, 8).putLong(confChangeId);
        System.arraycopy(addrBytes, 0, ctx, CONF_CHANGE_CTX_HEADER_LEN, addrBytes.length);

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

    public Node.BasicStatus basicStatus() {
        return node.basicStatus();
    }

    public CompletableFuture<Void> onRemoved() {
        return removedFuture;
    }

    // ==================== Ready Loop ====================

    private void readyLoop() {
        int consecutiveErrors = 0;
        try {
            while (running) {
                try {
                    processReady();
                    consecutiveErrors = 0;
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    if (e instanceof ApplyFailureException) {
                        LOG.error("fatal: apply failure, node must stop", e);
                        break;
                    }
                    consecutiveErrors++;
                    LOG.error("readyLoop error ({}/{})", consecutiveErrors, MAX_CONSECUTIVE_ERRORS, e);
                    if (consecutiveErrors >= MAX_CONSECUTIVE_ERRORS) {
                        LOG.error("readyLoop exceeded max consecutive errors, shutting down");
                        break;
                    }
                    try { Thread.sleep(100); } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
        } finally {
            drainPendingFutures();
        }
    }

    private void processReady() throws InterruptedException, InvalidProtocolBufferException {
        Ready rd = node.ready();

        if (asyncStorageWrites) {
            processReadyAsync(rd);
        } else {
            processReadySync(rd);
        }
    }

    private void processReadySync(Ready rd) throws InterruptedException, InvalidProtocolBufferException {
        // 1. Persist.
        storage.writeBatched(rd.entries(), rd.hardState(), rd.snapshot());

        // 2. Send messages.
        if (rd.messages() != null) {
            for (Eraftpb.Message m : rd.messages()) {
                if (m.getTo() == id) continue;
                if (m.getMsgType() == Eraftpb.MessageType.MsgSnapshot) {
                    if (snapshotStreaming) {
                        sendSnapshotOutOfBand(m);
                    } else {
                        sendSnapshotInline(m);
                    }
                } else {
                    transport.send(m.getTo(), m);
                }
            }
        }

        // 3. Apply snapshot to state machine.
        applySnapshotToStateMachine(rd.snapshot());

        // 4. Apply committed entries.
        long highestApplied = applyEntries(rd.committedEntries(), 0);

        // 5. Process ReadStates + drain waiting reads.
        processReadStates(rd.readStates(), highestApplied);

        // 6. Maybe snapshot + compact.
        if (highestApplied > 0) {
            maybeSnapshot(highestApplied);
        }

        // 7. Advance.
        node.advance();
    }

    private void processReadyAsync(Ready rd) {
        // Async mode: persistence and application run on dedicated threads.
        // readyLoop thread dispatches work and immediately returns to get the
        // next Ready — this is what enables pipelining.
        boolean hasSnapshot = rd.snapshot().getMetadata().getIndex() > 0;
        CompletableFuture<Void> persistDone = CompletableFuture.completedFuture(null);

        // Process ReadStates eagerly (uses best-known applied index).
        processReadStates(rd.readStates(), 0);

        if (rd.messages() == null) return;
        for (Eraftpb.Message m : rd.messages()) {
            switch (m.getMsgType()) {
                case MsgStorageAppend -> {
                    persistDone = CompletableFuture.runAsync(
                            () -> handleStorageAppend(m), persistExecutor);
                }
                case MsgStorageApply -> {
                    Runnable applyTask = () -> handleStorageApply(m);
                    if (hasSnapshot) {
                        persistDone.thenRunAsync(applyTask, applyExecutor);
                    } else {
                        CompletableFuture.runAsync(applyTask, applyExecutor);
                    }
                }
                default -> sendPeerMessage(m);
            }
        }
        // No advance() — readyLoop returns immediately.
    }

    private void handleStorageAppend(Eraftpb.Message m) {
        try {
            // Persist entries + hardState + snapshot from MsgStorageAppend.
            Eraftpb.HardState hs = (m.getTerm() != 0 || m.getVote() != 0 || m.getCommit() != 0)
                    ? Eraftpb.HardState.newBuilder()
                            .setTerm(m.getTerm()).setVote(m.getVote()).setCommit(m.getCommit()).build()
                    : Eraftpb.HardState.getDefaultInstance();
            storage.writeBatched(m.getEntriesList(), hs, m.getSnapshot());

            // Apply snapshot to state machine (must happen before MsgStorageApply entries).
            applySnapshotToStateMachine(m.getSnapshot());

            // Deliver responses: self-addressed (MsgStorageAppendResp) via node.step(),
            // peer-addressed (MsgAppResp etc.) via transport — all AFTER persistence.
            for (Eraftpb.Message resp : m.getResponsesList()) {
                deliverOrSend(resp);
            }
        } catch (Exception e) {
            LOG.error("async persist failed", e);
        }
    }

    private void handleStorageApply(Eraftpb.Message m) {
        try {
            // Apply committed entries from MsgStorageApply to state machine.
            long highestApplied = applyEntries(m.getEntriesList(), 0);

            // Drain reads waiting for apply to catch up.
            if (highestApplied > 0 && !waitingForApply.isEmpty()) {
                drainWaitingReads(highestApplied);
            }

            // Maybe snapshot + compact.
            if (highestApplied > 0) {
                maybeSnapshot(highestApplied);
            }

            // Deliver MsgStorageApplyResp to tell raft apply is done.
            for (Eraftpb.Message resp : m.getResponsesList()) {
                deliverOrSend(resp);
            }
        } catch (Exception e) {
            LOG.error("async apply failed", e);
        }
    }

    private long applyEntries(java.util.List<Eraftpb.Entry> entries, long currentHighest)
            throws InterruptedException, InvalidProtocolBufferException {
        if (entries == null || entries.isEmpty()) return currentHighest;
        long highest = currentHighest;
        for (Eraftpb.Entry e : entries) {
            if (e.getEntryType() == Eraftpb.EntryType.EntryNormal && e.getData().size() > 0) {
                applyNormalEntry(e);
            } else if (e.getEntryType() == Eraftpb.EntryType.EntryConfChange) {
                applyConfChangeV1(e);
            } else if (e.getEntryType() == Eraftpb.EntryType.EntryConfChangeV2) {
                applyConfChangeV2(e);
            }
            highest = e.getIndex();
        }
        if (highest > currentHighest) {
            storage.setApplied(highest);
        }
        return highest;
    }

    private void applySnapshotToStateMachine(Eraftpb.Snapshot snap) {
        if (snap.getMetadata().getIndex() == 0) return;
        byte[] appData;
        if (snapshotStreaming) {
            try (InputStream sin = storage.openSnapshotData(snap)) {
                appData = sin.readAllBytes();
            } catch (IOException | RaftException e) {
                LOG.error("failed to read snapshot side-car data", e);
                appData = ByteString.EMPTY.toByteArray();
            }
        } else {
            appData = snap.getData().toByteArray();
        }
        if (appData.length > 0) {
            stateMachine.restoreState(appData);
        }
    }

    private void processReadStates(java.util.List<ReadState> readStates, long highestApplied) {
        if (readStates == null || readStates.isEmpty()) return;
        long currentApplied = highestApplied > 0
                ? highestApplied : node.basicStatus().applied;
        for (ReadState rs : readStates) {
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

    private void sendPeerMessage(Eraftpb.Message m) {
        if (m.getTo() == id) return;
        if (snapshotStreaming && m.getMsgType() == Eraftpb.MessageType.MsgSnapshot) {
            sendSnapshotOutOfBand(m);
        } else {
            transport.send(m.getTo(), m);
        }
    }

    private void deliverOrSend(Eraftpb.Message resp) throws InterruptedException {
        if (resp.getTo() == id) {
            try { node.step(resp); }
            catch (RaftException e) { LOG.warn("node.step failed for async response: {}", e.getMessage()); }
        } else {
            sendPeerMessage(resp);
        }
    }

    private void applyNormalEntry(Eraftpb.Entry e) {
        byte[] raw = e.getData().toByteArray();
        if (raw.length > TRACKED_HEADER_LEN && raw[0] == TRACKED_PROPOSAL_TAG) {
            long proposalId = ByteBuffer.wrap(raw, 1, 8).getLong();
            byte[] cmdBytes = new byte[raw.length - TRACKED_HEADER_LEN];
            System.arraycopy(raw, TRACKED_HEADER_LEN, cmdBytes, 0, cmdBytes.length);
            CompletableFuture<Void> future = pendingProposals.remove(proposalId);
            try {
                applyKvCommand(e.getIndex(), cmdBytes);
                if (future != null) future.complete(null);
            } catch (Exception ex) {
                if (future != null) future.completeExceptionally(ex);
                throw ex;
            }
            return;
        }
        applyKvCommand(e.getIndex(), raw);
    }

    private void applyKvCommand(long index, byte[] cmdBytes) {
        try {
            KvCommand cmd = KvCommand.parseFrom(cmdBytes);
            stateMachine.apply(index, cmd);
        } catch (InvalidProtocolBufferException e) {
            throw new ApplyFailureException(
                    "FATAL: failed to parse committed entry at index " + index
                    + ", state machine may be inconsistent, node must stop", e);
        }
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
        updateTransportPeers(v2);
        completeConfChangeFuture(cc.getContext(), cs);
        checkSelfRemoval(v2, cs);
    }

    private void applyConfChangeV2(Eraftpb.Entry e) throws InvalidProtocolBufferException, InterruptedException {
        Eraftpb.ConfChangeV2 cc = Eraftpb.ConfChangeV2.parseFrom(e.getData());
        Eraftpb.ConfState cs = node.applyConfChange(cc);
        storage.setConfState(cs);
        updateTransportPeers(cc);
        completeConfChangeFuture(cc.getContext(), cs);
        checkSelfRemoval(cc, cs);
    }

    private void checkSelfRemoval(Eraftpb.ConfChangeV2 cc, Eraftpb.ConfState cs) {
        if (!cs.getVotersList().contains(id)
                && !cs.getLearnersList().contains(id)
                && !cs.getVotersOutgoingList().contains(id)) {
            LOG.info("node {} removed from cluster, shutting down", id);
            removedFuture.complete(null);
            close();
        }
    }

    private void updateTransportPeers(Eraftpb.ConfChangeV2 cc) {
        String address = extractAddressFromContext(cc.getContext());
        for (Eraftpb.ConfChangeSingle change : cc.getChangesList()) {
            long nodeId = change.getNodeId();
            if (nodeId == id) continue;
            switch (change.getType()) {
                case ConfChangeAddNode, ConfChangeAddLearnerNode -> {
                    if (address != null && !address.isEmpty()) {
                        peerAddresses.put(nodeId, address);
                        transport.addPeer(nodeId, address);
                    }
                }
                case ConfChangeRemoveNode -> {
                    peerAddresses.remove(nodeId);
                    transport.removePeer(nodeId);
                }
                default -> { }
            }
        }
    }

    private static String extractAddressFromContext(ByteString context) {
        if (context == null || context.size() <= CONF_CHANGE_CTX_HEADER_LEN) return null;
        byte[] raw = context.toByteArray();
        return new String(raw, CONF_CHANGE_CTX_HEADER_LEN, raw.length - CONF_CHANGE_CTX_HEADER_LEN,
                java.nio.charset.StandardCharsets.UTF_8);
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

    private void maybeSnapshot(long applied) {
        if (applied - lastSnapshotIndex < SNAPSHOT_ENTRIES_THRESHOLD) return;
        try {
            Eraftpb.ConfState cs = storage.initialState().confState();
            if (snapshotStreaming) {
                storage.createSnapshotStreaming(applied, cs, out -> out.write(stateMachine.serializeState()));
            } else {
                storage.createSnapshot(applied, cs, stateMachine.serializeState());
            }
            lastSnapshotIndex = applied;
            LOG.info("node {} created {} snapshot at index {}", id, snapshotStreaming ? "streaming" : "inline", applied);
        } catch (RaftException e) {
            if (e.code() != RaftException.Code.SNAP_OUT_OF_DATE) {
                LOG.warn("node {} snapshot creation failed at index {}", id, applied, e);
            }
            return;
        } catch (IOException e) {
            LOG.warn("node {} streaming snapshot I/O failed at index {}", id, applied, e);
            return;
        }
        try {
            storage.compact(applied);
            LOG.info("node {} compacted log up to index {}", id, applied);
        } catch (RaftException e) {
            if (e.code() != RaftException.Code.COMPACTED) {
                LOG.warn("node {} log compaction failed at index {}", id, applied, e);
            }
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

    /**
     * Inline snapshot send with completion reporting. The payload already sits
     * inside {@code m}'s snapshot data, so it goes over the normal (reliable)
     * {@code transport.send} channel. Unlike a plain message send, we always
     * report the outcome back to the core so the leader's Progress leaves
     * {@code StateSnapshot}: without this, a {@code MsgSnapshot} lost on a
     * broken connection would strand the follower in {@code StateSnapshot}
     * indefinitely (heartbeat-driven appends stay paused there).
     *
     * <p>{@code transport.send} is fire-and-forget, so success here means
     * "submitted", not "delivered": if the snapshot never arrives, the follower
     * stays behind, the subsequent probe append is rejected, and the leader
     * re-sends the snapshot — a self-healing loop. On a submit-time exception we
     * report {@link SnapshotStatus#SnapshotFailure} so the leader retries after
     * a heartbeat interval.
     */
    private void sendSnapshotInline(Eraftpb.Message m) {
        long to = m.getTo();
        boolean ok = true;
        try {
            transport.send(to, m);
        } catch (RuntimeException e) {
            ok = false;
            LOG.warn("inline snapshot send to {} failed: {}", to, e.toString());
        }
        node.reportSnapshot(to, ok ? SnapshotStatus.SnapshotFinish : SnapshotStatus.SnapshotFailure);
    }

    private void drainPendingFutures() {
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

    @Override
    public void close() {
        running = false;
        ticker.shutdownNow();
        if (persistExecutor != null) persistExecutor.shutdownNow();
        if (applyExecutor != null) applyExecutor.shutdownNow();
        try {
            applier.interrupt();
            applier.join(2000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        try { node.stop(5, TimeUnit.SECONDS); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        transport.close();
        drainPendingFutures();
        stateMachine.close();
        storage.close();
    }
}
