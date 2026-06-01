/*
 * Copyright 2024-2026 The x-raft-lib Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * SPDX-License-Identifier: Apache-2.0
 */
package io.github.xinfra.lab.raft.examples;

import com.google.protobuf.InvalidProtocolBufferException;
import io.github.xinfra.lab.raft.Config;
import io.github.xinfra.lab.raft.Node;
import io.github.xinfra.lab.raft.Peer;
import io.github.xinfra.lab.raft.RaftException;
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
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

/**
 * End-to-end glue: {@link Node} (raft-core) + {@link RocksDbStorage}
 * (raft-storage-rocksdb) + {@link GrpcTransport} (raft-transport-grpc).
 *
 * <p>This is a reference scaffold for hosts: it shows the expected
 * Ready→persist→send→apply→advance loop with a real transport and a
 * real storage backend. It is NOT a finished service — there's no client
 * RPC, no read-index demo, and no failure-injection harness.
 *
 * <p>Threading: one daemon "applier" thread drains {@code Ready} and
 * funnels the loop. The transport runs its own threads. Tick is driven
 * by a single-threaded scheduled executor.
 */
public class RaftPeer implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(RaftPeer.class);

    public final long id;
    public final RocksDbStorage storage;
    public final Transport transport;
    public final Node node;
    private final Thread applier;
    private final boolean snapshotStreaming;
    private volatile boolean running = true;

    /** Apply callback fired for every committed normal entry. */
    private final BiConsumer<Long, byte[]> applier_cb;

    public RaftPeer(long id,
                    int grpcPort,
                    Path storageDir,
                    Map<Long, String> peerAddresses,
                    boolean bootstrap,
                    BiConsumer<Long, byte[]> applyCallback) throws Exception {
        this(id, storageDir, peerAddresses, bootstrap, applyCallback,
                new GrpcTransport(id, grpcPort));
    }

    /**
     * Constructor that accepts a pre-built {@link Transport} instead of
     * building the default gRPC one, so tests (or alternative deployments)
     * can inject a decorator — e.g. a fault-injecting "chaos" transport that
     * wraps the real gRPC transport to drop/partition messages.
     *
     * <p>The supplied transport must NOT be started yet: this peer wires its
     * receiver, registers peers, and calls {@code start()} on it.
     */
    public RaftPeer(long id,
                    Path storageDir,
                    Map<Long, String> peerAddresses,
                    boolean bootstrap,
                    BiConsumer<Long, byte[]> applyCallback,
                    Transport transport) throws Exception {
        this.id = id;
        this.applier_cb = applyCallback;
        this.storage = new RocksDbStorage(storageDir);
        this.transport = transport;

        Config cfg = Config.builder()
                .id(id)
                .electionTick(10)
                .heartbeatTick(1)
                .storage(storage)
                .maxSizePerMsg(1L << 20)             // 1 MiB
                .maxInflightMsgs(256)
                .maxUncommittedEntriesSize(64L << 20) // 64 MiB
                .preVote(true)
                .checkQuorum(true)
                // Recover applied-index from disk so raft skips previously-applied
                // entries instead of re-delivering them to the host on restart.
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

        // Wire transport: receiver routes to the node's step(). Raft-layer
        // rejections (e.g. forwarded proposal hitting ErrProposalDropped on a
        // demoted leader) are not fatal to the transport — log and continue.
        transport.setReceiver(msg -> {
            try { node.step(msg); }
            catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            catch (RaftException e) {
                // Best-effort: the sender will retry via raft's normal flow.
            }
        });

        // Zero-copy snapshots are only end-to-end when BOTH the Storage and the
        // Transport support streaming; otherwise inbound MsgSnapshot flows through
        // the receiver above and outbound through transport.send() (inline path).
        this.snapshotStreaming =
                storage.supportsStreamingSnapshot() && transport.supportsSnapshotStreaming();
        if (snapshotStreaming) {
            // Inbound out-of-band snapshot: stage the (large) payload durably to
            // its side-car, THEN feed the metadata-only MsgSnapshot to the core.
            // Crucially we do NOT commit the snapshot metadata / truncate the log
            // here — that would make the core's restore read storage, see the
            // snapshot already present, and "ignore" it, stranding applied behind
            // a compacted log. The Ready cycle's writeBatched finalizes the
            // already-staged side-car after the core has restored.
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

        // Start the Ready loop on a dedicated thread.
        this.applier = new Thread(this::readyLoop, "raft-peer-" + id + "-applier");
        this.applier.setDaemon(false);
        this.applier.start();

        // Tick on a tiny scheduler.
        java.util.concurrent.ScheduledExecutorService ticker =
                java.util.concurrent.Executors.newSingleThreadScheduledExecutor(r -> {
                    Thread t = new Thread(r, "raft-peer-" + id + "-tick");
                    t.setDaemon(true);
                    return t;
                });
        ticker.scheduleAtFixedRate(node::tick, 100, 100, TimeUnit.MILLISECONDS);
    }

    private void readyLoop() {
        try {
            while (running) {
                Ready rd = node.ready();
                // 1. Persist (atomic Ready cycle).
                storage.writeBatched(rd.entries(), rd.hardState(), rd.snapshot());
                // 2. Send messages.
                if (rd.messages() != null) {
                    for (Eraftpb.Message m : rd.messages()) {
                        if (m.getTo() == id) continue; // raft routes self via msgsAfterAppend
                        if (snapshotStreaming && m.getMsgType() == Eraftpb.MessageType.MsgSnapshot) {
                            sendSnapshotOutOfBand(m);
                        } else {
                            transport.send(m.getTo(), m);
                        }
                    }
                }
                // 3. Apply committed entries.
                long highestApplied = 0;
                if (rd.committedEntries() != null) {
                    for (Eraftpb.Entry e : rd.committedEntries()) {
                        if (e.getEntryType() == Eraftpb.EntryType.EntryNormal && e.getData().size() > 0) {
                            applier_cb.accept(e.getIndex(), e.getData().toByteArray());
                        } else if (e.getEntryType() == Eraftpb.EntryType.EntryConfChange) {
                            Eraftpb.ConfChange cc = Eraftpb.ConfChange.parseFrom(e.getData());
                            Eraftpb.ConfChangeV2 v2 = Eraftpb.ConfChangeV2.newBuilder()
                                    .addChanges(Eraftpb.ConfChangeSingle.newBuilder()
                                            .setType(cc.getChangeType())
                                            .setNodeId(cc.getNodeId()))
                                    .build();
                            Eraftpb.ConfState cs = node.applyConfChange(v2);
                            // Persist new membership so a restart can recover
                            // voters without replaying the log into raft.
                            storage.setConfState(cs);
                        } else if (e.getEntryType() == Eraftpb.EntryType.EntryConfChangeV2) {
                            Eraftpb.ConfState cs = node.applyConfChange(
                                    Eraftpb.ConfChangeV2.parseFrom(e.getData()));
                            storage.setConfState(cs);
                        }
                        highestApplied = e.getIndex();
                    }
                    // Persist the applied watermark so a restart can skip
                    // these entries. Real production hosts should batch
                    // this with the application's own state-machine commit
                    // (one fsync covering both the user state and the
                    // watermark).
                    if (highestApplied > 0) {
                        storage.setApplied(highestApplied);
                    }
                }
                // 4. Tell raft we're done with this Ready.
                node.advance();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (InvalidProtocolBufferException e) {
            LOG.error("malformed conf-change entry", e);
        }
    }

    /**
     * Route an outbound {@code MsgSnapshot} through the zero-copy out-of-band
     * channel: open the side-car payload stream and hand both metadata + stream
     * to the transport, which streams the bytes Storage→Storage without ever
     * materializing the whole blob. The callback maps terminal success/failure
     * back to {@link Node#reportSnapshot} so the leader can resume or retry
     * replication (previously this was never called, leaving the leader stuck in
     * StateSnapshot until heartbeat timeout).
     */
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

    public void propose(byte[] data) throws InterruptedException, RaftException {
        node.propose(data);
    }

    /**
     * Propose a (V2) configuration change — add/remove voters or learners.
     * Throws {@link RaftException} if the proposal was dropped (e.g. this
     * node is not the leader, or a conf change is already in flight).
     */
    public void proposeConfChange(Eraftpb.ConfChangeV2 cc) throws InterruptedException, RaftException {
        node.proposeConfChange(cc);
    }

    /**
     * Take a snapshot at the current applied index and compact the log up to
     * it, discarding the now-redundant entries. After this, a follower whose
     * match index is below the compacted point can no longer be caught up with
     * log entries and must instead be sent a snapshot (MsgSnapshot) — this is
     * how the cross-node snapshot-install path is exercised in tests.
     *
     * <p>{@code snapshotData} is the application's serialized state (opaque to
     * raft). Returns the snapshot taken, or {@code null} if nothing has been
     * applied yet.
     *
     * @throws RaftException if the applied index is unavailable for snapshot
     *         (e.g. already compacted past).
     */
    public Eraftpb.Snapshot forceSnapshotAndCompact(byte[] snapshotData) throws RaftException {
        long applied = node.basicStatus().applied;
        if (applied == 0) {
            return null;
        }
        Eraftpb.ConfState cs = storage.initialState().confState();
        final byte[] data = snapshotData == null ? new byte[0] : snapshotData;
        Eraftpb.Snapshot snap;
        if (snapshotStreaming) {
            // Streaming creation writes the payload to a side-car and returns a
            // metadata-only snapshot, so the leader's MsgSnapshot carries no
            // inline bytes — the payload travels out-of-band on send.
            try {
                snap = storage.createSnapshotStreaming(applied, cs, out -> out.write(data));
            } catch (IOException e) {
                throw new RaftException(RaftException.Code.UNAVAILABLE, "streaming snapshot create failed", e);
            }
        } else {
            snap = storage.createSnapshot(applied, cs, data);
        }
        // Reclaim the now-redundant log entries. Some Storage backends (e.g.
        // RocksDbStorage) derive firstIndex() from the snapshot index, so
        // createSnapshot already advances the logical first index past
        // {@code applied} — a separate compact(applied) is then rejected as
        // already-compacted. That is benign: the snapshot itself is what forces
        // a lagging follower onto the snapshot-install path, so tolerate it.
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

    @Override
    public void close() {
        running = false;
        try {
            // Best-effort interrupt + bounded wait.
            applier.interrupt();
            applier.join(2000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        try { node.stop(5, TimeUnit.SECONDS); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        transport.close();
        storage.close();
    }
}
