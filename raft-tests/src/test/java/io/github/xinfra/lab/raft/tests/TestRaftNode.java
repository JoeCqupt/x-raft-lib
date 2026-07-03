/*
 * Copyright 2024-2026 The x-raft-lib Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * SPDX-License-Identifier: Apache-2.0
 */
package io.github.xinfra.lab.raft.tests;

import com.google.protobuf.ByteString;
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
import java.util.function.Consumer;

/**
 * Lightweight test harness: {@link Node} + {@link RocksDbStorage} +
 * {@link GrpcTransport}. Runs the Ready loop with raw propose/apply
 * (no pending-future tracking, no proposalId protocol). Test-only —
 * not for production use.
 */
final class TestRaftNode implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(TestRaftNode.class);

    public final long id;
    public final RocksDbStorage storage;
    public final Transport transport;
    public final Node node;
    private final Thread applier;
    private final boolean snapshotStreaming;
    private volatile boolean running = true;

    private final BiConsumer<Long, byte[]> applier_cb;
    private final Consumer<byte[]> snapshotRestore_cb;

    TestRaftNode(long id,
                 int grpcPort,
                 Path storageDir,
                 Map<Long, String> peerAddresses,
                 boolean bootstrap,
                 BiConsumer<Long, byte[]> applyCallback) throws Exception {
        this(id, storageDir, peerAddresses, bootstrap, applyCallback, data -> {},
                new GrpcTransport(id, grpcPort));
    }

    TestRaftNode(long id,
                 Path storageDir,
                 Map<Long, String> peerAddresses,
                 boolean bootstrap,
                 BiConsumer<Long, byte[]> applyCallback,
                 Transport transport) throws Exception {
        this(id, storageDir, peerAddresses, bootstrap, applyCallback, data -> {}, transport);
    }

    TestRaftNode(long id,
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

        this.applier = new Thread(this::readyLoop, "test-raft-node-" + id + "-applier");
        this.applier.setDaemon(false);
        this.applier.start();

        java.util.concurrent.ScheduledExecutorService ticker =
                java.util.concurrent.Executors.newSingleThreadScheduledExecutor(r -> {
                    Thread t = new Thread(r, "test-raft-node-" + id + "-tick");
                    t.setDaemon(true);
                    return t;
                });
        ticker.scheduleAtFixedRate(node::tick, 100, 100, TimeUnit.MILLISECONDS);
    }

    void propose(byte[] data) throws InterruptedException, RaftException {
        node.propose(data);
    }

    void proposeConfChange(Eraftpb.ConfChangeV2 cc) throws InterruptedException, RaftException {
        node.proposeConfChange(cc);
    }

    Node.BasicStatus basicStatus() {
        return node.basicStatus();
    }

    private void readyLoop() {
        try {
            while (running) {
                Ready rd = node.ready();

                long t0 = System.nanoTime();
                storage.writeBatched(rd.entries(), rd.hardState(), rd.snapshot());
                long t1 = System.nanoTime();

                int msgCount = 0;
                if (rd.messages() != null) {
                    for (Eraftpb.Message m : rd.messages()) {
                        if (m.getTo() == id) continue;
                        if (snapshotStreaming && m.getMsgType() == Eraftpb.MessageType.MsgSnapshot) {
                            sendSnapshotOutOfBand(m);
                        } else {
                            transport.send(m.getTo(), m);
                        }
                        msgCount++;
                    }
                }
                long t2 = System.nanoTime();

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

                long highestApplied = 0;
                if (rd.committedEntries() != null) {
                    for (Eraftpb.Entry e : rd.committedEntries()) {
                        if (e.getEntryType() == Eraftpb.EntryType.EntryNormal && e.getData().size() > 0) {
                            applier_cb.accept(e.getIndex(), e.getData().toByteArray());
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
                long t3 = System.nanoTime();

                node.advance();
                long t4 = System.nanoTime();

                long totalMs = (t4 - t0) / 1_000_000;
                if (totalMs > 100) {
                    LOG.debug("node {} readyLoop: total={}ms write={}ms send={}ms apply={}ms advance={}ms "
                                    + "entries={} committed={} messages={} snapshot={}",
                            id, totalMs,
                            (t1 - t0) / 1_000_000,
                            (t2 - t1) / 1_000_000,
                            (t3 - t2) / 1_000_000,
                            (t4 - t3) / 1_000_000,
                            rd.entries().size(),
                            rd.committedEntries().size(),
                            msgCount,
                            rd.snapshot().getMetadata().getIndex() > 0 ? rd.snapshot().getMetadata().getIndex() : 0);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (InvalidProtocolBufferException e) {
            LOG.error("malformed conf-change entry", e);
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
    }

    private void applyConfChangeV2(Eraftpb.Entry e) throws InvalidProtocolBufferException, InterruptedException {
        Eraftpb.ConfChangeV2 cc = Eraftpb.ConfChangeV2.parseFrom(e.getData());
        Eraftpb.ConfState cs = node.applyConfChange(cc);
        storage.setConfState(cs);
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
    }
}
