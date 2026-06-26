/*
 * Copyright 2024-2026 The x-raft-lib Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * SPDX-License-Identifier: Apache-2.0
 */
package io.github.xinfra.lab.raft.examples;

import com.google.protobuf.InvalidProtocolBufferException;
import io.github.xinfra.lab.raft.Node;
import io.github.xinfra.lab.raft.RaftException;
import io.github.xinfra.lab.raft.ReadState;
import io.github.xinfra.lab.raft.examples.proto.KvCommand;
import io.github.xinfra.lab.raft.proto.Eraftpb;
import io.github.xinfra.lab.raft.transport.grpc.GrpcTransport;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public final class KvServer implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(KvServer.class);

    private final RaftPeer raftPeer;
    private final KvStateMachine stateMachine;
    private final Server grpcServer;

    private final ConcurrentHashMap<Long, CompletableFuture<Void>> pendingProposals = new ConcurrentHashMap<>();
    private final AtomicLong proposalIdGen = new AtomicLong(0);

    private final ConcurrentHashMap<ByteBuffer, CompletableFuture<Long>> pendingReads = new ConcurrentHashMap<>();

    private final Thread readStatePoller;
    private volatile boolean running = true;

    public KvServer(long nodeId, int raftPort, int kvPort, Path dataDir,
                    Map<Long, String> peerAddresses, boolean bootstrap) throws Exception {
        this.stateMachine = new KvStateMachine(dataDir.resolve("kv"));

        GrpcTransport transport = new GrpcTransport(nodeId, raftPort);
        this.raftPeer = new RaftPeer(
                nodeId, dataDir.resolve("raft"), peerAddresses, bootstrap,
                this::onApply, this::onSnapshotRestore, transport);

        this.grpcServer = ServerBuilder.forPort(kvPort)
                .addService(new KvServiceImpl(this))
                .addService(new KvAdminServiceImpl(this))
                .build()
                .start();

        this.readStatePoller = new Thread(this::pollReadStates, "kv-server-" + nodeId + "-read-poller");
        this.readStatePoller.setDaemon(true);
        this.readStatePoller.start();

        LOG.info("KvServer node {} started: raft={}, kv={}", nodeId, raftPort, kvPort);
    }

    private void onApply(long index, byte[] data) {
        if (data.length <= 8) return;

        long proposalId = ByteBuffer.wrap(data, 0, 8).getLong();
        byte[] cmdBytes = Arrays.copyOfRange(data, 8, data.length);

        try {
            KvCommand cmd = KvCommand.parseFrom(cmdBytes);
            stateMachine.apply(index, cmd);
        } catch (InvalidProtocolBufferException e) {
            LOG.error("failed to parse KvCommand at index {}", index, e);
        }

        CompletableFuture<Void> future = pendingProposals.remove(proposalId);
        if (future != null) {
            future.complete(null);
        }
    }

    private void onSnapshotRestore(byte[] data) {
        stateMachine.restoreState(data);
    }

    public CompletableFuture<Void> proposeCommand(KvCommand cmd) {
        long proposalId = proposalIdGen.incrementAndGet();
        CompletableFuture<Void> future = new CompletableFuture<>();
        pendingProposals.put(proposalId, future);

        byte[] cmdBytes = cmd.toByteArray();
        byte[] data = new byte[8 + cmdBytes.length];
        ByteBuffer.wrap(data, 0, 8).putLong(proposalId);
        System.arraycopy(cmdBytes, 0, data, 8, cmdBytes.length);

        try {
            raftPeer.propose(data);
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

    public CompletableFuture<Optional<String>> linearizableGet(String key) {
        byte[] ctx = ByteBuffer.allocate(8).putLong(proposalIdGen.incrementAndGet()).array();
        ByteBuffer ctxKey = ByteBuffer.wrap(ctx);
        CompletableFuture<Long> readFuture = new CompletableFuture<>();
        pendingReads.put(ctxKey, readFuture);

        try {
            raftPeer.node.readIndex(ctx);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            pendingReads.remove(ctxKey);
            return CompletableFuture.failedFuture(e);
        } catch (RaftException e) {
            pendingReads.remove(ctxKey);
            return CompletableFuture.failedFuture(e);
        }

        return readFuture.thenCompose(safeIndex -> {
            CompletableFuture<Optional<String>> result = new CompletableFuture<>();
            new Thread(() -> {
                try {
                    long deadline = System.currentTimeMillis() + 10_000;
                    while (raftPeer.basicStatus().applied < safeIndex) {
                        if (System.currentTimeMillis() > deadline) {
                            result.completeExceptionally(
                                    new RaftException(RaftException.Code.UNAVAILABLE, "timeout waiting for applied to reach " + safeIndex));
                            return;
                        }
                        Thread.sleep(5);
                    }
                    result.complete(stateMachine.get(key));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    result.completeExceptionally(e);
                }
            }).start();
            return result;
        });
    }

    private void pollReadStates() {
        while (running) {
            try {
                for (ReadState rs : raftPeer.drainReadStates()) {
                    ByteBuffer ctxKey = ByteBuffer.wrap(rs.requestCtx());
                    CompletableFuture<Long> future = pendingReads.remove(ctxKey);
                    if (future != null) {
                        future.complete(rs.index());
                    }
                }
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    public CompletableFuture<Void> addNode(long addNodeId, String address, boolean isLearner) {
        Eraftpb.ConfChangeType type = isLearner
                ? Eraftpb.ConfChangeType.ConfChangeAddLearnerNode
                : Eraftpb.ConfChangeType.ConfChangeAddNode;

        Eraftpb.ConfChangeV2 cc = Eraftpb.ConfChangeV2.newBuilder()
                .addChanges(Eraftpb.ConfChangeSingle.newBuilder()
                        .setType(type)
                        .setNodeId(addNodeId))
                .build();

        raftPeer.transport.addPeer(addNodeId, address);

        CompletableFuture<Void> future = new CompletableFuture<>();
        try {
            raftPeer.proposeConfChange(cc);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return CompletableFuture.failedFuture(e);
        } catch (RaftException e) {
            return CompletableFuture.failedFuture(e);
        }

        new Thread(() -> {
            try {
                long deadline = System.currentTimeMillis() + 30_000;
                while (System.currentTimeMillis() < deadline) {
                    Eraftpb.ConfState cs = raftPeer.storage.initialState().confState();
                    if (isLearner && cs.getLearnersList().contains(addNodeId)) {
                        future.complete(null);
                        return;
                    }
                    if (!isLearner && cs.getVotersList().contains(addNodeId)) {
                        future.complete(null);
                        return;
                    }
                    Thread.sleep(25);
                }
                future.completeExceptionally(
                        new RaftException(RaftException.Code.UNAVAILABLE, "timeout waiting for conf change"));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                future.completeExceptionally(e);
            }
        }).start();
        return future;
    }

    public CompletableFuture<Void> removeNode(long removeNodeId) {
        Eraftpb.ConfChangeV2 cc = Eraftpb.ConfChangeV2.newBuilder()
                .addChanges(Eraftpb.ConfChangeSingle.newBuilder()
                        .setType(Eraftpb.ConfChangeType.ConfChangeRemoveNode)
                        .setNodeId(removeNodeId))
                .build();

        CompletableFuture<Void> future = new CompletableFuture<>();
        try {
            raftPeer.proposeConfChange(cc);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return CompletableFuture.failedFuture(e);
        } catch (RaftException e) {
            return CompletableFuture.failedFuture(e);
        }

        new Thread(() -> {
            try {
                long deadline = System.currentTimeMillis() + 30_000;
                while (System.currentTimeMillis() < deadline) {
                    Eraftpb.ConfState cs = raftPeer.storage.initialState().confState();
                    if (!cs.getVotersList().contains(removeNodeId)
                            && !cs.getLearnersList().contains(removeNodeId)) {
                        future.complete(null);
                        return;
                    }
                    Thread.sleep(25);
                }
                future.completeExceptionally(
                        new RaftException(RaftException.Code.UNAVAILABLE, "timeout waiting for node removal"));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                future.completeExceptionally(e);
            }
        }).start();
        return future;
    }

    public void transferLeader(long transferee) throws InterruptedException {
        raftPeer.node.transferLeadership(raftPeer.basicStatus().lead, transferee);
    }

    public long forceSnapshot() throws RaftException {
        byte[] snapshotData = stateMachine.serializeState();
        Eraftpb.Snapshot snap = raftPeer.forceSnapshotAndCompact(snapshotData);
        return snap != null ? snap.getMetadata().getIndex() : 0;
    }

    public Node.BasicStatus status() {
        return raftPeer.basicStatus();
    }

    public RaftPeer raftPeer() {
        return raftPeer;
    }

    public KvStateMachine stateMachine() {
        return stateMachine;
    }

    @Override
    public void close() {
        running = false;
        readStatePoller.interrupt();
        try { readStatePoller.join(2000); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }

        grpcServer.shutdown();
        try { grpcServer.awaitTermination(5, TimeUnit.SECONDS); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }

        raftPeer.close();
        stateMachine.close();

        pendingProposals.forEach((id, f) -> f.completeExceptionally(
                new RaftException(RaftException.Code.UNAVAILABLE, "server shutting down")));
        pendingProposals.clear();
        pendingReads.forEach((id, f) -> f.completeExceptionally(
                new RaftException(RaftException.Code.UNAVAILABLE, "server shutting down")));
        pendingReads.clear();
    }
}
