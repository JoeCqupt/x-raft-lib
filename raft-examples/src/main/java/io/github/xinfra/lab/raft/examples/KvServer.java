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
import io.github.xinfra.lab.raft.examples.proto.KvCommand;
import io.github.xinfra.lab.raft.proto.Eraftpb;
import io.github.xinfra.lab.raft.transport.grpc.GrpcTransport;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public final class KvServer implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(KvServer.class);

    private final RaftKVNode raftKvNode;
    private final KvStateMachine stateMachine;
    private final Server grpcServer;

    public KvServer(long nodeId, int raftPort, int kvPort, Path dataDir,
                    Map<Long, String> peerAddresses, boolean bootstrap) throws Exception {
        this.stateMachine = new KvStateMachine(dataDir.resolve("kv"));

        GrpcTransport transport = new GrpcTransport(nodeId, raftPort);
        this.raftKvNode = new RaftKVNode(
                nodeId, dataDir.resolve("raft"), peerAddresses, bootstrap,
                this::onApply, this::onSnapshotRestore, transport);

        this.grpcServer = ServerBuilder.forPort(kvPort)
                .addService(new KvServiceImpl(this))
                .addService(new KvAdminServiceImpl(this))
                .build()
                .start();

        LOG.info("KvServer node {} started: raft={}, kv={}", nodeId, raftPort, kvPort);
    }

    private void onApply(long index, byte[] cmdBytes) {
        try {
            KvCommand cmd = KvCommand.parseFrom(cmdBytes);
            stateMachine.apply(index, cmd);
        } catch (InvalidProtocolBufferException e) {
            LOG.error("failed to parse KvCommand at index {}", index, e);
        }
    }

    private void onSnapshotRestore(byte[] data) {
        stateMachine.restoreState(data);
    }

    public CompletableFuture<Void> proposeCommand(KvCommand cmd) {
        return raftKvNode.proposeWithFuture(cmd.toByteArray());
    }

    public CompletableFuture<Optional<String>> linearizableGet(String key) {
        return raftKvNode.readIndexWithFuture()
                .thenApply(v -> stateMachine.get(key));
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

        raftKvNode.transport.addPeer(addNodeId, address);

        return raftKvNode.proposeConfChangeWithFuture(cc).thenApply(cs -> null);
    }

    public CompletableFuture<Void> removeNode(long removeNodeId) {
        Eraftpb.ConfChangeV2 cc = Eraftpb.ConfChangeV2.newBuilder()
                .addChanges(Eraftpb.ConfChangeSingle.newBuilder()
                        .setType(Eraftpb.ConfChangeType.ConfChangeRemoveNode)
                        .setNodeId(removeNodeId))
                .build();

        return raftKvNode.proposeConfChangeWithFuture(cc).thenApply(cs -> null);
    }

    public void transferLeader(long transferee) throws InterruptedException {
        raftKvNode.transferLeader(raftKvNode.basicStatus().lead, transferee);
    }

    public long forceSnapshot() throws RaftException {
        byte[] snapshotData = stateMachine.serializeState();
        Eraftpb.Snapshot snap = raftKvNode.forceSnapshotAndCompact(snapshotData);
        return snap != null ? snap.getMetadata().getIndex() : 0;
    }

    public Node.BasicStatus status() {
        return raftKvNode.basicStatus();
    }

    public RaftKVNode raftKvNode() {
        return raftKvNode;
    }

    public KvStateMachine stateMachine() {
        return stateMachine;
    }

    @Override
    public void close() {
        grpcServer.shutdown();
        try { grpcServer.awaitTermination(5, TimeUnit.SECONDS); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        raftKvNode.close();
        stateMachine.close();
    }
}
