/*
 * Copyright 2024-2026 The x-raft-lib Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * SPDX-License-Identifier: Apache-2.0
 */
package io.github.xinfra.lab.raft.examples;

import io.github.xinfra.lab.raft.Node;
import io.github.xinfra.lab.raft.RaftStateType;
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
    private final Server grpcServer;

    public KvServer(long nodeId, int raftPort, int kvPort, Path dataDir,
                    Map<Long, String> peerAddresses, boolean bootstrap) throws Exception {
        GrpcTransport transport = new GrpcTransport(nodeId, raftPort);
        this.raftKvNode = new RaftKVNode(
                nodeId, dataDir.resolve("raft"), dataDir.resolve("kv"),
                peerAddresses, bootstrap, transport);

        this.grpcServer = ServerBuilder.forPort(kvPort)
                .addService(new KvServiceImpl(this))
                .addService(new KvAdminServiceImpl(this))
                .build()
                .start();

        raftKvNode.onRemoved().thenRun(this::close);

        LOG.info("KvServer node {} started: raft={}, kv={}", nodeId, raftPort, kvPort);
    }

    public void checkLeader() {
        Node.BasicStatus bs = raftKvNode.basicStatus();
        if (bs.state != RaftStateType.StateLeader) {
            String leaderAddr = raftKvNode.getPeerAddress(bs.lead);
            throw new NotLeaderException(bs.lead, leaderAddr);
        }
    }

    public CompletableFuture<Void> proposeCommand(KvCommand cmd) {
        return raftKvNode.proposeWithFuture(cmd.toByteArray());
    }

    public CompletableFuture<Optional<String>> linearizableGet(String key) {
        return raftKvNode.readIndexWithFuture()
                .thenApply(v -> raftKvNode.stateMachine().get(key));
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

        raftKvNode.registerPeerAddress(addNodeId, address);

        return raftKvNode.proposeConfChangeWithFuture(cc, address)
                .<Void>thenApply(cs -> null);
    }

    public CompletableFuture<Void> removeNode(long removeNodeId) {
        Eraftpb.ConfChangeV2 cc = Eraftpb.ConfChangeV2.newBuilder()
                .addChanges(Eraftpb.ConfChangeSingle.newBuilder()
                        .setType(Eraftpb.ConfChangeType.ConfChangeRemoveNode)
                        .setNodeId(removeNodeId))
                .build();

        return raftKvNode.proposeConfChangeWithFuture(cc, null)
                .<Void>thenApply(cs -> null);
    }

    public void transferLeader(long transferee) throws InterruptedException {
        raftKvNode.transferLeader(raftKvNode.basicStatus().lead, transferee);
    }

    public Node.BasicStatus status() {
        return raftKvNode.basicStatus();
    }

    public RaftKVNode raftKvNode() {
        return raftKvNode;
    }

    public KvStateMachine stateMachine() {
        return raftKvNode.stateMachine();
    }

    @Override
    public void close() {
        grpcServer.shutdown();
        try { grpcServer.awaitTermination(5, TimeUnit.SECONDS); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        raftKvNode.close();
    }

    static final class NotLeaderException extends RuntimeException {
        final long leaderId;
        final String leaderAddress;
        NotLeaderException(long leaderId, String leaderAddress) {
            super(leaderAddress != null
                    ? "not leader, current leader: " + leaderId + " at " + leaderAddress
                    : "not leader, current leader: " + leaderId);
            this.leaderId = leaderId;
            this.leaderAddress = leaderAddress;
        }
    }
}
