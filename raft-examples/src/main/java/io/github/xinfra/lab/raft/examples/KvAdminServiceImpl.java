/*
 * Copyright 2024-2026 The x-raft-lib Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * SPDX-License-Identifier: Apache-2.0
 */
package io.github.xinfra.lab.raft.examples;

import io.github.xinfra.lab.raft.examples.proto.AddNodeRequest;
import io.github.xinfra.lab.raft.examples.proto.AddNodeResponse;
import io.github.xinfra.lab.raft.examples.proto.GetClusterInfoRequest;
import io.github.xinfra.lab.raft.examples.proto.GetClusterInfoResponse;
import io.github.xinfra.lab.raft.examples.proto.KvAdminServiceGrpc;
import io.github.xinfra.lab.raft.examples.proto.RemoveNodeRequest;
import io.github.xinfra.lab.raft.examples.proto.RemoveNodeResponse;
import io.github.xinfra.lab.raft.examples.proto.TransferLeaderRequest;
import io.github.xinfra.lab.raft.examples.proto.TransferLeaderResponse;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

class KvAdminServiceImpl extends KvAdminServiceGrpc.KvAdminServiceImplBase {

    private static final long CONF_CHANGE_TIMEOUT_SECONDS = 30;

    private final KvServer server;

    KvAdminServiceImpl(KvServer server) {
        this.server = server;
    }

    @Override
    public void addNode(AddNodeRequest request, StreamObserver<AddNodeResponse> responseObserver) {
        try {
            server.checkLeader();
        } catch (KvServer.NotLeaderException e) {
            responseObserver.onError(notLeaderError(e));
            return;
        }
        server.addNode(request.getNodeId(), request.getAddress(), request.getIsLearner())
                .orTimeout(CONF_CHANGE_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .whenComplete((v, ex) -> {
                    if (ex != null) {
                        responseObserver.onError(toGrpcException(ex));
                    } else {
                        responseObserver.onNext(AddNodeResponse.getDefaultInstance());
                        responseObserver.onCompleted();
                    }
                });
    }

    @Override
    public void removeNode(RemoveNodeRequest request, StreamObserver<RemoveNodeResponse> responseObserver) {
        try {
            server.checkLeader();
        } catch (KvServer.NotLeaderException e) {
            responseObserver.onError(notLeaderError(e));
            return;
        }
        server.removeNode(request.getNodeId())
                .orTimeout(CONF_CHANGE_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .whenComplete((v, ex) -> {
                    if (ex != null) {
                        responseObserver.onError(toGrpcException(ex));
                    } else {
                        responseObserver.onNext(RemoveNodeResponse.getDefaultInstance());
                        responseObserver.onCompleted();
                    }
                });
    }

    @Override
    public void transferLeader(TransferLeaderRequest request, StreamObserver<TransferLeaderResponse> responseObserver) {
        try {
            server.transferLeader(request.getTransferee());
            responseObserver.onNext(TransferLeaderResponse.getDefaultInstance());
            responseObserver.onCompleted();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            responseObserver.onError(Status.CANCELLED.withCause(e).asRuntimeException());
        }
    }

    @Override
    public void getClusterInfo(GetClusterInfoRequest request, StreamObserver<GetClusterInfoResponse> responseObserver) {
        io.github.xinfra.lab.raft.Status s;
        try {
            s = server.raftKvNode().node.status();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            responseObserver.onError(Status.CANCELLED.withCause(e).asRuntimeException());
            return;
        }

        io.github.xinfra.lab.raft.Status.BasicStatus bs = s.basicStatus();
        responseObserver.onNext(GetClusterInfoResponse.newBuilder()
                .setLeaderId(bs.softState().lead())
                .setNodeId(bs.id())
                .setState(bs.softState().raftState().name())
                .setTerm(bs.hardState().getTerm())
                .setCommit(bs.hardState().getCommit())
                .setApplied(bs.applied())
                .addAllVoters(s.config().voters())
                .addAllLearners(s.config().learners())
                .build());
        responseObserver.onCompleted();
    }

    private static RuntimeException notLeaderError(KvServer.NotLeaderException e) {
        Metadata metadata = new Metadata();
        metadata.put(KvServiceImpl.LEADER_ID_KEY, String.valueOf(e.leaderId));
        if (e.leaderAddress != null) {
            metadata.put(KvServiceImpl.LEADER_ADDR_KEY, e.leaderAddress);
        }
        return Status.FAILED_PRECONDITION
                .withDescription(e.getMessage())
                .asRuntimeException(metadata);
    }

    private static RuntimeException toGrpcException(Throwable ex) {
        Throwable cause = ex instanceof java.util.concurrent.CompletionException ? ex.getCause() : ex;
        if (cause instanceof TimeoutException) {
            return Status.DEADLINE_EXCEEDED
                    .withDescription("request timed out")
                    .asRuntimeException();
        }
        return Status.INTERNAL
                .withDescription(cause.getMessage())
                .withCause(cause)
                .asRuntimeException();
    }

}
