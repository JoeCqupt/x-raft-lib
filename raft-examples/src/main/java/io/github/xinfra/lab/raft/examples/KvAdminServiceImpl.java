/*
 * Copyright 2024-2026 The x-raft-lib Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * SPDX-License-Identifier: Apache-2.0
 */
package io.github.xinfra.lab.raft.examples;

import io.github.xinfra.lab.raft.Node;
import io.github.xinfra.lab.raft.examples.proto.AddNodeRequest;
import io.github.xinfra.lab.raft.examples.proto.AddNodeResponse;
import io.github.xinfra.lab.raft.examples.proto.GetClusterInfoRequest;
import io.github.xinfra.lab.raft.examples.proto.GetClusterInfoResponse;
import io.github.xinfra.lab.raft.examples.proto.KvAdminServiceGrpc;
import io.github.xinfra.lab.raft.examples.proto.RemoveNodeRequest;
import io.github.xinfra.lab.raft.examples.proto.RemoveNodeResponse;
import io.github.xinfra.lab.raft.examples.proto.SnapshotRequest;
import io.github.xinfra.lab.raft.examples.proto.SnapshotResponse;
import io.github.xinfra.lab.raft.examples.proto.TransferLeaderRequest;
import io.github.xinfra.lab.raft.examples.proto.TransferLeaderResponse;
import io.github.xinfra.lab.raft.proto.Eraftpb;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

class KvAdminServiceImpl extends KvAdminServiceGrpc.KvAdminServiceImplBase {

    private final KvServer server;

    KvAdminServiceImpl(KvServer server) {
        this.server = server;
    }

    @Override
    public void addNode(AddNodeRequest request, StreamObserver<AddNodeResponse> responseObserver) {
        server.addNode(request.getNodeId(), request.getAddress(), request.getIsLearner())
                .whenComplete((v, ex) -> {
                    if (ex != null) {
                        responseObserver.onError(
                                Status.INTERNAL.withDescription(ex.getMessage()).withCause(ex).asRuntimeException());
                    } else {
                        responseObserver.onNext(AddNodeResponse.getDefaultInstance());
                        responseObserver.onCompleted();
                    }
                });
    }

    @Override
    public void removeNode(RemoveNodeRequest request, StreamObserver<RemoveNodeResponse> responseObserver) {
        server.removeNode(request.getNodeId())
                .whenComplete((v, ex) -> {
                    if (ex != null) {
                        responseObserver.onError(
                                Status.INTERNAL.withDescription(ex.getMessage()).withCause(ex).asRuntimeException());
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
        Node.BasicStatus bs = server.status();
        Eraftpb.ConfState cs = server.raftKvNode().storage.initialState().confState();

        responseObserver.onNext(GetClusterInfoResponse.newBuilder()
                .setLeaderId(bs.lead)
                .setNodeId(bs.id)
                .setState(bs.state.name())
                .setTerm(bs.term)
                .setCommit(bs.commit)
                .setApplied(bs.applied)
                .addAllVoters(cs.getVotersList())
                .addAllLearners(cs.getLearnersList())
                .build());
        responseObserver.onCompleted();
    }

    @Override
    public void snapshot(SnapshotRequest request, StreamObserver<SnapshotResponse> responseObserver) {
        try {
            long index = server.forceSnapshot();
            responseObserver.onNext(SnapshotResponse.newBuilder().setSnapshotIndex(index).build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(
                    Status.INTERNAL.withDescription(e.getMessage()).withCause(e).asRuntimeException());
        }
    }
}
