/*
 * Copyright 2024-2026 The x-raft-lib Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * SPDX-License-Identifier: Apache-2.0
 */
package io.github.xinfra.lab.raft.examples;

import io.github.xinfra.lab.raft.examples.proto.DeleteRequest;
import io.github.xinfra.lab.raft.examples.proto.DeleteResponse;
import io.github.xinfra.lab.raft.examples.proto.GetRequest;
import io.github.xinfra.lab.raft.examples.proto.GetResponse;
import io.github.xinfra.lab.raft.examples.proto.KvCommand;
import io.github.xinfra.lab.raft.examples.proto.KvServiceGrpc;
import io.github.xinfra.lab.raft.examples.proto.PutRequest;
import io.github.xinfra.lab.raft.examples.proto.PutResponse;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

class KvServiceImpl extends KvServiceGrpc.KvServiceImplBase {

    private static final long REQUEST_TIMEOUT_SECONDS = 5;

    private final KvServer server;

    KvServiceImpl(KvServer server) {
        this.server = server;
    }

    @Override
    public void get(GetRequest request, StreamObserver<GetResponse> responseObserver) {
        try {
            server.checkLeader();
        } catch (KvServer.NotLeaderException e) {
            responseObserver.onError(notLeaderError(e));
            return;
        }
        server.linearizableGet(request.getKey())
                .orTimeout(REQUEST_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .whenComplete((opt, ex) -> {
                    if (ex != null) {
                        responseObserver.onError(toGrpcException(ex));
                    } else {
                        responseObserver.onNext(GetResponse.newBuilder()
                                .setFound(opt.isPresent())
                                .setValue(opt.orElse(""))
                                .build());
                        responseObserver.onCompleted();
                    }
                });
    }

    @Override
    public void put(PutRequest request, StreamObserver<PutResponse> responseObserver) {
        try {
            server.checkLeader();
        } catch (KvServer.NotLeaderException e) {
            responseObserver.onError(notLeaderError(e));
            return;
        }
        KvCommand cmd = KvCommand.newBuilder()
                .setOp(KvCommand.Op.PUT)
                .setKey(request.getKey())
                .setValue(request.getValue())
                .build();
        server.proposeCommand(cmd)
                .orTimeout(REQUEST_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .whenComplete((v, ex) -> {
                    if (ex != null) {
                        responseObserver.onError(toGrpcException(ex));
                    } else {
                        responseObserver.onNext(PutResponse.getDefaultInstance());
                        responseObserver.onCompleted();
                    }
                });
    }

    @Override
    public void delete(DeleteRequest request, StreamObserver<DeleteResponse> responseObserver) {
        try {
            server.checkLeader();
        } catch (KvServer.NotLeaderException e) {
            responseObserver.onError(notLeaderError(e));
            return;
        }
        KvCommand cmd = KvCommand.newBuilder()
                .setOp(KvCommand.Op.DELETE)
                .setKey(request.getKey())
                .build();
        server.proposeCommand(cmd)
                .orTimeout(REQUEST_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .whenComplete((v, ex) -> {
                    if (ex != null) {
                        responseObserver.onError(toGrpcException(ex));
                    } else {
                        responseObserver.onNext(DeleteResponse.getDefaultInstance());
                        responseObserver.onCompleted();
                    }
                });
    }

    static final Metadata.Key<String> LEADER_ID_KEY =
            Metadata.Key.of("x-raft-leader-id", Metadata.ASCII_STRING_MARSHALLER);
    static final Metadata.Key<String> LEADER_ADDR_KEY =
            Metadata.Key.of("x-raft-leader-addr", Metadata.ASCII_STRING_MARSHALLER);

    private static RuntimeException notLeaderError(KvServer.NotLeaderException e) {
        Metadata metadata = new Metadata();
        metadata.put(LEADER_ID_KEY, String.valueOf(e.leaderId));
        if (e.leaderAddress != null) {
            metadata.put(LEADER_ADDR_KEY, e.leaderAddress);
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
