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
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

class KvServiceImpl extends KvServiceGrpc.KvServiceImplBase {

    private final KvServer server;

    KvServiceImpl(KvServer server) {
        this.server = server;
    }

    @Override
    public void get(GetRequest request, StreamObserver<GetResponse> responseObserver) {
        server.linearizableGet(request.getKey())
                .whenComplete((opt, ex) -> {
                    if (ex != null) {
                        responseObserver.onError(
                                Status.INTERNAL.withDescription(ex.getMessage()).withCause(ex).asRuntimeException());
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
        KvCommand cmd = KvCommand.newBuilder()
                .setOp(KvCommand.Op.PUT)
                .setKey(request.getKey())
                .setValue(request.getValue())
                .build();
        server.proposeCommand(cmd)
                .whenComplete((v, ex) -> {
                    if (ex != null) {
                        responseObserver.onError(
                                Status.INTERNAL.withDescription(ex.getMessage()).withCause(ex).asRuntimeException());
                    } else {
                        responseObserver.onNext(PutResponse.getDefaultInstance());
                        responseObserver.onCompleted();
                    }
                });
    }

    @Override
    public void delete(DeleteRequest request, StreamObserver<DeleteResponse> responseObserver) {
        KvCommand cmd = KvCommand.newBuilder()
                .setOp(KvCommand.Op.DELETE)
                .setKey(request.getKey())
                .build();
        server.proposeCommand(cmd)
                .whenComplete((v, ex) -> {
                    if (ex != null) {
                        responseObserver.onError(
                                Status.INTERNAL.withDescription(ex.getMessage()).withCause(ex).asRuntimeException());
                    } else {
                        responseObserver.onNext(DeleteResponse.getDefaultInstance());
                        responseObserver.onCompleted();
                    }
                });
    }
}
