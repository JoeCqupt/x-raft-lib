/*
 * Copyright 2024-2026 The x-raft-lib Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.github.xinfra.lab.raft.transport.grpc;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.github.xinfra.lab.raft.Transport;
import io.github.xinfra.lab.raft.proto.Eraftpb;
import io.github.xinfra.lab.raft.transport.grpc.proto.Ack;
import io.github.xinfra.lab.raft.transport.grpc.proto.RaftMessage;
import io.github.xinfra.lab.raft.transport.grpc.proto.RaftTransportServiceGrpc;
import io.github.xinfra.lab.raft.transport.grpc.proto.SnapshotChunk;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/** Server side of {@link GrpcTransport}. */
final class RaftServiceImpl extends RaftTransportServiceGrpc.RaftTransportServiceImplBase {

    private static final Logger LOG = LoggerFactory.getLogger(RaftServiceImpl.class);

    /** Pipe buffer between the gRPC stream thread and the snapshot-install worker. */
    private static final int PIPE_BUFFER_BYTES = 1 << 20;

    private final long localId;
    private final Supplier<Transport.MessageReceiver> receiverSupplier;
    private final Supplier<Transport.SnapshotSink> sinkSupplier;

    RaftServiceImpl(long localId,
                    Supplier<Transport.MessageReceiver> receiverSupplier,
                    Supplier<Transport.SnapshotSink> sinkSupplier) {
        this.localId = localId;
        this.receiverSupplier = receiverSupplier;
        this.sinkSupplier = sinkSupplier;
    }

    @Override
    public void send(RaftMessage request, StreamObserver<Ack> responseObserver) {
        if (request.getTo() != localId) {
            responseObserver.onNext(Ack.newBuilder().setOk(false)
                    .setError("misrouted: to=" + request.getTo() + " local=" + localId).build());
            responseObserver.onCompleted();
            return;
        }
        Transport.MessageReceiver r = receiverSupplier.get();
        if (r == null) {
            responseObserver.onNext(Ack.newBuilder().setOk(false).setError("receiver not ready").build());
            responseObserver.onCompleted();
            return;
        }
        try {
            Eraftpb.Message msg = Eraftpb.Message.parseFrom(request.getPayload());
            r.receive(msg);
            responseObserver.onNext(Ack.newBuilder().setOk(true).build());
            responseObserver.onCompleted();
        } catch (InvalidProtocolBufferException e) {
            LOG.warn("dropping malformed Eraftpb.Message from {}: {}", request.getFrom(), e.toString());
            responseObserver.onNext(Ack.newBuilder().setOk(false).setError("malformed payload").build());
            responseObserver.onCompleted();
        } catch (Throwable t) {
            LOG.error("receiver threw on inbound from {}: {}", request.getFrom(), t.toString());
            responseObserver.onNext(Ack.newBuilder().setOk(false).setError("receiver error").build());
            responseObserver.onCompleted();
        }
    }

    @Override
    public StreamObserver<SnapshotChunk> installSnapshot(StreamObserver<Ack> responseObserver) {
        // Framing: chunk[0].payload starts with [4-byte BE envelope length][envelope bytes][first snapData slice];
        // subsequent chunks are pure snapData slices. The envelope is a
        // metadata-only MsgSnapshot (snapshot.data empty).
        //
        // When a SnapshotSink is registered we stream the payload straight into
        // it via a pipe + worker thread (zero-copy, never fully buffered).
        // Otherwise we fall back to buffering + inline reassembly through the
        // MessageReceiver (back-compat for non-streaming hosts).
        return new StreamObserver<>() {
            final ByteArrayOutputStream headerBuf = new ByteArrayOutputStream();
            boolean envelopeReady = false;
            Eraftpb.Message envelope;
            long expectedFrom = -1;

            // Sink (zero-copy) path.
            PipedOutputStream pos;
            Thread worker;
            CompletableFuture<Void> workerDone;

            // Fallback (inline reassembly) path.
            ByteArrayOutputStream fallbackBuf;

            @Override
            public void onNext(SnapshotChunk chunk) {
                if (chunk.getTo() != localId) {
                    fail(new IllegalArgumentException(
                            "snapshot misrouted: to=" + chunk.getTo() + " local=" + localId));
                    return;
                }
                if (expectedFrom == -1) {
                    expectedFrom = chunk.getFrom();
                } else if (chunk.getFrom() != expectedFrom) {
                    fail(new IllegalArgumentException(
                            "snapshot stream interleaved from=" + chunk.getFrom() + " expected=" + expectedFrom));
                    return;
                }
                try {
                    if (envelopeReady) {
                        if (pos != null) {
                            chunk.getPayload().writeTo(pos);
                        } else {
                            chunk.getPayload().writeTo(fallbackBuf);
                        }
                        return;
                    }
                    // Still assembling the header; accumulate and try to parse.
                    chunk.getPayload().writeTo(headerBuf);
                    byte[] cur = headerBuf.toByteArray();
                    if (cur.length < 4) {
                        return;
                    }
                    int envLen = ((cur[0] & 0xff) << 24) | ((cur[1] & 0xff) << 16)
                            | ((cur[2] & 0xff) << 8) | (cur[3] & 0xff);
                    if (envLen < 0) {
                        fail(new IllegalStateException("snapshot envelope length invalid"));
                        return;
                    }
                    if (cur.length < 4 + envLen) {
                        return; // envelope not fully arrived yet
                    }
                    envelope = Eraftpb.Message.parseFrom(ByteString.copyFrom(cur, 4, envLen));
                    envelopeReady = true;

                    int dataStart = 4 + envLen;
                    int dataLen = cur.length - dataStart;
                    Transport.SnapshotSink sink = sinkSupplier.get();
                    if (sink != null) {
                        startWorker(sink, envelope);
                        if (dataLen > 0) {
                            pos.write(cur, dataStart, dataLen);
                        }
                    } else {
                        fallbackBuf = new ByteArrayOutputStream();
                        if (dataLen > 0) {
                            fallbackBuf.write(cur, dataStart, dataLen);
                        }
                    }
                } catch (Throwable t) {
                    fail(t);
                }
            }

            private void startWorker(Transport.SnapshotSink sink, Eraftpb.Message metaMsg) throws IOException {
                PipedInputStream pis = new PipedInputStream(PIPE_BUFFER_BYTES);
                pos = new PipedOutputStream(pis);
                workerDone = new CompletableFuture<>();
                worker = new Thread(() -> {
                    try (InputStream is = pis) {
                        sink.install(metaMsg, is);
                        workerDone.complete(null);
                    } catch (Throwable t) {
                        workerDone.completeExceptionally(t);
                    }
                }, "raft-snap-install-" + localId);
                worker.setDaemon(true);
                worker.start();
            }

            private void fail(Throwable t) {
                LOG.warn("snapshot stream from {} failed: {}", expectedFrom, t.toString());
                if (pos != null) {
                    try { pos.close(); } catch (IOException ignored) { /* worker will see EOF/error */ }
                }
                if (worker != null) {
                    worker.interrupt();
                }
                responseObserver.onError(t);
            }

            @Override
            public void onError(Throwable t) {
                LOG.warn("snapshot stream from {} aborted: {}", expectedFrom, t.toString());
                if (pos != null) {
                    try { pos.close(); } catch (IOException ignored) { /* best effort */ }
                }
                if (worker != null) {
                    worker.interrupt();
                }
            }

            @Override
            public void onCompleted() {
                if (!envelopeReady) {
                    responseObserver.onError(new IllegalStateException("snapshot stream truncated"));
                    return;
                }
                try {
                    if (pos != null) {
                        // Zero-copy path: signal EOF to the worker and await durability.
                        pos.close();
                        workerDone.get(60, TimeUnit.SECONDS);
                        responseObserver.onNext(Ack.newBuilder().setOk(true).build());
                        responseObserver.onCompleted();
                    } else {
                        // Fallback: reassemble inline and route through the receiver.
                        Eraftpb.Snapshot reassembled = envelope.getSnapshot().toBuilder()
                                .setData(ByteString.copyFrom(fallbackBuf.toByteArray()))
                                .build();
                        Eraftpb.Message full = envelope.toBuilder().setSnapshot(reassembled).build();
                        Transport.MessageReceiver r = receiverSupplier.get();
                        if (r == null) {
                            responseObserver.onNext(Ack.newBuilder().setOk(false).setError("receiver not ready").build());
                        } else {
                            r.receive(full);
                            responseObserver.onNext(Ack.newBuilder().setOk(true).build());
                        }
                        responseObserver.onCompleted();
                    }
                } catch (Throwable t) {
                    LOG.warn("snapshot install from {} failed: {}", expectedFrom, t.toString());
                    if (worker != null) {
                        worker.interrupt();
                    }
                    responseObserver.onError(t);
                }
            }
        };
    }
}
