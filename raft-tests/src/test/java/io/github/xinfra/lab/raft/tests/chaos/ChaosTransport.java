/*
 * Copyright 2024-2026 The x-raft-lib Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * SPDX-License-Identifier: Apache-2.0
 */
package io.github.xinfra.lab.raft.tests.chaos;

import io.github.xinfra.lab.raft.Transport;
import io.github.xinfra.lab.raft.proto.Eraftpb;

import java.io.IOException;
import java.io.InputStream;

/**
 * A fault-injecting {@link Transport} decorator. It wraps a real transport
 * (e.g. {@code GrpcTransport}) and consults a shared {@link ChaosController}
 * to drop messages on the way out and on the way in, simulating partitions,
 * isolated nodes, and lossy links — without changing raft-core or the real
 * transport at all.
 *
 * <p>Every node in a chaos test wraps its own transport with one of these,
 * all sharing a single {@link ChaosController}. Dropping is applied on both
 * the outbound {@link #send} path (link {@code localId -> peer}) and the
 * inbound receive path (link {@code msg.from -> localId}); doing both makes a
 * partition symmetric even if one side's send slips through a race.
 *
 * <p>This decorator deliberately models only loss/partition, the faults raft
 * is designed to tolerate. It never corrupts or reorders payloads.
 */
public final class ChaosTransport implements Transport {

    private final long localId;
    private final Transport delegate;
    private final ChaosController controller;

    public ChaosTransport(long localId, Transport delegate, ChaosController controller) {
        this.localId = localId;
        this.delegate = delegate;
        this.controller = controller;
    }

    @Override
    public void setReceiver(MessageReceiver receiver) {
        // Wrap the receiver so inbound messages from a partitioned-away peer
        // are dropped too — a partition must be symmetric.
        delegate.setReceiver(msg -> {
            if (controller.shouldDrop(msg.getFrom(), localId)) {
                return;
            }
            receiver.receive(msg);
        });
    }

    @Override
    public void addPeer(long peerId, String address) {
        delegate.addPeer(peerId, address);
    }

    @Override
    public void removePeer(long peerId) {
        delegate.removePeer(peerId);
    }

    @Override
    public void send(long peerId, Eraftpb.Message msg) {
        if (controller.shouldDrop(localId, peerId)) {
            return;
        }
        delegate.send(peerId, msg);
    }

    @Override
    public boolean supportsSnapshotStreaming() {
        return delegate.supportsSnapshotStreaming();
    }

    @Override
    public void sendSnapshot(long peerId, Eraftpb.Message metaMsg, InputStream payload, SnapshotSendCallback cb) {
        // Honour the partition on the out-of-band channel too — otherwise the
        // default materializing fallback (or the real transport) would deliver a
        // snapshot across a link the chaos controller has cut.
        if (controller.shouldDrop(localId, peerId)) {
            try { payload.close(); } catch (IOException ignored) { /* best effort */ }
            cb.onComplete(false, new IOException("chaos: dropped snapshot " + localId + "->" + peerId));
            return;
        }
        delegate.sendSnapshot(peerId, metaMsg, payload, cb);
    }

    @Override
    public void setSnapshotSink(SnapshotSink sink) {
        // Mirror the inbound receive-path drop so a partition stays symmetric on
        // the snapshot channel: an inbound snapshot from a partitioned-away peer
        // is dropped (NACK) before it reaches the real sink.
        delegate.setSnapshotSink((metaMsg, payload) -> {
            if (controller.shouldDrop(metaMsg.getFrom(), localId)) {
                try { payload.close(); } catch (IOException ignored) { /* best effort */ }
                throw new IOException("chaos: dropped inbound snapshot " + metaMsg.getFrom() + "->" + localId);
            }
            sink.install(metaMsg, payload);
        });
    }

    @Override
    public void start() {
        delegate.start();
    }

    @Override
    public void close() {
        delegate.close();
    }
}
