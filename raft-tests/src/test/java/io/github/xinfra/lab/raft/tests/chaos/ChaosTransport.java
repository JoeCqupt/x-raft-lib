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
import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A fault-injecting {@link Transport} decorator. It wraps a real transport
 * (e.g. {@code GrpcTransport}) and consults a shared {@link ChaosController}
 * to drop, delay, duplicate, or asymmetrically partition messages on the way
 * out and on the way in — without changing raft-core or the real transport
 * at all.
 *
 * <p>Every node in a chaos test wraps its own transport with one of these,
 * all sharing a single {@link ChaosController}. Dropping is applied on both
 * the outbound {@link #send} path (link {@code localId -> peer}) and the
 * inbound receive path (link {@code msg.from -> localId}); doing both makes a
 * partition symmetric even if one side's send slips through a race.
 *
 * <h2>Fault model</h2>
 * <ul>
 *   <li><b>Drop</b> — message vanishes. Raft tolerates loss; the cluster
 *       must still converge.</li>
 *   <li><b>Asymmetric partition</b> — drop on one direction of a link
 *       only ({@link ChaosController#blockLink}). Stress-tests the half-open
 *       failure modes (e.g. leader can heartbeat but no responses come back).</li>
 *   <li><b>Latency</b> — delay delivery by a random amount in
 *       {@code [0, bound]}; randomisation per message gives reorder for
 *       free on the same link.</li>
 *   <li><b>Duplicate</b> — deliver the same message twice. Exercises
 *       raft's idempotence (index/term-based deduplication).</li>
 * </ul>
 *
 * <p>This decorator deliberately models only loss/latency/duplicate,
 * the faults raft is designed to tolerate. It never corrupts payloads.
 */
public final class ChaosTransport implements Transport {

    private final long localId;
    private final Transport delegate;
    private final ChaosController controller;

    /**
     * Lazily-created daemon scheduler used only when latency or duplicate
     * is non-zero. Tests that only use drop / partition pay zero extra
     * threads.
     */
    private final AtomicReference<ScheduledExecutorService> scheduler = new AtomicReference<>();

    /**
     * Captured inbound receiver so we can also apply latency / duplicate
     * on the receive path, not just the send path. Without this, an
     * inbound message from a node configured with "fast send" but slow
     * link would race past raft's stableTo bookkeeping.
     */
    private volatile MessageReceiver wrappedReceiver;

    public ChaosTransport(long localId, Transport delegate, ChaosController controller) {
        this.localId = localId;
        this.delegate = delegate;
        this.controller = controller;
    }

    @Override
    public void setReceiver(MessageReceiver receiver) {
        // Wrap the receiver so inbound messages from a partitioned-away peer
        // are dropped too — a partition must be symmetric.
        this.wrappedReceiver = receiver;
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
        Duration delay = controller.sampleLatency(localId, peerId);
        boolean duplicate = controller.shouldDuplicate();

        if (delay.isZero()) {
            delegate.send(peerId, msg);
        } else {
            scheduleDelivery(peerId, msg, delay);
        }

        // Duplicate is always scheduled a few ms behind the first delivery
        // so raft sees it as a late retransmit, not a coincidence.
        if (duplicate) {
            // Add a tiny extra delay to the duplicate so it lands AFTER the
            // first delivery in wall-clock terms even when delay was 0.
            Duration dupDelay = delay.plusMillis(2);
            scheduleDelivery(peerId, msg, dupDelay);
        }
    }

    private void scheduleDelivery(long peerId, Eraftpb.Message msg, Duration delay) {
        ScheduledExecutorService es = scheduler.get();
        if (es == null) {
            es = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "chaos-schedule-" + localId);
                t.setDaemon(true);
                return t;
            });
            if (!scheduler.compareAndSet(null, es)) {
                es.shutdown();
                es = scheduler.get();
            }
        }
        es.schedule(() -> {
            try {
                delegate.send(peerId, msg);
            } catch (Throwable ignored) {
                // Scheduled delivery may race the delegate's close(); swallow.
            }
        }, delay.toNanos(), TimeUnit.NANOSECONDS);
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
        // Snapshots are not subject to latency / duplicate injection: the
        // stream is stateful (one InputStream → one consumer), and
        // duplicating it would re-deliver an already-closed stream.
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
        ScheduledExecutorService es = scheduler.getAndSet(null);
        if (es != null) {
            // Best-effort: cancel pending delayed deliveries on close so a
            // stop() doesn't race against a delegate already shut down.
            es.shutdownNow();
        }
        delegate.close();
    }
}
