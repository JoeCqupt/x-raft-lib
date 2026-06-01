/*
 * Copyright 2024-2026 The x-raft-lib Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * SPDX-License-Identifier: Apache-2.0
 */
package io.github.xinfra.lab.raft.tests.chaos;
import io.github.xinfra.lab.raft.internal.*;

import java.time.Duration;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Shared fault-injection state for a cluster of {@link ChaosTransport}s. One
 * controller is created per test and handed to every node's transport, so a
 * single call partitions or heals a link as seen by both endpoints.
 *
 * <p>Faults are evaluated per directed link {@code (from -> to)}:
 * <ul>
 *   <li><b>Isolation</b> — an isolated node drops every message it would send
 *       and every message addressed to it, in both directions. Models a node
 *       that fell off the network entirely.</li>
 *   <li><b>Partition</b> — two groups that cannot exchange messages, but each
 *       group's internal links stay healthy. Models a network split.</li>
 *   <li><b>Drop probability</b> — a global per-message loss rate, for
 *       lossy-link / chaos soak testing. Raft tolerates loss, so the cluster
 *       must still converge.</li>
 * </ul>
 *
 * <p>All mutators are safe to call from a test thread while the cluster runs;
 * reads on the hot path ({@link #shouldDrop}) are lock-free.
 */
public final class ChaosController {

    /** Fully isolated node ids (drop in both directions). */
    private final Set<Long> isolated = ConcurrentHashMap.newKeySet();

    /**
     * Blocked directed links, keyed by {@code from*SHIFT + to}. A partition is
     * expressed by blocking every cross-group link in both directions.
     */
    private final Set<Long> blockedLinks = ConcurrentHashMap.newKeySet();

    /** Global per-message drop probability in [0,1]; 0 disables. */
    private volatile double dropProbability = 0.0;

    /**
     * Per-link upper bound on injected latency. Effective delay is drawn
     * uniformly from [0, bound]. A null/zero bound means deliver
     * immediately. Used to model jitter and (via randomisation) reorder
     * on the same link.
     */
    private final ConcurrentMap<Long, Duration> latencyByLink = new ConcurrentHashMap<>();
    private volatile Duration globalLatencyBound = Duration.ZERO;

    /**
     * Per-message duplicate probability in [0,1]. When fired, the
     * transport schedules a second delivery a few ms after the first,
     * exercising raft's at-least-once tolerance.
     */
    private volatile double duplicateProbability = 0.0;

    /** Count of messages actually dropped, for assertions / debugging. */
    private final AtomicLong dropped = new AtomicLong();
    /** Count of messages duplicated, for assertions / debugging. */
    private final AtomicLong duplicated = new AtomicLong();
    /** Count of messages delayed (latency > 0), for assertions / debugging. */
    private final AtomicLong delayed = new AtomicLong();

    private static final long SHIFT = 1_000_000L;

    private static long linkKey(long from, long to) {
        return from * SHIFT + to;
    }

    /** Fully isolate {@code id}: it can neither send nor receive. */
    public void isolate(long id) {
        isolated.add(id);
    }

    /** Reconnect a previously {@link #isolate(long) isolated} node. */
    public void heal(long id) {
        isolated.remove(id);
    }

    /**
     * Split the cluster into two groups that cannot talk to each other.
     * Intra-group links are left healthy. Calling again replaces any prior
     * link blocks set up by this method (but not isolation).
     */
    public void partition(Collection<Long> groupA, Collection<Long> groupB) {
        for (long a : groupA) {
            for (long b : groupB) {
                blockedLinks.add(linkKey(a, b));
                blockedLinks.add(linkKey(b, a));
            }
        }
    }

    /**
     * Block a single directed link {@code from -> to} without affecting
     * the reverse direction. Models asymmetric / "partial-partition"
     * faults where one side observes the peer as unreachable but the
     * other side still receives — a class of fault raft must still
     * make progress under (leader can heartbeat but follower
     * responses are lost, etc.).
     */
    public void blockLink(long from, long to) {
        blockedLinks.add(linkKey(from, to));
    }

    /** Unblock a single directed link previously set by {@link #blockLink}. */
    public void unblockLink(long from, long to) {
        blockedLinks.remove(linkKey(from, to));
    }

    /**
     * Inject up to {@code maxDelay} of latency on the directed link
     * {@code from -> to}. The effective delay per message is drawn
     * uniformly from {@code [Duration.ZERO, maxDelay]}, so messages on
     * the same link may also reorder on the receiver. Pass {@code null}
     * or {@link Duration#ZERO} to clear.
     */
    public void setLinkLatency(long from, long to, Duration maxDelay) {
        if (maxDelay == null || maxDelay.isZero() || maxDelay.isNegative()) {
            latencyByLink.remove(linkKey(from, to));
        } else {
            latencyByLink.put(linkKey(from, to), maxDelay);
        }
    }

    /**
     * Global jitter bound applied to every link with no per-link
     * override. Same semantics as {@link #setLinkLatency} — effective
     * delay is uniform in {@code [0, maxDelay]}.
     */
    public void setGlobalLatency(Duration maxDelay) {
        this.globalLatencyBound = (maxDelay == null || maxDelay.isNegative()) ? Duration.ZERO : maxDelay;
    }

    /**
     * Probability that an otherwise-delivered message is also delivered
     * a second time a few ms later. Exercises raft's deduplication
     * (message-id / index-based idempotence). 0 disables.
     */
    public void setDuplicateProbability(double p) {
        if (p < 0 || p > 1) throw new IllegalArgumentException("p must be in [0,1]: " + p);
        this.duplicateProbability = p;
    }

    /** Remove all partition link-blocks and isolation; drop rate is untouched. */
    public void healAll() {
        blockedLinks.clear();
        isolated.clear();
        latencyByLink.clear();
        globalLatencyBound = Duration.ZERO;
    }

    /** Set the global per-message drop probability (0 disables). */
    public void setDropProbability(double p) {
        if (p < 0 || p > 1) throw new IllegalArgumentException("p must be in [0,1]: " + p);
        this.dropProbability = p;
    }

    /** Number of messages dropped so far across all transports. */
    public long droppedCount() {
        return dropped.get();
    }

    /** Number of messages duplicated so far across all transports. */
    public long duplicatedCount() {
        return duplicated.get();
    }

    /** Number of messages delayed (latency > 0) so far across all transports. */
    public long delayedCount() {
        return delayed.get();
    }

    /**
     * Whether a message on link {@code from -> to} should be dropped right now.
     * Consulted by {@link ChaosTransport} on both send and receive.
     */
    boolean shouldDrop(long from, long to) {
        if (isolated.contains(from) || isolated.contains(to)) {
            dropped.incrementAndGet();
            return true;
        }
        if (blockedLinks.contains(linkKey(from, to))) {
            dropped.incrementAndGet();
            return true;
        }
        double p = dropProbability;
        if (p > 0 && ThreadLocalRandom.current().nextDouble() < p) {
            dropped.incrementAndGet();
            return true;
        }
        return false;
    }

    /**
     * Effective per-message latency for link {@code from -> to}. Returns
     * {@link Duration#ZERO} (deliver immediately) when no latency is
     * configured; otherwise draws uniformly from [0, configured bound].
     */
    Duration sampleLatency(long from, long to) {
        Duration bound = latencyByLink.getOrDefault(linkKey(from, to), globalLatencyBound);
        if (bound.isZero()) return Duration.ZERO;
        long maxNanos = bound.toNanos();
        long nanos = (long) (ThreadLocalRandom.current().nextDouble() * maxNanos);
        if (nanos > 0) delayed.incrementAndGet();
        return Duration.ofNanos(nanos);
    }

    /** Whether this send should also be delivered a second time. */
    boolean shouldDuplicate() {
        double p = duplicateProbability;
        if (p > 0 && ThreadLocalRandom.current().nextDouble() < p) {
            duplicated.incrementAndGet();
            return true;
        }
        return false;
    }
}
