/*
 * Copyright 2024-2026 The x-raft-lib Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * SPDX-License-Identifier: Apache-2.0
 */
package io.github.xinfra.lab.raft.tests.chaos;
import io.github.xinfra.lab.raft.internal.*;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
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

    /** Count of messages actually dropped, for assertions / debugging. */
    private final AtomicLong dropped = new AtomicLong();

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

    /** Remove all partition link-blocks and isolation; drop rate is untouched. */
    public void healAll() {
        blockedLinks.clear();
        isolated.clear();
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
}
