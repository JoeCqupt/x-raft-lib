/*
 * Copyright 2024-2026 The x-raft-lib Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * SPDX-License-Identifier: Apache-2.0
 */
package io.github.xinfra.lab.raft.tests.simulation;

import io.github.xinfra.lab.raft.proto.Eraftpb;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;

/**
 * Deterministic simulated network for raft simulation testing.
 *
 * <p>All randomness is driven by a seeded {@link Random}, making every
 * execution fully reproducible given the same seed. Messages are queued
 * with a delivery tick and may be dropped, delayed, or duplicated
 * according to configurable probabilities.
 *
 * <p>Supports partition and isolation fault injection: messages between
 * partitioned groups are silently dropped.
 */
final class SimulatedNetwork {

    record PendingMessage(long from, long to, Eraftpb.Message message, int deliverAtTick) {}

    private final Random random;
    private final List<PendingMessage> queue = new ArrayList<>();

    private double dropRate;
    private double duplicateRate;
    private int maxDelay;

    private final Set<Long> isolatedNodes = new HashSet<>();
    private final List<long[]> partitions = new ArrayList<>();

    SimulatedNetwork(Random random) {
        this.random = random;
    }

    void setDropRate(double rate) {
        this.dropRate = rate;
    }

    void setDuplicateRate(double rate) {
        this.duplicateRate = rate;
    }

    void setMaxDelay(int ticks) {
        this.maxDelay = ticks;
    }

    void isolate(long nodeId) {
        isolatedNodes.add(nodeId);
    }

    void partition(Set<Long> groupA, Set<Long> groupB) {
        for (long a : groupA) {
            for (long b : groupB) {
                partitions.add(new long[]{a, b});
            }
        }
    }

    void healAll() {
        isolatedNodes.clear();
        partitions.clear();
    }

    void enqueue(long from, long to, Eraftpb.Message msg, int currentTick) {
        if (from == to) return;
        int delay = maxDelay > 0 ? random.nextInt(maxDelay + 1) : 0;
        queue.add(new PendingMessage(from, to, msg, currentTick + delay));

        if (duplicateRate > 0 && random.nextDouble() < duplicateRate) {
            int extraDelay = maxDelay > 0 ? random.nextInt(maxDelay + 1) : 0;
            queue.add(new PendingMessage(from, to, msg, currentTick + extraDelay));
        }
    }

    List<PendingMessage> deliverDue(int currentTick) {
        List<PendingMessage> due = new ArrayList<>();
        Iterator<PendingMessage> it = queue.iterator();
        while (it.hasNext()) {
            PendingMessage pm = it.next();
            if (pm.deliverAtTick() <= currentTick) {
                it.remove();
                if (!isBlocked(pm.from(), pm.to())) {
                    if (dropRate <= 0 || random.nextDouble() >= dropRate) {
                        due.add(pm);
                    }
                }
            }
        }
        Collections.shuffle(due, random);
        return due;
    }

    void dropMessagesFor(long nodeId) {
        queue.removeIf(pm -> pm.to() == nodeId || pm.from() == nodeId);
    }

    int pendingCount() {
        return queue.size();
    }

    private boolean isBlocked(long from, long to) {
        if (isolatedNodes.contains(from) || isolatedNodes.contains(to)) {
            return true;
        }
        for (long[] pair : partitions) {
            if ((pair[0] == from && pair[1] == to) || (pair[0] == to && pair[1] == from)) {
                return true;
            }
        }
        return false;
    }
}
