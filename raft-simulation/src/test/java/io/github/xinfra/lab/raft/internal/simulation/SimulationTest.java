/*
 * Copyright 2024-2026 The x-raft-lib Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * SPDX-License-Identifier: Apache-2.0
 */
package io.github.xinfra.lab.raft.internal.simulation;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.LongStream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Deterministic simulation tests for Raft invariants.
 *
 * <p>Each scenario runs with multiple random seeds. The default seed count is 50;
 * override with {@code -Dsimulation.seeds=N} for deeper exploration in CI.
 *
 * <p>Failures include the seed and tick for exact reproducibility.
 */
class SimulationTest {

    private static final Logger LOG = LoggerFactory.getLogger(SimulationTest.class);
    private static final int DEFAULT_SEED_COUNT = 50;

    static LongStream seeds() {
        int count = Integer.getInteger("simulation.seeds", DEFAULT_SEED_COUNT);
        return LongStream.range(1, count + 1);
    }

    @ParameterizedTest
    @MethodSource("seeds")
    void normalOperation(long seed) {
        SimulationCluster cluster = new SimulationCluster(seed, 3);

        // Run long enough for election to happen
        cluster.run(50);
        assertThat(cluster.hasLeader()).as("seed=%d: no leader elected after 50 ticks", seed).isTrue();

        // Continuously propose and tick
        for (int round = 0; round < 200; round++) {
            cluster.propose(("val-" + round).getBytes(StandardCharsets.UTF_8));
            cluster.stepOneTick();
        }

        // Run extra ticks for pending proposals to commit
        cluster.run(100);

        assertThat(cluster.hasLeader()).as("seed=%d: leader lost after normal operation", seed).isTrue();
        assertThat(cluster.totalApplied())
                .as("seed=%d: some proposals should have been applied", seed)
                .isGreaterThan(0);

        LOG.info(
                "seed={}: normalOperation completed — proposed={}, applied={}",
                seed,
                cluster.totalProposed(),
                cluster.totalApplied());
    }

    @ParameterizedTest
    @MethodSource("seeds")
    void normalOperationFiveNodes(long seed) {
        SimulationCluster cluster = new SimulationCluster(seed, 5);

        cluster.run(50);
        assertThat(cluster.hasLeader()).as("seed=%d: no leader elected", seed).isTrue();

        for (int round = 0; round < 200; round++) {
            cluster.propose(("v5-" + round).getBytes(StandardCharsets.UTF_8));
            cluster.stepOneTick();
        }

        cluster.run(100);

        assertThat(cluster.totalApplied())
                .as("seed=%d: proposals should have been applied", seed)
                .isGreaterThan(0);

        LOG.info(
                "seed={}: normalOperationFiveNodes completed — proposed={}, applied={}",
                seed,
                cluster.totalProposed(),
                cluster.totalApplied());
    }

    @ParameterizedTest
    @MethodSource("seeds")
    void randomPartitions(long seed) {
        SimulationCluster cluster = new SimulationCluster(seed, 5);
        cluster.network().setMaxDelay(2);

        cluster.run(50);

        for (int phase = 0; phase < 8; phase++) {
            // Propose some data
            for (int i = 0; i < 20; i++) {
                cluster.propose(("part-" + phase + "-" + i).getBytes(StandardCharsets.UTF_8));
                cluster.stepOneTick();
            }

            // Create a random partition
            List<Long> ids = cluster.allNodeIds();
            int splitPoint = 1 + cluster.random().nextInt(ids.size() - 1);
            Set<Long> groupA = new HashSet<>(ids.subList(0, splitPoint));
            Set<Long> groupB = new HashSet<>(ids.subList(splitPoint, ids.size()));
            cluster.network().partition(groupA, groupB);

            // Run under partition
            for (int t = 0; t < 80; t++) {
                if (t % 4 == 0) {
                    cluster.propose(("partitioned-" + phase + "-" + t).getBytes(StandardCharsets.UTF_8));
                }
                cluster.stepOneTick();
            }

            // Heal and allow recovery
            cluster.network().healAll();
            cluster.run(100);
        }

        // Final stabilization
        cluster.run(200);

        assertThat(cluster.totalApplied())
                .as("seed=%d: some proposals should survive partitions", seed)
                .isGreaterThan(0);

        LOG.info(
                "seed={}: randomPartitions completed — proposed={}, applied={}",
                seed,
                cluster.totalProposed(),
                cluster.totalApplied());
    }

    @ParameterizedTest
    @MethodSource("seeds")
    void leaderCrashAndRestart(long seed) {
        SimulationCluster cluster = new SimulationCluster(seed, 5);

        cluster.run(50);
        assertThat(cluster.hasLeader()).as("seed=%d: initial leader not elected", seed).isTrue();

        for (int cycle = 0; cycle < 6; cycle++) {
            // Propose some data
            for (int i = 0; i < 30; i++) {
                cluster.propose(("crash-" + cycle + "-" + i).getBytes(StandardCharsets.UTF_8));
                cluster.stepOneTick();
            }

            // Crash the leader
            long leaderId = cluster.findLeaderId();
            if (leaderId > 0) {
                cluster.crashNode(leaderId);
            }

            // Run without the crashed node — new leader should emerge
            cluster.run(80);

            // Restart the crashed node
            if (leaderId > 0) {
                cluster.restartNode(leaderId);
            }

            // Run for recovery
            cluster.run(80);
        }

        // Final stabilization
        cluster.run(200);

        assertThat(cluster.totalApplied())
                .as("seed=%d: proposals should survive leader crashes", seed)
                .isGreaterThan(0);

        LOG.info(
                "seed={}: leaderCrashAndRestart completed — proposed={}, applied={}",
                seed,
                cluster.totalProposed(),
                cluster.totalApplied());
    }

    @ParameterizedTest
    @MethodSource("seeds")
    void messageLossAndReorder(long seed) {
        SimulationCluster cluster = new SimulationCluster(seed, 3);
        cluster.network().setDropRate(0.3);
        cluster.network().setDuplicateRate(0.1);
        cluster.network().setMaxDelay(5);

        // Allow extra time for election under lossy network
        cluster.run(100);

        for (int round = 0; round < 300; round++) {
            if (round % 2 == 0) {
                cluster.propose(("lossy-" + round).getBytes(StandardCharsets.UTF_8));
            }
            cluster.stepOneTick();
        }

        // Stabilize with clean network
        cluster.network().setDropRate(0);
        cluster.network().setDuplicateRate(0);
        cluster.network().setMaxDelay(0);
        cluster.run(200);

        assertThat(cluster.totalApplied())
                .as("seed=%d: some proposals should survive message loss", seed)
                .isGreaterThan(0);

        LOG.info(
                "seed={}: messageLossAndReorder completed — proposed={}, applied={}",
                seed,
                cluster.totalProposed(),
                cluster.totalApplied());
    }

    @ParameterizedTest
    @MethodSource("seeds")
    void combinedChaos(long seed) {
        SimulationCluster cluster = new SimulationCluster(seed, 5);
        cluster.network().setDropRate(0.1);
        cluster.network().setMaxDelay(3);

        cluster.run(60);

        for (int phase = 0; phase < 10; phase++) {
            int action = cluster.random().nextInt(5);

            switch (action) {
                case 0 -> {
                    // Random partition
                    List<Long> ids = cluster.allNodeIds();
                    int split = 1 + cluster.random().nextInt(ids.size() - 1);
                    Set<Long> groupA = new HashSet<>(ids.subList(0, split));
                    Set<Long> groupB = new HashSet<>(ids.subList(split, ids.size()));
                    cluster.network().partition(groupA, groupB);
                }
                case 1 -> {
                    // Crash a random live node (keep majority alive)
                    if (cluster.liveNodes().size() > 3) {
                        List<Long> live = List.copyOf(cluster.liveNodes().keySet());
                        long victim = live.get(cluster.random().nextInt(live.size()));
                        cluster.crashNode(victim);
                    }
                }
                case 2 -> {
                    // Restart a crashed node
                    for (long id : cluster.allNodeIds()) {
                        if (!cluster.liveNodes().containsKey(id)) {
                            cluster.restartNode(id);
                            break;
                        }
                    }
                }
                case 3 -> {
                    // Heal all network faults
                    cluster.network().healAll();
                }
                case 4 -> {
                    // Isolate a random node
                    List<Long> live = List.copyOf(cluster.liveNodes().keySet());
                    if (live.size() > 3) {
                        long victim = live.get(cluster.random().nextInt(live.size()));
                        cluster.network().isolate(victim);
                    }
                }
            }

            // Propose and tick
            for (int t = 0; t < 60; t++) {
                if (t % 3 == 0) {
                    cluster.propose(("chaos-" + phase + "-" + t).getBytes(StandardCharsets.UTF_8));
                }
                cluster.stepOneTick();
            }
        }

        // Heal everything and restart all crashed nodes
        cluster.network().healAll();
        cluster.network().setDropRate(0);
        cluster.network().setMaxDelay(0);
        for (long id : cluster.allNodeIds()) {
            if (!cluster.liveNodes().containsKey(id)) {
                cluster.restartNode(id);
            }
        }

        // Final stabilization
        cluster.run(500);

        assertThat(cluster.totalApplied())
                .as("seed=%d: some proposals should survive chaos", seed)
                .isGreaterThan(0);

        LOG.info(
                "seed={}: combinedChaos completed — proposed={}, applied={}",
                seed,
                cluster.totalProposed(),
                cluster.totalApplied());
    }
}
