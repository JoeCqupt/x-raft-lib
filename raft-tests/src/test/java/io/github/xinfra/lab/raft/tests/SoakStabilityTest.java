/*
 * Copyright 2024-2026 The x-raft-lib Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * SPDX-License-Identifier: Apache-2.0
 */
package io.github.xinfra.lab.raft.tests;

import io.github.xinfra.lab.raft.examples.RaftPeer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static io.github.xinfra.lab.raft.tests.IntegrationTestSupport.awaitLeader;
import static io.github.xinfra.lab.raft.tests.IntegrationTestSupport.awaitTrue;
import static io.github.xinfra.lab.raft.tests.IntegrationTestSupport.findLeader;
import static io.github.xinfra.lab.raft.tests.IntegrationTestSupport.freePorts;
import static io.github.xinfra.lab.raft.tests.IntegrationTestSupport.peerMap;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Long-running soak test: hammer a 3-node cluster with proposals for a while,
 * periodically snapshotting and compacting, and check for the two classic
 * stability bugs — a thread/resource leak and an apply backlog that never
 * drains. Tagged {@code soak} so it is excluded from the default build; run it
 * explicitly with {@code mvn -P soak test} (optionally
 * {@code -Dsoak.durationSeconds=120}).
 */
@Tag("soak")
class SoakStabilityTest {

    @TempDir Path tmp;

    @Test
    void sustainedProposalsStayHealthy() throws Exception {
        long durationSeconds = Long.getLong("soak.durationSeconds", 60L);
        int[] ports = freePorts(3);
        Map<Long, String> peers = peerMap(ports);

        List<RaftPeer> nodes = new ArrayList<>();
        try {
            for (long id = 1; id <= 3; id++) {
                long fid = id;
                nodes.add(new RaftPeer(fid, ports[(int) (fid - 1)],
                        tmp.resolve("p" + fid), peers, true, (idx, data) -> { }));
            }

            assertThat(awaitLeader(nodes, 10_000)).as("must elect a leader").isPositive();

            ThreadMXBean threads = ManagementFactory.getThreadMXBean();
            // Let things warm up, then sample the baseline thread count.
            Thread.sleep(1_000);
            int baselineThreads = threads.getThreadCount();

            long deadline = System.currentTimeMillis() + durationSeconds * 1_000;
            long proposals = 0;
            long lastProgressCheck = System.currentTimeMillis();
            long lastSeenCommit = 0;
            int round = 0;

            while (System.currentTimeMillis() < deadline) {
                RaftPeer leader = findLeader(nodes);
                if (leader == null) {
                    // Re-elect window; back off briefly.
                    Thread.sleep(50);
                    continue;
                }
                for (int i = 0; i < 200; i++) {
                    leader.propose(("soak-" + (proposals++)).getBytes());
                }

                // Periodically snapshot + compact the leader so the log can't
                // grow without bound — exercises the snapshot path under load.
                if (++round % 10 == 0) {
                    try {
                        leader.forceSnapshotAndCompact(("snap@" + leader.basicStatus().applied).getBytes());
                    } catch (Exception ignored) {
                        // Leadership may have moved; ignore and continue.
                    }
                }

                // Every ~3s, assert the cluster is still making forward progress
                // (no deadlock / wedged loop).
                long now = System.currentTimeMillis();
                if (now - lastProgressCheck > 3_000) {
                    long commit = maxCommit(nodes);
                    assertThat(commit).as("commit must advance over time")
                            .isGreaterThan(lastSeenCommit);
                    lastSeenCommit = commit;
                    lastProgressCheck = now;
                }
                Thread.sleep(5);
            }

            // Final liveness: all nodes drain toward the leader's commit (no
            // permanent apply backlog).
            long finalCommit = maxCommit(nodes);
            assertThat(awaitTrue(() -> nodes.stream()
                    .allMatch(p -> p.basicStatus().commit >= finalCommit), 20_000))
                    .as("all nodes must catch up to commit %d (no backlog)", finalCommit)
                    .isTrue();
            // Applied should track committed once the firehose stops.
            assertThat(awaitTrue(() -> nodes.stream()
                    .allMatch(p -> p.basicStatus().applied >= p.basicStatus().commit), 20_000))
                    .as("applied must catch up to committed (no apply backlog)").isTrue();

            // No thread leak: the count should be stable (allow generous slack
            // for transient gRPC/event-loop threads).
            int endThreads = ManagementFactory.getThreadMXBean().getThreadCount();
            assertThat(endThreads)
                    .as("thread count must not grow unboundedly (baseline %d)", baselineThreads)
                    .isLessThanOrEqualTo(baselineThreads + 20);
        } finally {
            for (int i = nodes.size() - 1; i >= 0; i--) {
                try { nodes.get(i).close(); } catch (Throwable ignored) {}
            }
        }
    }

    private static long maxCommit(List<RaftPeer> nodes) {
        long max = 0;
        for (RaftPeer p : nodes) max = Math.max(max, p.basicStatus().commit);
        return max;
    }
}
