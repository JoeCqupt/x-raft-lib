/*
 * Copyright 2024-2026 The x-raft-lib Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * SPDX-License-Identifier: Apache-2.0
 */
package io.github.xinfra.lab.raft.tests;

import io.github.xinfra.lab.raft.RaftException;
import io.github.xinfra.lab.raft.examples.RaftKVNode;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.management.ManagementFactory;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.github.xinfra.lab.raft.tests.IntegrationTestSupport.awaitLeader;
import static io.github.xinfra.lab.raft.tests.IntegrationTestSupport.awaitTrue;
import static io.github.xinfra.lab.raft.tests.IntegrationTestSupport.findLeader;
import static io.github.xinfra.lab.raft.tests.IntegrationTestSupport.freePorts;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Soak test: repeatedly crash the leader, wait for re-election, restart
 * the crashed node, and verify that no committed entries are lost. Catches
 * resource leaks and state corruption that only manifest over many
 * leader-election cycles.
 *
 * <p>Tagged {@code soak} — runs in the weekly chaos-soak workflow, not
 * per-PR CI. Cycle count is configurable via {@code -Dsoak.electionCycles}.
 */
@Tag("soak")
class ElectionCycleSoakIntegrationTest {

    @TempDir Path tmp;

    @Test
    void repeatedLeaderCrashNeverLosesCommittedEntries() throws Exception {
        int cycles = Integer.getInteger("soak.electionCycles", 20);
        int clusterSize = 5;

        Path[] storageDirs = new Path[clusterSize];
        for (int i = 0; i < clusterSize; i++) {
            storageDirs[i] = tmp.resolve("p" + (i + 1));
        }

        // Track the current port for each node (changes on restart).
        int[] currentPorts = freePorts(clusterSize);
        List<RaftKVNode> nodes = new ArrayList<>();
        try {
            // Bootstrap the initial cluster.
            Map<Long, String> peers = buildPeerMap(currentPorts);
            for (long id = 1; id <= clusterSize; id++) {
                long fid = id;
                nodes.add(new RaftKVNode(fid, currentPorts[(int) (fid - 1)],
                        storageDirs[(int) (fid - 1)], peers, true,
                        (idx, data) -> { }));
            }

            assertThat(awaitLeader(nodes, 15_000)).as("initial leader").isPositive();

            Thread.sleep(1_000);
            int baselineThreads = ManagementFactory.getThreadMXBean().getThreadCount();

            long maxCommitSeen = 0;

            for (int cycle = 0; cycle < cycles; cycle++) {
                // Find the current leader.
                RaftKVNode leader = findLeader(nodes);
                if (leader == null) {
                    assertThat(awaitLeader(nodes, 15_000)).as("leader for cycle %d", cycle).isPositive();
                    leader = findLeader(nodes);
                }
                assertThat(leader).as("leader must exist in cycle %d", cycle).isNotNull();

                // Propose a batch and wait for quorum commit.
                int batch = 5;
                long preCommit = leader.basicStatus().commit;
                for (int i = 0; i < batch; i++) {
                    try {
                        leader.propose(("c" + cycle + "-" + i).getBytes());
                    } catch (RaftException ignored) {
                        break;
                    }
                }
                RaftKVNode fLeader = leader;
                awaitTrue(() -> fLeader.basicStatus().commit > preCommit, 10_000);

                long commitAfterBatch = maxCommit(nodes);
                assertThat(commitAfterBatch)
                        .as("commit must not regress in cycle %d", cycle)
                        .isGreaterThanOrEqualTo(maxCommitSeen);
                maxCommitSeen = commitAfterBatch;

                // Crash the leader.
                long crashedId = leader.id;
                leader.close();
                nodes.removeIf(p -> p.id == crashedId);

                // Wait for new election among survivors.
                assertThat(awaitTrue(() -> {
                    RaftKVNode l = findLeader(nodes);
                    return l != null && l.id != crashedId;
                }, 20_000)).as("new leader after crashing %d in cycle %d", crashedId, cycle).isTrue();

                // Verify survivors' commit didn't regress.
                for (RaftKVNode p : nodes) {
                    assertThat(p.basicStatus().commit)
                            .as("node %d commit must not regress in cycle %d", p.id, cycle)
                            .isGreaterThanOrEqualTo(maxCommitSeen - 1);
                }

                // Restart the crashed node with a fresh port.
                int freshPort = freePorts(1)[0];
                currentPorts[(int) (crashedId - 1)] = freshPort;
                Map<Long, String> updatedPeers = buildPeerMap(currentPorts);

                // Update surviving nodes' transport to know the new address.
                for (RaftKVNode p : nodes) {
                    p.transport.addPeer(crashedId, updatedPeers.get(crashedId));
                }

                RaftKVNode restarted = new RaftKVNode(crashedId,
                        storageDirs[(int) (crashedId - 1)],
                        updatedPeers, false,
                        (idx, data) -> { },
                        new io.github.xinfra.lab.raft.transport.grpc.GrpcTransport(crashedId, freshPort));
                nodes.add(restarted);

                // Brief pause to let the restarted node catch up before next cycle.
                Thread.sleep(500);
            }

            // Final convergence: all nodes should agree on commit.
            long finalMax = maxCommit(nodes);
            assertThat(awaitTrue(
                    () -> nodes.stream().allMatch(p -> p.basicStatus().commit >= finalMax),
                    30_000)).as("all nodes converge after %d cycles", cycles).isTrue();
            assertThat(awaitTrue(
                    () -> nodes.stream().allMatch(p -> p.basicStatus().applied >= p.basicStatus().commit),
                    30_000)).as("applied catches up to committed").isTrue();

            // Thread leak check.
            int endThreads = ManagementFactory.getThreadMXBean().getThreadCount();
            assertThat(endThreads)
                    .as("thread count must not grow unboundedly (baseline %d)", baselineThreads)
                    .isLessThanOrEqualTo(baselineThreads + 30);
        } finally {
            closeAll(nodes);
        }
    }

    private static Map<Long, String> buildPeerMap(int[] ports) {
        Map<Long, String> m = new HashMap<>();
        for (int i = 0; i < ports.length; i++) {
            m.put((long) (i + 1), "localhost:" + ports[i]);
        }
        return m;
    }

    private static long maxCommit(List<RaftKVNode> nodes) {
        long max = 0;
        for (RaftKVNode p : nodes) max = Math.max(max, p.basicStatus().commit);
        return max;
    }

    private static void closeAll(List<RaftKVNode> nodes) {
        for (int i = nodes.size() - 1; i >= 0; i--) {
            try { nodes.get(i).close(); } catch (Throwable ignored) {}
        }
    }
}
