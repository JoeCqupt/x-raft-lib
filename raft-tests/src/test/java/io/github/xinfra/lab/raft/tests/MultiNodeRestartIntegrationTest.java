/*
 * Copyright 2024-2026 The x-raft-lib Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * SPDX-License-Identifier: Apache-2.0
 */
package io.github.xinfra.lab.raft.tests;

import io.github.xinfra.lab.raft.RaftStateType;
import io.github.xinfra.lab.raft.examples.RaftPeer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import static io.github.xinfra.lab.raft.tests.IntegrationTestSupport.awaitLeader;
import static io.github.xinfra.lab.raft.tests.IntegrationTestSupport.awaitTrue;
import static io.github.xinfra.lab.raft.tests.IntegrationTestSupport.findLeader;
import static io.github.xinfra.lab.raft.tests.IntegrationTestSupport.freePorts;
import static io.github.xinfra.lab.raft.tests.IntegrationTestSupport.peerMap;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that a multi-node cluster survives simultaneous or majority
 * restart and recovers committed state from RocksDB without re-applying
 * old entries.
 */
class MultiNodeRestartIntegrationTest {

    @TempDir Path tmp;

    @Test
    void allNodesRestartAndRecoverCommittedState() throws Exception {
        Path[] storageDirs = {tmp.resolve("p1"), tmp.resolve("p2"), tmp.resolve("p3")};

        // ---- Phase 1: bootstrap, propose, observe apply ----
        int[] ports1 = freePorts(3);
        Map<Long, String> peers1 = peerMap(ports1);
        Map<Long, ConcurrentLinkedQueue<String>> phase1Apply = new ConcurrentHashMap<>();
        for (long id = 1; id <= 3; id++) phase1Apply.put(id, new ConcurrentLinkedQueue<>());

        long phase1Commit;
        long phase1Applied;
        {
            List<RaftPeer> nodes = new ArrayList<>();
            try {
                for (long id = 1; id <= 3; id++) {
                    long fid = id;
                    nodes.add(new RaftPeer(fid, ports1[(int) (fid - 1)],
                            storageDirs[(int) (fid - 1)], peers1, true,
                            (idx, data) -> phase1Apply.get(fid).add(new String(data))));
                }

                assertThat(awaitLeader(nodes, 10_000)).as("phase1 leader").isPositive();
                RaftPeer leader = findLeader(nodes);
                assertThat(leader).isNotNull();

                for (int i = 0; i < 10; i++) leader.propose(("v" + i).getBytes());
                assertThat(awaitTrue(
                        () -> phase1Apply.values().stream().allMatch(q -> q.size() >= 10),
                        15_000)).as("all nodes apply 10 entries").isTrue();

                phase1Commit = leader.basicStatus().commit;
                phase1Applied = leader.basicStatus().applied;
            } finally {
                closeAll(nodes);
            }
        }

        // ---- Phase 2: restart all nodes, verify recovery ----
        int[] ports2 = freePorts(3);
        Map<Long, String> peers2 = peerMap(ports2);
        Map<Long, ConcurrentLinkedQueue<String>> phase2Apply = new ConcurrentHashMap<>();
        for (long id = 1; id <= 3; id++) phase2Apply.put(id, new ConcurrentLinkedQueue<>());

        List<RaftPeer> nodes2 = new ArrayList<>();
        try {
            for (long id = 1; id <= 3; id++) {
                long fid = id;
                nodes2.add(new RaftPeer(fid, ports2[(int) (fid - 1)],
                        storageDirs[(int) (fid - 1)], peers2, false,
                        (idx, data) -> phase2Apply.get(fid).add(new String(data))));
            }

            assertThat(awaitLeader(nodes2, 15_000)).as("phase2 leader elected").isPositive();

            // Recovered state must be at least where we left off.
            for (RaftPeer p : nodes2) {
                assertThat(awaitTrue(
                        () -> p.basicStatus().commit >= phase1Commit, 10_000))
                        .as("node %d commit must recover to >= %d", p.id, phase1Commit).isTrue();
            }

            // No re-apply of old entries — give a brief window then assert empty.
            Thread.sleep(500);
            for (long id = 1; id <= 3; id++) {
                assertThat(phase2Apply.get(id))
                        .as("node %d must not re-apply phase1 entries", id).isEmpty();
            }

            // New proposals work.
            RaftPeer leader2 = findLeader(nodes2);
            assertThat(leader2).isNotNull();
            for (int i = 0; i < 5; i++) leader2.propose(("post-" + i).getBytes());
            assertThat(awaitTrue(
                    () -> phase2Apply.values().stream().allMatch(q -> q.size() >= 5),
                    15_000)).as("all nodes apply post-restart entries").isTrue();
        } finally {
            closeAll(nodes2);
        }
    }

    @Test
    void majorityRestartWhileMinoritySurvives() throws Exception {
        Path[] storageDirs = {tmp.resolve("p1"), tmp.resolve("p2"), tmp.resolve("p3")};
        int[] ports = freePorts(3);
        Map<Long, String> peers = peerMap(ports);
        Map<Long, ConcurrentLinkedQueue<String>> applyLogs = new ConcurrentHashMap<>();
        for (long id = 1; id <= 3; id++) applyLogs.put(id, new ConcurrentLinkedQueue<>());

        List<RaftPeer> nodes = new ArrayList<>();
        try {
            for (long id = 1; id <= 3; id++) {
                long fid = id;
                nodes.add(new RaftPeer(fid, ports[(int) (fid - 1)],
                        storageDirs[(int) (fid - 1)], peers, true,
                        (idx, data) -> applyLogs.get(fid).add(new String(data))));
            }

            assertThat(awaitLeader(nodes, 10_000)).isPositive();
            RaftPeer leader = findLeader(nodes);
            assertThat(leader).isNotNull();

            for (int i = 0; i < 5; i++) leader.propose(("pre-" + i).getBytes());
            assertThat(awaitTrue(
                    () -> applyLogs.values().stream().allMatch(q -> q.size() >= 5),
                    15_000)).isTrue();

            // Kill nodes 2 and 3 — node 1 is the sole survivor.
            RaftPeer survivor = nodes.get(0);
            nodes.get(1).close();
            nodes.get(2).close();

            // Node 1 alone cannot commit (no quorum in a 3-voter cluster).
            long commitBefore = survivor.basicStatus().commit;

            // Restart nodes 2 and 3 with fresh ports.
            int[] freshPorts = freePorts(2);
            Map<Long, String> newPeers = Map.of(
                    1L, "localhost:" + ports[0],
                    2L, "localhost:" + freshPorts[0],
                    3L, "localhost:" + freshPorts[1]);

            // Update survivor's transport to know new addresses.
            survivor.transport.addPeer(2L, newPeers.get(2L));
            survivor.transport.addPeer(3L, newPeers.get(3L));

            ConcurrentLinkedQueue<String> apply2 = new ConcurrentLinkedQueue<>();
            ConcurrentLinkedQueue<String> apply3 = new ConcurrentLinkedQueue<>();
            RaftPeer node2 = new RaftPeer(2L, storageDirs[1], newPeers, false,
                    (idx, data) -> apply2.add(new String(data)),
                    new io.github.xinfra.lab.raft.transport.grpc.GrpcTransport(2L, freshPorts[0]));
            RaftPeer node3 = new RaftPeer(3L, storageDirs[2], newPeers, false,
                    (idx, data) -> apply3.add(new String(data)),
                    new io.github.xinfra.lab.raft.transport.grpc.GrpcTransport(3L, freshPorts[1]));

            List<RaftPeer> allNodes = List.of(survivor, node2, node3);
            nodes.clear();
            nodes.addAll(allNodes);

            // The cluster re-forms and elects a leader.
            assertThat(awaitLeader(allNodes, 20_000)).as("re-elect after majority restart").isPositive();
            assertThat(awaitConvergedLeader(allNodes, 15_000)).isTrue();

            // New proposals commit on all 3.
            RaftPeer newLeader = findLeader(allNodes);
            assertThat(newLeader).isNotNull();
            for (int i = 0; i < 5; i++) newLeader.propose(("post-" + i).getBytes());

            assertThat(awaitTrue(
                    () -> allNodes.stream().allMatch(p -> p.basicStatus().commit > commitBefore + 4),
                    20_000)).as("all nodes commit post-restart proposals").isTrue();
        } finally {
            closeAll(nodes);
        }
    }

    private static boolean awaitConvergedLeader(Iterable<RaftPeer> peers, long timeoutMillis) throws InterruptedException {
        return IntegrationTestSupport.awaitConvergedLeader(peers, timeoutMillis);
    }

    private static void closeAll(List<RaftPeer> nodes) {
        for (int i = nodes.size() - 1; i >= 0; i--) {
            try { nodes.get(i).close(); } catch (Throwable ignored) {}
        }
    }
}
