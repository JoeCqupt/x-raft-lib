/*
 * Copyright 2024-2026 The x-raft-lib Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * SPDX-License-Identifier: Apache-2.0
 */
package io.github.xinfra.lab.raft.tests;

import io.github.xinfra.lab.raft.RaftException;
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

import static io.github.xinfra.lab.raft.tests.IntegrationTestSupport.awaitConvergedLeader;
import static io.github.xinfra.lab.raft.tests.IntegrationTestSupport.awaitLeader;
import static io.github.xinfra.lab.raft.tests.IntegrationTestSupport.awaitTrue;
import static io.github.xinfra.lab.raft.tests.IntegrationTestSupport.findLeader;
import static io.github.xinfra.lab.raft.tests.IntegrationTestSupport.freePorts;
import static io.github.xinfra.lab.raft.tests.IntegrationTestSupport.peerMap;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end leader transfer over the real gRPC + RocksDB stack. Verifies
 * that {@link io.github.xinfra.lab.raft.Node#transferLeadership} works
 * through the full transport round-trip and the new leader can serve.
 */
class LeaderTransferIntegrationTest {

    @TempDir Path tmp;

    @Test
    void transferLeadershipToFollower() throws Exception {
        int[] ports = freePorts(3);
        Map<Long, String> peers = peerMap(ports);
        Map<Long, ConcurrentLinkedQueue<String>> applyLogs = new ConcurrentHashMap<>();
        for (long id = 1; id <= 3; id++) applyLogs.put(id, new ConcurrentLinkedQueue<>());

        List<RaftPeer> nodes = new ArrayList<>();
        try {
            for (long id = 1; id <= 3; id++) {
                long fid = id;
                nodes.add(new RaftPeer(fid, ports[(int) (fid - 1)],
                        tmp.resolve("p" + fid), peers, true,
                        (idx, data) -> applyLogs.get(fid).add(new String(data))));
            }

            long leaderId = awaitLeader(nodes, 10_000);
            assertThat(leaderId).isPositive();
            assertThat(awaitConvergedLeader(nodes, 10_000)).isTrue();
            RaftPeer leader = findLeader(nodes);
            assertThat(leader).isNotNull();

            // Commit some entries so the transferee has state to catch up on.
            for (int i = 0; i < 5; i++) leader.propose(("pre-" + i).getBytes());
            assertThat(awaitTrue(
                    () -> applyLogs.values().stream().allMatch(q -> q.size() >= 5),
                    10_000)).isTrue();

            // Pick a follower as the transfer target.
            long transferee = nodes.stream()
                    .filter(p -> p.id != leaderId)
                    .findFirst().orElseThrow().id;

            leader.node.transferLeadership(leaderId, transferee);

            // The transferee becomes the new leader.
            assertThat(awaitTrue(
                    () -> {
                        for (RaftPeer p : nodes) {
                            if (p.id == transferee
                                    && p.basicStatus().state == RaftStateType.StateLeader) {
                                return true;
                            }
                        }
                        return false;
                    }, 15_000))
                    .as("node %d must become leader after transfer", transferee).isTrue();

            // The new leader can accept proposals.
            RaftPeer newLeader = findLeader(nodes);
            assertThat(newLeader).isNotNull();
            assertThat(newLeader.id).isEqualTo(transferee);

            for (int i = 0; i < 5; i++) newLeader.propose(("post-" + i).getBytes());
            assertThat(awaitTrue(
                    () -> applyLogs.values().stream().allMatch(q -> q.size() >= 10),
                    15_000)).as("all nodes apply post-transfer proposals").isTrue();
        } finally {
            closeAll(nodes);
        }
    }

    @Test
    void transferLeadershipDuringActiveProposals() throws Exception {
        int[] ports = freePorts(3);
        Map<Long, String> peers = peerMap(ports);
        Map<Long, ConcurrentLinkedQueue<String>> applyLogs = new ConcurrentHashMap<>();
        for (long id = 1; id <= 3; id++) applyLogs.put(id, new ConcurrentLinkedQueue<>());

        List<RaftPeer> nodes = new ArrayList<>();
        try {
            for (long id = 1; id <= 3; id++) {
                long fid = id;
                nodes.add(new RaftPeer(fid, ports[(int) (fid - 1)],
                        tmp.resolve("p" + fid), peers, true,
                        (idx, data) -> applyLogs.get(fid).add(new String(data))));
            }

            long leaderId = awaitLeader(nodes, 10_000);
            assertThat(leaderId).isPositive();
            assertThat(awaitConvergedLeader(nodes, 10_000)).isTrue();
            RaftPeer leader = findLeader(nodes);
            assertThat(leader).isNotNull();

            // Start proposing — some will land before transfer, some after.
            int totalProposals = 20;
            int transferAfter = 10;
            long transferee = nodes.stream()
                    .filter(p -> p.id != leaderId)
                    .findFirst().orElseThrow().id;

            for (int i = 0; i < totalProposals; i++) {
                if (i == transferAfter) {
                    leader.node.transferLeadership(leaderId, transferee);
                }
                try {
                    // During transfer, proposals on the old leader will be
                    // dropped — that's expected. We catch and continue.
                    RaftPeer currentLeader = findLeader(nodes);
                    if (currentLeader != null) {
                        currentLeader.propose(("msg-" + i).getBytes());
                    }
                } catch (RaftException ignored) {
                    // proposal dropped during transfer — expected
                }
                Thread.sleep(50);
            }

            // Wait for a new leader to emerge (may be the transferee or the
            // original if the transfer timed out — both are valid).
            assertThat(awaitLeader(nodes, 15_000)).isPositive();

            // Continue proposing on whoever is leader now.
            RaftPeer finalLeader = findLeader(nodes);
            assertThat(finalLeader).isNotNull();
            for (int i = 0; i < 5; i++) finalLeader.propose(("final-" + i).getBytes());

            // All nodes converge to the same commit.
            assertThat(awaitTrue(() -> {
                long maxCommit = nodes.stream()
                        .mapToLong(p -> p.basicStatus().commit).max().orElse(0);
                return nodes.stream().allMatch(p -> p.basicStatus().commit >= maxCommit);
            }, 20_000)).as("all nodes converge to the same commit").isTrue();

            // The final batch must be applied on all nodes.
            assertThat(awaitTrue(
                    () -> applyLogs.values().stream()
                            .allMatch(q -> q.stream().anyMatch(s -> s.equals("final-4"))),
                    15_000)).as("all nodes apply final proposals").isTrue();
        } finally {
            closeAll(nodes);
        }
    }

    private static void closeAll(List<RaftPeer> nodes) {
        for (int i = nodes.size() - 1; i >= 0; i--) {
            try { nodes.get(i).close(); } catch (Throwable ignored) {}
        }
    }
}
