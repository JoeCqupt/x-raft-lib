/*
 * Copyright 2024-2026 The x-raft-lib Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * SPDX-License-Identifier: Apache-2.0
 */
package io.github.xinfra.lab.raft.tests;

import io.github.xinfra.lab.raft.examples.RaftKVNode;
import io.github.xinfra.lab.raft.tests.chaos.ChaosController;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static io.github.xinfra.lab.raft.tests.IntegrationTestSupport.awaitLeader;
import static io.github.xinfra.lab.raft.tests.IntegrationTestSupport.awaitTrue;
import static io.github.xinfra.lab.raft.tests.IntegrationTestSupport.chaosPeer;
import static io.github.xinfra.lab.raft.tests.IntegrationTestSupport.findLeader;
import static io.github.xinfra.lab.raft.tests.IntegrationTestSupport.freePorts;
import static io.github.xinfra.lab.raft.tests.IntegrationTestSupport.peerMap;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Network-partition and lossy-link behaviour over the real gRPC transport,
 * driven by {@link ChaosController}. Verifies the two safety/liveness
 * properties that matter most: a minority partition cannot commit, while the
 * majority keeps making progress; and a lossy link still converges because
 * raft retransmits.
 */
class PartitionIntegrationTest {

    @TempDir Path tmp;

    @Test
    void minorityCannotCommitWhileMajorityProgresses() throws Exception {
        int[] ports = freePorts(5);
        Map<Long, String> peers = peerMap(ports);
        ChaosController chaos = new ChaosController();

        List<RaftKVNode> nodes = new ArrayList<>();
        try {
            for (long id = 1; id <= 5; id++) {
                long fid = id;
                nodes.add(chaosPeer(fid, ports, peers, tmp.resolve("p" + fid), true,
                        (idx, data) -> { }, chaos));
            }
            assertThat(awaitLeader(nodes, 10_000)).isPositive();

            // Split: {1,2} (minority) | {3,4,5} (majority).
            List<RaftKVNode> minority = List.of(nodes.get(0), nodes.get(1));
            List<RaftKVNode> majority = List.of(nodes.get(2), nodes.get(3), nodes.get(4));
            chaos.partition(List.of(1L, 2L), List.of(3L, 4L, 5L));

            // The majority side elects (or retains) a leader.
            long majLeader = awaitLeader(majority, 15_000);
            assertThat(majLeader).as("majority side must have a leader").isPositive();
            RaftKVNode leader = findLeader(majority);

            long majPre = leader.basicStatus().commit;
            for (int i = 0; i < 10; i++) leader.propose(("maj-" + i).getBytes());

            // Majority commits the batch.
            assertThat(awaitTrue(() -> majority.stream()
                    .allMatch(p -> p.basicStatus().commit >= majPre + 10), 15_000))
                    .as("majority must commit during the partition").isTrue();

            long majCommit = leader.basicStatus().commit;
            // Minority cannot have caught up to the majority's new commit — it
            // has no quorum on its side.
            assertThat(minority.stream().allMatch(p -> p.basicStatus().commit < majCommit))
                    .as("minority must not commit the majority's new entries").isTrue();

            // Heal: the whole cluster converges on the majority's state.
            chaos.healAll();
            assertThat(awaitTrue(() -> nodes.stream()
                    .allMatch(p -> p.basicStatus().commit >= majCommit), 25_000))
                    .as("all nodes converge after heal").isTrue();
        } finally {
            closeAll(nodes);
        }
    }

    @Test
    void lossyLinkStillConverges() throws Exception {
        int[] ports = freePorts(3);
        Map<Long, String> peers = peerMap(ports);
        ChaosController chaos = new ChaosController();

        List<RaftKVNode> nodes = new ArrayList<>();
        try {
            for (long id = 1; id <= 3; id++) {
                long fid = id;
                nodes.add(chaosPeer(fid, ports, peers, tmp.resolve("p" + fid), true,
                        (idx, data) -> { }, chaos));
            }
            assertThat(awaitLeader(nodes, 10_000)).isPositive();

            // Drop ~20% of all messages for the duration of the proposals.
            chaos.setDropProbability(0.2);
            RaftKVNode leader = findLeader(nodes);
            assertThat(leader).isNotNull();
            long pre = leader.basicStatus().commit;
            for (int i = 0; i < 30; i++) leader.propose(("lossy-" + i).getBytes());

            // Even with loss, retransmission drives the cluster to commit all 30.
            assertThat(awaitTrue(() -> {
                RaftKVNode l = findLeader(nodes);
                return l != null && l.basicStatus().commit >= pre + 30;
            }, 30_000)).as("leader must commit all proposals despite 20%% loss").isTrue();

            // Stop the loss and let followers catch up; all converge.
            chaos.setDropProbability(0.0);
            long commit = findLeader(nodes).basicStatus().commit;
            assertThat(awaitTrue(() -> nodes.stream()
                    .allMatch(p -> p.basicStatus().commit >= commit), 20_000))
                    .as("all nodes converge once the link recovers").isTrue();
        } finally {
            closeAll(nodes);
        }
    }

    private static void closeAll(List<RaftKVNode> nodes) {
        for (int i = nodes.size() - 1; i >= 0; i--) {
            try { nodes.get(i).close(); } catch (Throwable ignored) {}
        }
    }
}
