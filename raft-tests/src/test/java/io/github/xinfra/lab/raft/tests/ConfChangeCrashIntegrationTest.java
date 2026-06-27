/*
 * Copyright 2024-2026 The x-raft-lib Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * SPDX-License-Identifier: Apache-2.0
 */
package io.github.xinfra.lab.raft.tests;


import io.github.xinfra.lab.raft.proto.Eraftpb;
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
import static io.github.xinfra.lab.raft.tests.IntegrationTestSupport.findLeaderOrWait;
import static io.github.xinfra.lab.raft.tests.IntegrationTestSupport.freePorts;
import static io.github.xinfra.lab.raft.tests.IntegrationTestSupport.peerMap;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Exercises ConfChange resilience: leader crash during a membership change
 * and scaling up then back down.
 */
class ConfChangeCrashIntegrationTest {

    @TempDir Path tmp;

    /**
     * The leader proposes adding node 4 as a voter, then crashes immediately.
     * The surviving majority must complete the conf change and the 4-voter
     * cluster must keep committing.
     */
    @Test
    void leaderCrashDuringConfChangeCompletes() throws Exception {
        int[] ports = freePorts(4);
        Map<Long, String> bootPeers = peerMap(new int[]{ports[0], ports[1], ports[2]});
        Map<Long, String> allPeers = peerMap(ports);
        ChaosController chaos = new ChaosController();

        List<TestRaftNode> nodes = new ArrayList<>();
        try {
            // Bootstrap 3-node cluster.
            for (long id = 1; id <= 3; id++) {
                long fid = id;
                nodes.add(chaosPeer(fid, ports, bootPeers, tmp.resolve("p" + fid), true,
                        (idx, data) -> { }, chaos));
            }
            long leaderId = awaitLeader(nodes, 10_000);
            assertThat(leaderId).isPositive();
            TestRaftNode leader = findLeader(nodes);
            assertThat(leader).isNotNull();

            // Commit some seed entries.
            for (int i = 0; i < 5; i++) leader.propose(("seed-" + i).getBytes());
            assertThat(awaitTrue(
                    () -> nodes.stream().allMatch(p -> p.basicStatus().commit >= 5),
                    10_000)).isTrue();

            // Start node 4 (non-bootstrap, empty storage).
            TestRaftNode joiner = chaosPeer(4L, ports, allPeers, tmp.resolve("p4"), false,
                    (idx, data) -> { }, chaos);
            nodes.add(joiner);
            for (TestRaftNode p : nodes) {
                if (p.id != 4L) p.transport.addPeer(4L, allPeers.get(4L));
            }

            // Propose adding node 4 as a voter.
            long commitBeforeCC = leader.basicStatus().commit;
            Eraftpb.ConfChangeV2 addVoter = Eraftpb.ConfChangeV2.newBuilder()
                    .addChanges(Eraftpb.ConfChangeSingle.newBuilder()
                            .setType(Eraftpb.ConfChangeType.ConfChangeAddNode)
                            .setNodeId(4L))
                    .build();
            leader.proposeConfChange(addVoter);

            // Wait until the conf change entry is replicated to at least one
            // follower (commit advances), then crash. This simulates a leader
            // that crashes after replication but before it can fully process
            // the new configuration — the surviving majority completes it.
            TestRaftNode fLeader = leader;
            awaitTrue(() -> fLeader.basicStatus().commit > commitBeforeCC, 10_000);

            // Crash the leader.
            long crashedId = leader.id;
            leader.close();
            nodes.removeIf(p -> p.id == crashedId);

            // The surviving nodes must elect a new leader.
            assertThat(awaitTrue(() -> {
                TestRaftNode l = findLeader(nodes);
                return l != null && l.id != crashedId;
            }, 20_000)).as("new leader must emerge after crash").isTrue();

            // Wait for the conf change to be applied — node 4 should appear
            // in the voter set of the new leader.
            assertThat(awaitTrue(() -> {
                TestRaftNode l = findLeader(nodes);
                if (l == null) return false;
                List<Long> voters = l.storage.initialState().confState().getVotersList();
                return voters.contains(4L);
            }, 25_000)).as("node 4 must become a voter").isTrue();

            // The cluster (now 3 survivors of 4 voters) can commit new proposals.
            TestRaftNode newLeader = findLeaderOrWait(nodes, 10_000);
            assertThat(newLeader).isNotNull();
            long pre = newLeader.basicStatus().commit;
            for (int i = 0; i < 5; i++) newLeader.propose(("after-crash-" + i).getBytes());
            assertThat(awaitTrue(
                    () -> nodes.stream().allMatch(p -> p.basicStatus().commit > pre + 4),
                    15_000)).as("cluster commits post-crash proposals").isTrue();
        } finally {
            closeAll(nodes);
        }
    }

    /**
     * Scale a 3-node cluster up to 5 voters, verify it works, then scale
     * back down to 3 and verify again.
     */
    @Test
    void scaleUpThenScaleDownConverges() throws Exception {
        int[] ports = freePorts(5);
        Map<Long, String> bootPeers = peerMap(new int[]{ports[0], ports[1], ports[2]});
        Map<Long, String> allPeers = peerMap(ports);
        ChaosController chaos = new ChaosController();

        List<TestRaftNode> nodes = new ArrayList<>();
        try {
            // Bootstrap 3-node cluster.
            for (long id = 1; id <= 3; id++) {
                long fid = id;
                nodes.add(chaosPeer(fid, ports, bootPeers, tmp.resolve("p" + fid), true,
                        (idx, data) -> { }, chaos));
            }
            assertThat(awaitLeader(nodes, 10_000)).isPositive();

            // Scale up: add nodes 4 and 5.
            for (long newId = 4; newId <= 5; newId++) {
                TestRaftNode joiner = chaosPeer(newId, ports, allPeers, tmp.resolve("p" + newId), false,
                        (idx, data) -> { }, chaos);
                nodes.add(joiner);
                for (TestRaftNode p : nodes) {
                    if (p.id != newId) p.transport.addPeer(newId, allPeers.get(newId));
                }

                TestRaftNode leader = findLeaderOrWait(nodes, 10_000);
                assertThat(leader).isNotNull();
                Eraftpb.ConfChangeV2 cc = Eraftpb.ConfChangeV2.newBuilder()
                        .addChanges(Eraftpb.ConfChangeSingle.newBuilder()
                                .setType(Eraftpb.ConfChangeType.ConfChangeAddNode)
                                .setNodeId(newId))
                        .build();
                leader.proposeConfChange(cc);

                long fNewId = newId;
                assertThat(awaitTrue(() -> {
                    TestRaftNode l = findLeader(nodes);
                    if (l == null) return false;
                    return l.storage.initialState().confState().getVotersList().contains(fNewId);
                }, 20_000)).as("node %d must become a voter", newId).isTrue();
            }

            // 5-voter cluster commits proposals.
            TestRaftNode leader5 = findLeaderOrWait(nodes, 10_000);
            assertThat(leader5).isNotNull();
            long pre5 = leader5.basicStatus().commit;
            for (int i = 0; i < 5; i++) leader5.propose(("five-" + i).getBytes());
            assertThat(awaitTrue(
                    () -> nodes.stream().allMatch(p -> p.basicStatus().commit > pre5 + 4),
                    15_000)).as("5-voter cluster commits").isTrue();

            // Scale down: remove nodes 5 and 4.
            for (long removeId = 5; removeId >= 4; removeId--) {
                TestRaftNode leader = findLeaderOrWait(nodes, 10_000);
                assertThat(leader).isNotNull();
                Eraftpb.ConfChangeV2 cc = Eraftpb.ConfChangeV2.newBuilder()
                        .addChanges(Eraftpb.ConfChangeSingle.newBuilder()
                                .setType(Eraftpb.ConfChangeType.ConfChangeRemoveNode)
                                .setNodeId(removeId))
                        .build();
                leader.proposeConfChange(cc);

                long fRemoveId = removeId;
                assertThat(awaitTrue(() -> {
                    TestRaftNode l = findLeader(nodes);
                    if (l == null) return false;
                    List<Long> voters = l.storage.initialState().confState().getVotersList();
                    return !voters.contains(fRemoveId);
                }, 20_000)).as("node %d must be removed from voters", removeId).isTrue();
            }

            // 3-voter cluster (nodes 1-3) still commits.
            TestRaftNode leader3 = findLeaderOrWait(nodes, 10_000);
            assertThat(leader3).isNotNull();
            long pre3 = leader3.basicStatus().commit;
            for (int i = 0; i < 5; i++) leader3.propose(("three-" + i).getBytes());
            assertThat(awaitTrue(
                    () -> nodes.stream()
                            .filter(p -> p.id <= 3)
                            .allMatch(p -> p.basicStatus().commit > pre3 + 4),
                    15_000)).as("3-voter cluster still commits after scale-down").isTrue();
        } finally {
            closeAll(nodes);
        }
    }

    private static void closeAll(List<TestRaftNode> nodes) {
        for (int i = nodes.size() - 1; i >= 0; i--) {
            try { nodes.get(i).close(); } catch (Throwable ignored) {}
        }
    }
}
