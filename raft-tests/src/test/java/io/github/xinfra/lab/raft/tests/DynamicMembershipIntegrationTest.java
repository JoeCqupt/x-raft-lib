/*
 * Copyright 2024-2026 The x-raft-lib Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * SPDX-License-Identifier: Apache-2.0
 */
package io.github.xinfra.lab.raft.tests;

import io.github.xinfra.lab.raft.examples.RaftPeer;
import io.github.xinfra.lab.raft.proto.Eraftpb;
import io.github.xinfra.lab.raft.tests.chaos.ChaosController;
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
import static io.github.xinfra.lab.raft.tests.IntegrationTestSupport.chaosPeer;
import static io.github.xinfra.lab.raft.tests.IntegrationTestSupport.findLeader;
import static io.github.xinfra.lab.raft.tests.IntegrationTestSupport.freePorts;
import static io.github.xinfra.lab.raft.tests.IntegrationTestSupport.peerMap;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end dynamic membership changes over the real gRPC + RocksDB stack.
 *
 * <p>Membership is asserted by reading each node's persisted {@code ConfState}
 * (the host writes it back via {@code storage.setConfState} after applying a
 * conf-change entry), which is the source of truth a restart would recover.
 */
class DynamicMembershipIntegrationTest {

    @TempDir Path tmp;

    /** Removing a follower converges the voter set to the remaining two. */
    @Test
    void removeFollowerConvergesVoterSet() throws Exception {
        int[] ports = freePorts(3);
        Map<Long, String> peers = peerMap(ports);
        ChaosController chaos = new ChaosController();

        List<RaftPeer> nodes = new ArrayList<>();
        try {
            for (long id = 1; id <= 3; id++) {
                long fid = id;
                nodes.add(chaosPeer(fid, ports, peers, tmp.resolve("p" + fid), true,
                        (idx, data) -> { }, chaos));
            }

            long leaderId = awaitLeader(nodes, 10_000);
            assertThat(leaderId).isPositive();
            RaftPeer leader = findLeader(nodes);
            long removed = followerOf(nodes, leaderId);

            Eraftpb.ConfChangeV2 cc = Eraftpb.ConfChangeV2.newBuilder()
                    .addChanges(Eraftpb.ConfChangeSingle.newBuilder()
                            .setType(Eraftpb.ConfChangeType.ConfChangeRemoveNode)
                            .setNodeId(removed))
                    .build();
            assertThat(leader.proposeConfChange(cc))
                    .as("conf-change proposal on leader must be accepted").isNull();

            // The two surviving nodes must persist a 2-voter membership without
            // the removed id.
            assertThat(awaitTrue(() -> nodes.stream()
                            .filter(p -> p.id != removed)
                            .allMatch(p -> {
                                List<Long> voters = votersOf(p);
                                return voters.size() == 2 && !voters.contains(removed);
                            }),
                    15_000))
                    .as("survivors must converge to a 2-voter config without %d", removed)
                    .isTrue();

            // The reduced cluster still makes progress.
            RaftPeer leader2 = findLeader(nodes);
            assertThat(leader2).isNotNull();
            long pre = leader2.basicStatus().commit;
            for (int i = 0; i < 5; i++) leader2.propose(("after-remove-" + i).getBytes());
            assertThat(awaitTrue(() -> nodes.stream()
                            .filter(p -> p.id != removed)
                            .allMatch(p -> p.basicStatus().commit > pre + 4),
                    10_000))
                    .as("2-voter cluster must keep committing").isTrue();
        } finally {
            closeAll(nodes);
        }
    }

    /**
     * Adding a brand-new node as a learner: it joins with empty storage,
     * catches up via replication from the leader, and applies the cluster's
     * committed proposals. Then it is promoted to a voter.
     */
    @Test
    void addLearnerThenPromoteToVoter() throws Exception {
        int[] ports = freePorts(4);
        // Nodes 1..3 bootstrap as the initial voter set; node 4 is added later.
        Map<Long, String> bootPeers = peerMap(new int[]{ports[0], ports[1], ports[2]});
        Map<Long, String> allPeers = peerMap(ports);
        ChaosController chaos = new ChaosController();

        Map<Long, ConcurrentLinkedQueue<String>> applyLogs = new ConcurrentHashMap<>();
        for (long id = 1; id <= 4; id++) applyLogs.put(id, new ConcurrentLinkedQueue<>());

        List<RaftPeer> nodes = new ArrayList<>();
        try {
            for (long id = 1; id <= 3; id++) {
                long fid = id;
                nodes.add(chaosPeer(fid, ports, bootPeers, tmp.resolve("p" + fid), true,
                        (idx, data) -> applyLogs.get(fid).add(new String(data)), chaos));
            }
            long leaderId = awaitLeader(nodes, 10_000);
            assertThat(leaderId).isPositive();
            RaftPeer leader = findLeader(nodes);

            // Commit some entries before the join so the learner has history to
            // catch up on.
            for (int i = 0; i < 10; i++) leader.propose(("seed-" + i).getBytes());

            // Start node 4 (non-bootstrap, empty storage). It knows how to reach
            // 1..3; the existing nodes learn its address now.
            RaftPeer joiner = chaosPeer(4L, ports, allPeers, tmp.resolve("p4"), false,
                    (idx, data) -> applyLogs.get(4L).add(new String(data)), chaos);
            nodes.add(joiner);
            for (RaftPeer p : nodes) {
                if (p.id != 4L) p.transport.addPeer(4L, allPeers.get(4L));
            }

            // Add node 4 as a learner.
            Eraftpb.ConfChangeV2 addLearner = Eraftpb.ConfChangeV2.newBuilder()
                    .addChanges(Eraftpb.ConfChangeSingle.newBuilder()
                            .setType(Eraftpb.ConfChangeType.ConfChangeAddLearnerNode)
                            .setNodeId(4L))
                    .build();
            assertThat(leader.proposeConfChange(addLearner)).isNull();

            // The learner appears in the leader's persisted ConfState and the
            // joiner catches up on the committed log.
            assertThat(awaitTrue(() -> learnersOf(leader).contains(4L), 15_000))
                    .as("leader must record node 4 as a learner").isTrue();
            assertThat(awaitTrue(() -> joiner.basicStatus().commit >= leader.basicStatus().commit - 1,
                    25_000))
                    .as("learner must catch up to the leader's commit").isTrue();

            // Promote the learner to a full voter.
            Eraftpb.ConfChangeV2 promote = Eraftpb.ConfChangeV2.newBuilder()
                    .addChanges(Eraftpb.ConfChangeSingle.newBuilder()
                            .setType(Eraftpb.ConfChangeType.ConfChangeAddNode)
                            .setNodeId(4L))
                    .build();
            assertThat(leader.proposeConfChange(promote)).isNull();

            assertThat(awaitTrue(() -> votersOf(leader).contains(4L)
                            && votersOf(leader).size() == 4, 15_000))
                    .as("node 4 must become the 4th voter").isTrue();

            // The now-4-voter cluster keeps committing, learner included.
            long pre = leader.basicStatus().commit;
            for (int i = 0; i < 5; i++) leader.propose(("after-join-" + i).getBytes());
            assertThat(awaitTrue(() -> nodes.stream()
                            .allMatch(p -> p.basicStatus().commit > pre + 4), 20_000))
                    .as("all four nodes must commit post-join proposals").isTrue();
        } finally {
            closeAll(nodes);
        }
    }

    // ---- helpers ----

    private static List<Long> votersOf(RaftPeer p) {
        return p.storage.initialState().confState().getVotersList();
    }

    private static List<Long> learnersOf(RaftPeer p) {
        return p.storage.initialState().confState().getLearnersList();
    }

    private static long followerOf(List<RaftPeer> nodes, long leaderId) {
        for (RaftPeer p : nodes) if (p.id != leaderId) return p.id;
        throw new IllegalStateException("no follower");
    }

    private static void closeAll(List<RaftPeer> nodes) {
        for (int i = nodes.size() - 1; i >= 0; i--) {
            try { nodes.get(i).close(); } catch (Throwable ignored) {}
        }
    }
}
