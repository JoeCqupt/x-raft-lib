/*
 * Copyright 2024-2026 The x-raft-lib Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * SPDX-License-Identifier: Apache-2.0
 */
package io.github.xinfra.lab.raft.tests;

import io.github.xinfra.lab.raft.RaftStateType;

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
 * End-to-end tests for PreVote and CheckQuorum over real gRPC + RocksDB.
 *
 * <p>Note: {@link TestRaftNode} enables both preVote and checkQuorum by default,
 * which is the recommended production configuration.
 */
class PreVoteCheckQuorumIntegrationTest {

    @TempDir Path tmp;

    /**
     * An isolated node that rejoins should NOT cause term inflation when PreVote
     * is enabled. Without PreVote, the isolated node would increment its term
     * on every election timeout and force the cluster to adopt the higher term
     * on rejoin. With PreVote, the isolated node's pre-vote requests are rejected
     * (it has stale log) so its term stays low.
     */
    @Test
    void isolatedNodeDoesNotInflateTermWithPreVote() throws Exception {
        int[] ports = freePorts(3);
        Map<Long, String> peers = peerMap(ports);
        ChaosController chaos = new ChaosController();

        List<TestRaftNode> nodes = new ArrayList<>();
        try {
            for (long id = 1; id <= 3; id++) {
                long fid = id;
                nodes.add(chaosPeer(fid, ports, peers, tmp.resolve("p" + fid), true,
                        (idx, data) -> {}, chaos));
            }

            long leaderId = awaitLeader(nodes, 10_000);
            assertThat(leaderId).isPositive();

            // Record the term before isolation
            TestRaftNode leader = findLeader(nodes);
            assertThat(leader).isNotNull();

            // Propose some data to advance the log
            for (int i = 0; i < 5; i++) {
                leader.propose(("pre-" + i).getBytes());
            }
            assertThat(awaitTrue(
                    () -> nodes.stream().allMatch(p -> p.basicStatus().commit > 3), 10_000))
                    .isTrue();

            long termBefore = leader.basicStatus().term;

            // Pick a non-leader node and isolate it
            long isolatedId = 0;
            for (TestRaftNode p : nodes) {
                if (p.id != leaderId) { isolatedId = p.id; break; }
            }
            assertThat(isolatedId).isPositive();
            chaos.isolate(isolatedId);

            // While isolated, the node will attempt elections but PreVote prevents term bumps.
            // Wait long enough for several election timeouts (electionTick=10, tick=100ms → ~1s per timeout)
            Thread.sleep(5_000);

            // Meanwhile, the majority keeps committing
            for (int i = 0; i < 5; i++) {
                TestRaftNode l = findLeaderOrWait(nodes, 5_000);
                if (l != null) l.propose(("during-iso-" + i).getBytes());
            }

            // Heal and let the cluster converge
            chaos.healAll();

            assertThat(awaitTrue(() -> {
                long maxTerm = nodes.stream().mapToLong(p -> p.basicStatus().term).max().orElse(0);
                // The term should not have inflated significantly.
                // Without PreVote, the isolated node would have bumped term 4-5 times.
                // With PreVote, the term should only increase by 0-1 (at most one real election).
                return maxTerm <= termBefore + 2;
            }, 15_000)).as("term should not inflate significantly with PreVote").isTrue();

            // Cluster must converge on a single leader
            assertThat(awaitLeader(nodes, 10_000)).isPositive();
        } finally {
            closeAll(nodes);
        }
    }

    /**
     * CheckQuorum: if the leader becomes isolated from the majority, it should
     * step down after the election timeout (since it can't hear from a quorum).
     */
    @Test
    void leaderStepsDownWhenIsolatedWithCheckQuorum() throws Exception {
        int[] ports = freePorts(3);
        Map<Long, String> peers = peerMap(ports);
        ChaosController chaos = new ChaosController();

        List<TestRaftNode> nodes = new ArrayList<>();
        try {
            for (long id = 1; id <= 3; id++) {
                long fid = id;
                nodes.add(chaosPeer(fid, ports, peers, tmp.resolve("p" + fid), true,
                        (idx, data) -> {}, chaos));
            }

            long leaderId = awaitLeader(nodes, 10_000);
            assertThat(leaderId).isPositive();
            TestRaftNode leader = findLeader(nodes);
            assertThat(leader).isNotNull();

            // Isolate the leader from everyone
            chaos.isolate(leaderId);

            // CheckQuorum triggers after election timeout. With electionTick=10 and
            // tick interval 100ms, that's ~1s. Wait a bit longer for safety.
            assertThat(awaitTrue(
                    () -> leader.basicStatus().state != RaftStateType.StateLeader,
                    10_000))
                    .as("isolated leader must step down via CheckQuorum").isTrue();

            // The remaining two nodes should elect a new leader
            List<TestRaftNode> survivors = new ArrayList<>();
            for (TestRaftNode p : nodes) {
                if (p.id != leaderId) survivors.add(p);
            }
            long newLeader = awaitLeader(survivors, 15_000);
            assertThat(newLeader)
                    .as("survivors must elect a new leader")
                    .isPositive()
                    .isNotEqualTo(leaderId);

            // Heal and verify convergence
            chaos.healAll();
            assertThat(awaitTrue(
                    () -> nodes.stream().allMatch(p -> p.basicStatus().lead == newLeader
                            || p.basicStatus().lead > 0), 15_000))
                    .as("all nodes must converge on a leader after heal").isTrue();
        } finally {
            closeAll(nodes);
        }
    }

    /**
     * Normal election succeeds with PreVote enabled: the cluster bootstraps,
     * elects a leader, and can commit proposals.
     */
    @Test
    void normalElectionSucceedsWithPreVote() throws Exception {
        int[] ports = freePorts(3);
        Map<Long, String> peers = peerMap(ports);

        List<TestRaftNode> nodes = new ArrayList<>();
        try {
            for (long id = 1; id <= 3; id++) {
                long fid = id;
                nodes.add(new TestRaftNode(fid, ports[(int) (fid - 1)],
                        tmp.resolve("p" + fid), peers, true,
                        (idx, data) -> {}));
            }

            long leaderId = awaitLeader(nodes, 10_000);
            assertThat(leaderId).as("PreVote cluster must elect leader").isPositive();

            TestRaftNode leader = findLeader(nodes);
            assertThat(leader).isNotNull();
            for (int i = 0; i < 10; i++) {
                leader.propose(("pv-" + i).getBytes());
            }

            assertThat(awaitTrue(
                    () -> nodes.stream().allMatch(p -> p.basicStatus().commit > 5), 10_000))
                    .as("proposals must commit with PreVote enabled").isTrue();
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
