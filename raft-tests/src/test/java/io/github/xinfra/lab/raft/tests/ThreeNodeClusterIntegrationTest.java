/*
 * Copyright 2024-2026 The x-raft-lib Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * SPDX-License-Identifier: Apache-2.0
 */
package io.github.xinfra.lab.raft.tests;

import io.github.xinfra.lab.raft.examples.RaftKVNode;
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
 * Real 3-node cluster: each node runs its own grpc server, its own
 * RocksDB store, its own raft state machine. They discover one another
 * via {@code localhost:port} and elect a leader; proposed commands
 * commit on a quorum and replicate to all three.
 */
class ThreeNodeClusterIntegrationTest {

    @TempDir Path tmp;

    @Test
    void threeNodesElectLeaderAndReplicateProposals() throws Exception {
        int[] ports = freePorts(3);
        Map<Long, String> peers = peerMap(ports);

        // Each peer maintains its own apply log so we can assert that
        // all three see the same committed sequence.
        Map<Long, ConcurrentLinkedQueue<byte[]>> applyLogs = new ConcurrentHashMap<>();
        for (long id = 1; id <= 3; id++) {
            applyLogs.put(id, new ConcurrentLinkedQueue<>());
        }

        List<RaftKVNode> nodes = new ArrayList<>();
        try {
            for (long id = 1; id <= 3; id++) {
                long fid = id;
                ConcurrentLinkedQueue<byte[]> log = applyLogs.get(fid);
                RaftKVNode p = new RaftKVNode(
                        fid,
                        ports[(int) (fid - 1)],
                        tmp.resolve("p" + fid),
                        peers,
                        /*bootstrap=*/ true,
                        (idx, data) -> log.add(data));
                nodes.add(p);
            }

            // 1. A leader emerges within a few election timeouts.
            long leaderId = awaitLeader(nodes, 10_000);
            assertThat(leaderId).as("3-node cluster must elect a leader").isPositive();

            // 2. All peers converge on the same leader id.
            assertThat(awaitConvergedLeader(nodes, 10_000))
                    .as("all peers must agree on the leader").isTrue();

            // 3. Propose a batch through the leader. The host's apply
            //    callback runs on each peer's applier thread, so the
            //    queues should grow on all three.
            RaftKVNode leader = findLeader(nodes);
            assertThat(leader).isNotNull();
            int batch = 20;
            for (int i = 0; i < batch; i++) {
                leader.propose(("cmd-" + i).getBytes()); // accepted unless RaftException thrown
            }

            // 4. All three apply logs converge on the same sequence.
            //    Bootstrap ConfChange entries (3 of them, one per peer)
            //    also flow through apply, so we ignore the conf-change
            //    prefix and look only at the data entries.
            assertThat(awaitTrue(
                    () -> applyLogs.values().stream().allMatch(q -> q.size() >= batch),
                    20_000))
                    .as("each peer must apply all %d proposals", batch).isTrue();

            for (long id = 1; id <= 3; id++) {
                List<String> seen = applyLogs.get(id).stream()
                        .map(String::new)
                        .toList();
                assertThat(seen).as("peer %d must see all proposals in order", id)
                        .hasSize(batch);
                for (int i = 0; i < batch; i++) {
                    assertThat(seen.get(i)).isEqualTo("cmd-" + i);
                }
            }
        } finally {
            // Close in reverse order to avoid grpc shutdown-noise from
            // a peer trying to send to one already stopped.
            for (int i = nodes.size() - 1; i >= 0; i--) {
                try { nodes.get(i).close(); } catch (Throwable ignored) {}
            }
        }
    }

    @Test
    void clusterCommitIndexAdvancesPastInitialBootstrap() throws Exception {
        // Sanity: every peer's BasicStatus.commit and lastIndex move forward
        // beyond the bootstrap entries (which are 3 ConfChanges + 1 noop).
        int[] ports = freePorts(3);
        Map<Long, String> peers = peerMap(ports);

        List<RaftKVNode> nodes = new ArrayList<>();
        try {
            for (long id = 1; id <= 3; id++) {
                long fid = id;
                nodes.add(new RaftKVNode(fid, ports[(int) (fid - 1)],
                        tmp.resolve("p" + fid), peers, true,
                        (idx, data) -> { }));
            }

            long leaderId = awaitLeader(nodes, 10_000);
            assertThat(leaderId).isPositive();
            RaftKVNode leader = findLeader(nodes);
            assertThat(leader).isNotNull();

            long preCommit = leader.basicStatus().commit;
            for (int i = 0; i < 5; i++) leader.propose(("p" + i).getBytes());

            assertThat(awaitTrue(
                    () -> nodes.stream().allMatch(p -> p.basicStatus().commit > preCommit + 4),
                    10_000))
                    .as("commit index must advance on every peer").isTrue();
        } finally {
            for (int i = nodes.size() - 1; i >= 0; i--) {
                try { nodes.get(i).close(); } catch (Throwable ignored) {}
            }
        }
    }
}
