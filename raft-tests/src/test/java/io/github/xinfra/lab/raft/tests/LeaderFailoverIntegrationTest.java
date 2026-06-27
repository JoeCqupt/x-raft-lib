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
import java.util.concurrent.ConcurrentLinkedQueue;

import static io.github.xinfra.lab.raft.tests.IntegrationTestSupport.awaitConvergedLeader;
import static io.github.xinfra.lab.raft.tests.IntegrationTestSupport.awaitLeader;
import static io.github.xinfra.lab.raft.tests.IntegrationTestSupport.awaitTrue;
import static io.github.xinfra.lab.raft.tests.IntegrationTestSupport.findLeader;
import static io.github.xinfra.lab.raft.tests.IntegrationTestSupport.freePorts;
import static io.github.xinfra.lab.raft.tests.IntegrationTestSupport.peerMap;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Kill the leader of a 3-node cluster and verify the remaining 2 nodes
 * elect a new leader and continue committing proposals. Exercises:
 * <ul>
 *   <li>raft-core election timeout + pre-vote convergence,</li>
 *   <li>raft-transport-grpc handling abrupt peer disappearance
 *       (server shutdown, in-flight RPC errors),</li>
 *   <li>raft-storage-rocksdb on the surviving nodes accepting
 *       writes from the new leader.</li>
 * </ul>
 */
class LeaderFailoverIntegrationTest {

    @TempDir Path tmp;

    @Test
    void newLeaderTakesOverWhenOldLeaderClosed() throws Exception {
        int[] ports = freePorts(3);
        Map<Long, String> peers = peerMap(ports);

        Map<Long, ConcurrentLinkedQueue<byte[]>> applyLogs = new java.util.concurrent.ConcurrentHashMap<>();
        for (long id = 1; id <= 3; id++) {
            applyLogs.put(id, new ConcurrentLinkedQueue<>());
        }

        List<RaftKVNode> nodes = new ArrayList<>();
        try {
            for (long id = 1; id <= 3; id++) {
                long fid = id;
                ConcurrentLinkedQueue<byte[]> log = applyLogs.get(fid);
                nodes.add(new RaftKVNode(
                        fid,
                        ports[(int) (fid - 1)],
                        tmp.resolve("p" + fid),
                        peers,
                        true,
                        (idx, data) -> log.add(data)));
            }

            long firstLeaderId = awaitLeader(nodes, 10_000);
            assertThat(firstLeaderId).as("initial leader elected").isPositive();
            assertThat(awaitConvergedLeader(nodes, 10_000)).isTrue();
            RaftKVNode firstLeader = findLeader(nodes);
            assertThat(firstLeader).isNotNull();

            // Commit a couple of entries through the first leader so all
            // nodes have non-trivial state when failover happens.
            for (int i = 0; i < 5; i++) firstLeader.propose(("pre-" + i).getBytes());
            assertThat(awaitTrue(
                    () -> applyLogs.values().stream().allMatch(q -> q.size() >= 5),
                    10_000)).isTrue();

            // Take down the leader hard. close() invokes stop() on the
            // node and shuts down the grpc server.
            long oldLeaderId = firstLeaderId;
            firstLeader.close();
            nodes.remove(firstLeader);

            // The remaining two nodes form a quorum (2 of 3) and elect a
            // new leader within a few election timeouts.
            assertThat(awaitTrue(
                    () -> {
                        RaftKVNode l = findLeader(nodes);
                        return l != null && l.id != oldLeaderId;
                    },
                    20_000))
                    .as("new leader (id != %s) must emerge", oldLeaderId).isTrue();

            RaftKVNode newLeader = findLeader(nodes);
            assertThat(newLeader).isNotNull();
            assertThat(newLeader.id).isNotEqualTo(oldLeaderId);

            // The new leader can accept proposals and the surviving
            // follower applies them.
            for (int i = 0; i < 5; i++) {
                newLeader.propose(("post-" + i).getBytes());
            }
            assertThat(awaitTrue(
                    () -> nodes.stream().allMatch(p -> applyLogs.get(p.id).size() >= 10),
                    20_000))
                    .as("surviving nodes must apply post-failover proposals").isTrue();

            for (RaftKVNode p : nodes) {
                List<String> seen = applyLogs.get(p.id).stream().map(String::new).toList();
                assertThat(seen.subList(0, 5))
                        .as("pre-failover prefix on peer %d", p.id)
                        .containsExactly("pre-0", "pre-1", "pre-2", "pre-3", "pre-4");
                assertThat(seen.subList(5, 10))
                        .as("post-failover suffix on peer %d", p.id)
                        .containsExactly("post-0", "post-1", "post-2", "post-3", "post-4");
            }
        } finally {
            for (int i = nodes.size() - 1; i >= 0; i--) {
                try { nodes.get(i).close(); } catch (Throwable ignored) {}
            }
        }
    }
}
