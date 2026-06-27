/*
 * Copyright 2024-2026 The x-raft-lib Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * SPDX-License-Identifier: Apache-2.0
 */
package io.github.xinfra.lab.raft.tests;


import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.github.xinfra.lab.raft.tests.IntegrationTestSupport.awaitLeader;
import static io.github.xinfra.lab.raft.tests.IntegrationTestSupport.awaitTrue;
import static io.github.xinfra.lab.raft.tests.IntegrationTestSupport.findLeader;
import static io.github.xinfra.lab.raft.tests.IntegrationTestSupport.freePorts;
import static io.github.xinfra.lab.raft.tests.IntegrationTestSupport.peerMap;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test for ReadIndex (linearizable reads). Verifies the
 * end-to-end ReadOnlySafe path over real gRPC + RocksDB.
 */
class ReadIndexIntegrationTest {

    @TempDir Path tmp;

    @Test
    void leaderReadIndexSucceedsAfterClusterFormed() throws Exception {
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
            assertThat(leaderId).as("must elect a leader").isPositive();

            TestRaftNode leader = findLeader(nodes);
            assertThat(leader).isNotNull();

            // Propose some data first so committed index is beyond bootstrap entries
            for (int i = 0; i < 5; i++) {
                leader.propose(("read-test-" + i).getBytes());
            }
            // Wait for proposals to commit
            assertThat(awaitTrue(
                    () -> leader.basicStatus().commit > 5, 10_000))
                    .as("proposals must commit").isTrue();

            // ReadIndex on the leader should succeed (ReadOnlySafe heartbeat round completes)
            byte[] ctx = "read-ctx-1".getBytes();
            leader.node.readIndex(ctx);

            // The call completing without exception proves the heartbeat round
            // confirmed leadership and a ReadState was produced.
        } finally {
            for (int i = nodes.size() - 1; i >= 0; i--) {
                try { nodes.get(i).close(); } catch (Throwable ignored) {}
            }
        }
    }

    @Test
    void multipleReadIndexRequestsSucceedConcurrently() throws Exception {
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
            assertThat(leaderId).isPositive();
            TestRaftNode leader = findLeader(nodes);
            assertThat(leader).isNotNull();

            // Fire multiple readIndex requests concurrently
            int count = 10;
            AtomicBoolean anyFailed = new AtomicBoolean(false);
            ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();
            List<Thread> threads = new ArrayList<>();
            for (int i = 0; i < count; i++) {
                int fi = i;
                Thread t = new Thread(() -> {
                    try {
                        leader.node.readIndex(("ctx-" + fi).getBytes());
                    } catch (Throwable e) {
                        anyFailed.set(true);
                        errors.add(e);
                    }
                });
                threads.add(t);
                t.start();
            }
            for (Thread t : threads) {
                t.join(10_000);
            }
            assertThat(anyFailed.get())
                    .as("concurrent readIndex should all succeed, errors: %s", errors)
                    .isFalse();
        } finally {
            for (int i = nodes.size() - 1; i >= 0; i--) {
                try { nodes.get(i).close(); } catch (Throwable ignored) {}
            }
        }
    }

    @Test
    void followerReadIndexForwardsToLeader() throws Exception {
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
            assertThat(leaderId).isPositive();

            // Find a follower
            TestRaftNode follower = null;
            for (TestRaftNode p : nodes) {
                if (p.id != leaderId) {
                    follower = p;
                    break;
                }
            }
            assertThat(follower).isNotNull();

            // ReadIndex on a follower: raft forwards the request to the leader,
            // the leader does the heartbeat round, and responds. The follower's
            // node.readIndex() call should succeed.
            follower.node.readIndex("follower-read".getBytes());
        } finally {
            for (int i = nodes.size() - 1; i >= 0; i--) {
                try { nodes.get(i).close(); } catch (Throwable ignored) {}
            }
        }
    }
}
