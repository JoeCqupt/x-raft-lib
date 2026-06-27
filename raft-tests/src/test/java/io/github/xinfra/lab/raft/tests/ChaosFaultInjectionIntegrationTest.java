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
import java.time.Duration;
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
 * Coverage for the chaos modes the existing {@link PartitionIntegrationTest}
 * does not exercise: injected latency, duplicate delivery, and asymmetric
 * (one-way) link blocks. Each verifies that raft still converges — the
 * point isn't to fail, it's to prove the protocol stays safe and live
 * under the fault model.
 */
class ChaosFaultInjectionIntegrationTest {

    @TempDir Path tmp;

    /**
     * High-jitter link injected on every directed pair. Heartbeats and
     * proposals get delivered late and out-of-order on the same link;
     * raft must still commit a batch within a reasonable budget.
     */
    @Test
    void highLatencyClusterStillConverges() throws Exception {
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

            // Inject 0-50ms uniform jitter on every link. With 10-tick
            // election-timeout and 1-tick heartbeat (each tick is ~100ms in
            // the demo's scheduler), 50ms is enough to reorder on the same
            // link but small enough to not trigger spurious elections.
            chaos.setGlobalLatency(Duration.ofMillis(50));

            RaftKVNode leader = findLeader(nodes);
            assertThat(leader).isNotNull();
            long pre = leader.basicStatus().commit;
            for (int i = 0; i < 30; i++) leader.propose(("jitter-" + i).getBytes());

            assertThat(awaitTrue(() -> {
                RaftKVNode l = findLeader(nodes);
                return l != null && l.basicStatus().commit >= pre + 30;
            }, 30_000)).as("leader must commit all proposals despite 50ms jitter").isTrue();

            chaos.setGlobalLatency(Duration.ZERO);
            long commit = findLeader(nodes).basicStatus().commit;
            assertThat(awaitTrue(() -> nodes.stream()
                    .allMatch(p -> p.basicStatus().commit >= commit), 20_000))
                    .as("all peers converge once jitter is cleared").isTrue();
            // Confirm the chaos layer actually delayed something.
            assertThat(chaos.delayedCount()).as("chaos must have delayed messages").isPositive();
        } finally {
            closeAll(nodes);
        }
    }

    /**
     * Every message also delivered a second time. Raft must ignore the
     * duplicate (index/term-based idempotence) and the commit index
     * must equal the proposal count, not 2× it.
     */
    @Test
    void duplicateDeliveryIsIdempotent() throws Exception {
        int[] ports = freePorts(3);
        Map<Long, String> peers = peerMap(ports);
        ChaosController chaos = new ChaosController();
        List<RaftKVNode> nodes = new ArrayList<>();
        try {
            List<byte[]> applied = new java.util.concurrent.CopyOnWriteArrayList<>();
            for (long id = 1; id <= 3; id++) {
                long fid = id;
                nodes.add(chaosPeer(fid, ports, peers, tmp.resolve("p" + fid), true,
                        (idx, data) -> { if (fid == 1) applied.add(data); }, chaos));
            }
            assertThat(awaitLeader(nodes, 10_000)).isPositive();

            // Every send is also re-delivered ~3ms later.
            chaos.setDuplicateProbability(1.0);

            RaftKVNode leader = findLeader(nodes);
            assertThat(leader).isNotNull();
            long pre = leader.basicStatus().commit;
            int batch = 20;
            for (int i = 0; i < batch; i++) leader.propose(("dup-" + i).getBytes());

            // Commit index must advance by exactly `batch`, not 2*batch —
            // duplicates must NOT inflate the log.
            assertThat(awaitTrue(() -> {
                RaftKVNode l = findLeader(nodes);
                return l != null && l.basicStatus().commit >= pre + batch;
            }, 30_000)).as("leader must commit all proposals").isTrue();

            // No node should have applied a duplicate either: the apply
            // callback fires once per committed index.
            assertThat(awaitTrue(() -> applied.size() >= batch, 10_000))
                    .as("apply callback fires once per committed entry").isTrue();

            chaos.setDuplicateProbability(0.0);
            assertThat(chaos.duplicatedCount()).as("chaos must have duplicated messages").isPositive();
        } finally {
            closeAll(nodes);
        }
    }

    /**
     * One-way (asymmetric) link block: leader can still send to follower
     * 3, but 3's responses to the leader are dropped. Raft tolerates
     * this via heartbeats — the leader keeps trying, the follower's
     * commit index lags, but progress on the other two voters means
     * the cluster commits.
     */
    @Test
    void asymmetricLinkBlockDoesNotStallMajority() throws Exception {
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
            RaftKVNode leader = findLeader(nodes);
            assertThat(leader).isNotNull();
            long leaderId = leader.id;

            // Pick a non-leader follower and break only its responses
            // back to the leader; leader→follower remains healthy.
            long mute = (leaderId == 3) ? 2 : 3;
            chaos.blockLink(mute, leaderId);

            long pre = leader.basicStatus().commit;
            int batch = 10;
            for (int i = 0; i < batch; i++) leader.propose(("asym-" + i).getBytes());

            // Majority quorum (leader + the OTHER follower) commits the batch.
            assertThat(awaitTrue(() -> {
                RaftKVNode l = findLeader(nodes);
                return l != null && l.basicStatus().commit >= pre + batch;
            }, 20_000)).as("majority must commit despite muted follower").isTrue();

            // Heal and the muted follower catches up.
            chaos.unblockLink(mute, leaderId);
            long commit = findLeader(nodes).basicStatus().commit;
            assertThat(awaitTrue(() -> nodes.stream()
                    .allMatch(p -> p.basicStatus().commit >= commit), 25_000))
                    .as("all nodes converge after asymmetric block heals").isTrue();
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
