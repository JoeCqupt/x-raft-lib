/*
 * Copyright 2024-2026 The x-raft-lib Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * SPDX-License-Identifier: Apache-2.0
 */
package io.github.xinfra.lab.raft.tests;

import io.github.xinfra.lab.raft.examples.RaftPeer;
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
 * End-to-end cross-node snapshot install over the real gRPC transport and
 * RocksDB storage.
 *
 * <p>Scenario: a 3-node cluster where one follower is partitioned away. The
 * remaining two (a majority) keep committing proposals; the leader then takes
 * a snapshot and compacts its log past the partitioned follower's match index.
 * When the partition heals, the leader can no longer replicate the missing
 * entries one by one — it must ship a {@code MsgSnapshot}. The follower
 * installs it, truncates its log, and resumes replication, converging on the
 * cluster state. Post-heal proposals must then apply on all three nodes,
 * including the one that recovered via snapshot.
 */
class SnapshotInstallIntegrationTest {

    @TempDir Path tmp;

    @Test
    void partitionedFollowerCatchesUpViaSnapshot() throws Exception {
        int[] ports = freePorts(3);
        Map<Long, String> peers = peerMap(ports);
        ChaosController chaos = new ChaosController();

        Map<Long, ConcurrentLinkedQueue<String>> applyLogs = new ConcurrentHashMap<>();
        for (long id = 1; id <= 3; id++) applyLogs.put(id, new ConcurrentLinkedQueue<>());

        List<RaftPeer> nodes = new ArrayList<>();
        try {
            for (long id = 1; id <= 3; id++) {
                long fid = id;
                nodes.add(chaosPeer(fid, ports, peers, tmp.resolve("p" + fid), true,
                        (idx, data) -> applyLogs.get(fid).add(new String(data)), chaos));
            }

            long leaderId = awaitLeader(nodes, 10_000);
            assertThat(leaderId).as("cluster must elect a leader").isPositive();

            // Partition follower #3 away from the rest. Pick a victim that is
            // not the current leader so the majority {leader, other} keeps
            // committing.
            long victim = pickFollower(nodes, leaderId);
            chaos.isolate(victim);

            // The remaining majority keeps committing a sizable batch.
            RaftPeer leader = findLeader(nodes);
            assertThat(leader).isNotNull();
            int batch = 60;
            for (int i = 0; i < batch; i++) {
                leader.propose(("pre-" + i).getBytes());
            }

            // Wait until the two connected nodes have applied the batch.
            assertThat(awaitTrue(() -> nodes.stream()
                            .filter(p -> p.id != victim)
                            .allMatch(p -> p.basicStatus().applied >= batch),
                    15_000))
                    .as("connected majority must apply the pre-partition batch").isTrue();

            // Snapshot + compact on every connected node so the log entries the
            // victim is missing are gone — forcing a snapshot install on heal.
            for (RaftPeer p : nodes) {
                if (p.id != victim) {
                    p.forceSnapshotAndCompact(("state@" + p.basicStatus().applied).getBytes());
                }
            }

            long leaderCommit = findLeader(nodes).basicStatus().commit;

            // Heal the partition; the victim must now catch up via snapshot.
            chaos.heal(victim);

            RaftPeer recovered = nodes.stream().filter(p -> p.id == victim).findFirst().orElseThrow();
            assertThat(awaitTrue(() -> recovered.basicStatus().commit >= leaderCommit, 25_000))
                    .as("partitioned follower must catch up to commit %d via snapshot", leaderCommit)
                    .isTrue();

            // Zero-copy assertions: the snapshot travelled out-of-band, so the
            // payload must live in a side-car file, NOT inline in the snapshot.
            // (a) The recovered follower's stored snapshot carries metadata only.
            assertThat(recovered.storage.snapshot().getData().isEmpty())
                    .as("recovered follower snapshot must be metadata-only (payload out-of-band)")
                    .isTrue();
            assertThat(recovered.storage.snapshot().getMetadata().getIndex())
                    .as("recovered follower must have a real snapshot index").isPositive();
            // (b) A side-car payload file exists under the victim's snapshots dir.
            Path victimSnapDir = tmp.resolve("p" + victim).resolve("snapshots");
            try (var stream = java.nio.file.Files.list(victimSnapDir)) {
                boolean hasSidecar = stream.anyMatch(f -> {
                    String n = f.getFileName().toString();
                    return n.startsWith("snap-") && n.endsWith(".data");
                });
                assertThat(hasSidecar)
                        .as("a snap-*.data side-car must exist under %s", victimSnapDir).isTrue();
            }
            // (c) The leader's snapshot is metadata-only as well.
            assertThat(findLeader(nodes).storage.snapshot().getData().isEmpty())
                    .as("leader snapshot must be metadata-only (created via streaming)").isTrue();

            // Post-heal proposals apply on all three, recovered node included.
            RaftPeer leader2 = findLeader(nodes);
            assertThat(leader2).isNotNull();
            int post = 10;
            for (int i = 0; i < post; i++) {
                leader2.propose(("post-" + i).getBytes());
            }
            assertThat(awaitTrue(() -> nodes.stream().allMatch(p -> {
                List<String> seen = new ArrayList<>(applyLogs.get(p.id));
                return seen.contains("post-" + (post - 1));
            }), 20_000)).as("all nodes must apply post-heal proposals").isTrue();

            // The recovered node adopted the snapshot, so it never replayed the
            // compacted "pre-*" entries — it should have the post-heal ones.
            List<String> recoveredSeen = new ArrayList<>(applyLogs.get(victim));
            assertThat(recoveredSeen).contains("post-0", "post-" + (post - 1));
        } finally {
            for (int i = nodes.size() - 1; i >= 0; i--) {
                try { nodes.get(i).close(); } catch (Throwable ignored) {}
            }
        }
    }

    private static long pickFollower(List<RaftPeer> nodes, long leaderId) {
        for (RaftPeer p : nodes) {
            if (p.id != leaderId) return p.id;
        }
        throw new IllegalStateException("no follower found");
    }
}
