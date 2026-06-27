/*
 * Copyright 2024-2026 The x-raft-lib Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * SPDX-License-Identifier: Apache-2.0
 */
package io.github.xinfra.lab.raft.tests;


import io.github.xinfra.lab.raft.tests.chaos.ChaosController;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedInputStream;
import java.io.InputStream;
import java.nio.file.Files;
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
import static io.github.xinfra.lab.raft.tests.IntegrationTestSupport.forceSnapshotAndCompact;
import static io.github.xinfra.lab.raft.tests.IntegrationTestSupport.findLeaderOrWait;
import static io.github.xinfra.lab.raft.tests.IntegrationTestSupport.freePorts;
import static io.github.xinfra.lab.raft.tests.IntegrationTestSupport.peerMap;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Proves the snapshot payload travels end-to-end <i>out-of-band</i> (zero-copy):
 * a large, verifiable payload is written to the leader's side-car, streamed to a
 * partitioned follower Storage→Storage, and lands byte-for-byte in the
 * follower's side-car — while the {@code MsgSnapshot} that crosses the wire (and
 * the snapshots persisted on both ends) carry metadata only, never the bytes.
 *
 * <p>The payload ({@value #PAYLOAD_BYTES} bytes) is far larger than the gRPC
 * snapshot chunk size, so a working install necessarily exercised chunking +
 * piped reassembly without ever buffering the whole blob. Verification streams
 * the recovered side-car and checks every byte against a position-dependent
 * generator, so any truncation, reordering, or corruption fails the test.
 */
class ZeroCopySnapshotStreamingTest {

    /** 64 MiB: large enough to force multi-chunk streaming, small enough for CI heap. */
    private static final int PAYLOAD_BYTES = 64 << 20;

    @TempDir Path tmp;

    @Test
    void largeSnapshotStreamsOutOfBandByteForByte() throws Exception {
        int[] ports = freePorts(3);
        Map<Long, String> peers = peerMap(ports);
        ChaosController chaos = new ChaosController();

        Map<Long, ConcurrentLinkedQueue<String>> applyLogs = new ConcurrentHashMap<>();
        for (long id = 1; id <= 3; id++) applyLogs.put(id, new ConcurrentLinkedQueue<>());

        List<TestRaftNode> nodes = new ArrayList<>();
        try {
            for (long id = 1; id <= 3; id++) {
                long fid = id;
                nodes.add(chaosPeer(fid, ports, peers, tmp.resolve("p" + fid), true,
                        (idx, data) -> applyLogs.get(fid).add(new String(data)), chaos));
            }

            long leaderId = awaitLeader(nodes, 10_000);
            assertThat(leaderId).as("cluster must elect a leader").isPositive();

            long victim = pickFollower(nodes, leaderId);
            chaos.isolate(victim);

            TestRaftNode leader = findLeader(nodes);
            assertThat(leader).isNotNull();
            int batch = 30;
            for (int i = 0; i < batch; i++) {
                leader.propose(("pre-" + i).getBytes());
            }
            assertThat(awaitTrue(() -> nodes.stream()
                            .filter(p -> p.id != victim)
                            .allMatch(p -> p.basicStatus().applied >= batch),
                    15_000))
                    .as("connected majority must apply the pre-partition batch").isTrue();

            // Large, verifiable application state captured in the snapshot.
            byte[] payload = makePayload(PAYLOAD_BYTES);

            // Snapshot + compact on the connected nodes so the victim can only
            // catch up via a snapshot install; the leader's snapshot carries the
            // large payload (out-of-band, in its side-car).
            for (TestRaftNode p : nodes) {
                if (p.id != victim) {
                    forceSnapshotAndCompact(p, payload);
                }
            }

            // forceSnapshotAndCompact can briefly stall the event loop and
            // make the incumbent leader step down before re-winning, so wait
            // past the gap rather than snapshotting the cluster mid-election.
            TestRaftNode leaderAfterCompact = findLeaderOrWait(nodes, 5_000);
            assertThat(leaderAfterCompact).as("a leader must be visible after snapshot+compact").isNotNull();
            long leaderCommit = leaderAfterCompact.basicStatus().commit;

            chaos.heal(victim);

            TestRaftNode recovered = nodes.stream().filter(p -> p.id == victim).findFirst().orElseThrow();
            // Raft considers the snapshot installed (commit advances) the moment
            // it processes MsgSnapshot in-memory, BUT the Storage.applySnapshot
            // write happens on the next Ready cycle when the host drains
            // rd.snapshot through writeBatched. Wait for BOTH layers — the test
            // is asserting on storage state, so polling only commit leaks the
            // gap between in-memory and on-disk snapshot.
            assertThat(awaitTrue(() -> {
                if (recovered.basicStatus().commit < leaderCommit) return false;
                try {
                    return recovered.storage.snapshot().getMetadata().getIndex() > 0;
                } catch (Exception e) {
                    return false;
                }
            }, 30_000))
                    .as("partitioned follower must catch up to commit %d AND persist a snapshot " +
                            "(otherwise the snapshot-install path was bypassed by log replication)",
                            leaderCommit)
                    .isTrue();

            // Inline data is empty on BOTH ends — the bytes never rode in the message.
            TestRaftNode leaderForInspection = findLeaderOrWait(nodes, 5_000);
            assertThat(leaderForInspection).as("a leader must be visible for snapshot inspection").isNotNull();
            assertThat(leaderForInspection.storage.snapshot().getData().isEmpty())
                    .as("leader snapshot must be metadata-only").isTrue();
            assertThat(recovered.storage.snapshot().getData().isEmpty())
                    .as("recovered follower snapshot must be metadata-only").isTrue();

            // The payload landed byte-for-byte in the victim's side-car file.
            Path sidecar = findSidecar(tmp.resolve("p" + victim).resolve("snapshots"));
            assertThat(Files.size(sidecar))
                    .as("recovered side-car size must equal the payload size").isEqualTo(PAYLOAD_BYTES);
            assertVerifiable(sidecar, PAYLOAD_BYTES);

            // Cluster still makes progress post-heal on all three nodes.
            TestRaftNode leader2 = findLeader(nodes);
            assertThat(leader2).isNotNull();
            int post = 5;
            for (int i = 0; i < post; i++) {
                leader2.propose(("post-" + i).getBytes());
            }
            assertThat(awaitTrue(() -> nodes.stream().allMatch(p -> {
                List<String> seen = new ArrayList<>(applyLogs.get(p.id));
                return seen.contains("post-" + (post - 1));
            }), 20_000)).as("all nodes must apply post-heal proposals").isTrue();
        } finally {
            for (int i = nodes.size() - 1; i >= 0; i--) {
                try { nodes.get(i).close(); } catch (Throwable ignored) {}
            }
        }
    }

    /** Position-dependent generator so any truncation/reorder/corruption is caught. */
    private static byte genByte(long i) {
        return (byte) ((i ^ (i >>> 8) ^ (i >>> 16) ^ 0x5AL));
    }

    private static byte[] makePayload(int n) {
        byte[] b = new byte[n];
        for (int i = 0; i < n; i++) {
            b[i] = genByte(i);
        }
        return b;
    }

    private static void assertVerifiable(Path file, int expectedLen) throws Exception {
        byte[] buf = new byte[1 << 16];
        long pos = 0;
        try (InputStream in = new BufferedInputStream(Files.newInputStream(file))) {
            int n;
            while ((n = in.read(buf)) != -1) {
                for (int i = 0; i < n; i++) {
                    if (buf[i] != genByte(pos + i)) {
                        throw new AssertionError("side-car byte mismatch at offset " + (pos + i));
                    }
                }
                pos += n;
            }
        }
        assertThat(pos).as("side-car total bytes read").isEqualTo(expectedLen);
    }

    private static Path findSidecar(Path snapDir) throws Exception {
        try (var stream = Files.list(snapDir)) {
            return stream.filter(f -> {
                String n = f.getFileName().toString();
                return n.startsWith("snap-") && n.endsWith(".data");
            }).findFirst().orElseThrow(() ->
                    new AssertionError("no snap-*.data side-car under " + snapDir));
        }
    }

    private static long pickFollower(List<TestRaftNode> nodes, long leaderId) {
        for (TestRaftNode p : nodes) {
            if (p.id != leaderId) return p.id;
        }
        throw new IllegalStateException("no follower found");
    }
}
