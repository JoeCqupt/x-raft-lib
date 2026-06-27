/*
 * Copyright 2024-2026 The x-raft-lib Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * SPDX-License-Identifier: Apache-2.0
 */
package io.github.xinfra.lab.raft.tests;

import io.github.xinfra.lab.raft.RaftStateType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

import static io.github.xinfra.lab.raft.tests.IntegrationTestSupport.awaitTrue;
import static io.github.xinfra.lab.raft.tests.IntegrationTestSupport.freePorts;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that RocksDB persistence survives a process-equivalent
 * restart of a raft node. Workflow:
 *
 * <ol>
 *   <li>Start a single-node cluster, propose entries, wait for apply.</li>
 *   <li>Close the peer (closes grpc transport + flushes/closes RocksDB).</li>
 *   <li>Re-open the same RocksDB directory in {@code bootstrap=false}
 *       mode. The new peer must recover commit / applied state from
 *       disk and resume serving without re-applying old entries.</li>
 * </ol>
 *
 * <p>Critically, the apply callback in phase 2 must NOT see any of the
 * phase-1 entries — those are below the persisted {@code applied}
 * watermark.
 */
class RestartFromDiskIntegrationTest {

    @TempDir Path tmp;

    @Test
    void singleNodeRecoversCommittedStateAfterRestart() throws Exception {
        int[] ports = freePorts(1);
        Path storageDir = tmp.resolve("storage");
        Map<Long, String> peers = Map.of(1L, "localhost:" + ports[0]);

        // ------ Phase 1: bootstrap, propose 3 entries, observe apply ------
        ConcurrentLinkedQueue<String> phase1Apply = new ConcurrentLinkedQueue<>();
        long phase1LastApplied;
        long phase1Commit;
        try (TestRaftNode p = new TestRaftNode(
                1L, ports[0], storageDir, peers, /*bootstrap=*/ true,
                (idx, data) -> phase1Apply.add(new String(data)))) {

            assertThat(awaitTrue(() -> p.basicStatus().state == RaftStateType.StateLeader, 5_000)).isTrue();
            for (int i = 0; i < 3; i++) {
                p.propose(("v" + i).getBytes());
            }
            assertThat(awaitTrue(() -> phase1Apply.size() == 3, 5_000)).isTrue();

            phase1LastApplied = p.basicStatus().applied;
            phase1Commit = p.basicStatus().commit;
            assertThat(phase1LastApplied).as("applied must advance past bootstrap entries").isGreaterThanOrEqualTo(3);
            assertThat(phase1Commit).isGreaterThanOrEqualTo(phase1LastApplied);
        }

        // ------ Phase 2: re-open same dir, expect zero re-applies ------
        ConcurrentLinkedQueue<String> phase2Apply = new ConcurrentLinkedQueue<>();
        // Use a fresh port; transport identity is independent of the
        // raft state, which is what we're testing for persistence.
        int[] ports2 = freePorts(1);
        Map<Long, String> peers2 = Map.of(1L, "localhost:" + ports2[0]);
        try (TestRaftNode p2 = new TestRaftNode(
                1L, ports2[0], storageDir, peers2, /*bootstrap=*/ false,
                (idx, data) -> phase2Apply.add(new String(data)))) {

            // The node should re-elect itself (single-node cluster) within
            // a couple of election timeouts using the recovered state.
            assertThat(awaitTrue(() -> p2.basicStatus().state == RaftStateType.StateLeader, 5_000))
                    .as("restarted single node must re-elect").isTrue();

            // Recovered commit/applied should be at least where we left off.
            assertThat(p2.basicStatus().commit).isGreaterThanOrEqualTo(phase1Commit);
            assertThat(p2.basicStatus().applied).isGreaterThanOrEqualTo(phase1LastApplied);

            // CRITICAL: applying old entries on restart would corrupt the
            // host state machine. The applier must not see anything until
            // we propose a NEW entry.
            //
            // We give it some time to (incorrectly) re-apply, then assert.
            Thread.sleep(300);
            assertThat(phase2Apply).as("no re-apply of phase-1 entries").isEmpty();

            // Propose a new entry and verify it applies normally.
            p2.propose("after-restart".getBytes());
            assertThat(awaitTrue(() -> !phase2Apply.isEmpty(), 5_000)).isTrue();
            assertThat(phase2Apply).containsExactly("after-restart");
        }
    }
}
