/*
 * Copyright 2024-2026 The x-raft-lib Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * SPDX-License-Identifier: Apache-2.0
 */
package io.github.xinfra.lab.raft.tests;

import io.github.xinfra.lab.raft.RaftStateType;
import io.github.xinfra.lab.raft.examples.RaftPeer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static io.github.xinfra.lab.raft.tests.IntegrationTestSupport.awaitTrue;
import static io.github.xinfra.lab.raft.tests.IntegrationTestSupport.freePorts;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Single-node smoke test against a real grpc transport + a real RocksDB
 * storage. Proves that the raft-core protocol drives the I/O modules to
 * boot, elect, propose, and apply.
 */
class SingleNodeIntegrationTest {

    @TempDir Path tmp;

    @Test
    void singleNodeBootsAndApplies() throws Exception {
        int[] ports = freePorts(1);
        ConcurrentHashMap<Long, byte[]> applied = new ConcurrentHashMap<>();

        try (RaftPeer p = new RaftPeer(
                1L,
                ports[0],
                tmp.resolve("p1"),
                Map.of(1L, "localhost:" + ports[0]),
                /*bootstrap=*/ true,
                applied::put)) {

            assertThat(awaitTrue(() -> p.basicStatus().state == RaftStateType.StateLeader, 5_000))
                    .as("single node must elect itself").isTrue();

            p.propose("hello".getBytes());
            assertThat(awaitTrue(() -> !applied.isEmpty(), 5_000)).isTrue();
            assertThat(applied).hasSize(1);
            assertThat(applied.values().iterator().next()).isEqualTo("hello".getBytes());
        }
    }

    @Test
    void singleNodeAppliesMultipleProposalsInOrder() throws Exception {
        int[] ports = freePorts(1);
        java.util.List<byte[]> appliedInOrder = java.util.Collections.synchronizedList(new java.util.ArrayList<>());

        try (RaftPeer p = new RaftPeer(
                1L,
                ports[0],
                tmp.resolve("p1"),
                Map.of(1L, "localhost:" + ports[0]),
                true,
                (idx, data) -> appliedInOrder.add(data))) {

            assertThat(awaitTrue(() -> p.basicStatus().state == RaftStateType.StateLeader, 5_000)).isTrue();

            for (int i = 0; i < 25; i++) {
                p.propose(("v" + i).getBytes());
            }
            assertThat(awaitTrue(() -> appliedInOrder.size() == 25, 10_000))
                    .as("all 25 proposals must apply").isTrue();
            for (int i = 0; i < 25; i++) {
                assertThat(new String(appliedInOrder.get(i))).isEqualTo("v" + i);
            }
        }
    }
}
