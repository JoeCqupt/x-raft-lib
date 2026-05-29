/*
 * Copyright 2024-2026 The x-raft-lib Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * SPDX-License-Identifier: Apache-2.0
 */
package io.github.xinfra.lab.raft.examples;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Smoke test for {@link KvClusterDemo}: bring up a real 3-node cluster
 * (gRPC + RocksDB), run the scripted KV workload, and assert that every
 * node converges on the same final state.
 *
 * <p>This is the end-to-end exercise of the whole stack — raft-core for
 * consensus, raft-transport-grpc for the wire, raft-storage-rocksdb for
 * the raft log, and a RocksDB-backed application state machine.
 */
class KvClusterDemoTest {

    @TempDir Path tmp;

    @Test
    void threeNodeClusterConvergesOnScriptedWorkload() throws Exception {
        KvClusterDemo.Result r = KvClusterDemo.run(3, tmp);

        // A leader must have been elected.
        assertThat(r.leaderId).as("a leader must be elected").isPositive();

        // Every node holds an identical KV snapshot.
        assertThat(r.allConverged()).as("all nodes must converge").isTrue();

        // And that snapshot matches the scripted workload's expected
        // final state: user:1 overwritten, config:mode set, user:2 deleted.
        for (Map.Entry<Long, Map<String, String>> e : r.perNode.entrySet()) {
            Map<String, String> kv = e.getValue();
            assertThat(kv).as("node %d final KV", e.getKey())
                    .containsEntry("user:1", "alice-updated")
                    .containsEntry("config:mode", "demo")
                    .doesNotContainKey("user:2");
        }
    }
}
