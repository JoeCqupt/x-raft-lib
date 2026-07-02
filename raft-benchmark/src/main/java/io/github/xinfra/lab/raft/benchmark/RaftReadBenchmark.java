/*
 * Copyright 2024-2026 The x-raft-lib Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * SPDX-License-Identifier: Apache-2.0
 */
package io.github.xinfra.lab.raft.benchmark;

import io.github.xinfra.lab.raft.examples.KvServer;
import io.github.xinfra.lab.raft.examples.proto.KvCommand;
import org.openjdk.jmh.annotations.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

@BenchmarkMode(Mode.SampleTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 5)
@Measurement(iterations = 5, time = 10)
@Fork(1)
@Threads(1)
public class RaftReadBenchmark {

    @AuxCounters(AuxCounters.Type.EVENTS)
    @State(Scope.Thread)
    public static class ErrorCounters {
        public long success;
        public long errors;
    }

    @State(Scope.Benchmark)
    public static class ClusterState {

        KvServer[] servers;
        KvServer leader;
        Path tmpDir;

        @Setup(Level.Trial)
        public void setUp() throws Exception {
            tmpDir = Files.createTempDirectory("raft-bench-read-");

            int[] raftPorts = RaftProposeBenchmark.findFreePorts(3);
            int[] kvPorts = RaftProposeBenchmark.findFreePorts(3);
            Map<Long, String> peers = new LinkedHashMap<>();
            peers.put(1L, "localhost:" + raftPorts[0]);
            peers.put(2L, "localhost:" + raftPorts[1]);
            peers.put(3L, "localhost:" + raftPorts[2]);

            servers = new KvServer[3];
            for (int i = 0; i < 3; i++) {
                long id = i + 1;
                Path dir = tmpDir.resolve("node-" + id);
                Files.createDirectories(dir);
                servers[i] = new KvServer(id, raftPorts[i], kvPorts[i], dir, peers, true);
            }

            leader = RaftProposeBenchmark.waitForLeader(servers, 30_000);

            for (int i = 0; i < 100; i++) {
                KvCommand cmd = KvCommand.newBuilder()
                        .setOp(KvCommand.Op.PUT)
                        .setKey("read-key-" + i)
                        .setValue("value-" + i)
                        .build();
                leader.proposeCommand(cmd).get(10, TimeUnit.SECONDS);
            }
        }

        @TearDown(Level.Trial)
        public void tearDown() throws Exception {
            if (servers != null) {
                for (int i = servers.length - 1; i >= 0; i--) {
                    if (servers[i] != null) {
                        try { servers[i].close(); } catch (Exception ignored) {}
                    }
                }
            }
            Thread.sleep(500);
            if (tmpDir != null) {
                try (Stream<Path> walk = Files.walk(tmpDir)) {
                    walk.sorted(Comparator.reverseOrder()).forEach(p -> {
                        try { Files.deleteIfExists(p); } catch (IOException ignored) {}
                    });
                }
            }
        }
    }

    @Benchmark
    public void linearizableGet(ClusterState state, ErrorCounters counters) {
        try {
            state.leader.linearizableGet("read-key-42").get(5, TimeUnit.SECONDS);
            counters.success++;
        } catch (Exception e) {
            counters.errors++;
        }
    }
}
