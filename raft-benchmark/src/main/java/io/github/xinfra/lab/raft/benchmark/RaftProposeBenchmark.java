/*
 * Copyright 2024-2026 The x-raft-lib Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * SPDX-License-Identifier: Apache-2.0
 */
package io.github.xinfra.lab.raft.benchmark;

import io.github.xinfra.lab.raft.RaftStateType;
import io.github.xinfra.lab.raft.examples.KvServer;
import io.github.xinfra.lab.raft.examples.proto.KvCommand;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

@BenchmarkMode({Mode.Throughput, Mode.SampleTime})
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 5)
@Measurement(iterations = 5, time = 10)
@Fork(1)
@Threads(1)
public class RaftProposeBenchmark {

    @AuxCounters(AuxCounters.Type.EVENTS)
    @State(Scope.Thread)
    public static class ErrorCounters {
        public long success;
        public long errors;
    }

    @State(Scope.Benchmark)
    public static class ClusterState {

        @Param({"1024", "2048", "4096", "8192", "16384", "32768", "65536"})
        int payloadSize;

        KvServer[] servers;
        KvServer leader;
        Path tmpDir;
        KvCommand cmd;
        private final AtomicLong keySeq = new AtomicLong();

        @Setup(Level.Trial)
        public void setUp() throws Exception {
            tmpDir = Files.createTempDirectory("raft-bench-propose-");

            int[] raftPorts = findFreePorts(3);
            int[] kvPorts = findFreePorts(3);
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

            char[] padding = new char[payloadSize];
            Arrays.fill(padding, 'x');
            String value = new String(padding);
            cmd = KvCommand.newBuilder()
                    .setOp(KvCommand.Op.PUT)
                    .setKey("bench")
                    .setValue(value)
                    .build();

            leader = waitForLeader(servers, 30_000);
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
    public void propose(ClusterState state, ErrorCounters counters) {
        try {
            state.leader.proposeCommand(state.cmd).get(5, TimeUnit.SECONDS);
            counters.success++;
        } catch (Exception e) {
            counters.errors++;
        }
    }

    static KvServer waitForLeader(KvServer[] servers, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            for (KvServer s : servers) {
                if (s.status().state == RaftStateType.StateLeader) {
                    return s;
                }
            }
            Thread.sleep(200);
        }
        throw new IllegalStateException("no leader elected within " + timeoutMs + "ms");
    }

    static int[] findFreePorts(int count) throws IOException {
        int[] ports = new int[count];
        ServerSocket[] sockets = new ServerSocket[count];
        for (int i = 0; i < count; i++) {
            sockets[i] = new ServerSocket(0);
            ports[i] = sockets[i].getLocalPort();
        }
        for (ServerSocket s : sockets) s.close();
        return ports;
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(RaftProposeBenchmark.class.getSimpleName())
                .result("benchmark-result.json")
                .resultFormat(org.openjdk.jmh.results.format.ResultFormatType.JSON)
                .build();
        new Runner(opt).run();
    }
}
