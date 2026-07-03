/*
 * Copyright 2024-2026 The x-raft-lib Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * SPDX-License-Identifier: Apache-2.0
 */
package io.github.xinfra.lab.raft.benchmark;

import io.github.xinfra.lab.raft.examples.KvServer;
import io.github.xinfra.lab.raft.examples.proto.KvCommand;
import org.openjdk.jmh.annotations.AuxCounters;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Stream;

@Warmup(iterations = 3, time = 5)
@Measurement(iterations = 5, time = 10)
@Fork(1)
@Threads(1)
public class RaftReadBenchmark {

    // ======== Shared cluster state ========

    @State(Scope.Benchmark)
    public static class ClusterState {

        static final int MAX_INFLIGHT = 1024;
        static final int READ_KEY_COUNT = 100;

        @Param({"0", "100", "500", "1000", "2000"})
        int proposeQps;

        KvServer[] servers;
        KvServer leader;
        Path tmpDir;
        Semaphore inflight;
        ScheduledExecutorService bgProposer;
        private final AtomicLong bgKeySeq = new AtomicLong();

        @Setup(Level.Trial)
        public void setUp() throws Exception {
            tmpDir = Files.createTempDirectory("raft-bench-read-");
            inflight = new Semaphore(MAX_INFLIGHT);

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

            for (int i = 0; i < READ_KEY_COUNT; i++) {
                KvCommand cmd = KvCommand.newBuilder()
                        .setOp(KvCommand.Op.PUT)
                        .setKey("read-key-" + i)
                        .setValue("value-" + i)
                        .build();
                leader.proposeCommand(cmd).get(10, TimeUnit.SECONDS);
            }

            if (proposeQps > 0) {
                bgProposer = Executors.newSingleThreadScheduledExecutor(r -> {
                    Thread t = new Thread(r, "bg-proposer");
                    t.setDaemon(true);
                    return t;
                });
                long periodUs = 1_000_000L / proposeQps;
                bgProposer.scheduleAtFixedRate(this::backgroundPropose, 0, periodUs, TimeUnit.MICROSECONDS);
            }
        }

        private void backgroundPropose() {
            long seq = bgKeySeq.incrementAndGet();
            KvCommand cmd = KvCommand.newBuilder()
                    .setOp(KvCommand.Op.PUT)
                    .setKey("bg-key-" + (seq % 1000))
                    .setValue("bg-val-" + seq)
                    .build();
            leader.proposeCommand(cmd);
        }

        @TearDown(Level.Trial)
        public void tearDown() throws Exception {
            if (bgProposer != null) {
                bgProposer.shutdownNow();
                bgProposer.awaitTermination(5, TimeUnit.SECONDS);
            }

            try {
                inflight.acquire(MAX_INFLIGHT);
                inflight.release(MAX_INFLIGHT);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

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

    // ======== Sync counters (per-thread, safe for JMH AuxCounters) ========

    @AuxCounters(AuxCounters.Type.EVENTS)
    @State(Scope.Thread)
    public static class SyncCounters {
        public long success;
        public long errors;
        public long timeouts;
    }

    // ======== Async metrics (shared, thread-safe via LongAdder) ========

    @State(Scope.Benchmark)
    public static class AsyncMetrics {
        final LongAdder success = new LongAdder();
        final LongAdder errors = new LongAdder();
        final LongAdder timeouts = new LongAdder();
        final LongAdder totalLatencyUs = new LongAdder();

        @TearDown(Level.Iteration)
        public void report() {
            long s = success.sumThenReset();
            long e = errors.sumThenReset();
            long t = timeouts.sumThenReset();
            long lat = totalLatencyUs.sumThenReset();
            System.out.printf("[async read] success=%d errors=%d timeouts=%d avgLatency=%.1fus%n",
                    s, e, t, s > 0 ? (double) lat / s : 0);
        }
    }

    // ======== Sync read ========

    @Benchmark
    @BenchmarkMode({Mode.Throughput, Mode.SampleTime})
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void syncRead(ClusterState state, SyncCounters counters) {
        String key = "read-key-" + ThreadLocalRandom.current().nextInt(ClusterState.READ_KEY_COUNT);
        try {
            state.leader.linearizableGet(key).get(5, TimeUnit.SECONDS);
            counters.success++;
        } catch (TimeoutException e) {
            counters.timeouts++;
        } catch (ExecutionException e) {
            counters.errors++;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            counters.errors++;
        }
    }

    // ======== Async read ========

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void asyncRead(ClusterState state, AsyncMetrics metrics) {
        long startNs = System.nanoTime();
        state.inflight.acquireUninterruptibly();
        String key = "read-key-" + ThreadLocalRandom.current().nextInt(ClusterState.READ_KEY_COUNT);
        state.leader.linearizableGet(key)
                .orTimeout(5, TimeUnit.SECONDS)
                .whenComplete((v, ex) -> {
                    state.inflight.release();
                    long latencyUs = (System.nanoTime() - startNs) / 1000;
                    if (ex != null) {
                        Throwable cause = ex instanceof CompletionException ? ex.getCause() : ex;
                        if (cause instanceof TimeoutException) {
                            metrics.timeouts.increment();
                        } else {
                            metrics.errors.increment();
                        }
                    } else {
                        metrics.success.increment();
                        metrics.totalLatencyUs.add(latencyUs);
                    }
                });
    }

    // ======== Main ========

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(RaftReadBenchmark.class.getSimpleName())
                .result("benchmark-result.json")
                .resultFormat(org.openjdk.jmh.results.format.ResultFormatType.JSON)
                .build();
        new Runner(opt).run();
    }
}
