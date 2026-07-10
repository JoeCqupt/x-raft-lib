/*
 * Copyright 2024-2026 The x-raft-lib Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * SPDX-License-Identifier: Apache-2.0
 */
package io.github.xinfra.lab.raft.benchmark;

import io.github.xinfra.lab.raft.examples.KvServer;
import io.github.xinfra.lab.raft.examples.proto.KvCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Stream;

/**
 * Rate-sweep capacity benchmark for finding the real service limits of
 * Raft propose and linearizable read operations.
 *
 * <p>Unlike JMH (which fires as fast as possible), this benchmark sends
 * requests at a controlled, fixed rate and measures success throughput,
 * error rate, and latency percentiles at each rate. By sweeping from low
 * to high rates, the saturation point — where errors appear or latency
 * spikes — becomes clearly visible.
 *
 * <p>Not a JMH benchmark. Run directly via {@code main()}.
 */
public class RaftCapacityBenchmark {

    private static final Logger LOG = LoggerFactory.getLogger(RaftCapacityBenchmark.class);

    static final int WARMUP_SECONDS = 5;
    static final int MEASURE_SECONDS = 30;
    static final int TIMEOUT_SECONDS = 5;
    static final int MAX_INFLIGHT = 1024;
    static final double ERROR_RATE_CUTOFF = 0.5;

    static final int[] PAYLOAD_SIZES = {128, 1024, 4096, 16384, 65536};
    static final int[] TARGET_RATES = {100, 500, 1000, 2000, 3000, 5000, 8000, 10000};
    static final int[] READ_TARGET_RATES = {100, 500, 1000, 2000, 3000, 5000, 8000, 10000};
    static final int READ_BG_WRITE_QPS = 500;
    static final int READ_KEY_COUNT = 100;

    // ======================== Result types ========================

    static class RunResult {
        final String type;
        final int payloadSize;
        final int targetRate;
        final long successCount;
        final long errorCount;
        final long timeoutCount;
        final long durationMs;
        final long[] latenciesUs;
        final int latencyCount;

        RunResult(String type, int payloadSize, int targetRate,
                  long successCount, long errorCount, long timeoutCount,
                  long durationMs, long[] latenciesUs, int latencyCount) {
            this.type = type;
            this.payloadSize = payloadSize;
            this.targetRate = targetRate;
            this.successCount = successCount;
            this.errorCount = errorCount;
            this.timeoutCount = timeoutCount;
            this.durationMs = durationMs;
            this.latenciesUs = latenciesUs;
            this.latencyCount = latencyCount;
        }

        double errorRate() {
            long total = successCount + errorCount + timeoutCount;
            return total == 0 ? 0 : (double) (errorCount + timeoutCount) / total;
        }

        double actualRate() {
            return durationMs == 0 ? 0 : successCount * 1000.0 / durationMs;
        }

        long percentile(double p) {
            if (latencyCount == 0) return 0;
            long[] sorted = Arrays.copyOf(latenciesUs, latencyCount);
            Arrays.sort(sorted);
            int idx = Math.min((int) (p / 100.0 * sorted.length), sorted.length - 1);
            return sorted[idx];
        }
    }

    // ======================== Core test runner ========================

    static RunResult runOneRate(KvServer leader, KvCommand cmd,
                                String type, int payloadSize, int targetRate) {
        Semaphore inflight = new Semaphore(MAX_INFLIGHT);
        long[] latencies = new long[targetRate * MEASURE_SECONDS + targetRate];
        AtomicInteger latIdx = new AtomicInteger(0);
        LongAdder warmupSuccess = new LongAdder();
        LongAdder measureSuccess = new LongAdder();
        LongAdder measureErrors = new LongAdder();
        LongAdder measureTimeouts = new LongAdder();
        AtomicBoolean measuring = new AtomicBoolean(false);
        AtomicBoolean stopped = new AtomicBoolean(false);

        long intervalUs = 1_000_000L / targetRate;

        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "rate-sender");
            t.setDaemon(true);
            return t;
        });

        scheduler.scheduleAtFixedRate(() -> {
            if (stopped.get()) return;
            if (!inflight.tryAcquire()) return;

            boolean isMeasuring = measuring.get();
            long sendNs = System.nanoTime();

            CompletableFuture<?> future;
            if ("read".equals(type)) {
                String key = "read-key-" + (int) (Math.random() * READ_KEY_COUNT);
                future = leader.linearizableGet(key);
            } else {
                future = leader.proposeCommand(cmd);
            }

            future.orTimeout(TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .whenComplete((v, ex) -> {
                    inflight.release();
                    long latencyUs = (System.nanoTime() - sendNs) / 1000;
                    if (!isMeasuring) {
                        if (ex == null) warmupSuccess.increment();
                        return;
                    }
                    if (ex != null) {
                        Throwable cause = ex instanceof CompletionException ? ex.getCause() : ex;
                        if (cause instanceof TimeoutException) {
                            measureTimeouts.increment();
                        } else {
                            measureErrors.increment();
                        }
                    } else {
                        measureSuccess.increment();
                        int i = latIdx.getAndIncrement();
                        if (i < latencies.length) {
                            latencies[i] = latencyUs;
                        }
                    }
                });
        }, 0, intervalUs, TimeUnit.MICROSECONDS);

        // Warmup phase
        try {
            Thread.sleep(WARMUP_SECONDS * 1000L);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // Measurement phase
        measuring.set(true);
        long measureStartMs = System.currentTimeMillis();
        try {
            Thread.sleep(MEASURE_SECONDS * 1000L);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        stopped.set(true);
        long measureDurationMs = System.currentTimeMillis() - measureStartMs;

        // Shutdown and drain
        scheduler.shutdown();
        try {
            scheduler.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        try {
            inflight.acquire(MAX_INFLIGHT);
            inflight.release(MAX_INFLIGHT);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        return new RunResult(type, payloadSize, targetRate,
            measureSuccess.sum(), measureErrors.sum(), measureTimeouts.sum(),
            measureDurationMs, latencies, latIdx.get());
    }

    // ======================== Propose sweep ========================

    static List<RunResult> runProposeSweep(KvServer leader) {
        List<RunResult> all = new ArrayList<>();

        for (int payloadSize : PAYLOAD_SIZES) {
            char[] padding = new char[payloadSize];
            Arrays.fill(padding, 'x');
            KvCommand cmd = KvCommand.newBuilder()
                .setOp(KvCommand.Op.PUT)
                .setKey("cap-bench")
                .setValue(new String(padding))
                .build();

            LOG.info("=== Propose capacity sweep: payload={}B ===", payloadSize);

            for (int rate : TARGET_RATES) {
                LOG.info("  rate={} ops/s ...", rate);
                RunResult r = runOneRate(leader, cmd, "propose", payloadSize, rate);
                all.add(r);

                LOG.info("  -> success={} errors={} timeouts={} actualRate={} p50={}us p99={}us errorRate={}%",
                    r.successCount, r.errorCount, r.timeoutCount,
                    String.format("%.0f", r.actualRate()),
                    r.percentile(50), r.percentile(99),
                    String.format("%.1f", r.errorRate() * 100));

                if (r.errorRate() > ERROR_RATE_CUTOFF) {
                    LOG.info("  -> error rate > {}%, stopping sweep for this payload",
                        (int) (ERROR_RATE_CUTOFF * 100));
                    break;
                }
            }
        }

        return all;
    }

    // ======================== Read sweep ========================

    static List<RunResult> runReadSweep(KvServer leader) {
        List<RunResult> all = new ArrayList<>();

        // Background proposer at fixed QPS
        ScheduledExecutorService bgProposer = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "bg-proposer");
            t.setDaemon(true);
            return t;
        });
        AtomicInteger bgSeq = new AtomicInteger();
        long bgIntervalUs = 1_000_000L / READ_BG_WRITE_QPS;
        bgProposer.scheduleAtFixedRate(() -> {
            int seq = bgSeq.incrementAndGet();
            KvCommand cmd = KvCommand.newBuilder()
                .setOp(KvCommand.Op.PUT)
                .setKey("bg-key-" + (seq % 1000))
                .setValue("bg-val-" + seq)
                .build();
            leader.proposeCommand(cmd);
        }, 0, bgIntervalUs, TimeUnit.MICROSECONDS);

        LOG.info("=== Read capacity sweep (bg write {}qps) ===", READ_BG_WRITE_QPS);

        KvCommand dummyCmd = KvCommand.getDefaultInstance();
        for (int rate : READ_TARGET_RATES) {
            LOG.info("  rate={} ops/s ...", rate);
            RunResult r = runOneRate(leader, dummyCmd, "read", 0, rate);
            all.add(r);

            LOG.info("  -> success={} errors={} timeouts={} actualRate={} p50={}us p99={}us errorRate={}%",
                r.successCount, r.errorCount, r.timeoutCount,
                String.format("%.0f", r.actualRate()),
                r.percentile(50), r.percentile(99),
                String.format("%.1f", r.errorRate() * 100));

            if (r.errorRate() > ERROR_RATE_CUTOFF) {
                LOG.info("  -> error rate > {}%, stopping sweep",
                    (int) (ERROR_RATE_CUTOFF * 100));
                break;
            }
        }

        bgProposer.shutdownNow();
        try {
            bgProposer.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        return all;
    }

    // ======================== JSON output ========================

    static void writeJson(List<RunResult> proposeResults, List<RunResult> readResults,
                          String outputPath) throws IOException {
        StringBuilder sb = new StringBuilder();
        sb.append("{\n");
        sb.append("  \"timestamp\": \"").append(Instant.now()).append("\",\n");

        sb.append("  \"propose\": [\n");
        appendResults(sb, proposeResults);
        sb.append("  ],\n");

        sb.append("  \"read\": [\n");
        appendResults(sb, readResults);
        sb.append("  ]\n");

        sb.append("}\n");

        try (PrintWriter pw = new PrintWriter(Files.newBufferedWriter(Path.of(outputPath)))) {
            pw.print(sb);
        }
        LOG.info("Results written to {}", outputPath);
    }

    private static void appendResults(StringBuilder sb, List<RunResult> results) {
        for (int i = 0; i < results.size(); i++) {
            RunResult r = results.get(i);
            sb.append("    {\n");
            sb.append("      \"payloadSize\": ").append(r.payloadSize).append(",\n");
            sb.append("      \"targetRate\": ").append(r.targetRate).append(",\n");
            sb.append("      \"actualRate\": ").append(String.format("%.1f", r.actualRate())).append(",\n");
            sb.append("      \"successCount\": ").append(r.successCount).append(",\n");
            sb.append("      \"errorCount\": ").append(r.errorCount).append(",\n");
            sb.append("      \"timeoutCount\": ").append(r.timeoutCount).append(",\n");
            sb.append("      \"errorRate\": ").append(String.format("%.4f", r.errorRate())).append(",\n");
            sb.append("      \"p50Us\": ").append(r.percentile(50)).append(",\n");
            sb.append("      \"p90Us\": ").append(r.percentile(90)).append(",\n");
            sb.append("      \"p99Us\": ").append(r.percentile(99)).append(",\n");
            sb.append("      \"p999Us\": ").append(r.percentile(99.9)).append(",\n");
            sb.append("      \"durationMs\": ").append(r.durationMs).append("\n");
            sb.append("    }");
            if (i < results.size() - 1) sb.append(",");
            sb.append("\n");
        }
    }

    // ======================== Console summary ========================

    static void printSummary(List<RunResult> proposeResults, List<RunResult> readResults) {
        if (!proposeResults.isEmpty()) {
            System.out.println("\n========== Propose Capacity Sweep ==========");
            System.out.printf("%-8s %8s %8s %8s %8s %10s %10s %10s %8s%n",
                "Payload", "Target", "Actual", "Success", "Errors", "p50(us)", "p99(us)", "p99.9(us)", "ErrRate");
            System.out.println("-".repeat(90));
            for (RunResult r : proposeResults) {
                System.out.printf("%-8s %8d %8.0f %8d %8d %10d %10d %10d %7.1f%%%n",
                    fmtBytes(r.payloadSize), r.targetRate, r.actualRate(),
                    r.successCount, r.errorCount + r.timeoutCount,
                    r.percentile(50), r.percentile(99), r.percentile(99.9),
                    r.errorRate() * 100);
            }
        }

        if (!readResults.isEmpty()) {
            System.out.println("\n========== Read Capacity Sweep (bg write " + READ_BG_WRITE_QPS + "qps) ==========");
            System.out.printf("%8s %8s %8s %8s %10s %10s %10s %8s%n",
                "Target", "Actual", "Success", "Errors", "p50(us)", "p99(us)", "p99.9(us)", "ErrRate");
            System.out.println("-".repeat(82));
            for (RunResult r : readResults) {
                System.out.printf("%8d %8.0f %8d %8d %10d %10d %10d %7.1f%%%n",
                    r.targetRate, r.actualRate(),
                    r.successCount, r.errorCount + r.timeoutCount,
                    r.percentile(50), r.percentile(99), r.percentile(99.9),
                    r.errorRate() * 100);
            }
        }
    }

    private static String fmtBytes(int n) {
        if (n == 0) return "0B";
        if (n < 1024) return n + "B";
        return (n / 1024) + "KB";
    }

    // ======================== Main ========================

    public static void main(String[] args) throws Exception {
        String outputPath = "capacity-result.json";
        boolean runPropose = true;
        boolean runRead = true;

        for (int i = 0; i < args.length; i++) {
            if ("-o".equals(args[i]) && i + 1 < args.length) {
                outputPath = args[++i];
            } else if ("propose".equals(args[i])) {
                runRead = false;
            } else if ("read".equals(args[i])) {
                runPropose = false;
            }
        }

        // Start 3-node cluster
        Path tmpDir = Files.createTempDirectory("raft-capacity-");
        int[] raftPorts = RaftProposeBenchmark.findFreePorts(3);
        int[] kvPorts = RaftProposeBenchmark.findFreePorts(3);
        Map<Long, String> peers = new LinkedHashMap<>();
        peers.put(1L, "localhost:" + raftPorts[0]);
        peers.put(2L, "localhost:" + raftPorts[1]);
        peers.put(3L, "localhost:" + raftPorts[2]);

        KvServer[] servers = new KvServer[3];
        try {
            for (int i = 0; i < 3; i++) {
                long id = i + 1;
                Path dir = tmpDir.resolve("node-" + id);
                Files.createDirectories(dir);
                servers[i] = new KvServer(id, raftPorts[i], kvPorts[i], dir, peers, true, true, true);
            }

            KvServer leader = RaftProposeBenchmark.waitForLeader(servers, 30_000);
            LOG.info("Cluster started, leader = node {}", leader.status().id);

            // Pre-populate keys for read benchmark
            if (runRead) {
                LOG.info("Pre-populating {} keys for read benchmark...", READ_KEY_COUNT);
                for (int i = 0; i < READ_KEY_COUNT; i++) {
                    KvCommand cmd = KvCommand.newBuilder()
                        .setOp(KvCommand.Op.PUT)
                        .setKey("read-key-" + i)
                        .setValue("value-" + i)
                        .build();
                    leader.proposeCommand(cmd).get(10, TimeUnit.SECONDS);
                }
            }

            List<RunResult> proposeResults = runPropose ? runProposeSweep(leader) : List.of();
            List<RunResult> readResults = runRead ? runReadSweep(leader) : List.of();

            printSummary(proposeResults, readResults);
            writeJson(proposeResults, readResults, outputPath);

        } finally {
            for (int i = servers.length - 1; i >= 0; i--) {
                if (servers[i] != null) {
                    try {
                        servers[i].close();
                    } catch (Exception ignored) {
                    }
                }
            }
            Thread.sleep(500);
            try (Stream<Path> walk = Files.walk(tmpDir)) {
                walk.sorted(Comparator.reverseOrder()).forEach(p -> {
                    try {
                        Files.deleteIfExists(p);
                    } catch (IOException ignored) {
                    }
                });
            }
        }
    }
}
