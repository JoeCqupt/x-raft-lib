/*
 * Copyright 2024-2026 The x-raft-lib Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * SPDX-License-Identifier: Apache-2.0
 */
package io.github.xinfra.lab.raft.tests;

import io.github.xinfra.lab.raft.RaftException;
import io.github.xinfra.lab.raft.examples.KvCommand;
import io.github.xinfra.lab.raft.examples.RaftPeer;
import io.github.xinfra.lab.raft.tests.chaos.ChaosController;
import io.github.xinfra.lab.raft.tests.linearizability.History;
import io.github.xinfra.lab.raft.tests.linearizability.LinearizabilityChecker;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import static io.github.xinfra.lab.raft.tests.IntegrationTestSupport.awaitLeader;
import static io.github.xinfra.lab.raft.tests.IntegrationTestSupport.chaosPeer;
import static io.github.xinfra.lab.raft.tests.IntegrationTestSupport.findLeader;
import static io.github.xinfra.lab.raft.tests.IntegrationTestSupport.freePorts;
import static io.github.xinfra.lab.raft.tests.IntegrationTestSupport.peerMap;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Records a real KV workload against the 3-node raft cluster and runs
 * the result through {@link LinearizabilityChecker}. Any history raft
 * produces must be linearizable — that is the safety property the
 * protocol exists to provide. We exercise it under healthy network and
 * under chaos (latency + drop + duplicate), because that's the case
 * where a real bug would surface.
 *
 * <p>The on-cluster apply log is reconstructed from each node's apply
 * callback (which fires in commit-index order). We feed the leader's
 * apply view as the "observed" sequential ground truth and compare it
 * against the per-client {@link History}.
 *
 * <p>Note: ops go through the leader's {@code propose} — this is the
 * standard write-side path, which raft makes linearizable by
 * construction. {@code get} in this test reads the node-local applied
 * state without raft serialization, so a {@code get} sees the
 * snapshot of that node's state at read time. The checker handles the
 * resulting concurrency in real time.
 */
class KvLinearizabilityIntegrationTest {

    @TempDir Path tmp;

    /** Healthy network: every history raft produces must be linearizable. */
    @Test
    void healthyClusterHistoryIsLinearizable() throws Exception {
        runWorkloadAndCheck(/*latencyMs*/ 0, /*dropPct*/ 0.0, /*duplicatePct*/ 0.0);
    }

    /**
     * Adversarial network: lossy + duplicating + jittery. Raft's safety
     * is unconditional, so the recorded history must still be
     * linearizable.
     */
    @Test
    void chaosClusterHistoryIsLinearizable() throws Exception {
        runWorkloadAndCheck(/*latencyMs*/ 30, /*dropPct*/ 0.1, /*duplicatePct*/ 0.1);
    }

    // ------------ workload + recording harness ------------

    private void runWorkloadAndCheck(int latencyMs, double dropPct, double duplicatePct)
            throws Exception {
        int[] ports = freePorts(3);
        Map<Long, String> peers = peerMap(ports);
        ChaosController chaos = new ChaosController();

        // Per-node mirror of the applied KV state. The leader's mirror is
        // what we read from for GET, AFTER waiting for the leader to catch
        // up past its current commit index (so the read happens-after any
        // PUT whose complete is in the past).
        Map<Long, Map<String, String>> nodeStates = new ConcurrentHashMap<>();
        for (long id = 1; id <= 3; id++) nodeStates.put(id, new ConcurrentHashMap<>());

        // Tracks the maximum applied index across the cluster's leader.
        // GET ops block until applied advances past the most-recent PUT's
        // index, making reads "linearization barrier reads".
        Map<Long, AtomicLong> appliedHigh = new ConcurrentHashMap<>();
        for (long id = 1; id <= 3; id++) appliedHigh.put(id, new AtomicLong());

        // PUT/DELETE register a future keyed by their unique command id;
        // the apply callback completes the future once the matching entry
        // has been applied locally. This lets the client wait for "actually
        // applied" rather than "merely accepted by propose()".
        Map<String, CompletableFuture<Long>> applyFutures = new ConcurrentHashMap<>();

        List<RaftPeer> nodes = new ArrayList<>();
        try {
            for (long id = 1; id <= 3; id++) {
                long fid = id;
                Map<String, String> state = nodeStates.get(fid);
                AtomicLong applied = appliedHigh.get(fid);
                nodes.add(chaosPeer(fid, ports, peers, tmp.resolve("p" + fid), true,
                        (idx, data) -> {
                            applyToState(state, data, applyFutures, idx);
                            applied.set(idx);
                        }, chaos));
            }
            assertThat(awaitLeader(nodes, 10_000)).isPositive();

            if (latencyMs > 0) chaos.setGlobalLatency(Duration.ofMillis(latencyMs));
            if (dropPct > 0) chaos.setDropProbability(dropPct);
            if (duplicatePct > 0) chaos.setDuplicateProbability(duplicatePct);

            History history = runMixedWorkload(nodes, nodeStates, appliedHigh, applyFutures);

            // Clear faults before checking so the checker sees a settled
            // cluster and the test teardown doesn't race the chaos scheduler.
            chaos.healAll();
            chaos.setDropProbability(0.0);
            chaos.setDuplicateProbability(0.0);

            LinearizabilityChecker.Result r = LinearizabilityChecker.checkKvRegister(history);
            assertThat(r.linearizable).as("%s; %d events recorded", r, history.size()).isTrue();
        } finally {
            closeAll(nodes);
        }
    }

    /**
     * Two client threads, each issuing ~40 mixed put/get/delete ops on
     * a small key space so PUTs and GETs frequently target the same
     * key. Keeps history sizes ≤200 ops — within the checker's fast
     * regime.
     *
     * <p>Linearizability requires the client-observed "complete"
     * timestamp to be the moment the operation's effect is durably
     * visible, not the moment {@code propose()} returns. We achieve
     * that by tagging each command with a unique id, registering a
     * future, and waiting for the apply callback to complete it.
     */
    private History runMixedWorkload(List<RaftPeer> nodes,
                                     Map<Long, Map<String, String>> nodeStates,
                                     Map<Long, AtomicLong> appliedHigh,
                                     Map<String, CompletableFuture<Long>> applyFutures) throws Exception {
        History history = new History();
        String[] keys = {"k1", "k2", "k3"};
        int opsPerClient = 30;
        int clients = 2;
        AtomicLong cmdSerial = new AtomicLong();

        ExecutorService pool = Executors.newFixedThreadPool(clients);
        CountDownLatch done = new CountDownLatch(clients);
        try {
            for (int c = 0; c < clients; c++) {
                final long process = 100 + c;
                pool.submit(() -> {
                    try {
                        for (int i = 0; i < opsPerClient; i++) {
                            int roll = ThreadLocalRandom.current().nextInt(10);
                            String key = keys[ThreadLocalRandom.current().nextInt(keys.length)];
                            if (roll < 4) {
                                // GET: block until our local view has caught up
                                // past any PUT we've already completed.
                                long seq = history.invoke(process, History.OpType.GET, key, null);
                                String observed = readAfterCatchUp(nodes, nodeStates, appliedHigh, key);
                                history.complete(seq, process, History.OpType.GET, key, observed);
                            } else if (roll < 9) {
                                String val = "v" + process + "-" + i;
                                String cmdId = "p" + process + "-" + cmdSerial.incrementAndGet();
                                long seq = history.invoke(process, History.OpType.PUT, key, val);
                                proposeAndAwaitApply(nodes, KvCommand.put(key, val + "#" + cmdId), cmdId, applyFutures);
                                history.complete(seq, process, History.OpType.PUT, key, val);
                            } else {
                                String cmdId = "d" + process + "-" + cmdSerial.incrementAndGet();
                                long seq = history.invoke(process, History.OpType.DELETE, key, null);
                                proposeAndAwaitApply(nodes, KvCommand.delete(key + "#" + cmdId), cmdId, applyFutures);
                                // The real delete is also issued (otherwise the
                                // synthetic-keyed apply above would just touch
                                // a side key). For a register we want the
                                // canonical key's value gone.
                                proposeAndAwaitApply(nodes, KvCommand.delete(key),
                                        "real-" + cmdId, applyFutures);
                                history.complete(seq, process, History.OpType.DELETE, key, null);
                            }
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } catch (TimeoutException | ExecutionException ex) {
                        // Surface as test failure via the assertion below.
                        throw new RuntimeException(ex);
                    } finally {
                        done.countDown();
                    }
                });
            }
            assertThat(done.await(120, TimeUnit.SECONDS))
                    .as("workload must complete within timeout").isTrue();
        } finally {
            pool.shutdownNow();
        }
        return history;
    }

    /**
     * Apply the inbound raft entry to this node's local KV state, then
     * resolve the matching apply future so the client can observe that
     * its proposed command has actually landed.
     */
    private static void applyToState(Map<String, String> state, byte[] data,
                                     Map<String, CompletableFuture<Long>> applyFutures, long idx) {
        KvCommand cmd = KvCommand.deserialize(data);
        // Commands carry their canonical key + value; ids are appended in
        // value (PUT) or key (DELETE) so the leader's apply can recover
        // them and complete the matching future.
        String canonicalKey = cmd.key;
        String canonicalValue = cmd.value;
        String idFromKey = null;
        int kHash = canonicalKey == null ? -1 : canonicalKey.indexOf('#');
        if (kHash >= 0) {
            idFromKey = canonicalKey.substring(kHash + 1);
            canonicalKey = canonicalKey.substring(0, kHash);
        }
        String idFromVal = null;
        if (canonicalValue != null) {
            int vHash = canonicalValue.indexOf('#');
            if (vHash >= 0) {
                idFromVal = canonicalValue.substring(vHash + 1);
                canonicalValue = canonicalValue.substring(0, vHash);
            }
        }
        switch (cmd.op) {
            case PUT -> state.put(canonicalKey, canonicalValue);
            case DELETE -> state.remove(canonicalKey);
        }
        String id = idFromVal != null ? idFromVal : idFromKey;
        if (id != null) {
            CompletableFuture<Long> f = applyFutures.get(id);
            if (f != null) f.complete(idx);
        }
    }

    /**
     * Read AFTER waiting for the leader to apply at least one more
     * entry past its current commit index — guarantees the read
     * observes any PUT whose complete is already in the past for this
     * client. (Without the barrier, the read could return a stale
     * value applied before the PUT had a chance to land, producing
     * spurious linearizability violations against an asynchronous
     * apply log.)
     */
    private static String readAfterCatchUp(List<RaftPeer> nodes,
                                           Map<Long, Map<String, String>> nodeStates,
                                           Map<Long, AtomicLong> appliedHigh,
                                           String key) throws InterruptedException {
        RaftPeer leader = findLeader(nodes);
        if (leader == null) return null;
        long target = leader.basicStatus().commit;
        AtomicLong applied = appliedHigh.get(leader.id);
        long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
        while (applied.get() < target && System.nanoTime() < deadline) {
            Thread.sleep(2);
        }
        return nodeStates.get(leader.id).get(key);
    }

    /**
     * Propose a command and block until the apply callback acknowledges
     * it (via the {@code cmdId} we encoded in the payload). Without this,
     * a client's "PUT complete" timestamp would refer to "propose
     * returned" — which is before the entry is applied, and a
     * subsequent GET could legitimately observe the old value, breaking
     * linearizability under the per-client real-time order.
     */
    private static void proposeAndAwaitApply(List<RaftPeer> nodes,
                                             KvCommand cmd,
                                             String cmdId,
                                             Map<String, CompletableFuture<Long>> applyFutures)
            throws InterruptedException, TimeoutException, ExecutionException {
        CompletableFuture<Long> done = new CompletableFuture<>();
        applyFutures.put(cmdId, done);
        try {
            proposeOnLeader(nodes, cmd);
            done.get(10, TimeUnit.SECONDS);
        } finally {
            applyFutures.remove(cmdId);
        }
    }

    /** Retry propose across leader changes. */
    private static void proposeOnLeader(List<RaftPeer> nodes, KvCommand cmd) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            RaftPeer leader = findLeader(nodes);
            if (leader != null) {
                try {
                    leader.propose(cmd.serialize());
                    return;
                } catch (RaftException ignored) {
                    // dropped (e.g. lost leadership) — retry on the new leader
                }
            }
            Thread.sleep(20);
        }
        throw new IllegalStateException("could not propose within timeout");
    }

    private static void closeAll(List<RaftPeer> nodes) {
        for (int i = nodes.size() - 1; i >= 0; i--) {
            try { nodes.get(i).close(); } catch (Throwable ignored) {}
        }
    }

}
