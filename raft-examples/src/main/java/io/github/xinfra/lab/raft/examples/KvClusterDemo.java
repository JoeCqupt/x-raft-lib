/*
 * Copyright 2024-2026 The x-raft-lib Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * SPDX-License-Identifier: Apache-2.0
 */
package io.github.xinfra.lab.raft.examples;

import io.github.xinfra.lab.raft.RaftException;
import io.github.xinfra.lab.raft.RaftStateType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BooleanSupplier;
import java.util.stream.Stream;

/**
 * Runnable end-to-end demo: a small distributed key-value system built on
 * the full x-raft-lib stack.
 *
 * <ul>
 *   <li><b>raft-core</b> drives consensus (election, replication, commit).</li>
 *   <li><b>raft-transport-grpc</b> carries messages between nodes over real
 *       gRPC sockets on {@code localhost}.</li>
 *   <li><b>raft-storage-rocksdb</b> persists each node's raft log and
 *       hard-state to RocksDB.</li>
 *   <li>The application state machine ({@link RocksKvStore}) is itself a
 *       RocksDB-backed KV store — committed commands replicate to every
 *       node, so all replicas converge on the same keys.</li>
 * </ul>
 *
 * <p>Everything runs in one JVM (each node binds its own gRPC port and owns
 * its own RocksDB directories) so the demo is a single {@code main} you can
 * run without provisioning machines. The same {@link RaftPeer} wiring works
 * across real hosts — only the addresses in the peer map change.
 *
 * <pre>{@code
 * mvn -f raft-examples/pom.xml compile exec:java \
 *     -Dexec.mainClass=io.github.xinfra.lab.raft.examples.KvClusterDemo
 * }</pre>
 */
public final class KvClusterDemo {

    private static final Logger LOG = LoggerFactory.getLogger(KvClusterDemo.class);

    private KvClusterDemo() {}

    /** Result of a demo run: the elected leader and each node's final KV view. */
    public static final class Result {
        public final long leaderId;
        /** Per-node KV snapshot, keyed by node id (sorted). */
        public final Map<Long, Map<String, String>> perNode;

        Result(long leaderId, Map<Long, Map<String, String>> perNode) {
            this.leaderId = leaderId;
            this.perNode = perNode;
        }

        /** True if every node holds an identical KV snapshot. */
        public boolean allConverged() {
            Map<String, String> first = null;
            for (Map<String, String> kv : perNode.values()) {
                if (first == null) first = kv;
                else if (!first.equals(kv)) return false;
            }
            return first != null;
        }
    }

    public static void main(String[] args) throws Exception {
        int nodeCount = args.length > 0 ? Integer.parseInt(args[0]) : 3;
        Path base = Files.createTempDirectory("x-raft-lib-kvdemo");
        try {
            Result r = run(nodeCount, base);
            System.out.println();
            System.out.println("=== x-raft-lib KV demo ===");
            System.out.println("nodes      : " + nodeCount);
            System.out.println("leader     : node " + r.leaderId);
            System.out.println("converged  : " + r.allConverged());
            r.perNode.forEach((id, kv) ->
                    System.out.println("node " + id + " KV : " + kv));
            System.out.println("===========================");
        } finally {
            deleteRecursively(base);
        }
    }

    /**
     * Bring up an {@code nodeCount}-node cluster under {@code baseDir}, run a
     * short scripted workload through the leader, wait for it to replicate to
     * every node, and return each node's final KV snapshot. All nodes and
     * stores are closed before returning.
     */
    public static Result run(int nodeCount, Path baseDir) throws Exception {
        int[] ports = freePorts(nodeCount);
        Map<Long, String> peerMap = new LinkedHashMap<>();
        for (int i = 0; i < nodeCount; i++) {
            peerMap.put((long) (i + 1), "localhost:" + ports[i]);
        }

        List<RaftPeer> peers = new ArrayList<>();
        List<RocksKvStore> stores = new ArrayList<>();
        try {
            for (long id = 1; id <= nodeCount; id++) {
                RocksKvStore kv = new RocksKvStore(baseDir.resolve("kv-" + id));
                stores.add(kv);
                RaftPeer peer = new RaftPeer(
                        id,
                        ports[(int) (id - 1)],
                        baseDir.resolve("raft-" + id),
                        peerMap,
                        /*bootstrap=*/ true,
                        (idx, data) -> kv.apply(idx, KvCommand.deserialize(data)));
                peers.add(peer);
            }

            long leaderId = awaitLeader(peers, 15_000);
            if (leaderId == 0) {
                throw new IllegalStateException("cluster failed to elect a leader");
            }
            LOG.info("leader elected: node {}", leaderId);

            // Scripted workload. Final expected state:
            //   user:1   -> alice-updated   (overwritten)
            //   config:mode -> demo
            //   user:2   -> (deleted)
            List<KvCommand> script = List.of(
                    KvCommand.put("user:1", "alice"),
                    KvCommand.put("user:2", "bob"),
                    KvCommand.put("config:mode", "demo"),
                    KvCommand.put("user:1", "alice-updated"),
                    KvCommand.delete("user:2"));
            for (KvCommand cmd : script) {
                proposeOnLeader(peers, cmd);
            }

            // Wait until the workload has replicated to every node.
            boolean replicated = awaitTrue(() -> stores.stream().allMatch(s ->
                    s.get("config:mode").isPresent()
                            && s.get("user:1").orElse("").equals("alice-updated")
                            && s.get("user:2").isEmpty()), 15_000);
            if (!replicated) {
                throw new IllegalStateException("workload did not replicate to all nodes in time");
            }

            Map<Long, Map<String, String>> perNode = new LinkedHashMap<>();
            for (RaftPeer p : peers) {
                perNode.put(p.id, stores.get((int) (p.id - 1)).snapshot());
            }
            return new Result(leaderId, perNode);
        } finally {
            // Close peers first (reverse order to mute grpc shutdown noise),
            // then their application stores.
            for (int i = peers.size() - 1; i >= 0; i--) {
                try { peers.get(i).close(); } catch (Throwable ignored) {}
            }
            for (RocksKvStore s : stores) {
                try { s.close(); } catch (Throwable ignored) {}
            }
        }
    }

    /** Propose a command on the current leader, retrying across leader changes. */
    private static void proposeOnLeader(List<RaftPeer> peers, KvCommand cmd) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            RaftPeer leader = findLeader(peers);
            if (leader != null) {
                try {
                    leader.propose(cmd.serialize());
                    return;                         // accepted
                } catch (RaftException ignored) {
                    // dropped (e.g. lost leadership mid-propose) — fall through to retry
                }
            }
            Thread.sleep(25);                       // no leader yet, or rejected — retry
        }
        throw new IllegalStateException("failed to propose " + cmd + " within timeout");
    }

    // ---- small in-process helpers (mirror the integration test harness) ----

    private static int[] freePorts(int n) throws Exception {
        ServerSocket[] sockets = new ServerSocket[n];
        int[] ports = new int[n];
        try {
            for (int i = 0; i < n; i++) {
                sockets[i] = new ServerSocket(0);
                ports[i] = sockets[i].getLocalPort();
            }
        } finally {
            for (ServerSocket s : sockets) {
                if (s != null) s.close();
            }
        }
        return ports;
    }

    private static RaftPeer findLeader(List<RaftPeer> peers) {
        for (RaftPeer p : peers) {
            if (p.basicStatus().state == RaftStateType.StateLeader) return p;
        }
        return null;
    }

    private static long awaitLeader(List<RaftPeer> peers, long timeoutMillis) throws InterruptedException {
        long[] leader = {0L};
        awaitTrue(() -> {
            RaftPeer l = findLeader(peers);
            if (l != null) { leader[0] = l.id; return true; }
            return false;
        }, timeoutMillis);
        return leader[0];
    }

    private static boolean awaitTrue(BooleanSupplier cond, long timeoutMillis) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            if (cond.getAsBoolean()) return true;
            Thread.sleep(25);
        }
        return cond.getAsBoolean();
    }

    private static void deleteRecursively(Path root) {
        if (root == null || !Files.exists(root)) return;
        try (Stream<Path> walk = Files.walk(root)) {
            walk.sorted(Comparator.reverseOrder()).forEach(p -> {
                try { Files.deleteIfExists(p); } catch (Exception ignored) {}
            });
        } catch (Exception ignored) {}
    }
}
