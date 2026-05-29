/*
 * Copyright 2024-2026 The x-raft-lib Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * SPDX-License-Identifier: Apache-2.0
 */
package io.github.xinfra.lab.raft.tests;

import io.github.xinfra.lab.raft.Node;
import io.github.xinfra.lab.raft.RaftStateType;
import io.github.xinfra.lab.raft.examples.RaftPeer;
import io.github.xinfra.lab.raft.tests.chaos.ChaosController;
import io.github.xinfra.lab.raft.tests.chaos.ChaosTransport;
import io.github.xinfra.lab.raft.transport.grpc.GrpcTransport;

import java.net.ServerSocket;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.BooleanSupplier;

/** Shared helpers for the integration suite. */
final class IntegrationTestSupport {

    private IntegrationTestSupport() {}

    /**
     * Allocate {@code n} ephemeral free ports. Released back to the OS
     * before return; there's a small race vs. another process binding
     * the same port, but that's acceptable for tests.
     */
    static int[] freePorts(int n) throws Exception {
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

    /**
     * Build the peerAddresses map for a localhost cluster: id N → "localhost:port[N-1]".
     */
    static Map<Long, String> peerMap(int[] ports) {
        Map<Long, String> m = new LinkedHashMap<>();
        for (int i = 0; i < ports.length; i++) {
            m.put((long) (i + 1), "localhost:" + ports[i]);
        }
        return m;
    }

    /** Poll a condition with a deadline. Returns true if it became true. */
    static boolean awaitTrue(BooleanSupplier cond, long timeoutMillis) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            if (cond.getAsBoolean()) return true;
            Thread.sleep(25);
        }
        return cond.getAsBoolean();
    }

    /** Wait until at least one peer is in StateLeader; returns its id, or 0 on timeout. */
    static long awaitLeader(Iterable<RaftPeer> peers, long timeoutMillis) throws InterruptedException {
        AtomicLong leader = new AtomicLong();
        boolean ok = awaitTrue(() -> {
            for (RaftPeer p : peers) {
                if (p.basicStatus().state == RaftStateType.StateLeader) {
                    leader.set(p.id);
                    return true;
                }
            }
            return false;
        }, timeoutMillis);
        return ok ? leader.get() : 0L;
    }

    /**
     * Build a {@link RaftPeer} whose gRPC transport is wrapped in a
     * {@link ChaosTransport} sharing {@code controller}, so the test can drop
     * / partition this node's traffic. {@code ports} is the full cluster's
     * port array; this node's port is {@code ports[id-1]}.
     */
    static RaftPeer chaosPeer(long id,
                              int[] ports,
                              Map<Long, String> peerAddresses,
                              Path storageDir,
                              boolean bootstrap,
                              BiConsumer<Long, byte[]> applyCallback,
                              ChaosController controller) throws Exception {
        GrpcTransport inner = new GrpcTransport(id, ports[(int) (id - 1)]);
        ChaosTransport chaos = new ChaosTransport(id, inner, controller);
        return new RaftPeer(id, storageDir, peerAddresses, bootstrap, applyCallback, chaos);
    }

    /** Find the (single, best-effort) peer currently reporting StateLeader, or null. */
    static RaftPeer findLeader(Iterable<RaftPeer> peers) {
        for (RaftPeer p : peers) {
            if (p.basicStatus().state == RaftStateType.StateLeader) return p;
        }
        return null;
    }

    /** Wait until the basicStatus().lead converges to a single non-zero value across all peers. */
    static boolean awaitConvergedLeader(Iterable<RaftPeer> peers, long timeoutMillis) throws InterruptedException {
        return awaitTrue(() -> {
            long lead = -1;
            for (RaftPeer p : peers) {
                Node.BasicStatus s = p.basicStatus();
                if (s.lead == 0) return false;
                if (lead == -1) lead = s.lead;
                else if (lead != s.lead) return false;
            }
            return lead > 0;
        }, timeoutMillis);
    }
}
