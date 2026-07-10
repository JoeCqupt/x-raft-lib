/*
 * Copyright 2024-2026 The x-raft-lib Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * SPDX-License-Identifier: Apache-2.0
 */
package io.github.xinfra.lab.raft.examples;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Command-line entry point for the raft-kv server.
 *
 * <pre>{@code
 * java -cp ... io.github.xinfra.lab.raft.examples.KvServerBootstrap \
 *     --id=1 --raft-port=8081 --kv-port=9001 --data-dir=/tmp/node1 \
 *     --peers=1=localhost:8081,2=localhost:8082,3=localhost:8083 --bootstrap
 * }</pre>
 */
public final class KvServerBootstrap {

    private static final Logger LOG = LoggerFactory.getLogger(KvServerBootstrap.class);

    private KvServerBootstrap() {}

    public static void main(String[] args) throws Exception {
        long id = 0;
        int raftPort = 0;
        int kvPort = 0;
        String dataDir = null;
        String peersStr = null;
        boolean bootstrap = false;
        boolean snapshotStreaming = false;
        boolean asyncStorageWrites = false;

        for (String arg : args) {
            if (arg.startsWith("--id=")) {
                id = Long.parseLong(arg.substring(5));
            } else if (arg.startsWith("--raft-port=")) {
                raftPort = Integer.parseInt(arg.substring(12));
            } else if (arg.startsWith("--kv-port=")) {
                kvPort = Integer.parseInt(arg.substring(10));
            } else if (arg.startsWith("--data-dir=")) {
                dataDir = arg.substring(11);
            } else if (arg.startsWith("--peers=")) {
                peersStr = arg.substring(8);
            } else if (arg.equals("--bootstrap")) {
                bootstrap = true;
            } else if (arg.equals("--snapshot-streaming")) {
                snapshotStreaming = true;
            } else if (arg.equals("--async-storage-writes")) {
                asyncStorageWrites = true;
            }
        }

        if (id == 0 || raftPort == 0 || kvPort == 0 || dataDir == null || peersStr == null) {
            System.err.println("Usage: --id=<N> --raft-port=<P> --kv-port=<P> --data-dir=<DIR> --peers=<ID=HOST:PORT,...> [--bootstrap] [--snapshot-streaming] [--async-storage-writes]");
            System.exit(1);
        }

        Map<Long, String> peers = parsePeers(peersStr);

        KvServer server = new KvServer(id, raftPort, kvPort, Path.of(dataDir), peers, bootstrap, snapshotStreaming, asyncStorageWrites, 10_000);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("shutting down KvServer node {}", server.status().id);
            server.close();
        }));

        LOG.info("KvServer node {} running. Press Ctrl+C to stop.", id);
        Thread.currentThread().join();
    }

    private static Map<Long, String> parsePeers(String s) {
        Map<Long, String> peers = new LinkedHashMap<>();
        for (String entry : s.split(",")) {
            String[] kv = entry.split("=", 2);
            peers.put(Long.parseLong(kv[0].trim()), kv[1].trim());
        }
        return peers;
    }
}
