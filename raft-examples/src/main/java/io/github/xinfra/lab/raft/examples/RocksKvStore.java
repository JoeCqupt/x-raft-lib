/*
 * Copyright 2024-2026 The x-raft-lib Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * SPDX-License-Identifier: Apache-2.0
 */
package io.github.xinfra.lab.raft.examples;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * The demo's replicated state machine: a simple key-value store persisted
 * in its own RocksDB database.
 *
 * <p>This is deliberately separate from the {@code raft-storage-rocksdb}
 * store that holds the raft log / hard-state. That one is raft's business;
 * this one is the <em>application</em> data. Each {@link RaftPeer} owns one
 * of these and feeds it committed commands through its apply callback, so
 * after replication every node's RocksKvStore holds the same keys.
 *
 * <p>Durability note: this demo applies a command to RocksDB and then lets
 * {@link RaftPeer} persist raft's applied-index watermark separately. A
 * production system would commit the application write and the watermark in
 * one atomic batch (one fsync covering both) so a crash can't leave the two
 * out of sync.
 */
public final class RocksKvStore implements AutoCloseable {

    static { RocksDB.loadLibrary(); }

    private final Options options;
    private final RocksDB db;
    /** Highest raft index applied to this store; for observability only. */
    private volatile long appliedIndex;

    public RocksKvStore(Path dir) {
        this.options = new Options().setCreateIfMissing(true);
        try {
            this.db = RocksDB.open(options, dir.toString());
        } catch (RocksDBException e) {
            options.close();
            throw new IllegalStateException("failed to open RocksDB at " + dir, e);
        }
    }

    /** Apply one committed command at the given raft index. */
    public void apply(long index, KvCommand cmd) {
        try {
            switch (cmd.op) {
                case PUT -> db.put(bytes(cmd.key), bytes(cmd.value));
                case DELETE -> db.delete(bytes(cmd.key));
            }
        } catch (RocksDBException e) {
            throw new IllegalStateException("apply failed for " + cmd, e);
        }
        appliedIndex = index;
    }

    public Optional<String> get(String key) {
        try {
            byte[] v = db.get(bytes(key));
            return v == null ? Optional.empty() : Optional.of(new String(v, StandardCharsets.UTF_8));
        } catch (RocksDBException e) {
            throw new IllegalStateException("get failed for key " + key, e);
        }
    }

    /** A full snapshot of the store, key-ordered. Demo/inspection helper. */
    public Map<String, String> snapshot() {
        Map<String, String> out = new LinkedHashMap<>();
        try (RocksIterator it = db.newIterator()) {
            for (it.seekToFirst(); it.isValid(); it.next()) {
                out.put(new String(it.key(), StandardCharsets.UTF_8),
                        new String(it.value(), StandardCharsets.UTF_8));
            }
        }
        return out;
    }

    public int size() {
        int n = 0;
        try (RocksIterator it = db.newIterator()) {
            for (it.seekToFirst(); it.isValid(); it.next()) n++;
        }
        return n;
    }

    public long appliedIndex() { return appliedIndex; }

    @Override
    public void close() {
        db.close();
        options.close();
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }
}
