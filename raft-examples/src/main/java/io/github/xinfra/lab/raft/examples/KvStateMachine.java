/*
 * Copyright 2024-2026 The x-raft-lib Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * SPDX-License-Identifier: Apache-2.0
 */
package io.github.xinfra.lab.raft.examples;

import io.github.xinfra.lab.raft.examples.proto.KvCommand;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

public final class KvStateMachine implements AutoCloseable {

    static { RocksDB.loadLibrary(); }

    private final Options options;
    private final RocksDB db;
    private volatile long appliedIndex;

    public KvStateMachine(Path dir) {
        this.options = new Options().setCreateIfMissing(true);
        try {
            Files.createDirectories(dir);
            this.db = RocksDB.open(options, dir.toString());
        } catch (RocksDBException | IOException e) {
            options.close();
            throw new IllegalStateException("failed to open RocksDB at " + dir, e);
        }
    }

    public void apply(long index, KvCommand cmd) {
        try {
            switch (cmd.getOp()) {
                case PUT -> db.put(bytes(cmd.getKey()), bytes(cmd.getValue()));
                case DELETE -> db.delete(bytes(cmd.getKey()));
                default -> { }
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

    public Map<String, String> dumpAll() {
        Map<String, String> out = new LinkedHashMap<>();
        try (RocksIterator it = db.newIterator()) {
            for (it.seekToFirst(); it.isValid(); it.next()) {
                out.put(new String(it.key(), StandardCharsets.UTF_8),
                        new String(it.value(), StandardCharsets.UTF_8));
            }
        }
        return out;
    }

    public byte[] serializeState() {
        StringBuilder sb = new StringBuilder();
        try (RocksIterator it = db.newIterator()) {
            for (it.seekToFirst(); it.isValid(); it.next()) {
                sb.append(new String(it.key(), StandardCharsets.UTF_8))
                        .append('|')
                        .append(new String(it.value(), StandardCharsets.UTF_8))
                        .append('\n');
            }
        }
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }

    public void restoreState(byte[] data) {
        try (RocksIterator it = db.newIterator()) {
            for (it.seekToFirst(); it.isValid(); it.next()) {
                db.delete(it.key());
            }
        } catch (RocksDBException e) {
            throw new IllegalStateException("clear failed during restore", e);
        }
        String s = new String(data, StandardCharsets.UTF_8);
        for (String line : s.split("\n")) {
            if (line.isEmpty()) continue;
            String[] parts = line.split("\\|", 2);
            try {
                db.put(bytes(parts[0]), bytes(parts.length > 1 ? parts[1] : ""));
            } catch (RocksDBException e) {
                throw new IllegalStateException("restore put failed", e);
            }
        }
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
