/*
 * Copyright 2024-2026 The x-raft-lib Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * SPDX-License-Identifier: Apache-2.0
 */
package io.github.xinfra.lab.raft.examples;

import java.nio.charset.StandardCharsets;

/**
 * A single key-value mutation, serialized as the {@code data} payload of a
 * raft {@code EntryNormal}. The cluster replicates these commands; every
 * node applies the same committed sequence to its local
 * {@link RocksKvStore}, so all replicas converge on the same KV state.
 *
 * <p>Wire format is deliberately trivial and human-readable so the demo is
 * easy to follow: {@code "P|key|value"} for a put, {@code "D|key"} for a
 * delete. A real system would use protobuf or another schema'd encoding.
 */
public final class KvCommand {

    public enum Op { PUT, DELETE }

    public final Op op;
    public final String key;
    public final String value;

    private KvCommand(Op op, String key, String value) {
        this.op = op;
        this.key = key;
        this.value = value;
    }

    public static KvCommand put(String key, String value) {
        return new KvCommand(Op.PUT, key, value == null ? "" : value);
    }

    public static KvCommand delete(String key) {
        return new KvCommand(Op.DELETE, key, null);
    }

    public byte[] serialize() {
        String s = switch (op) {
            case PUT -> "P|" + key + "|" + value;
            case DELETE -> "D|" + key;
        };
        return s.getBytes(StandardCharsets.UTF_8);
    }

    public static KvCommand deserialize(byte[] data) {
        String s = new String(data, StandardCharsets.UTF_8);
        // limit -1 keeps trailing empty fields (e.g. a put of "" value).
        String[] parts = s.split("\\|", -1);
        return switch (parts[0]) {
            case "P" -> put(parts[1], parts.length > 2 ? parts[2] : "");
            case "D" -> delete(parts[1]);
            default -> throw new IllegalArgumentException("unknown op: " + parts[0]);
        };
    }

    @Override
    public String toString() {
        return op == Op.PUT ? ("PUT " + key + "=" + value) : ("DELETE " + key);
    }
}
