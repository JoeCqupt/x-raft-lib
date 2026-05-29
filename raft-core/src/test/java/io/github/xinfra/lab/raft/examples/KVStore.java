/*
 * Copyright 2024-2026 The x-raft-lib Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.github.xinfra.lab.raft.examples;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Simple replicated key-value state machine — applied via raft.
 *
 * <p>Each committed raft entry is a serialized {@link Command}; this state
 * machine applies the commands in order. Reads happen via {@link #get(String)}
 * after the caller confirms read-safety via ReadIndex.
 *
 * <p>Thread safety: methods are synchronized so concurrent readers / the apply
 * thread can interact safely. In a real app you'd use a more granular scheme.
 */
public class KVStore {
    private final Map<String, String> store = new HashMap<>();
    /** The last applied raft index. */
    private long appliedIndex;

    public synchronized void applyCommand(long index, Command cmd) {
        switch (cmd.op) {
            case PUT -> store.put(cmd.key, cmd.value);
            case DELETE -> store.remove(cmd.key);
        }
        appliedIndex = index;
    }

    public synchronized Optional<String> get(String key) {
        return Optional.ofNullable(store.get(key));
    }

    public synchronized long appliedIndex() { return appliedIndex; }

    public synchronized int size() { return store.size(); }

    /** A KV command, serialized as raft entry data. */
    public static class Command {
        public enum Op { PUT, DELETE }
        public final Op op;
        public final String key;
        public final String value;

        public Command(Op op, String key, String value) {
            this.op = op;
            this.key = key;
            this.value = value;
        }

        /** Wire format: "P|key|value" or "D|key". Trivial and human-readable. */
        public byte[] serialize() {
            String s = switch (op) {
                case PUT -> "P|" + key + "|" + (value == null ? "" : value);
                case DELETE -> "D|" + key;
            };
            return s.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        }

        public static Command deserialize(byte[] data) {
            String s = new String(data, java.nio.charset.StandardCharsets.UTF_8);
            String[] parts = s.split("\\|", -1);
            return switch (parts[0]) {
                case "P" -> new Command(Op.PUT, parts[1], parts.length > 2 ? parts[2] : "");
                case "D" -> new Command(Op.DELETE, parts[1], null);
                default -> throw new IllegalArgumentException("unknown op: " + parts[0]);
            };
        }
    }
}
