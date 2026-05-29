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
package io.github.xinfra.lab.raft;

/**
 * Thrown when raft-internal state violates an invariant that should not
 * happen with a correctly-functioning storage and message stream — e.g.
 * out-of-bound slice requests, conflict with already-committed entries,
 * or a term lookup that storage cannot fulfill.
 *
 * <p>Distinct from {@link RaftException}: a {@code RaftException} represents
 * an expected, recoverable condition (a follower lagging behind compaction,
 * a proposal dropped because the node is stopped). A
 * {@code RaftInvariantException} means the system is in an unexpected state
 * and should crash rather than continue.
 *
 * <p>Every instance carries a {@link Category} so operators can triage a
 * crash without reading the message text: a {@link Category#STORAGE_IO}
 * failure points at the disk / RocksDB and is potentially transient on
 * restart, a {@link Category#DATA_CORRUPTION} points at a damaged WAL or
 * snapshot that needs a rebuild, while a {@link Category#BUG} indicates a
 * logic error in raft itself. Dashboards and alerts can route on
 * {@link #category()} accordingly. Constructors without an explicit category
 * default to {@link Category#BUG} for source compatibility.
 */
public class RaftInvariantException extends RuntimeException {

    /**
     * Coarse cause of an invariant violation, for triage and alerting.
     */
    public enum Category {
        /**
         * A logic error inside raft (or a caller contract violation):
         * out-of-bound slice, conflict with committed entries, impossible
         * state transition. Not recoverable by retry — it's a defect.
         */
        BUG,
        /**
         * Persisted bytes could not be parsed or failed an integrity check
         * (corrupt log entry, unparyseable snapshot/hard-state). The on-disk
         * state is damaged; recovery typically means restoring from a peer
         * or backup.
         */
        DATA_CORRUPTION,
        /**
         * The storage backend failed an I/O operation (disk full, fsync
         * error, RocksDB exception). May be transient — a restart on healthy
         * hardware can succeed.
         */
        STORAGE_IO,
        /**
         * Misconfiguration that can't be reconciled at runtime (incompatible
         * persisted state vs. supplied config).
         */
        CONFIG,
    }

    private final Category category;

    public RaftInvariantException(String message) {
        this(Category.BUG, message);
    }

    public RaftInvariantException(String message, Throwable cause) {
        this(Category.BUG, message, cause);
    }

    public RaftInvariantException(Category category, String message) {
        super(message);
        this.category = category == null ? Category.BUG : category;
    }

    public RaftInvariantException(Category category, String message, Throwable cause) {
        super(message, cause);
        this.category = category == null ? Category.BUG : category;
    }

    /** The triage category of this violation. Never {@code null}. */
    public Category category() {
        return category;
    }

    @Override
    public String getMessage() {
        return "[" + category + "] " + super.getMessage();
    }
}
