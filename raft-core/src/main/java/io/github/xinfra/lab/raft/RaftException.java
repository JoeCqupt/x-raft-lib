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

import java.util.Objects;

/**
 * Domain-level error for the raft state machine and storage.
 *
 * <p>Errors carry an explicit {@link Code} rather than relying on identity
 * (==) comparison of sentinel instances. Use {@link #code()} to dispatch on
 * the error category. Sentinel instances ({@code ErrCompacted}, ...) are
 * still exposed as a convenience for the common case where no extra context
 * is needed; new instances with the same code compare equal via
 * {@link #equals(Object)}.
 *
 * <p>This class is reserved for expected, recoverable raft-layer errors.
 * Invariant violations (out-of-bound indices, conflict with committed
 * entries, etc.) should throw an unchecked exception and crash the node —
 * see {@link RaftInvariantException}.
 */
public class RaftException extends Exception {

    public enum Code {
        COMPACTED,
        SNAP_OUT_OF_DATE,
        UNAVAILABLE,
        SNAPSHOT_TEMPORARILY_UNAVAILABLE,
        STEP_LOCAL_MSG,
        STEP_PEER_NOT_FOUND,
        PROPOSAL_DROPPED,
        STOPPED,
    }

    public static final RaftException ErrCompacted = new RaftException(Code.COMPACTED, "requested index is unavailable due to compaction");
    public static final RaftException ErrSnapOutOfDate = new RaftException(Code.SNAP_OUT_OF_DATE, "requested index is older than the existing snapshot");
    public static final RaftException ErrUnavailable = new RaftException(Code.UNAVAILABLE, "requested entry at index is unavailable");
    public static final RaftException ErrSnapshotTemporarilyUnavailable = new RaftException(Code.SNAPSHOT_TEMPORARILY_UNAVAILABLE, "snapshot is temporarily unavailable");
    public static final RaftException ErrStepLocalMsg = new RaftException(Code.STEP_LOCAL_MSG, "raft: cannot step raft local message");
    public static final RaftException ErrStepPeerNotFound = new RaftException(Code.STEP_PEER_NOT_FOUND, "raft: cannot step as peer not found");
    public static final RaftException ErrProposalDropped = new RaftException(Code.PROPOSAL_DROPPED, "raft proposal dropped");
    public static final RaftException ErrStopped = new RaftException(Code.STOPPED, "raft: stopped");

    private final Code code;

    public RaftException(Code code, String message) {
        super(message);
        this.code = Objects.requireNonNull(code, "code");
    }

    public RaftException(Code code, String message, Throwable cause) {
        super(message, cause);
        this.code = Objects.requireNonNull(code, "code");
    }

    public Code code() {
        return code;
    }

    public boolean is(Code other) {
        return code == other;
    }

    /**
     * Two RaftException values are equal iff they share a {@link Code}. Lets
     * tests use {@code assertThat(err).isEqualTo(RaftException.ErrXxx)} without
     * caring whether the implementation reused the sentinel instance or built
     * a new one with extra context.
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof RaftException other)) return false;
        return code == other.code;
    }

    @Override
    public int hashCode() {
        return code.hashCode();
    }

    // Override to avoid expensive stack trace for sentinel exceptions
    @Override
    public synchronized Throwable fillInStackTrace() {
        return this;
    }
}
