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

import io.github.xinfra.lab.raft.proto.Eraftpb;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

/**
 * Storage is the host-supplied persistence layer for raft. It is a strict
 * separation: raft owns the protocol; you own the bytes.
 *
 * <h2>Required guarantees (the host is responsible for these)</h2>
 *
 * <ul>
 *   <li><b>Atomicity within a single Ready cycle.</b> {@link #append} and the
 *   matching {@link #setHardState} for one Ready must commit together —
 *   either both visible after a crash or neither. Most hosts achieve this
 *   with a single WAL fsync that batches both.</li>
 *
 *   <li><b>Durability before responding.</b> Before you let raft's outbound
 *   {@code Ready.messages} leave your transport, the entries / hardState /
 *   snapshot from the same Ready must be durable. Otherwise a crash after
 *   sending a vote response can violate Raft safety.</li>
 *
 *   <li><b>Concurrency.</b> Read methods ({@link #entries}, {@link #term},
 *   {@link #firstIndex}, {@link #lastIndex}, {@link #snapshot},
 *   {@link #initialState}) MAY be called concurrently with each other and
 *   with the write methods on the local-append thread. Implementations must
 *   be safe under that read/write concurrency. {@code MemoryStorage} ships
 *   a coarse-grained {@code synchronized} reference impl; production
 *   backends should prefer per-CF locks or MVCC reads.</li>
 *
 *   <li><b>Stable bounds.</b> Once {@link #compact} succeeds at index N,
 *   subsequent {@link #firstIndex} must be ≥ N+1 and {@link #term}/
 *   {@link #entries} for indices &lt; N+1 must throw
 *   {@link RaftException#ErrCompacted}.</li>
 * </ul>
 *
 * <h2>Errors</h2>
 *
 * <ul>
 *   <li>{@link RaftException#ErrCompacted} — index has been truncated by
 *   {@link #compact} (always recoverable on the leader by sending a
 *   snapshot instead).</li>
 *   <li>{@link RaftException#ErrUnavailable} — index is past the end of
 *   the log; raft will retry once more entries are committed.</li>
 *   <li>{@link RaftException#ErrSnapOutOfDate} — for {@link #createSnapshot}
 *   / {@link #applySnapshot} when the requested index is older than what
 *   we already have.</li>
 *   <li>{@link RaftException#ErrSnapshotTemporarilyUnavailable} — for
 *   {@link #snapshot}, signals "try again later" without aborting.</li>
 *   <li>Unchecked {@link RaftInvariantException} or storage-specific
 *   IOException — fatal; the event loop will terminate.</li>
 * </ul>
 *
 * <h2>Failure handling &amp; acknowledgement (don't swallow)</h2>
 *
 * A write method that cannot make the data durable MUST signal failure by
 * throwing — never return normally on a failed write, because raft then
 * believes the entry / hardState / snapshot is safe and may release outbound
 * messages, violating safety. Storage implementations should throw a
 * {@link RaftInvariantException} tagged with the right
 * {@link RaftInvariantException.Category} so the host can triage the failure
 * instead of seeing an opaque crash:
 *
 * <ul>
 *   <li>{@link RaftInvariantException.Category#STORAGE_IO} — disk/backend I/O
 *   failure. Often transient; the host may stop this node and restart it (a
 *   healthy replica keeps the cluster available).</li>
 *   <li>{@link RaftInvariantException.Category#DATA_CORRUPTION} — persisted
 *   bytes failed to parse / integrity-check. The host should not restart
 *   blindly; recover by wiping local state and re-syncing from a peer
 *   snapshot, or restoring a backup.</li>
 *   <li>{@link RaftInvariantException.Category#BUG} — a contract/logic
 *   violation. File a bug; restarting will hit the same path.</li>
 * </ul>
 *
 * <p><b>Host acknowledgement contract.</b> The synchronous Ready loop is the
 * acknowledgement: the host calls {@link Node#ready()}, persists the entries
 * / hardState / snapshot via this Storage, and only then sends
 * {@code Ready.messages} and calls {@link Node#advance()}. A throw from a
 * write method is the "nack" — the host MUST catch it, refrain from sending
 * the corresponding messages, and stop (or degrade) the node based on the
 * {@code Category}; it must NOT call {@code advance()} as if the write
 * succeeded. In async mode the equivalent nack is "do not feed back the
 * matching {@code MsgStorageAppendResp}/{@code MsgStorageApplyResp}" and stop
 * the node instead (see below).
 *
 * <h2>Async-storage-writes mode</h2>
 *
 * When {@code Config.asyncStorageWrites = true}, the entries appearing in
 * {@code Ready.messages} as {@code MsgStorageAppend} (target =
 * {@link Util#LOCAL_APPEND_THREAD}) and {@code MsgStorageApply}
 * ({@link Util#LOCAL_APPLY_THREAD}) MUST be persisted on a single dedicated
 * thread per category, in the order received, before the matching
 * {@code MsgStorageAppendResp} / {@code MsgStorageApplyResp} is fed back via
 * {@link Node#step(Eraftpb.Message)}. Out-of-order completion violates
 * stableTo monotonicity.
 *
 * <p>If a persist (append) or an apply fails on its dedicated thread, the
 * host MUST NOT feed back the corresponding {@code *Resp} message — doing so
 * would tell raft the data is durable / applied when it is not. Instead, log
 * the {@link RaftInvariantException.Category} and stop the node; resuming the
 * loop after a swallowed failure is the classic source of silent data loss
 * and un-debuggable divergence.
 */
public interface Storage {

    /**
     * Returns the saved HardState and ConfState. Called once at startup.
     */
    InitialStateResult initialState();

    /**
     * Persists the given hard state. Must be durable before raft's outbound
     * messages from the same Ready leave the host's transport.
     *
     * <p>Default implementation throws {@link UnsupportedOperationException};
     * override if your Storage participates in raft's persistence cycle. The
     * default keeps source compatibility for read-only Storage facades used
     * in tests / replays.
     */
    default void setHardState(Eraftpb.HardState hs) {
        throw new UnsupportedOperationException("setHardState not implemented");
    }

    /**
     * Returns a slice of log entries in the range [lo, hi), capped at
     * {@code maxSize} bytes (always at least one entry returned if any are
     * available).
     */
    List<Eraftpb.Entry> entries(long lo, long hi, long maxSize) throws RaftException;

    /**
     * Returns the term of the entry at index {@code i}, or
     * {@link RaftException#ErrCompacted}/{@link RaftException#ErrUnavailable}.
     */
    long term(long i) throws RaftException;

    /** Index of the last entry in the log. */
    long lastIndex();

    /** Index of the first log entry that is possibly available. */
    long firstIndex();

    /**
     * Returns the most recent snapshot. May throw
     * {@link RaftException#ErrSnapshotTemporarilyUnavailable} during async
     * snapshot creation; raft will retry on the next tick.
     */
    Eraftpb.Snapshot snapshot() throws RaftException;

    /**
     * Appends entries to the log. Indices in {@code entries} must be
     * contiguous and immediately follow {@link #lastIndex()} (or overwrite
     * uncommitted suffix). Default throws {@link UnsupportedOperationException}.
     */
    default void append(List<Eraftpb.Entry> entries) {
        throw new UnsupportedOperationException("append not implemented");
    }

    /**
     * Installs a snapshot, replacing the local log up to
     * {@code snap.metadata.index}. Must be durable before raft observes the
     * snapshot as installed. Default throws.
     */
    default void applySnapshot(Eraftpb.Snapshot snap) throws RaftException {
        throw new UnsupportedOperationException("applySnapshot not implemented");
    }

    /**
     * Creates a snapshot at index {@code i} with the given conf state and
     * application data. Default throws.
     */
    default Eraftpb.Snapshot createSnapshot(long i, Eraftpb.ConfState cs, byte[] data) throws RaftException {
        throw new UnsupportedOperationException("createSnapshot not implemented");
    }

    /**
     * Discards log entries with index ≤ {@code compactIndex}. After return,
     * {@link #firstIndex()} ≥ {@code compactIndex + 1}. Default throws.
     */
    default void compact(long compactIndex) throws RaftException {
        throw new UnsupportedOperationException("compact not implemented");
    }

    /**
     * Optional cleanup hook called by the host on shutdown. Default no-op.
     */
    default void close() {}

    // ====================== Streaming snapshots (optional) ======================
    //
    // The byte[]-based createSnapshot / Snapshot-with-inline-data applySnapshot
    // above require the entire snapshot payload to live in heap at once. For a
    // petabyte-scale or otherwise large state machine that is an OOM waiting to
    // happen. The methods below let a Storage persist the snapshot *payload* to
    // a side-car file (or any out-of-band sink) and keep only the small
    // SnapshotMetadata inline, never materializing the full payload.
    //
    // They are additive and default to UnsupportedOperationException so existing
    // Storage implementations keep compiling. Hosts that want streaming should
    // first check {@link #supportsStreamingSnapshot()}.

    /**
     * Sink for snapshot payload bytes. The application writes its serialized
     * state-machine snapshot to {@code out}; the Storage decides where those
     * bytes physically land (e.g. a side-car file) and how they are made
     * durable. The stream is closed by the Storage, not the writer.
     */
    @FunctionalInterface
    interface SnapshotDataWriter {
        void writeTo(OutputStream out) throws IOException;
    }

    /**
     * Whether this Storage implements the streaming snapshot methods
     * ({@link #createSnapshotStreaming(long, Eraftpb.ConfState, SnapshotDataWriter)},
     * {@link #applySnapshot(Eraftpb.Snapshot, InputStream)},
     * {@link #openSnapshotData(Eraftpb.Snapshot)}). Default {@code false}.
     */
    default boolean supportsStreamingSnapshot() {
        return false;
    }

    /**
     * Streaming counterpart of
     * {@link #createSnapshot(long, Eraftpb.ConfState, byte[])}: the payload is
     * produced lazily through {@code writer} into a Storage-managed sink rather
     * than passed as a heap {@code byte[]}.
     *
     * <p>Named distinctly (rather than overloading {@code createSnapshot}) so a
     * {@code createSnapshot(i, cs, null)} call stays unambiguous for existing
     * callers.
     *
     * <p>The returned {@link Eraftpb.Snapshot} carries only metadata — its
     * {@code data} field is empty. Retrieve the payload via
     * {@link #openSnapshotData(Eraftpb.Snapshot)}. Must be durable (payload +
     * metadata) before return. Default throws.
     */
    default Eraftpb.Snapshot createSnapshotStreaming(long i, Eraftpb.ConfState cs, SnapshotDataWriter writer)
            throws RaftException, IOException {
        throw new UnsupportedOperationException("streaming createSnapshot not implemented");
    }

    /**
     * Streaming counterpart of {@link #applySnapshot(Eraftpb.Snapshot)}:
     * installs {@code meta} (metadata only; any inline {@code data} is ignored)
     * and streams the payload from {@code data} into a Storage-managed sink,
     * never holding it all in heap. Must be durable before return and must
     * truncate the local log up to {@code meta.metadata.index}. Default throws.
     */
    default void applySnapshot(Eraftpb.Snapshot meta, InputStream data)
            throws RaftException, IOException {
        throw new UnsupportedOperationException("streaming applySnapshot not implemented");
    }

    /**
     * Opens the payload of the given snapshot for reading as a stream, so a
     * host/transport can send a large snapshot without materializing it. The
     * caller closes the returned stream. Implementations should return the
     * side-car file's stream when present and fall back to the snapshot's
     * inline {@code data} otherwise. Default throws.
     */
    default InputStream openSnapshotData(Eraftpb.Snapshot snap)
            throws RaftException, IOException {
        throw new UnsupportedOperationException("openSnapshotData not implemented");
    }

    record InitialStateResult(Eraftpb.HardState hardState, Eraftpb.ConfState confState) {}
}
