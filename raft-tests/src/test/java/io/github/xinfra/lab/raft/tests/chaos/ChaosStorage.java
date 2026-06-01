/*
 * Copyright 2024-2026 The x-raft-lib Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * SPDX-License-Identifier: Apache-2.0
 */
package io.github.xinfra.lab.raft.tests.chaos;

import io.github.xinfra.lab.raft.RaftException;
import io.github.xinfra.lab.raft.RaftInvariantException;
import io.github.xinfra.lab.raft.Storage;
import io.github.xinfra.lab.raft.proto.Eraftpb;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A fault-injecting {@link Storage} decorator that simulates the persistence
 * failures every Storage backend can plausibly hit in production:
 *
 * <ul>
 *   <li><b>Disk full</b> — {@link #append}, {@link #setHardState},
 *       {@link #applySnapshot}, {@link #createSnapshot} throw
 *       {@link RaftInvariantException}({@code STORAGE_IO}) immediately.
 *       The Storage contract demands the host then refuse to release the
 *       matching {@code Ready.messages}; this fault verifies that the host
 *       does that.</li>
 *
 *   <li><b>fsync failure (silent loss)</b> — {@link #append} <i>succeeds</i>
 *       but the next read (or restart simulation) returns the previous
 *       state. Models a backend whose fsync was lost. Raft's WAL safety
 *       guarantees protect against this only if the host correctly
 *       declares the write as failed.</li>
 *
 *   <li><b>Partial write</b> — only the first {@code N} entries of an
 *       {@link #append} batch are actually persisted; the rest are
 *       silently dropped. Models a torn write at the page boundary.</li>
 * </ul>
 *
 * <p>Faults are toggled per-instance via simple setters so a test can
 * inject "now disk fills up", trigger raft's recovery / failover path,
 * and then heal. All other read methods pass through to the delegate.
 *
 * <p>This wrapper is for tests only. It does NOT model corruption (the
 * audit calls that out separately); only failures that map to existing
 * {@link RaftInvariantException.Category} buckets.
 */
public final class ChaosStorage implements Storage {

    public enum Mode {
        /** Faults disabled: pass through to delegate. */
        HEALTHY,
        /** Every write fails immediately with STORAGE_IO. */
        DISK_FULL,
        /** Write succeeds but data is silently dropped. */
        FSYNC_FAIL,
        /** Only the first {@link #partialWriteLimit} entries of a batch persist. */
        PARTIAL_WRITE
    }

    private final Storage delegate;
    private final AtomicReference<Mode> mode = new AtomicReference<>(Mode.HEALTHY);
    private final AtomicLong partialWriteLimit = new AtomicLong(1);

    /** How many write attempts have hit a fault since construction. */
    private final AtomicLong injectedFailures = new AtomicLong();

    public ChaosStorage(Storage delegate) {
        this.delegate = delegate;
    }

    /** Activate / change the fault mode. */
    public void setMode(Mode m) {
        if (m == null) throw new IllegalArgumentException("mode");
        mode.set(m);
    }

    /** Cap on entries persisted per {@link #append} call in PARTIAL_WRITE mode. */
    public void setPartialWriteLimit(long n) {
        if (n < 0) throw new IllegalArgumentException("n must be >= 0: " + n);
        partialWriteLimit.set(n);
    }

    /** Number of write attempts a fault was injected on so far. */
    public long injectedFailureCount() {
        return injectedFailures.get();
    }

    // ---- Storage write paths ----

    @Override
    public void setHardState(Eraftpb.HardState hs) {
        switch (mode.get()) {
            case DISK_FULL -> {
                injectedFailures.incrementAndGet();
                throw new RaftInvariantException(RaftInvariantException.Category.STORAGE_IO,
                        "chaos: disk full on setHardState");
            }
            case FSYNC_FAIL -> {
                // Silently drop the call. Subsequent initialState() will
                // surface the previous hard state — exactly the kind of
                // post-restart inconsistency raft must not rely on.
                injectedFailures.incrementAndGet();
            }
            default -> delegate.setHardState(hs);
        }
    }

    @Override
    public void append(List<Eraftpb.Entry> entries) {
        switch (mode.get()) {
            case DISK_FULL -> {
                injectedFailures.incrementAndGet();
                throw new RaftInvariantException(RaftInvariantException.Category.STORAGE_IO,
                        "chaos: disk full on append (" + entries.size() + " entries refused)");
            }
            case FSYNC_FAIL -> {
                // Pretend the append succeeded but skip the delegate so a
                // subsequent read sees the previous state.
                injectedFailures.incrementAndGet();
            }
            case PARTIAL_WRITE -> {
                long limit = partialWriteLimit.get();
                if (limit < entries.size()) {
                    injectedFailures.incrementAndGet();
                    delegate.append(entries.subList(0, (int) limit));
                } else {
                    delegate.append(entries);
                }
            }
            default -> delegate.append(entries);
        }
    }

    @Override
    public void applySnapshot(Eraftpb.Snapshot snap) throws RaftException {
        if (mode.get() == Mode.DISK_FULL) {
            injectedFailures.incrementAndGet();
            throw new RaftInvariantException(RaftInvariantException.Category.STORAGE_IO,
                    "chaos: disk full on applySnapshot");
        }
        delegate.applySnapshot(snap);
    }

    @Override
    public Eraftpb.Snapshot createSnapshot(long i, Eraftpb.ConfState cs, byte[] data) throws RaftException {
        if (mode.get() == Mode.DISK_FULL) {
            injectedFailures.incrementAndGet();
            throw new RaftInvariantException(RaftInvariantException.Category.STORAGE_IO,
                    "chaos: disk full on createSnapshot");
        }
        return delegate.createSnapshot(i, cs, data);
    }

    @Override
    public void compact(long compactIndex) throws RaftException {
        if (mode.get() == Mode.DISK_FULL) {
            injectedFailures.incrementAndGet();
            throw new RaftInvariantException(RaftInvariantException.Category.STORAGE_IO,
                    "chaos: disk full on compact");
        }
        delegate.compact(compactIndex);
    }

    // ---- Storage read paths (pass-through) ----

    @Override
    public InitialStateResult initialState() {
        return delegate.initialState();
    }

    @Override
    public List<Eraftpb.Entry> entries(long lo, long hi, long maxSize) throws RaftException {
        return delegate.entries(lo, hi, maxSize);
    }

    @Override
    public long term(long i) throws RaftException {
        return delegate.term(i);
    }

    @Override
    public long lastIndex() {
        return delegate.lastIndex();
    }

    @Override
    public long firstIndex() {
        return delegate.firstIndex();
    }

    @Override
    public Eraftpb.Snapshot snapshot() throws RaftException {
        return delegate.snapshot();
    }

    // ---- Streaming snapshot (pass-through) ----

    @Override
    public boolean supportsStreamingSnapshot() {
        return delegate.supportsStreamingSnapshot();
    }

    @Override
    public Eraftpb.Snapshot createSnapshotStreaming(long i, Eraftpb.ConfState cs, SnapshotDataWriter writer)
            throws RaftException, IOException {
        if (mode.get() == Mode.DISK_FULL) {
            injectedFailures.incrementAndGet();
            throw new RaftInvariantException(RaftInvariantException.Category.STORAGE_IO,
                    "chaos: disk full on streaming createSnapshot");
        }
        return delegate.createSnapshotStreaming(i, cs, writer);
    }

    @Override
    public void applySnapshot(Eraftpb.Snapshot meta, InputStream data) throws RaftException, IOException {
        if (mode.get() == Mode.DISK_FULL) {
            injectedFailures.incrementAndGet();
            try { data.close(); } catch (IOException ignored) { /* best-effort */ }
            throw new RaftInvariantException(RaftInvariantException.Category.STORAGE_IO,
                    "chaos: disk full on streaming applySnapshot");
        }
        delegate.applySnapshot(meta, data);
    }

    @Override
    public InputStream openSnapshotData(Eraftpb.Snapshot snap) throws RaftException, IOException {
        return delegate.openSnapshotData(snap);
    }

    @Override
    public void close() {
        delegate.close();
    }

    // Convenience helper for tests that want to write the SnapshotDataWriter
    // pattern themselves.
    public OutputStream noop() {
        return OutputStream.nullOutputStream();
    }
}
