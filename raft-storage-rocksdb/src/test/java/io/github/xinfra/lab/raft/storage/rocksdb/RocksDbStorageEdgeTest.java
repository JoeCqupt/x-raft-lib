/*
 * Copyright 2024-2026 The x-raft-lib Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * SPDX-License-Identifier: Apache-2.0
 */
package io.github.xinfra.lab.raft.storage.rocksdb;

import com.google.protobuf.ByteString;
import io.github.xinfra.lab.raft.RaftException;
import io.github.xinfra.lab.raft.proto.Eraftpb;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Targeted coverage for {@link RocksDbStorage} paths the original test
 * class doesn't reach — error branches ({@code ErrCompacted},
 * {@code ErrSnapOutOfDate}), invariant checks, the stage-then-finalize
 * out-of-band flow, the {@code openSnapshotData} inline fallback,
 * {@code setApplied}/{@code setConfState} persistence, the
 * fsync=false ctor, and the boot-time *.tmp side-car reaper.
 *
 * <p>The two test classes intentionally overlap on nothing — happy
 * paths stay in {@code RocksDbStorageTest}, error / edge paths live
 * here. Splitting keeps each file short and easy to grep.
 */
class RocksDbStorageEdgeTest {

    @TempDir Path tmp;
    RocksDbStorage storage;

    @BeforeEach
    void open() throws Exception {
        storage = new RocksDbStorage(tmp.resolve("db"), false);
    }

    @AfterEach
    void close() {
        if (storage != null) storage.close();
    }

    private static Eraftpb.Entry entry(long idx, long term, String data) {
        return Eraftpb.Entry.newBuilder()
                .setIndex(idx).setTerm(term)
                .setData(ByteString.copyFromUtf8(data))
                .build();
    }

    private static Eraftpb.Snapshot snap(long index, long term, byte[] data, long voter) {
        Eraftpb.Snapshot.Builder b = Eraftpb.Snapshot.newBuilder()
                .setMetadata(Eraftpb.SnapshotMetadata.newBuilder()
                        .setIndex(index).setTerm(term)
                        .setConfState(Eraftpb.ConfState.newBuilder().addVoters(voter)));
        if (data != null) b.setData(ByteString.copyFrom(data));
        return b.build();
    }

    // ============= entries() error branches =============

    @Test
    void entriesBelowFirstIndexThrowsErrCompacted() throws Exception {
        storage.append(List.of(entry(1, 1, "a"), entry(2, 1, "b"), entry(3, 1, "c")));
        // Establish a snapshot at index 2; firstIndex becomes 3.
        storage.applySnapshot(snap(2, 1, new byte[]{0}, 1));
        assertThatThrownBy(() -> storage.entries(1, 4, Long.MAX_VALUE))
                .isInstanceOf(RaftException.class)
                .extracting("code").isEqualTo(RaftException.Code.COMPACTED);
    }

    @Test
    void entriesBeyondLastIndexThrowsInvariant() throws Exception {
        storage.append(List.of(entry(1, 1, "a"), entry(2, 1, "b")));
        // hi > lastIndex+1 is a hard invariant violation (not a regular
        // RaftException-coded condition) — callers must clamp themselves.
        assertThatThrownBy(() -> storage.entries(1, 10, Long.MAX_VALUE))
                .isInstanceOf(io.github.xinfra.lab.raft.RaftInvariantException.class)
                .hasMessageContaining("out of bound");
    }

    @Test
    void entriesHonoursMaxSizeBound() throws Exception {
        storage.append(List.of(
                entry(1, 1, "aaaaaaaa"),
                entry(2, 1, "bbbbbbbb"),
                entry(3, 1, "cccccccc")));
        // maxSize=0 still returns at least one entry (etcd-raft semantic:
        // never return empty just because maxSize is tiny).
        List<Eraftpb.Entry> got = storage.entries(1, 4, 0);
        assertThat(got).hasSize(1);
        assertThat(got.get(0).getIndex()).isEqualTo(1L);
    }

    // ============= term() boundary cases =============

    @Test
    void termAtIndexZeroIsZeroOnFreshStorage() throws Exception {
        // Bootstrap convention: term(0) == 0 on fresh storage so
        // RaftLog.lastEntryID() works at the very start.
        assertThat(storage.term(0)).isZero();
    }

    @Test
    void termBelowSnapshotIndexThrowsErrCompacted() throws Exception {
        storage.append(List.of(entry(1, 1, "a"), entry(2, 1, "b"), entry(3, 1, "c")));
        storage.applySnapshot(snap(2, 1, new byte[]{0}, 1));
        assertThatThrownBy(() -> storage.term(1))
                .isInstanceOf(RaftException.class)
                .extracting("code").isEqualTo(RaftException.Code.COMPACTED);
    }

    @Test
    void termBeyondLastIndexThrowsErrUnavailable() throws Exception {
        storage.append(List.of(entry(1, 1, "a")));
        assertThatThrownBy(() -> storage.term(5))
                .isInstanceOf(RaftException.class)
                .extracting("code").isEqualTo(RaftException.Code.UNAVAILABLE);
    }

    @Test
    void termAtSnapshotIndexReturnsSnapshotTerm() throws Exception {
        storage.append(List.of(entry(1, 1, "a"), entry(2, 1, "b")));
        storage.applySnapshot(snap(2, 7, new byte[]{0}, 1));
        // Snapshot establishes the dummy term at its index — even though
        // log entry 2 is now compacted, term(2) still returns the
        // snapshot's term so RaftLog matchTerm at the boundary works.
        assertThat(storage.term(2)).isEqualTo(7);
    }

    // ============= applied / confState helpers =============

    @Test
    void appliedRoundTripsAcrossReopen() throws Exception {
        assertThat(storage.getApplied()).as("default applied is 0").isZero();
        storage.setApplied(42L);
        assertThat(storage.getApplied()).isEqualTo(42L);
        storage.close();
        storage = new RocksDbStorage(tmp.resolve("db"), false);
        assertThat(storage.getApplied())
                .as("applied watermark must survive reopen — host uses it on restart")
                .isEqualTo(42L);
    }

    @Test
    void confStateRoundTripsAcrossReopen() throws Exception {
        Eraftpb.ConfState cs = Eraftpb.ConfState.newBuilder()
                .addVoters(1).addVoters(2).addVoters(3)
                .addLearners(4)
                .build();
        storage.setConfState(cs);
        storage.close();
        storage = new RocksDbStorage(tmp.resolve("db"), false);
        // The explicitly persisted ConfState takes precedence over the
        // snapshot's ConfState in initialState() recovery order.
        Eraftpb.ConfState recovered = storage.initialState().confState();
        assertThat(recovered.getVotersList()).containsExactly(1L, 2L, 3L);
        assertThat(recovered.getLearnersList()).containsExactly(4L);
    }

    // ============= applySnapshot inline — error branches =============

    @Test
    void applySnapshotInlineRejectsStaleIndex() throws Exception {
        storage.append(List.of(entry(1, 1, "a"), entry(2, 1, "b")));
        storage.applySnapshot(snap(2, 1, new byte[]{0}, 1));
        // Re-applying the same index (or older) is ErrSnapOutOfDate.
        assertThatThrownBy(() -> storage.applySnapshot(snap(2, 1, new byte[]{1}, 1)))
                .isInstanceOf(RaftException.class)
                .extracting("code").isEqualTo(RaftException.Code.SNAP_OUT_OF_DATE);
    }

    @Test
    void applySnapshotInlineSkipsAlreadyInstalledOutOfBand() throws Exception {
        // Streaming path installs a side-car at index 5 with payload.
        // A subsequent inline applySnapshot with the SAME index and
        // EMPTY data must NO-OP — re-persisting inline would orphan
        // the side-car. This is the {@code alreadyInstalledOutOfBand}
        // short-circuit; it's load-bearing for the OOB ready cycle.
        storage.append(List.of(entry(1, 1, "a")));
        storage.applySnapshot(snap(5, 1, null, 1),
                new ByteArrayInputStream(new byte[]{1, 2, 3, 4, 5}));
        // Now caller comes back with the same metadata-only snapshot.
        // The inline applySnapshot must not corrupt the side-car link.
        storage.applySnapshot(snap(5, 1, null, 1));
        // Side-car still serves the original payload.
        try (InputStream in = storage.openSnapshotData(storage.snapshot())) {
            assertThat(in.readAllBytes()).isEqualTo(new byte[]{1, 2, 3, 4, 5});
        }
    }

    // ============= createSnapshot — error branches =============

    @Test
    void createSnapshotRejectsStaleIndex() throws Exception {
        storage.append(List.of(entry(1, 1, "a"), entry(2, 1, "b")));
        storage.createSnapshot(2, Eraftpb.ConfState.newBuilder().addVoters(1).build(), new byte[]{0});
        // Same index second time → ErrSnapOutOfDate.
        assertThatThrownBy(() -> storage.createSnapshot(2,
                Eraftpb.ConfState.newBuilder().addVoters(1).build(), new byte[]{1}))
                .isInstanceOf(RaftException.class)
                .extracting("code").isEqualTo(RaftException.Code.SNAP_OUT_OF_DATE);
    }

    @Test
    void createSnapshotBeyondLastIndexThrowsInvariant() throws Exception {
        storage.append(List.of(entry(1, 1, "a"), entry(2, 1, "b")));
        assertThatThrownBy(() -> storage.createSnapshot(99,
                Eraftpb.ConfState.newBuilder().addVoters(1).build(), new byte[]{0}))
                .hasMessageContaining("lastIndex");
    }

    @Test
    void createSnapshotStreamingRejectsStaleIndex() throws Exception {
        storage.append(List.of(entry(1, 1, "a"), entry(2, 1, "b")));
        storage.createSnapshotStreaming(2,
                Eraftpb.ConfState.newBuilder().addVoters(1).build(),
                out -> out.write(new byte[]{0}));
        assertThatThrownBy(() -> storage.createSnapshotStreaming(2,
                Eraftpb.ConfState.newBuilder().addVoters(1).build(),
                out -> out.write(new byte[]{1})))
                .isInstanceOf(RaftException.class)
                .extracting("code").isEqualTo(RaftException.Code.SNAP_OUT_OF_DATE);
    }

    // ============= compact() error branches =============

    @Test
    void compactBelowFirstIndexThrowsErrCompacted() throws Exception {
        storage.append(List.of(entry(1, 1, "a"), entry(2, 1, "b")));
        storage.applySnapshot(snap(2, 1, new byte[]{0}, 1));
        // firstIndex is now 3; compact(2) → ErrCompacted.
        assertThatThrownBy(() -> storage.compact(2))
                .isInstanceOf(RaftException.class)
                .extracting("code").isEqualTo(RaftException.Code.COMPACTED);
    }

    @Test
    void compactBeyondLastIndexThrowsInvariant() throws Exception {
        storage.append(List.of(entry(1, 1, "a"), entry(2, 1, "b")));
        assertThatThrownBy(() -> storage.compact(99))
                .hasMessageContaining("lastIndex");
    }

    // ============= stage-then-finalize OOB flow =============

    @Test
    void stageSnapshotDataThenWriteBatchedFinalises() throws Exception {
        // Mimic the host integration: transport calls stageSnapshotData
        // with the payload before raft restores. Then raft drains a Ready
        // containing the metadata-only snapshot, and the host calls
        // writeBatched(rd.entries, rd.hardState, rd.snapshot) to commit.
        // The pre-staged side-car must be LINKED, not dropped, by that
        // writeBatched call — otherwise the bytes leak.
        storage.append(List.of(entry(1, 1, "a")));
        byte[] payload = "staged-then-finalised".getBytes(StandardCharsets.UTF_8);
        Eraftpb.Snapshot meta = snap(7, 2, null, 1);
        storage.stageSnapshotData(meta, new ByteArrayInputStream(payload));
        // After stageSnapshotData, KEY_SNAPSHOT must still be empty —
        // raft hasn't restored yet, so storage shouldn't expose a snapshot.
        assertThat(storage.snapshot().getMetadata().getIndex())
                .as("staged-only — snapshot not yet visible to raft").isZero();
        // Now the host finalizes:
        storage.writeBatched(List.of(), Eraftpb.HardState.getDefaultInstance(), meta);
        assertThat(storage.snapshot().getMetadata().getIndex()).isEqualTo(7L);
        // Side-car is reachable through openSnapshotData.
        try (InputStream in = storage.openSnapshotData(storage.snapshot())) {
            assertThat(in.readAllBytes()).isEqualTo(payload);
        }
    }

    @Test
    void stageSnapshotDataDiscardsWhenLocalIsAhead() throws Exception {
        storage.append(List.of(entry(1, 1, "a"), entry(2, 1, "b")));
        storage.applySnapshot(snap(5, 1, new byte[]{0xa}, 1));
        long existingIdx = storage.snapshot().getMetadata().getIndex();

        // A staged snapshot at LOWER index than what we already have:
        // contract is "drain payload, do nothing". No exception; the
        // bytes don't leak as a side-car file because the caller's
        // existing-snapshot precondition is already past this index.
        byte[] payload = new byte[256];
        storage.stageSnapshotData(snap(3, 1, null, 1), new ByteArrayInputStream(payload));

        // Our snapshot is unchanged.
        assertThat(storage.snapshot().getMetadata().getIndex()).isEqualTo(existingIdx);
        // Side-car directory should have no orphan from the staged-and-discarded
        // attempt — verify no file at the would-have-been-staged name.
        try (Stream<Path> s = Files.list(tmp.resolve("db").resolve("snapshots"))) {
            long stagedCount = s.filter(p -> p.getFileName().toString().contains("-3-1.")).count();
            assertThat(stagedCount).as("discarded stage must not leak a side-car").isZero();
        }
    }

    // ============= openSnapshotData inline fallback =============

    @Test
    void openSnapshotDataReturnsInlineBytesWhenNoSidecar() throws Exception {
        // No side-car has ever been written. openSnapshotData(snap)
        // returns the inline payload from the snapshot proto directly.
        // This is the path for hosts that mix inline + streaming.
        byte[] inline = "inline-only".getBytes(StandardCharsets.UTF_8);
        try (InputStream in = storage.openSnapshotData(snap(99, 1, inline, 1))) {
            assertThat(in.readAllBytes()).isEqualTo(inline);
        }
    }

    @Test
    void openSnapshotDataReturnsEmptyForNullSnapshot() throws Exception {
        // Defensive: caller may pass null to mean "current snapshot, no
        // hint". If no side-car AND no current snapshot, the result is
        // an empty stream rather than NPE.
        try (InputStream in = storage.openSnapshotData(null)) {
            assertThat(in.readAllBytes()).isEmpty();
        }
    }

    // ============= writeBatched fast paths =============

    @Test
    void writeBatchedSkipsWhenSnapshotAlreadyInstalled() throws Exception {
        // The same metadata-only snapshot fed twice through writeBatched
        // must no-op the second time — the side-car link is already
        // KEY_SNAPSHOT + KEY_SNAPSHOT_FILE consistent; re-persisting
        // could orphan the side-car.
        storage.append(List.of(entry(1, 1, "a")));
        byte[] payload = "once".getBytes(StandardCharsets.UTF_8);
        Eraftpb.Snapshot meta = snap(7, 2, null, 1);
        storage.stageSnapshotData(meta, new ByteArrayInputStream(payload));
        storage.writeBatched(List.of(), Eraftpb.HardState.getDefaultInstance(), meta);
        // Second writeBatched with the same metadata-only snap — must
        // no-op without throwing or losing the side-car.
        storage.writeBatched(List.of(), Eraftpb.HardState.getDefaultInstance(), meta);
        try (InputStream in = storage.openSnapshotData(storage.snapshot())) {
            assertThat(in.readAllBytes()).isEqualTo(payload);
        }
    }

    @Test
    void writeBatchedAcceptsHardStateAndEntriesOnly() throws Exception {
        // Smoke: the very common Ready-cycle shape (entries + hardState,
        // no snapshot). Must produce a coherent state.
        Eraftpb.HardState hs = Eraftpb.HardState.newBuilder()
                .setTerm(3).setVote(1).setCommit(2).build();
        storage.writeBatched(List.of(entry(1, 3, "x"), entry(2, 3, "y")), hs, null);
        assertThat(storage.lastIndex()).isEqualTo(2);
        assertThat(storage.initialState().hardState().getTerm()).isEqualTo(3);
        assertThat(storage.initialState().hardState().getCommit()).isEqualTo(2);
    }

    // ============= fsync=false ctor (default for tests) =============

    @Test
    void fsyncFalseCtorWritesAreVisibleWithoutDurabilityGuarantee() throws Exception {
        // We're already using fsync=false in the @BeforeEach; this test
        // explicitly verifies the alt-ctor wires the WriteOptions
        // correctly (writes succeed and read back). Durability vs sync
        // is the host's choice.
        storage.append(List.of(entry(1, 1, "no-fsync"), entry(2, 1, "second")));
        assertThat(storage.lastIndex()).isEqualTo(2);
        List<Eraftpb.Entry> got = storage.entries(1, 3, Long.MAX_VALUE);
        assertThat(got).extracting(Eraftpb.Entry::getData)
                .containsExactly(ByteString.copyFromUtf8("no-fsync"), ByteString.copyFromUtf8("second"));
    }

    @Test
    void fsyncTrueCtorIsTheDefault() throws Exception {
        // Path through the no-arg ctor — sets fsync=true. We don't
        // assert on the durability semantic (would need a crash sim);
        // we just verify the ctor wiring and a round-trip work.
        Path dir = tmp.resolve("default-ctor-db");
        try (RocksDbStorage s = new RocksDbStorage(dir)) {
            s.append(List.of(entry(1, 1, "fsync-on")));
            assertThat(s.lastIndex()).isEqualTo(1);
        }
    }

    // ============= boot-time .tmp side-car reaper =============

    @Test
    void openReapsLeftoverTmpSidecarFiles() throws Exception {
        // Simulate a crash mid side-car write: a stale *.tmp file was
        // left behind in the snapshots subdir. On reopen, the storage
        // must drop those before serving any requests — otherwise they
        // accumulate forever.
        storage.close();
        Path snapDir = tmp.resolve("db").resolve("snapshots");
        Files.createDirectories(snapDir);
        Path stale = snapDir.resolve("snap-99-1.data.tmp");
        Files.write(stale, new byte[]{1, 2, 3});
        assertThat(Files.exists(stale)).isTrue();

        // Reopen — boot should reap the leftover .tmp.
        storage = new RocksDbStorage(tmp.resolve("db"), false);
        assertThat(Files.exists(stale))
                .as("leftover .tmp from prior crash must be reaped on open")
                .isFalse();
    }

    @Test
    void closeIsIdempotent() throws Exception {
        // Double-close must not throw — hosts may close on shutdown AND
        // implicitly via try-with-resources, or call it from multiple
        // shutdown hooks.
        storage.close();
        storage.close();
        // Mark already-closed so the @AfterEach doesn't double-close.
        storage = null;
    }
}
