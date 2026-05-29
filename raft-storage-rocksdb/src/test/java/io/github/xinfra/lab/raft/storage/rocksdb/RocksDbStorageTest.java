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

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RocksDbStorageTest {

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

    @Test
    void emptyStorageReturnsDefaults() throws Exception {
        assertThat(storage.firstIndex()).isEqualTo(1);
        assertThat(storage.lastIndex()).isEqualTo(0);
        assertThat(storage.snapshot().getMetadata().getIndex()).isEqualTo(0);
        Storage_InitialStateAssertions.assertEmpty(storage.initialState());
    }

    @Test
    void appendAndRead() throws Exception {
        storage.append(List.of(entry(1, 1, "a"), entry(2, 1, "b"), entry(3, 1, "c")));
        assertThat(storage.firstIndex()).isEqualTo(1);
        assertThat(storage.lastIndex()).isEqualTo(3);
        assertThat(storage.term(2)).isEqualTo(1);
        List<Eraftpb.Entry> got = storage.entries(1, 4, Long.MAX_VALUE);
        assertThat(got).hasSize(3);
        assertThat(got.get(2).getData().toStringUtf8()).isEqualTo("c");
    }

    @Test
    void truncatingAppendOverwritesUncommittedSuffix() throws Exception {
        storage.append(List.of(entry(1, 1, "a"), entry(2, 1, "b"), entry(3, 1, "c")));
        // Replace suffix from index 2 with two entries at term 2.
        storage.append(List.of(entry(2, 2, "B"), entry(3, 2, "C")));
        assertThat(storage.lastIndex()).isEqualTo(3);
        assertThat(storage.term(3)).isEqualTo(2);
        List<Eraftpb.Entry> got = storage.entries(1, 4, Long.MAX_VALUE);
        assertThat(got.get(0).getTerm()).isEqualTo(1);
        assertThat(got.get(1).getTerm()).isEqualTo(2);
        assertThat(got.get(2).getTerm()).isEqualTo(2);
    }

    @Test
    void compactDiscardsPrefix() throws Exception {
        List<Eraftpb.Entry> ents = new ArrayList<>();
        for (int i = 1; i <= 10; i++) ents.add(entry(i, 1, String.valueOf(i)));
        storage.append(ents);
        storage.compact(5);
        // After compact(5), firstIndex must be > 5.
        assertThat(storage.firstIndex()).isGreaterThan(5);
        assertThatThrownBy(() -> storage.entries(1, 5, Long.MAX_VALUE))
                .isSameAs(RaftException.ErrCompacted);
        List<Eraftpb.Entry> got = storage.entries(storage.firstIndex(), 11, Long.MAX_VALUE);
        assertThat(got.get(got.size() - 1).getIndex()).isEqualTo(10L);
    }

    @Test
    void snapshotInstallTruncatesLog() throws Exception {
        List<Eraftpb.Entry> ents = new ArrayList<>();
        for (int i = 1; i <= 10; i++) ents.add(entry(i, 2, String.valueOf(i)));
        storage.append(ents);

        Eraftpb.Snapshot snap = Eraftpb.Snapshot.newBuilder()
                .setMetadata(Eraftpb.SnapshotMetadata.newBuilder()
                        .setIndex(7).setTerm(2)
                        .setConfState(Eraftpb.ConfState.newBuilder().addVoters(1).addVoters(2)))
                .setData(ByteString.copyFromUtf8("opaque"))
                .build();
        storage.applySnapshot(snap);

        assertThat(storage.snapshot().getMetadata().getIndex()).isEqualTo(7L);
        // Entries 1..7 are gone; firstIndex = 8.
        assertThat(storage.firstIndex()).isEqualTo(8L);
        // term(7) is from snapshot meta, not log.
        assertThat(storage.term(7)).isEqualTo(2L);
    }

    @Test
    void hardStateRoundTrips() throws Exception {
        Eraftpb.HardState hs = Eraftpb.HardState.newBuilder()
                .setTerm(5).setVote(3).setCommit(7).build();
        storage.setHardState(hs);

        // Reopen — fsync=false here, but RocksDB still holds it until close.
        storage.close();
        storage = new RocksDbStorage(tmp.resolve("db"), false);

        Eraftpb.HardState got = storage.initialState().hardState();
        assertThat(got.getTerm()).isEqualTo(5);
        assertThat(got.getVote()).isEqualTo(3);
        assertThat(got.getCommit()).isEqualTo(7);
    }

    @Test
    void writeBatchedIsAtomicAcrossEntriesAndState() throws Exception {
        List<Eraftpb.Entry> ents = List.of(entry(1, 1, "a"), entry(2, 1, "b"));
        Eraftpb.HardState hs = Eraftpb.HardState.newBuilder()
                .setTerm(1).setVote(1).setCommit(2).build();
        storage.writeBatched(ents, hs, null);

        assertThat(storage.lastIndex()).isEqualTo(2);
        assertThat(storage.initialState().hardState().getCommit()).isEqualTo(2);
    }

    @Test
    void persistenceAcrossReopen() throws Exception {
        // fsync mode for this scenario.
        storage.close();
        storage = new RocksDbStorage(tmp.resolve("db"), true);
        storage.append(List.of(entry(1, 1, "x"), entry(2, 1, "y")));
        storage.setHardState(Eraftpb.HardState.newBuilder().setTerm(1).setCommit(2).build());
        storage.close();

        storage = new RocksDbStorage(tmp.resolve("db"), true);
        assertThat(storage.lastIndex()).isEqualTo(2);
        assertThat(storage.initialState().hardState().getCommit()).isEqualTo(2);
        List<Eraftpb.Entry> got = storage.entries(1, 3, Long.MAX_VALUE);
        assertThat(got).hasSize(2);
        assertThat(got.get(1).getData().toStringUtf8()).isEqualTo("y");
    }

    // ====================== Streaming snapshots (side-car file) ======================

    @Test
    void streamingCreateSnapshotPersistsToSidecarAndReadsBack() throws Exception {
        storage.append(List.of(entry(1, 1, "a"), entry(2, 1, "b"), entry(3, 1, "c")));

        byte[] payload = new byte[3 * (1 << 20) + 11]; // ~3 MiB
        for (int i = 0; i < payload.length; i++) payload[i] = (byte) (i * 13 + 5);

        Eraftpb.ConfState cs = Eraftpb.ConfState.newBuilder().addVoters(1).build();
        Eraftpb.Snapshot meta = storage.createSnapshotStreaming(3, cs, out -> out.write(payload));

        // Returned snapshot carries metadata only — no inline payload.
        assertThat(meta.getMetadata().getIndex()).isEqualTo(3);
        assertThat(meta.getData().isEmpty()).isTrue();
        // The side-car file exists on disk.
        try (java.util.stream.Stream<Path> s =
                     java.nio.file.Files.list(tmp.resolve("db").resolve("snapshots"))) {
            assertThat(s.count()).isGreaterThanOrEqualTo(1);
        }

        // openSnapshotData streams the full payload back byte-for-byte.
        try (java.io.InputStream in = storage.openSnapshotData(storage.snapshot())) {
            assertThat(in.readAllBytes()).isEqualTo(payload);
        }
    }

    @Test
    void streamingApplySnapshotInstallsAndTruncatesLog() throws Exception {
        storage.append(List.of(entry(1, 1, "a"), entry(2, 1, "b")));

        byte[] payload = new byte[1024];
        for (int i = 0; i < payload.length; i++) payload[i] = (byte) i;

        Eraftpb.Snapshot incoming = Eraftpb.Snapshot.newBuilder()
                .setMetadata(Eraftpb.SnapshotMetadata.newBuilder().setIndex(5).setTerm(3))
                .build();
        storage.applySnapshot(incoming, new java.io.ByteArrayInputStream(payload));

        assertThat(storage.snapshot().getMetadata().getIndex()).isEqualTo(5);
        assertThat(storage.firstIndex()).isEqualTo(6);
        assertThat(storage.lastIndex()).isEqualTo(5);
        // Old log entries (<=5) are gone.
        assertThatThrownBy(() -> storage.entries(1, 2, Long.MAX_VALUE))
                .isInstanceOf(RaftException.class);
        try (java.io.InputStream in = storage.openSnapshotData(storage.snapshot())) {
            assertThat(in.readAllBytes()).isEqualTo(payload);
        }
    }

    @Test
    void streamingSnapshotSurvivesReopenAndReapsStale() throws Exception {
        storage.close();
        storage = new RocksDbStorage(tmp.resolve("db"), true);
        storage.append(List.of(entry(1, 1, "a"), entry(2, 1, "b")));

        byte[] first = "first-snapshot-payload".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        storage.createSnapshotStreaming(2, Eraftpb.ConfState.newBuilder().addVoters(1).build(),
                out -> out.write(first));

        // A second, newer snapshot must replace the first side-car file.
        storage.append(List.of(entry(3, 1, "c")));
        byte[] second = "second-snapshot-payload".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        storage.createSnapshotStreaming(3, Eraftpb.ConfState.newBuilder().addVoters(1).build(),
                out -> out.write(second));
        storage.close();

        // Reopen: streamed snapshot + payload still readable.
        storage = new RocksDbStorage(tmp.resolve("db"), true);
        assertThat(storage.snapshot().getMetadata().getIndex()).isEqualTo(3);
        try (java.io.InputStream in = storage.openSnapshotData(storage.snapshot())) {
            assertThat(in.readAllBytes()).isEqualTo(second);
        }
        // Only one side-car file remains (the stale one was reaped).
        try (java.util.stream.Stream<Path> s =
                     java.nio.file.Files.list(tmp.resolve("db").resolve("snapshots"))) {
            assertThat(s.count()).isEqualTo(1);
        }
    }

    @Test
    void inlineSnapshotAfterStreamingDropsSidecarPointer() throws Exception {
        storage.append(List.of(entry(1, 1, "a"), entry(2, 1, "b")));
        storage.createSnapshotStreaming(2, Eraftpb.ConfState.newBuilder().addVoters(1).build(),
                out -> out.write(new byte[]{1, 2, 3}));

        // Now take an inline snapshot at a higher index.
        storage.append(List.of(entry(3, 1, "c")));
        byte[] inline = new byte[]{9, 8, 7, 6};
        storage.createSnapshot(3, Eraftpb.ConfState.newBuilder().addVoters(1).build(), inline);

        // openSnapshotData must now return the inline bytes, not the old file.
        try (java.io.InputStream in = storage.openSnapshotData(storage.snapshot())) {
            assertThat(in.readAllBytes()).isEqualTo(inline);
        }
    }

    @Test
    void supportsStreamingSnapshotIsTrue() {
        assertThat(storage.supportsStreamingSnapshot()).isTrue();
    }

    static class Storage_InitialStateAssertions {
        static void assertEmpty(io.github.xinfra.lab.raft.Storage.InitialStateResult r) {
            assertThat(r.hardState().getTerm()).isZero();
            assertThat(r.hardState().getVote()).isZero();
            assertThat(r.hardState().getCommit()).isZero();
            assertThat(r.confState().getVotersCount()).isZero();
        }
    }
}
