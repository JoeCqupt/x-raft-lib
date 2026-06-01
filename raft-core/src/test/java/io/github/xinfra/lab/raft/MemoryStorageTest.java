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
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.github.xinfra.lab.raft.internal.TestUtil.index;
import static org.assertj.core.api.Assertions.*;

/**
 * Tests for MemoryStorage, ported from etcd-raft storage_test.go.
 */
class MemoryStorageTest {

    @Test
    void testStorageTerm() throws RaftException {
        List<Eraftpb.Entry> ents = index(3).terms(3, 4, 5);

        // i=2: ErrCompacted
        assertThatThrownBy(() -> new MemoryStorage(ents).term(2))
                .isSameAs(RaftException.ErrCompacted);
        // i=3..5: normal
        assertThat(new MemoryStorage(ents).term(3)).isEqualTo(3);
        assertThat(new MemoryStorage(ents).term(4)).isEqualTo(4);
        assertThat(new MemoryStorage(ents).term(5)).isEqualTo(5);
        // i=6: ErrUnavailable
        assertThatThrownBy(() -> new MemoryStorage(ents).term(6))
                .isSameAs(RaftException.ErrUnavailable);
    }

    @Test
    void testStorageEntries() throws RaftException {
        List<Eraftpb.Entry> ents = index(3).terms(3, 4, 5, 6);

        // lo=2: ErrCompacted
        assertThatThrownBy(() -> new MemoryStorage(ents).entries(2, 6, Long.MAX_VALUE))
                .isSameAs(RaftException.ErrCompacted);
        // lo=3: ErrCompacted (lo <= offset)
        assertThatThrownBy(() -> new MemoryStorage(ents).entries(3, 4, Long.MAX_VALUE))
                .isSameAs(RaftException.ErrCompacted);

        MemoryStorage s = new MemoryStorage(ents);
        assertThat(s.entries(4, 5, Long.MAX_VALUE)).isEqualTo(index(4).terms(4));
        assertThat(s.entries(4, 6, Long.MAX_VALUE)).isEqualTo(index(4).terms(4, 5));
        assertThat(s.entries(4, 7, Long.MAX_VALUE)).isEqualTo(index(4).terms(4, 5, 6));

        // even if maxsize is zero, the first entry should be returned
        assertThat(s.entries(4, 7, 0)).isEqualTo(index(4).terms(4));

        // limit to 2
        long size2 = ents.get(1).getSerializedSize() + ents.get(2).getSerializedSize();
        assertThat(s.entries(4, 7, size2)).isEqualTo(index(4).terms(4, 5));

        // all
        long sizeAll = size2 + ents.get(3).getSerializedSize();
        assertThat(s.entries(4, 7, sizeAll)).isEqualTo(index(4).terms(4, 5, 6));
    }

    @Test
    void testStorageLastIndex() throws RaftException {
        List<Eraftpb.Entry> ents = index(3).terms(3, 4, 5);
        MemoryStorage s = new MemoryStorage(ents);

        assertThat(s.lastIndex()).isEqualTo(5);

        s.append(index(6).terms(5));
        assertThat(s.lastIndex()).isEqualTo(6);
    }

    @Test
    void testStorageFirstIndex() throws RaftException {
        List<Eraftpb.Entry> ents = index(3).terms(3, 4, 5);
        MemoryStorage s = new MemoryStorage(ents);

        assertThat(s.firstIndex()).isEqualTo(4);

        s.compact(4);
        assertThat(s.firstIndex()).isEqualTo(5);
    }

    @Test
    void testStorageCompact() throws RaftException {
        List<Eraftpb.Entry> ents = index(3).terms(3, 4, 5);

        assertCompact(ents, 2, RaftException.ErrCompacted, 3, 3, 3);
        assertCompact(ents, 3, RaftException.ErrCompacted, 3, 3, 3);
        assertCompact(ents, 4, null, 4, 4, 2);
        assertCompact(ents, 5, null, 5, 5, 1);
    }

    private void assertCompact(List<Eraftpb.Entry> ents, long compactIndex,
                               RaftException expectedErr, long wIndex, long wTerm, int wLen) {
        MemoryStorage s = new MemoryStorage(ents);
        if (expectedErr != null) {
            assertThatThrownBy(() -> s.compact(compactIndex)).isSameAs(expectedErr);
        } else {
            assertThatNoException().isThrownBy(() -> s.compact(compactIndex));
        }
        List<Eraftpb.Entry> remaining = s.getEntries();
        assertThat(remaining.get(0).getIndex()).isEqualTo(wIndex);
        assertThat(remaining.get(0).getTerm()).isEqualTo(wTerm);
        assertThat(remaining).hasSize(wLen);
    }

    @Test
    void testStorageCreateSnapshot() throws RaftException {
        List<Eraftpb.Entry> ents = index(3).terms(3, 4, 5);
        Eraftpb.ConfState cs = Eraftpb.ConfState.newBuilder()
                .addVoters(1).addVoters(2).addVoters(3).build();
        byte[] data = "data".getBytes();

        MemoryStorage s1 = new MemoryStorage(ents);
        Eraftpb.Snapshot snap1 = s1.createSnapshot(4, cs, data);
        assertThat(snap1.getMetadata().getIndex()).isEqualTo(4);
        assertThat(snap1.getMetadata().getTerm()).isEqualTo(4);

        MemoryStorage s2 = new MemoryStorage(ents);
        Eraftpb.Snapshot snap2 = s2.createSnapshot(5, cs, data);
        assertThat(snap2.getMetadata().getIndex()).isEqualTo(5);
        assertThat(snap2.getMetadata().getTerm()).isEqualTo(5);
    }

    @Test
    void testStorageAppend() throws RaftException {
        List<Eraftpb.Entry> ents = index(3).terms(3, 4, 5);

        assertAppend(ents, index(1).terms(1, 2), index(3).terms(3, 4, 5));
        assertAppend(ents, index(3).terms(3, 4, 5), index(3).terms(3, 4, 5));
        assertAppend(ents, index(3).terms(3, 6, 6), index(3).terms(3, 6, 6));
        assertAppend(ents, index(3).terms(3, 4, 5, 5), index(3).terms(3, 4, 5, 5));
        assertAppend(ents, index(2).terms(3, 3, 5), index(3).terms(3, 5));
        assertAppend(ents, index(4).terms(5), index(3).terms(3, 5));
        assertAppend(ents, index(6).terms(5), index(3).terms(3, 4, 5, 5));
    }

    private void assertAppend(List<Eraftpb.Entry> initial, List<Eraftpb.Entry> toAppend,
                              List<Eraftpb.Entry> expected) {
        MemoryStorage s = new MemoryStorage(initial);
        s.append(toAppend);
        assertThat(s.getEntries()).isEqualTo(expected);
    }

    @Test
    void testStorageApplySnapshot() throws RaftException {
        Eraftpb.ConfState cs = Eraftpb.ConfState.newBuilder()
                .addVoters(1).addVoters(2).addVoters(3).build();
        byte[] data = "data".getBytes();

        // normal case
        MemoryStorage s1 = new MemoryStorage();
        Eraftpb.Snapshot snap = Eraftpb.Snapshot.newBuilder()
                .setData(com.google.protobuf.ByteString.copyFrom(data))
                .setMetadata(Eraftpb.SnapshotMetadata.newBuilder()
                        .setIndex(4).setTerm(4).setConfState(cs))
                .build();
        assertThatNoException().isThrownBy(() -> s1.applySnapshot(snap));

        // snapshot out of date
        MemoryStorage s2 = new MemoryStorage();
        s2.applySnapshot(Eraftpb.Snapshot.newBuilder()
                .setData(com.google.protobuf.ByteString.copyFrom(data))
                .setMetadata(Eraftpb.SnapshotMetadata.newBuilder()
                        .setIndex(4).setTerm(4).setConfState(cs))
                .build());
        Eraftpb.Snapshot oldSnap = Eraftpb.Snapshot.newBuilder()
                .setData(com.google.protobuf.ByteString.copyFrom(data))
                .setMetadata(Eraftpb.SnapshotMetadata.newBuilder()
                        .setIndex(3).setTerm(3).setConfState(cs))
                .build();
        assertThatThrownBy(() -> s2.applySnapshot(oldSnap))
                .isSameAs(RaftException.ErrSnapOutOfDate);
    }
}
