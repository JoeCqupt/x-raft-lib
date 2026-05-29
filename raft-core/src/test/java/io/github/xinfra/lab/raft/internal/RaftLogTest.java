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
package io.github.xinfra.lab.raft.internal;
import io.github.xinfra.lab.raft.*;

import io.github.xinfra.lab.raft.proto.Eraftpb;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static io.github.xinfra.lab.raft.internal.TestUtil.index;
import static org.assertj.core.api.Assertions.*;

/**
 * Tests for RaftLog, ported from etcd-raft log_test.go.
 */
class RaftLogTest {

    // ============= TestFindConflict =============

    @Test
    void testFindConflict() {
        List<Eraftpb.Entry> previousEnts = index(1).terms(1, 2, 3);
        Object[][] tests = {
                {Collections.emptyList(), 0L},
                {index(1).terms(1, 2, 3), 0L},
                {index(2).terms(2, 3), 0L},
                {index(3).terms(3), 0L},
                {index(1).terms(1, 2, 3, 4, 4), 4L},
                {index(2).terms(2, 3, 4, 5), 4L},
                {index(3).terms(3, 4, 4), 4L},
                {index(4).terms(4, 4), 4L},
                {index(1).terms(4, 4), 1L},
                {index(2).terms(1, 4, 4), 2L},
                {index(3).terms(1, 2, 4, 4), 3L},
        };

        for (Object[] tt : tests) {
            @SuppressWarnings("unchecked")
            List<Eraftpb.Entry> ents = (List<Eraftpb.Entry>) tt[0];
            long wconflict = (long) tt[1];

            RaftLog raftLog = RaftLog.newLog(new MemoryStorage());
            raftLog.append(previousEnts);
            assertThat(raftLog.findConflict(ents)).isEqualTo(wconflict);
        }
    }

    // ============= TestFindConflictByTerm =============

    @Test
    void testFindConflictByTerm() throws RaftException {
        // Log starts from index 1
        assertFCBT(index(0).terms(0, 2, 2, 5, 5, 5), 0, 100, 2, 100);
        assertFCBT(index(0).terms(0, 2, 2, 5, 5, 5), 0, 5, 6, 5);
        assertFCBT(index(0).terms(0, 2, 2, 5, 5, 5), 0, 5, 5, 5);
        assertFCBT(index(0).terms(0, 2, 2, 5, 5, 5), 0, 5, 4, 2);
        assertFCBT(index(0).terms(0, 2, 2, 5, 5, 5), 0, 5, 2, 2);
        assertFCBT(index(0).terms(0, 2, 2, 5, 5, 5), 0, 5, 1, 0);
        assertFCBT(index(0).terms(0, 2, 2, 5, 5, 5), 0, 1, 2, 1);
        assertFCBT(index(0).terms(0, 2, 2, 5, 5, 5), 0, 1, 1, 0);
        assertFCBT(index(0).terms(0, 2, 2, 5, 5, 5), 0, 0, 0, 0);

        // Log with compacted entries
        assertFCBT(index(10).terms(3, 3, 3, 4, 4, 4), 10, 30, 3, 30);
        assertFCBT(index(10).terms(3, 3, 3, 4, 4, 4), 10, 14, 9, 14);
        assertFCBT(index(10).terms(3, 3, 3, 4, 4, 4), 10, 14, 4, 14);
        assertFCBT(index(10).terms(3, 3, 3, 4, 4, 4), 10, 14, 3, 12);
        assertFCBT(index(10).terms(3, 3, 3, 4, 4, 4), 10, 14, 2, 9);
        assertFCBT(index(10).terms(3, 3, 3, 4, 4, 4), 10, 10, 3, 10);
        assertFCBT(index(10).terms(3, 3, 3, 4, 4, 4), 10, 10, 2, 9);
    }

    private void assertFCBT(List<Eraftpb.Entry> ents, long snapIndex,
                             long queryIndex, long queryTerm, long wantIndex) throws RaftException {
        MemoryStorage st = new MemoryStorage();
        st.applySnapshot(Eraftpb.Snapshot.newBuilder()
                .setMetadata(Eraftpb.SnapshotMetadata.newBuilder()
                        .setIndex(ents.get(0).getIndex())
                        .setTerm(ents.get(0).getTerm()))
                .build());
        RaftLog l = RaftLog.newLog(st);
        l.append(ents.subList(1, ents.size()));

        RaftLog.FindConflictResult fcr = l.findConflictByTerm(queryIndex, queryTerm);
        assertThat(fcr.index()).isEqualTo(wantIndex);
    }

    // ============= TestIsUpToDate =============

    @Test
    void testIsUpToDate() {
        List<Eraftpb.Entry> previousEnts = index(1).terms(1, 2, 3);
        RaftLog raftLog = RaftLog.newLog(new MemoryStorage());
        raftLog.append(previousEnts);

        long lastIdx = raftLog.lastIndex();
        // greater term, ignore lastIndex
        assertThat(raftLog.isUpToDate(new EntryID(4, lastIdx - 1))).isTrue();
        assertThat(raftLog.isUpToDate(new EntryID(4, lastIdx))).isTrue();
        assertThat(raftLog.isUpToDate(new EntryID(4, lastIdx + 1))).isTrue();
        // smaller term
        assertThat(raftLog.isUpToDate(new EntryID(2, lastIdx - 1))).isFalse();
        assertThat(raftLog.isUpToDate(new EntryID(2, lastIdx))).isFalse();
        assertThat(raftLog.isUpToDate(new EntryID(2, lastIdx + 1))).isFalse();
        // equal term
        assertThat(raftLog.isUpToDate(new EntryID(3, lastIdx - 1))).isFalse();
        assertThat(raftLog.isUpToDate(new EntryID(3, lastIdx))).isTrue();
        assertThat(raftLog.isUpToDate(new EntryID(3, lastIdx + 1))).isTrue();
    }

    // ============= TestAppend =============

    @Test
    void testAppend() throws RaftException {
        List<Eraftpb.Entry> previousEnts = index(1).terms(1, 2);

        assertAppend(previousEnts, List.of(), 2, index(1).terms(1, 2), 3);
        assertAppend(previousEnts, index(3).terms(2), 3, index(1).terms(1, 2, 2), 3);
        assertAppend(previousEnts, index(1).terms(2), 1, index(1).terms(2), 1);
        assertAppend(previousEnts, index(2).terms(3, 3), 3, index(1).terms(1, 3, 3), 2);
    }

    private void assertAppend(List<Eraftpb.Entry> previousEnts, List<Eraftpb.Entry> ents,
                              long windex, List<Eraftpb.Entry> wents, long wunstable) throws RaftException {
        MemoryStorage storage = new MemoryStorage();
        storage.append(previousEnts);
        RaftLog raftLog = RaftLog.newLog(storage);

        assertThat(raftLog.append(ents)).isEqualTo(windex);
        assertThat(raftLog.entries(1, RaftLog.NO_LIMIT)).isEqualTo(wents);
        assertThat(raftLog.unstable.getOffset()).isEqualTo(wunstable);
    }

    // ============= TestLogMaybeAppend =============

    @Test
    void testLogMaybeAppend() {
        List<Eraftpb.Entry> previousEnts = index(1).terms(1, 2, 3);
        long lastindex = 3;
        long lastterm = 3;
        long commit = 1;

        Object[][] tests = {
                // not match: term is different
                {new EntryID(lastterm - 1, lastindex), lastindex, index(lastindex + 1).terms(4), 0L, false, commit},
                // not match: index out of bound
                {new EntryID(lastterm, lastindex + 1), lastindex, index(lastindex + 2).terms(4), 0L, false, commit},
                // match with last existing entry
                {new EntryID(lastterm, lastindex), lastindex, List.of(), lastindex, true, lastindex},
                {new EntryID(lastterm, lastindex), lastindex + 1, List.of(), lastindex, true, lastindex},
                {new EntryID(lastterm, lastindex), lastindex - 1, List.of(), lastindex, true, lastindex - 1},
                {new EntryID(lastterm, lastindex), 0L, List.of(), lastindex, true, commit},
                {new EntryID(0, 0), lastindex, List.of(), 0L, true, commit},
                // with new entries
                {new EntryID(lastterm, lastindex), lastindex, index(lastindex + 1).terms(4), lastindex + 1, true, lastindex},
                {new EntryID(lastterm, lastindex), lastindex + 1, index(lastindex + 1).terms(4), lastindex + 1, true, lastindex + 1},
                {new EntryID(lastterm, lastindex), lastindex + 2, index(lastindex + 1).terms(4), lastindex + 1, true, lastindex + 1},
                {new EntryID(lastterm, lastindex), lastindex + 2, index(lastindex + 1).terms(4, 4), lastindex + 2, true, lastindex + 2},
                // match with entry in the middle
                {new EntryID(lastterm - 1, lastindex - 1), lastindex, index(lastindex).terms(4), lastindex, true, lastindex},
                {new EntryID(lastterm - 2, lastindex - 2), lastindex, index(lastindex - 1).terms(4), lastindex - 1, true, lastindex - 1},
        };

        for (Object[] tt : tests) {
            EntryID prev = (EntryID) tt[0];
            long committed = (long) tt[1];
            @SuppressWarnings("unchecked")
            List<Eraftpb.Entry> ents = (List<Eraftpb.Entry>) tt[2];
            long wlasti = (long) tt[3];
            boolean wappend = (boolean) tt[4];
            long wcommit = (long) tt[5];

            RaftLog raftLog = RaftLog.newLog(new MemoryStorage());
            raftLog.append(previousEnts);
            raftLog.committed = commit;

            LogSlice app = new LogSlice(100, prev, ents);

            RaftLog.MaybeAppendResult result = raftLog.maybeAppend(app, committed);
            assertThat(result.lastNewIndex()).isEqualTo(wlasti);
            assertThat(result.ok()).isEqualTo(wappend);
            assertThat(raftLog.committed).isEqualTo(wcommit);
        }
    }

    // ============= TestCommitTo =============

    @Test
    void testCommitTo() {
        List<Eraftpb.Entry> previousEnts = index(1).terms(1, 2, 3);
        long commit = 2;

        // increase commit
        RaftLog l1 = RaftLog.newLog(new MemoryStorage());
        l1.append(previousEnts);
        l1.committed = commit;
        l1.commitTo(3);
        assertThat(l1.committed).isEqualTo(3);

        // never decrease
        RaftLog l2 = RaftLog.newLog(new MemoryStorage());
        l2.append(previousEnts);
        l2.committed = commit;
        l2.commitTo(1);
        assertThat(l2.committed).isEqualTo(commit);

        // out of range -> panic
        RaftLog l3 = RaftLog.newLog(new MemoryStorage());
        l3.append(previousEnts);
        l3.committed = commit;
        assertThatThrownBy(() -> l3.commitTo(4)).isInstanceOf(RuntimeException.class);
    }

    // ============= TestStableTo =============

    @Test
    void testStableTo() {
        Object[][] tests = {
                {1L, 1L, 2L},
                {2L, 2L, 3L},
                {2L, 1L, 1L},  // bad term
                {3L, 1L, 1L},  // bad index
        };

        for (Object[] tt : tests) {
            long stablei = (long) tt[0];
            long stablet = (long) tt[1];
            long wunstable = (long) tt[2];

            RaftLog raftLog = RaftLog.newLog(new MemoryStorage());
            raftLog.append(index(1).terms(1, 2));
            raftLog.stableTo(new EntryID(stablet, stablei));
            assertThat(raftLog.unstable.getOffset()).isEqualTo(wunstable);
        }
    }

    // ============= TestStableToWithSnap =============

    @Test
    void testStableToWithSnap() throws RaftException {
        long snapi = 5, snapt = 2;
        Object[][] tests = {
                {snapi + 1, snapt, List.of(), snapi + 1},
                {snapi, snapt, List.of(), snapi + 1},
                {snapi - 1, snapt, List.of(), snapi + 1},
                {snapi + 1, snapt + 1, List.of(), snapi + 1},
                {snapi, snapt + 1, List.of(), snapi + 1},
                {snapi - 1, snapt + 1, List.of(), snapi + 1},
                {snapi + 1, snapt, index(snapi + 1).terms(snapt), snapi + 2},
                {snapi, snapt, index(snapi + 1).terms(snapt), snapi + 1},
                {snapi - 1, snapt, index(snapi + 1).terms(snapt), snapi + 1},
                {snapi + 1, snapt + 1, index(snapi + 1).terms(snapt), snapi + 1},
                {snapi, snapt + 1, index(snapi + 1).terms(snapt), snapi + 1},
                {snapi - 1, snapt + 1, index(snapi + 1).terms(snapt), snapi + 1},
        };

        for (Object[] tt : tests) {
            long stablei = (long) tt[0];
            long stablet = (long) tt[1];
            @SuppressWarnings("unchecked")
            List<Eraftpb.Entry> newEnts = (List<Eraftpb.Entry>) tt[2];
            long wunstable = (long) tt[3];

            MemoryStorage s = new MemoryStorage();
            s.applySnapshot(Eraftpb.Snapshot.newBuilder()
                    .setMetadata(Eraftpb.SnapshotMetadata.newBuilder()
                            .setIndex(snapi).setTerm(snapt))
                    .build());
            RaftLog raftLog = RaftLog.newLog(s);
            raftLog.append(newEnts);
            raftLog.stableTo(new EntryID(stablet, stablei));
            assertThat(raftLog.unstable.getOffset()).isEqualTo(wunstable);
        }
    }

    // ============= TestNextUnstableEnts =============

    @Test
    void testNextUnstableEnts() {
        List<Eraftpb.Entry> previousEnts = index(1).terms(1, 2);

        // all stable
        MemoryStorage s1 = new MemoryStorage();
        s1.append(previousEnts);
        RaftLog l1 = RaftLog.newLog(s1);
        assertThat(l1.nextUnstableEnts()).isNull();

        // all unstable
        MemoryStorage s2 = new MemoryStorage();
        RaftLog l2 = RaftLog.newLog(s2);
        l2.append(previousEnts);
        List<Eraftpb.Entry> ents = l2.nextUnstableEnts();
        assertThat(ents).isEqualTo(previousEnts);
        l2.stableTo(EntryID.of(ents.get(ents.size() - 1)));
        assertThat(l2.unstable.getOffset()).isEqualTo(previousEnts.get(previousEnts.size() - 1).getIndex() + 1);
    }

    // ============= TestHasNextCommittedEnts =============

    @Test
    void testHasNextCommittedEnts() throws RaftException {
        Eraftpb.Snapshot snap = Eraftpb.Snapshot.newBuilder()
                .setMetadata(Eraftpb.SnapshotMetadata.newBuilder().setTerm(1).setIndex(3)).build();
        List<Eraftpb.Entry> ents = index(4).terms(1, 1, 1);

        Object[][] tests = {
                // applied, applying, allowUnstable, paused, snap, whasNext
                {3L, 3L, true, false, false, true},
                {3L, 4L, true, false, false, true},
                {3L, 5L, true, false, false, false},
                {4L, 4L, true, false, false, true},
                {4L, 5L, true, false, false, false},
                {5L, 5L, true, false, false, false},
                // Don't allow unstable
                {3L, 3L, false, false, false, true},
                {3L, 4L, false, false, false, false},
                // Paused
                {3L, 3L, true, true, false, false},
                // With snapshot
                {3L, 3L, true, false, true, false},
        };

        for (Object[] tt : tests) {
            long applied = (long) tt[0];
            long applying = (long) tt[1];
            boolean allowUnstable = (boolean) tt[2];
            boolean paused = (boolean) tt[3];
            boolean hasSnap = (boolean) tt[4];
            boolean whasNext = (boolean) tt[5];

            MemoryStorage storage = new MemoryStorage();
            storage.applySnapshot(snap);
            storage.append(ents.subList(0, 1));

            RaftLog raftLog = RaftLog.newLog(storage);
            raftLog.append(ents);
            raftLog.stableTo(new EntryID(1, 4));
            raftLog.maybeCommit(new EntryID(1, 5));
            raftLog.appliedTo(applied, 0);
            raftLog.acceptApplying(applying, 0, allowUnstable);
            raftLog.applyingEntsPaused = paused;
            if (hasSnap) {
                Eraftpb.Snapshot newSnap = snap.toBuilder()
                        .setMetadata(snap.getMetadata().toBuilder().setIndex(snap.getMetadata().getIndex() + 1))
                        .build();
                raftLog.restore(newSnap);
            }
            assertThat(raftLog.hasNextCommittedEnts(allowUnstable)).isEqualTo(whasNext);
        }
    }

    // ============= TestNextCommittedEnts =============

    @Test
    void testNextCommittedEnts() throws RaftException {
        Eraftpb.Snapshot snap = Eraftpb.Snapshot.newBuilder()
                .setMetadata(Eraftpb.SnapshotMetadata.newBuilder().setTerm(1).setIndex(3)).build();
        List<Eraftpb.Entry> ents = index(4).terms(1, 1, 1);

        Object[][] tests = {
                // applied, applying, allowUnstable, paused, snap, wents
                {3L, 3L, true, false, false, ents.subList(0, 2)},
                {3L, 4L, true, false, false, ents.subList(1, 2)},
                {3L, 5L, true, false, false, null},
                {4L, 4L, true, false, false, ents.subList(1, 2)},
                {4L, 5L, true, false, false, null},
                {5L, 5L, true, false, false, null},
                // Don't allow unstable
                {3L, 3L, false, false, false, ents.subList(0, 1)},
                {3L, 4L, false, false, false, null},
                // Paused
                {3L, 3L, true, true, false, null},
                // With snapshot
                {3L, 3L, true, false, true, null},
        };

        for (Object[] tt : tests) {
            long applied = (long) tt[0];
            long applying = (long) tt[1];
            boolean allowUnstable = (boolean) tt[2];
            boolean paused = (boolean) tt[3];
            boolean hasSnap = (boolean) tt[4];
            @SuppressWarnings("unchecked")
            List<Eraftpb.Entry> wents = (List<Eraftpb.Entry>) tt[5];

            MemoryStorage storage = new MemoryStorage();
            storage.applySnapshot(snap);
            storage.append(ents.subList(0, 1));

            RaftLog raftLog = RaftLog.newLog(storage);
            raftLog.append(ents);
            raftLog.stableTo(new EntryID(1, 4));
            raftLog.maybeCommit(new EntryID(1, 5));
            raftLog.appliedTo(applied, 0);
            raftLog.acceptApplying(applying, 0, allowUnstable);
            raftLog.applyingEntsPaused = paused;
            if (hasSnap) {
                Eraftpb.Snapshot newSnap = snap.toBuilder()
                        .setMetadata(snap.getMetadata().toBuilder().setIndex(snap.getMetadata().getIndex() + 1))
                        .build();
                raftLog.restore(newSnap);
            }
            assertThat(raftLog.nextCommittedEnts(allowUnstable)).isEqualTo(wents);
        }
    }

    // ============= TestCompactionSideEffects =============

    @Test
    void testCompactionSideEffects() throws RaftException {
        long lastIndex = 1000;
        long unstableIndex = 750;
        MemoryStorage storage = new MemoryStorage();
        storage.append(index(1).termRange(1, unstableIndex + 1));
        RaftLog raftLog = RaftLog.newLog(storage);
        raftLog.append(index(unstableIndex + 1).termRange(unstableIndex + 1, lastIndex + 1));

        assertThat(raftLog.maybeCommit(raftLog.lastEntryID())).isTrue();
        raftLog.appliedTo(raftLog.committed, 0);

        long offset = 500;
        storage.compact(offset);
        assertThat(raftLog.lastIndex()).isEqualTo(lastIndex);

        for (long j = offset; j <= raftLog.lastIndex(); j++) {
            assertThat(raftLog.term(j)).isEqualTo(j);
        }

        for (long j = offset; j <= raftLog.lastIndex(); j++) {
            assertThat(raftLog.matchTerm(new EntryID(j, j))).isTrue();
        }

        List<Eraftpb.Entry> unstableEnts = raftLog.nextUnstableEnts();
        assertThat(unstableEnts).hasSize(250);
        assertThat(unstableEnts.get(0).getIndex()).isEqualTo(751);
    }

    // ============= TestLogRestore =============

    @Test
    void testLogRestore() throws RaftException {
        long idx = 1000;
        long term = 1000;
        MemoryStorage storage = new MemoryStorage();
        storage.applySnapshot(Eraftpb.Snapshot.newBuilder()
                .setMetadata(Eraftpb.SnapshotMetadata.newBuilder()
                        .setIndex(idx).setTerm(term))
                .build());
        RaftLog raftLog = RaftLog.newLog(storage);

        assertThat(raftLog.allEntries()).isEmpty();
        assertThat(raftLog.firstIndex()).isEqualTo(idx + 1);
        assertThat(raftLog.committed).isEqualTo(idx);
        assertThat(raftLog.unstable.getOffset()).isEqualTo(idx + 1);
        assertThat(raftLog.term(idx)).isEqualTo(term);
    }

    // ============= TestTerm =============

    @Test
    void testTerm() throws RaftException {
        long offset = 100;
        long num = 100;

        MemoryStorage storage = new MemoryStorage();
        storage.applySnapshot(Eraftpb.Snapshot.newBuilder()
                .setMetadata(Eraftpb.SnapshotMetadata.newBuilder()
                        .setIndex(offset).setTerm(1))
                .build());
        RaftLog l = RaftLog.newLog(storage);
        l.append(index(offset + 1).termRange(1, num));

        assertThatThrownBy(() -> l.term(offset - 1)).isSameAs(RaftException.ErrCompacted);
        assertThat(l.term(offset)).isEqualTo(1);
        assertThat(l.term(offset + num / 2)).isEqualTo(num / 2);
        assertThat(l.term(offset + num - 1)).isEqualTo(num - 1);
        assertThatThrownBy(() -> l.term(offset + num)).isSameAs(RaftException.ErrUnavailable);
    }

    // ============= TestSlice =============

    @Test
    void testSlice() throws RaftException {
        long offset = 100;
        long num = 100;
        long last = offset + num;
        long half = offset + num / 2;

        MemoryStorage storage = new MemoryStorage();
        storage.applySnapshot(Eraftpb.Snapshot.newBuilder()
                .setMetadata(Eraftpb.SnapshotMetadata.newBuilder().setIndex(offset)).build());
        storage.append(index(offset + 1).termRange(offset + 1, half));
        RaftLog l = RaftLog.newLog(storage);
        l.append(index(half).termRange(half, last));

        // ErrCompacted
        assertThatThrownBy(() -> l.slice(offset - 1, offset + 1, RaftLog.NO_LIMIT))
                .isSameAs(RaftException.ErrCompacted);
        assertThatThrownBy(() -> l.slice(offset, offset + 1, RaftLog.NO_LIMIT))
                .isSameAs(RaftException.ErrCompacted);

        // No limit
        assertThat(l.slice(offset + 1, offset + 1, RaftLog.NO_LIMIT)).isEmpty();
        assertThat(l.slice(offset + 1, half, RaftLog.NO_LIMIT)).isEqualTo(index(offset + 1).termRange(offset + 1, half));
        assertThat(l.slice(offset + 1, half + 1, RaftLog.NO_LIMIT)).isEqualTo(index(offset + 1).termRange(offset + 1, half + 1));
        assertThat(l.slice(offset + 1, last, RaftLog.NO_LIMIT)).isEqualTo(index(offset + 1).termRange(offset + 1, last));
        assertThat(l.slice(half, half + 1, RaftLog.NO_LIMIT)).isEqualTo(index(half).termRange(half, half + 1));
        assertThat(l.slice(half, last, RaftLog.NO_LIMIT)).isEqualTo(index(half).termRange(half, last));
        assertThat(l.slice(last - 1, last, RaftLog.NO_LIMIT)).isEqualTo(index(last - 1).termRange(last - 1, last));

        // With zero limit - at least one entry is always returned
        assertThat(l.slice(offset + 1, last, 0)).isEqualTo(index(offset + 1).termRange(offset + 1, offset + 2));
        assertThat(l.slice(half, last, 0)).isEqualTo(index(half).termRange(half, half + 1));
    }

    // ============= TestAcceptApplying =============

    @Test
    void testAcceptApplying() throws RaftException {
        long maxSize = 100;
        Eraftpb.Snapshot snap = Eraftpb.Snapshot.newBuilder()
                .setMetadata(Eraftpb.SnapshotMetadata.newBuilder().setTerm(1).setIndex(3))
                .build();
        List<Eraftpb.Entry> ents = index(4).terms(1, 1, 1);

        Object[][] tests = {
                // allowUnstable = true
                {3L, true, maxSize - 1, true},
                {3L, true, maxSize, true},
                {3L, true, maxSize + 1, true},
                {4L, true, maxSize - 1, true},
                {4L, true, maxSize, true},
                {4L, true, maxSize + 1, true},
                {5L, true, maxSize - 1, false},
                {5L, true, maxSize, true},
                {5L, true, maxSize + 1, true},
                // allowUnstable = false
                {3L, false, maxSize - 1, true},
                {3L, false, maxSize, true},
                {3L, false, maxSize + 1, true},
                {4L, false, maxSize - 1, false},
                {4L, false, maxSize, true},
                {4L, false, maxSize + 1, true},
                {5L, false, maxSize - 1, false},
                {5L, false, maxSize, true},
                {5L, false, maxSize + 1, true},
        };

        for (Object[] tt : tests) {
            long idx = (long) tt[0];
            boolean allowUnstable = (boolean) tt[1];
            long size = (long) tt[2];
            boolean wpaused = (boolean) tt[3];

            MemoryStorage storage = new MemoryStorage();
            storage.applySnapshot(snap);
            storage.append(ents.subList(0, 1));

            RaftLog raftLog = RaftLog.newLogWithSize(storage, maxSize);
            raftLog.append(ents);
            raftLog.stableTo(new EntryID(1, 4));
            raftLog.maybeCommit(new EntryID(1, 5));
            raftLog.appliedTo(3, 0);

            raftLog.acceptApplying(idx, size, allowUnstable);
            assertThat(raftLog.applyingEntsPaused).as("idx=%d unstable=%b size=%d", idx, allowUnstable, size)
                    .isEqualTo(wpaused);
        }
    }

    // ============= TestAppliedTo =============

    @Test
    void testAppliedTo() throws RaftException {
        long maxSize = 100;
        long overshoot = 5;
        Eraftpb.Snapshot snap = Eraftpb.Snapshot.newBuilder()
                .setMetadata(Eraftpb.SnapshotMetadata.newBuilder().setTerm(1).setIndex(3))
                .build();
        List<Eraftpb.Entry> ents = index(4).terms(1, 1, 1);

        Object[][] tests = {
                {4L, overshoot - 1, maxSize + 1, true},
                {4L, overshoot, maxSize, true},
                {4L, overshoot + 1, maxSize - 1, false},
                {5L, overshoot - 1, maxSize + 1, true},
                {5L, overshoot, maxSize, true},
                {5L, overshoot + 1, maxSize - 1, false},
                {4L, maxSize + overshoot, 0L, false},
                {4L, maxSize + overshoot + 1, 0L, false},
        };

        for (Object[] tt : tests) {
            long idx = (long) tt[0];
            long size = (long) tt[1];
            long wApplyingSize = (long) tt[2];
            boolean wpaused = (boolean) tt[3];

            MemoryStorage storage = new MemoryStorage();
            storage.applySnapshot(snap);
            storage.append(ents.subList(0, 1));

            RaftLog raftLog = RaftLog.newLogWithSize(storage, maxSize);
            raftLog.append(ents);
            raftLog.stableTo(new EntryID(1, 4));
            raftLog.maybeCommit(new EntryID(1, 5));
            raftLog.appliedTo(3, 0);
            raftLog.acceptApplying(5, maxSize + overshoot, false);

            raftLog.appliedTo(idx, size);
            assertThat(raftLog.applied).isEqualTo(idx);
            assertThat(raftLog.applying).isEqualTo(5);
            assertThat(raftLog.applyingEntsSize).isEqualTo(wApplyingSize);
            assertThat(raftLog.applyingEntsPaused).isEqualTo(wpaused);
        }
    }

    // ============= TestCompaction =============

    @Test
    void testCompaction() throws RaftException {
        long lastIndex = 1000;
        MemoryStorage storage = new MemoryStorage();
        storage.append(index(1).termRange(1, lastIndex + 1));
        RaftLog raftLog = RaftLog.newLog(storage);
        raftLog.appliedTo(raftLog.committed, 0);

        // Series of compactions
        long[] compacts = {300, 500, 800, 900};
        int[] wleft = {700, 500, 200, 100};
        for (int j = 0; j < compacts.length; j++) {
            storage.compact(compacts[j]);
            assertThat(raftLog.allEntries().size()).isEqualTo(wleft[j]);
        }
    }

    @Test
    void testCompactionOutOfBounds() throws RaftException {
        long lastIndex = 1000;
        MemoryStorage storage = new MemoryStorage();
        storage.append(index(1).termRange(1, lastIndex + 1));
        RaftLog raftLog = RaftLog.newLog(storage);
        raftLog.appliedTo(raftLog.committed, 0);

        // Compact out of upper bound is an invariant violation (caller bug),
        // not a recoverable raft-layer error.
        assertThatThrownBy(() -> storage.compact(1001)).isInstanceOf(RaftInvariantException.class);

        // Compact out of lower bound after first compact is recoverable
        // (raced with another compaction): expect ErrCompacted.
        storage.compact(300);
        assertThatThrownBy(() -> storage.compact(299)).isSameAs(RaftException.ErrCompacted);
    }

    // ============= TestTermWithUnstableSnapshot =============

    @Test
    void testTermWithUnstableSnapshot() throws RaftException {
        long storagesnapi = 100;
        long unstablesnapi = storagesnapi + 5;

        MemoryStorage storage = new MemoryStorage();
        storage.applySnapshot(Eraftpb.Snapshot.newBuilder()
                .setMetadata(Eraftpb.SnapshotMetadata.newBuilder()
                        .setIndex(storagesnapi).setTerm(1))
                .build());
        RaftLog l = RaftLog.newLog(storage);
        l.restore(Eraftpb.Snapshot.newBuilder()
                .setMetadata(Eraftpb.SnapshotMetadata.newBuilder()
                        .setIndex(unstablesnapi).setTerm(1))
                .build());

        // cannot get term from storage
        assertThatThrownBy(() -> l.term(storagesnapi)).isInstanceOf(RaftException.class);
        // cannot get term from the gap
        assertThatThrownBy(() -> l.term(storagesnapi + 1)).isInstanceOf(RaftException.class);
        assertThatThrownBy(() -> l.term(unstablesnapi - 1)).isInstanceOf(RaftException.class);
        // get term from unstable snapshot index
        assertThat(l.term(unstablesnapi)).isEqualTo(1);
        // beyond unstable snapshot
        assertThatThrownBy(() -> l.term(unstablesnapi + 1)).isInstanceOf(RaftException.class);
    }

    // ============= TestScan =============

    @Test
    void testScan() throws RaftException {
        long offset = 47;
        long num = 20;
        long last = offset + num;
        long half = offset + num / 2;

        MemoryStorage storage = new MemoryStorage();
        storage.applySnapshot(Eraftpb.Snapshot.newBuilder()
                .setMetadata(Eraftpb.SnapshotMetadata.newBuilder().setIndex(offset))
                .build());
        storage.append(index(offset + 1).termRange(offset + 1, half));
        RaftLog l = RaftLog.newLog(storage);
        l.append(index(half).termRange(half, last));

        // Test that scan() returns same as slice()
        for (long lo = offset + 1; lo < last; lo++) {
            for (long hi = lo; hi <= last; hi++) {
                List<Eraftpb.Entry> want = l.slice(lo, hi, RaftLog.NO_LIMIT);
                List<Eraftpb.Entry> got = new java.util.ArrayList<>();
                long finalLo = lo;
                long finalHi = hi;
                l.scan(finalLo, finalHi, 0, ents -> {
                    got.addAll(ents);
                    return false; // continue
                });
                assertThat(got).isEqualTo(want);
            }
        }
    }
}
