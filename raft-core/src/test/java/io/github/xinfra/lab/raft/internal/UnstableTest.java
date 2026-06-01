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

import java.util.List;

import static io.github.xinfra.lab.raft.internal.TestUtil.index;
import static io.github.xinfra.lab.raft.internal.TestUtil.snapshot;
import static org.assertj.core.api.Assertions.*;

/**
 * Tests for Unstable, ported from etcd-raft log_unstable_test.go.
 */
class UnstableTest {

    private Unstable newUnstable(long offset, long offsetInProgress,
                                 List<Eraftpb.Entry> entries,
                                 Eraftpb.Snapshot snap, boolean snapInProgress) {
        Unstable u = new Unstable(offset);
        u.setOffsetInProgress(offsetInProgress);
        if (entries != null && !entries.isEmpty()) {
            u.setEntriesForTest(entries);
        }
        u.setSnapshotForTest(snap);
        u.setSnapshotInProgressForTest(snapInProgress);
        return u;
    }

    // ============= TestUnstableMaybeFirstIndex =============

    @Test
    void testMaybeFirstIndex_noSnapshot() throws RaftException {
        // entries but no snapshot
        Unstable u = newUnstable(5, 5, index(5).terms(1), null, false);
        assertThat(u.maybeFirstIndex()).isEmpty();

        // empty, no snapshot
        Unstable u2 = new Unstable(0);
        assertThat(u2.maybeFirstIndex()).isEmpty();
    }

    @Test
    void testMaybeFirstIndex_withSnapshot() throws RaftException {
        // has snapshot with entries
        Unstable u = newUnstable(5, 5, index(5).terms(1), snapshot(4, 1), false);
        assertThat(u.maybeFirstIndex()).hasValue(5);

        // has snapshot without entries
        Unstable u2 = newUnstable(5, 5, List.of(), snapshot(4, 1), false);
        assertThat(u2.maybeFirstIndex()).hasValue(5);
    }

    // ============= TestMaybeLastIndex =============

    @Test
    void testMaybeLastIndex() throws RaftException {
        // last in entries
        assertThat(newUnstable(5, 5, index(5).terms(1), null, false).maybeLastIndex()).hasValue(5);
        assertThat(newUnstable(5, 5, index(5).terms(1), snapshot(4, 1), false).maybeLastIndex()).hasValue(5);
        // last in snapshot
        assertThat(newUnstable(5, 5, List.of(), snapshot(4, 1), false).maybeLastIndex()).hasValue(4);
        // empty
        assertThat(new Unstable(0).maybeLastIndex()).isEmpty();
    }

    // ============= TestUnstableMaybeTerm =============

    @Test
    void testMaybeTerm() throws RaftException {
        // term from entries
        assertThat(newUnstable(5, 5, index(5).terms(1), null, false).maybeTerm(5)).hasValue(1);
        assertThat(newUnstable(5, 5, index(5).terms(1), null, false).maybeTerm(6)).isEmpty();
        assertThat(newUnstable(5, 5, index(5).terms(1), null, false).maybeTerm(4)).isEmpty();

        // with snapshot
        assertThat(newUnstable(5, 5, index(5).terms(1), snapshot(4, 1), false).maybeTerm(5)).hasValue(1);
        assertThat(newUnstable(5, 5, index(5).terms(1), snapshot(4, 1), false).maybeTerm(6)).isEmpty();

        // term from snapshot
        assertThat(newUnstable(5, 5, index(5).terms(1), snapshot(4, 1), false).maybeTerm(4)).hasValue(1);
        assertThat(newUnstable(5, 5, index(5).terms(1), snapshot(4, 1), false).maybeTerm(3)).isEmpty();

        // snapshot only
        assertThat(newUnstable(5, 5, List.of(), snapshot(4, 1), false).maybeTerm(5)).isEmpty();
        assertThat(newUnstable(5, 5, List.of(), snapshot(4, 1), false).maybeTerm(4)).hasValue(1);

        // empty
        assertThat(new Unstable(0).maybeTerm(5)).isEmpty();
    }

    // ============= TestUnstableRestore =============

    @Test
    void testRestore() throws RaftException {
        Unstable u = newUnstable(5, 6, index(5).terms(1), snapshot(4, 1), true);
        Eraftpb.Snapshot s = snapshot(6, 2);
        u.restore(s);

        assertThat(u.getOffset()).isEqualTo(7);
        assertThat(u.getOffsetInProgress()).isEqualTo(7);
        assertThat(u.getEntries()).isEmpty();
        assertThat(u.getSnapshot()).isEqualTo(s);
    }

    // ============= TestUnstableNextEntries =============

    @Test
    void testNextEntries() throws RaftException {
        // nothing in progress
        assertThat(newUnstable(5, 5, index(5).terms(1, 1), null, false).nextEntries())
                .isEqualTo(index(5).terms(1, 1));

        // partially in progress
        assertThat(newUnstable(5, 6, index(5).terms(1, 1), null, false).nextEntries())
                .isEqualTo(index(6).terms(1));

        // everything in progress
        assertThat(newUnstable(5, 7, index(5).terms(1, 1), null, false).nextEntries())
                .isNull();
    }

    // ============= TestUnstableNextSnapshot =============

    @Test
    void testNextSnapshot() throws RaftException {
        Eraftpb.Snapshot s = snapshot(4, 1);

        // no snapshot
        assertThat(new Unstable(0).nextSnapshot()).isNull();
        // snapshot not in progress
        assertThat(newUnstable(5, 5, List.of(), s, false).nextSnapshot()).isEqualTo(s);
        // snapshot in progress
        assertThat(newUnstable(5, 5, List.of(), s, true).nextSnapshot()).isNull();
    }

    // ============= TestUnstableAcceptInProgress =============

    @Test
    void testAcceptInProgress() throws RaftException {
        // no entries, no snapshot
        Unstable u1 = newUnstable(5, 5, List.of(), null, false);
        u1.acceptInProgress();
        assertThat(u1.getOffsetInProgress()).isEqualTo(5);

        // entries not in progress
        Unstable u2 = newUnstable(5, 5, index(5).terms(1), null, false);
        u2.acceptInProgress();
        assertThat(u2.getOffsetInProgress()).isEqualTo(6);

        // two entries not in progress
        Unstable u3 = newUnstable(5, 5, index(5).terms(1, 1), null, false);
        u3.acceptInProgress();
        assertThat(u3.getOffsetInProgress()).isEqualTo(7);

        // in-progress to first entry, accept advances
        Unstable u4 = newUnstable(5, 6, index(5).terms(1, 1), null, false);
        u4.acceptInProgress();
        assertThat(u4.getOffsetInProgress()).isEqualTo(7);

        // already fully in-progress
        Unstable u5 = newUnstable(5, 7, index(5).terms(1, 1), null, false);
        u5.acceptInProgress();
        assertThat(u5.getOffsetInProgress()).isEqualTo(7);

        // with snapshot, not in progress
        Unstable u6 = newUnstable(5, 5, List.of(), snapshot(4, 1), false);
        u6.acceptInProgress();
        assertThat(u6.getOffsetInProgress()).isEqualTo(5);
        assertThat(u6.isSnapshotInProgress()).isTrue();

        // with snapshot + entries
        Unstable u7 = newUnstable(5, 5, index(5).terms(1), snapshot(4, 1), false);
        u7.acceptInProgress();
        assertThat(u7.getOffsetInProgress()).isEqualTo(6);
        assertThat(u7.isSnapshotInProgress()).isTrue();
    }

    // ============= TestUnstableStableTo =============

    @Test
    void testStableTo() throws RaftException {
        // empty
        Unstable u0 = new Unstable(0);
        u0.stableTo(new EntryID(1, 5));
        assertThat(u0.getOffset()).isEqualTo(0);
        assertThat(u0.getEntries()).isEmpty();

        // stable to the first entry
        Unstable u1 = newUnstable(5, 6, index(5).terms(1), null, false);
        u1.stableTo(new EntryID(1, 5));
        assertThat(u1.getOffset()).isEqualTo(6);
        assertThat(u1.getOffsetInProgress()).isEqualTo(6);
        assertThat(u1.getEntries()).isEmpty();

        // stable to first entry, two entries
        Unstable u2 = newUnstable(5, 6, index(5).terms(1, 1), null, false);
        u2.stableTo(new EntryID(1, 5));
        assertThat(u2.getOffset()).isEqualTo(6);
        assertThat(u2.getOffsetInProgress()).isEqualTo(6);
        assertThat(u2.getEntries()).hasSize(1);

        // stable to first, in-progress ahead
        Unstable u3 = newUnstable(5, 7, index(5).terms(1, 1), null, false);
        u3.stableTo(new EntryID(1, 5));
        assertThat(u3.getOffset()).isEqualTo(6);
        assertThat(u3.getOffsetInProgress()).isEqualTo(7);
        assertThat(u3.getEntries()).hasSize(1);

        // term mismatch
        Unstable u4 = newUnstable(6, 7, index(6).terms(2), null, false);
        u4.stableTo(new EntryID(1, 6));
        assertThat(u4.getOffset()).isEqualTo(6);
        assertThat(u4.getEntries()).hasSize(1);

        // stable to old entry
        Unstable u5 = newUnstable(5, 6, index(5).terms(1), null, false);
        u5.stableTo(new EntryID(1, 4));
        assertThat(u5.getOffset()).isEqualTo(5);
        assertThat(u5.getEntries()).hasSize(1);
    }

    // ============= TestUnstableTruncateAndAppend =============

    @Test
    void testTruncateAndAppend() throws RaftException {
        // append to the end
        Unstable u1 = newUnstable(5, 5, index(5).terms(1), null, false);
        u1.truncateAndAppend(index(6).terms(1, 1));
        assertThat(u1.getOffset()).isEqualTo(5);
        assertThat(u1.getOffsetInProgress()).isEqualTo(5);
        assertThat(u1.getEntries()).isEqualTo(index(5).terms(1, 1, 1));

        // append with in-progress
        Unstable u2 = newUnstable(5, 6, index(5).terms(1), null, false);
        u2.truncateAndAppend(index(6).terms(1, 1));
        assertThat(u2.getOffset()).isEqualTo(5);
        assertThat(u2.getOffsetInProgress()).isEqualTo(6);
        assertThat(u2.getEntries()).isEqualTo(index(5).terms(1, 1, 1));

        // replace all
        Unstable u3 = newUnstable(5, 5, index(5).terms(1), null, false);
        u3.truncateAndAppend(index(5).terms(2, 2));
        assertThat(u3.getOffset()).isEqualTo(5);
        assertThat(u3.getOffsetInProgress()).isEqualTo(5);
        assertThat(u3.getEntries()).isEqualTo(index(5).terms(2, 2));

        // replace before offset
        Unstable u4 = newUnstable(5, 5, index(5).terms(1), null, false);
        u4.truncateAndAppend(index(4).terms(2, 2, 2));
        assertThat(u4.getOffset()).isEqualTo(4);
        assertThat(u4.getOffsetInProgress()).isEqualTo(4);
        assertThat(u4.getEntries()).isEqualTo(index(4).terms(2, 2, 2));

        // truncate existing with in-progress
        Unstable u5 = newUnstable(5, 6, index(5).terms(1), null, false);
        u5.truncateAndAppend(index(5).terms(2, 2));
        assertThat(u5.getOffset()).isEqualTo(5);
        assertThat(u5.getOffsetInProgress()).isEqualTo(5);
        assertThat(u5.getEntries()).isEqualTo(index(5).terms(2, 2));

        // truncate middle
        Unstable u6 = newUnstable(5, 5, index(5).terms(1, 1, 1), null, false);
        u6.truncateAndAppend(index(6).terms(2));
        assertThat(u6.getOffset()).isEqualTo(5);
        assertThat(u6.getEntries()).isEqualTo(index(5).terms(1, 2));

        // append without truncate
        Unstable u7 = newUnstable(5, 5, index(5).terms(1, 1, 1), null, false);
        u7.truncateAndAppend(index(7).terms(2, 2));
        assertThat(u7.getOffset()).isEqualTo(5);
        assertThat(u7.getEntries()).isEqualTo(index(5).terms(1, 1, 2, 2));
    }
}
