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

import static io.github.xinfra.lab.raft.internal.TestUtil.entry;
import static org.assertj.core.api.Assertions.*;

/**
 * Tests for EntryID and LogSlice, ported from etcd-raft types_test.go.
 */
class LogSliceTest {

    @Test
    void testEntryID() {
        assertThat(new EntryID(5, 10)).isEqualTo(new EntryID(5, 10));
        assertThat(new EntryID(4, 10)).isNotEqualTo(new EntryID(5, 10));
        assertThat(new EntryID(5, 9)).isNotEqualTo(new EntryID(5, 10));

        assertThat(EntryID.of(Eraftpb.Entry.getDefaultInstance())).isEqualTo(new EntryID(0, 0));
        assertThat(EntryID.of(entry(2, 1))).isEqualTo(new EntryID(1, 2));
        assertThat(EntryID.of(entry(123, 10))).isEqualTo(new EntryID(10, 123));
    }

    private EntryID id(long index, long term) {
        return new EntryID(term, index);
    }

    private Eraftpb.Entry e(long index, long term) {
        return entry(index, term);
    }

    @Test
    void testLogSlice() {
        // Empty "dummy" slice at (0,0) origin
        assertValid(0, id(0, 0), List.of(), id(0, 0));

        // Empty slice with prev ID. Valid only if term >= prev.term
        assertInvalid(0, id(123, 10), List.of());
        assertInvalid(9, id(123, 10), List.of());
        assertValid(10, id(123, 10), List.of(), id(123, 10));
        assertValid(11, id(123, 10), List.of(), id(123, 10));

        // Single entry
        assertInvalid(0, id(0, 0), List.of(e(1, 1)));
        assertValid(1, id(0, 0), List.of(e(1, 1)), id(1, 1));
        assertValid(2, id(0, 0), List.of(e(1, 1)), id(1, 1));

        // Multiple entries
        assertInvalid(2, id(0, 0), List.of(e(2, 1), e(3, 1), e(4, 2)));
        assertInvalid(1, id(1, 1), List.of(e(2, 1), e(3, 1), e(4, 2)));
        assertValid(2, id(1, 1), List.of(e(2, 1), e(3, 1), e(4, 2)), id(4, 2));

        // First entry inconsistent with prev
        assertInvalid(10, id(123, 5), List.of(e(111, 5)));
        assertInvalid(10, id(123, 5), List.of(e(124, 4)));
        assertInvalid(10, id(123, 5), List.of(e(234, 6)));
        assertValid(10, id(123, 5), List.of(e(124, 6)), id(124, 6));

        // Inconsistent entries
        assertInvalid(10, id(12, 2), List.of(e(13, 2), e(12, 2)));
        assertInvalid(10, id(12, 2), List.of(e(13, 2), e(15, 2)));
        assertInvalid(10, id(12, 2), List.of(e(13, 2), e(14, 1)));
        assertValid(10, id(12, 2), List.of(e(13, 2), e(14, 3)), id(14, 3));
    }

    private void assertValid(long term, EntryID prev, List<Eraftpb.Entry> entries, EntryID expectedLast) {
        LogSlice s = new LogSlice(term, prev, entries);
        assertThat(s.validate()).isNull();
        assertThat(s.lastEntryID()).isEqualTo(expectedLast);
        assertThat(s.lastIndex()).isEqualTo(expectedLast.index());
    }

    private void assertInvalid(long term, EntryID prev, List<Eraftpb.Entry> entries) {
        LogSlice s = new LogSlice(term, prev, entries);
        assertThat(s.validate()).isNotNull();
    }
}
