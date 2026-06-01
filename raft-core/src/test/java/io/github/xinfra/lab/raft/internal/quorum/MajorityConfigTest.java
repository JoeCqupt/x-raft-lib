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
package io.github.xinfra.lab.raft.internal.quorum;
import io.github.xinfra.lab.raft.*;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for quorum package, mirroring etcd-raft quorum tests.
 */
class MajorityConfigTest {

    @Test
    void testCommittedIndex_singleVoter() throws RaftException {
        MajorityConfig c = new MajorityConfig(Set.of(1L));
        long idx = c.committedIndex(id -> id == 1 ? OptionalLong.of(100L) : OptionalLong.empty());
        assertThat(idx).isEqualTo(100);
    }

    @Test
    void testCommittedIndex_threeVoters() throws RaftException {
        MajorityConfig c = new MajorityConfig(Set.of(1L, 2L, 3L));
        // With indices 101, 102, 103 the committed (majority) index is 102
        long idx = c.committedIndex(id -> {
            if (id == 1) return OptionalLong.of(101L);
            if (id == 2) return OptionalLong.of(102L);
            if (id == 3) return OptionalLong.of(103L);
            return OptionalLong.empty();
        });
        assertThat(idx).isEqualTo(102);
    }

    @Test
    void testCommittedIndex_threeVotersOneMissing() throws RaftException {
        MajorityConfig c = new MajorityConfig(Set.of(1L, 2L, 3L));
        // Missing voter 2 - treated as 0
        long idx = c.committedIndex(id -> {
            if (id == 1) return OptionalLong.of(100L);
            if (id == 3) return OptionalLong.of(103L);
            return OptionalLong.empty();
        });
        // Sorted: [0, 100, 103], majority pos = 1, so committed = 100
        assertThat(idx).isEqualTo(100);
    }

    @Test
    void testCommittedIndex_fiveVoters() throws RaftException {
        MajorityConfig c = new MajorityConfig(Set.of(1L, 2L, 3L, 4L, 5L));
        long idx = c.committedIndex(id -> {
            if (id == 1) return OptionalLong.of(10L);
            if (id == 2) return OptionalLong.of(20L);
            if (id == 3) return OptionalLong.of(30L);
            if (id == 4) return OptionalLong.of(40L);
            if (id == 5) return OptionalLong.of(50L);
            return OptionalLong.empty();
        });
        // Majority needs 3 votes. Sorted: [10,20,30,40,50], pos = 5-3=2 → index 30
        assertThat(idx).isEqualTo(30);
    }

    @Test
    void testCommittedIndex_empty() throws RaftException {
        MajorityConfig c = new MajorityConfig();
        long idx = c.committedIndex(id -> OptionalLong.empty());
        assertThat(idx).isEqualTo(Long.MAX_VALUE);
    }

    @Test
    void testVoteResult_won() throws RaftException {
        MajorityConfig c = new MajorityConfig(Set.of(1L, 2L, 3L));
        Map<Long, Boolean> votes = new HashMap<>();
        votes.put(1L, true);
        votes.put(2L, true);
        assertThat(c.voteResult(votes)).isEqualTo(VoteResult.VoteWon);
    }

    @Test
    void testVoteResult_lost() throws RaftException {
        MajorityConfig c = new MajorityConfig(Set.of(1L, 2L, 3L));
        Map<Long, Boolean> votes = new HashMap<>();
        votes.put(1L, true);
        votes.put(2L, false);
        votes.put(3L, false);
        assertThat(c.voteResult(votes)).isEqualTo(VoteResult.VoteLost);
    }

    @Test
    void testVoteResult_pending() throws RaftException {
        MajorityConfig c = new MajorityConfig(Set.of(1L, 2L, 3L));
        Map<Long, Boolean> votes = new HashMap<>();
        votes.put(1L, true);
        // 2 and 3 haven't voted yet
        assertThat(c.voteResult(votes)).isEqualTo(VoteResult.VotePending);
    }

    @Test
    void testVoteResult_empty() throws RaftException {
        MajorityConfig c = new MajorityConfig();
        Map<Long, Boolean> votes = new HashMap<>();
        assertThat(c.voteResult(votes)).isEqualTo(VoteResult.VoteWon);
    }

    @Test
    void testVoteResult_singleVoter() throws RaftException {
        MajorityConfig c = new MajorityConfig(Set.of(1L));
        Map<Long, Boolean> votes = new HashMap<>();
        votes.put(1L, true);
        assertThat(c.voteResult(votes)).isEqualTo(VoteResult.VoteWon);

        Map<Long, Boolean> votes2 = new HashMap<>();
        votes2.put(1L, false);
        assertThat(c.voteResult(votes2)).isEqualTo(VoteResult.VoteLost);
    }
}
