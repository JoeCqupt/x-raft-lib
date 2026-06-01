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
 * Tests for JointConfig.
 */
class JointConfigTest {

    @Test
    void testJointCommittedIndex() throws RaftException {
        MajorityConfig incoming = new MajorityConfig(Set.of(1L, 2L, 3L));
        MajorityConfig outgoing = new MajorityConfig(Set.of(1L, 2L, 4L));
        JointConfig jc = new JointConfig(incoming, outgoing);

        long idx = jc.committedIndex(id -> {
            if (id == 1) return OptionalLong.of(10L);
            if (id == 2) return OptionalLong.of(20L);
            if (id == 3) return OptionalLong.of(30L);
            if (id == 4) return OptionalLong.of(15L);
            return OptionalLong.empty();
        });
        // incoming: sorted [10,20,30] → committed = 20
        // outgoing: sorted [10,15,20] → committed = 15
        // joint = min(20, 15) = 15
        assertThat(idx).isEqualTo(15);
    }

    @Test
    void testJointCommittedIndex_emptyOutgoing() throws RaftException {
        MajorityConfig incoming = new MajorityConfig(Set.of(1L, 2L, 3L));
        JointConfig jc = new JointConfig(incoming, null);

        long idx = jc.committedIndex(id -> {
            if (id == 1) return OptionalLong.of(10L);
            if (id == 2) return OptionalLong.of(20L);
            if (id == 3) return OptionalLong.of(30L);
            return OptionalLong.empty();
        });
        // incoming: committed = 20
        // outgoing (empty): returns MAX_VALUE
        // joint = min(20, MAX_VALUE) = 20
        assertThat(idx).isEqualTo(20);
    }

    @Test
    void testJointVoteResult_bothWon() throws RaftException {
        MajorityConfig incoming = new MajorityConfig(Set.of(1L, 2L, 3L));
        MajorityConfig outgoing = new MajorityConfig(Set.of(1L, 2L, 4L));
        JointConfig jc = new JointConfig(incoming, outgoing);

        Map<Long, Boolean> votes = new HashMap<>();
        votes.put(1L, true);
        votes.put(2L, true);
        votes.put(3L, true);
        votes.put(4L, true);
        assertThat(jc.voteResult(votes)).isEqualTo(VoteResult.VoteWon);
    }

    @Test
    void testJointVoteResult_oneLost() throws RaftException {
        MajorityConfig incoming = new MajorityConfig(Set.of(1L, 2L, 3L));
        MajorityConfig outgoing = new MajorityConfig(Set.of(1L, 2L, 4L));
        JointConfig jc = new JointConfig(incoming, outgoing);

        Map<Long, Boolean> votes = new HashMap<>();
        votes.put(1L, true);
        votes.put(2L, false);
        votes.put(3L, true);
        votes.put(4L, false);
        // incoming: 2 yes (1,3) ≥ 2 → won
        // outgoing: 1 yes (1), 2 no (2,4) → lost
        // joint = lost
        assertThat(jc.voteResult(votes)).isEqualTo(VoteResult.VoteLost);
    }

    @Test
    void testJointVoteResult_pending() throws RaftException {
        MajorityConfig incoming = new MajorityConfig(Set.of(1L, 2L, 3L));
        MajorityConfig outgoing = new MajorityConfig(Set.of(1L, 2L, 4L));
        JointConfig jc = new JointConfig(incoming, outgoing);

        Map<Long, Boolean> votes = new HashMap<>();
        votes.put(1L, true);
        votes.put(2L, true);
        // incoming: 2 yes ≥ 2 → won
        // outgoing: 2 yes ≥ 2 → won (since 1,2 are both in outgoing too)
        assertThat(jc.voteResult(votes)).isEqualTo(VoteResult.VoteWon);

        Map<Long, Boolean> votes2 = new HashMap<>();
        votes2.put(1L, true);
        // incoming: 1 yes, 2 missing → pending
        // outgoing: 1 yes, 2 missing → pending
        assertThat(jc.voteResult(votes2)).isEqualTo(VoteResult.VotePending);
    }

    @Test
    void testJointIds() throws RaftException {
        MajorityConfig incoming = new MajorityConfig(Set.of(1L, 2L, 3L));
        MajorityConfig outgoing = new MajorityConfig(Set.of(2L, 3L, 4L));
        JointConfig jc = new JointConfig(incoming, outgoing);

        assertThat(jc.ids()).containsExactlyInAnyOrder(1L, 2L, 3L, 4L);
    }
}
