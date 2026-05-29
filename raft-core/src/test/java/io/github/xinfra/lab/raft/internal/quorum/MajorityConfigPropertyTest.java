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

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class MajorityConfigPropertyTestSupport {
    static AckedIndexer mapIndexer(Map<Long, Long> acks) {
        return id -> {
            Long v = acks.get(id);
            return v == null ? OptionalLong.empty() : OptionalLong.of(v);
        };
    }

    static AckedIndexer fixed(long v) {
        return id -> OptionalLong.of(v);
    }
}

/**
 * Property-based tests for {@link MajorityConfig#committedIndex} and
 * {@link MajorityConfig#voteResult}.
 *
 * <p>The hand-coded {@link MajorityConfigTest} exercises a fixed set of
 * voter/ack patterns. These properties cover arbitrary voter sets (1..15
 * nodes) and arbitrary ack maps, validating the median-based committedIndex
 * and the standard quorum/vote rules.
 */
class MajorityConfigPropertyTest {

    /** Generate a random non-empty voter set of up to 15 IDs. */
    @Provide
    Arbitrary<Set<Long>> voterSets() {
        return Arbitraries.longs().between(1L, 30L).set().ofMinSize(1).ofMaxSize(15);
    }

    /**
     * committedIndex returns the q-th largest acked index, where q = floor(n/2)+1.
     * Equivalently: for a sorted list of acks (with 0 for unacked voters),
     * the result is the median in 0-indexed position n - q.
     */
    @Property(tries = 300)
    void committedIndexEqualsKthLargest(@ForAll("voterSets") Set<Long> voters,
                                        @ForAll long seed) {
        java.util.Random rand = new java.util.Random(seed);
        MajorityConfig cfg = new MajorityConfig(voters);

        // Random ack pattern: each voter has 50% chance of being acked, with
        // a random ack index in [0, 1000].
        Map<Long, Long> acks = new HashMap<>();
        for (long id : voters) {
            if (rand.nextBoolean()) {
                acks.put(id, (long) rand.nextInt(1000));
            }
        }

        long actual = cfg.committedIndex(MajorityConfigPropertyTestSupport.mapIndexer(acks));

        // Reference: for each voter, fetch ack (or 0 if unacked), sort, take median.
        long[] sorted = new long[voters.size()];
        int i = 0;
        for (long id : voters) {
            Long a = acks.get(id);
            sorted[i++] = a == null ? 0L : a;
        }
        Arrays.sort(sorted);
        int q = voters.size() / 2 + 1;
        long expected = sorted[voters.size() - q];

        assertThat(actual).as("committedIndex for voters=%s acks=%s", voters, acks)
                .isEqualTo(expected);
    }

    /**
     * For any (voters, votes), VoteResult is:
     *   VoteWon  iff yes >= quorum
     *   VoteLost iff no  >  voters - quorum (i.e. yes can never reach quorum)
     *   VotePending otherwise
     */
    @Property(tries = 300)
    void voteResultMatchesQuorumRules(@ForAll("voterSets") Set<Long> voters,
                                      @ForAll long seed) {
        java.util.Random rand = new java.util.Random(seed);
        MajorityConfig cfg = new MajorityConfig(voters);

        Map<Long, Boolean> votes = new HashMap<>();
        int yes = 0, no = 0;
        for (long id : voters) {
            int r = rand.nextInt(3);
            if (r == 0) { votes.put(id, true); yes++; }
            else if (r == 1) { votes.put(id, false); no++; }
            // r==2 → undecided
        }

        VoteResult actual = cfg.voteResult(votes);

        int n = voters.size();
        int q = n / 2 + 1;
        VoteResult expected;
        if (yes >= q) expected = VoteResult.VoteWon;
        else if (no > n - q) expected = VoteResult.VoteLost;
        else expected = VoteResult.VotePending;

        assertThat(actual).as("voteResult for voters=%s votes=%s", voters, votes)
                .isEqualTo(expected);
    }

    /**
     * Empty voter set: committedIndex must return MAX_VALUE (no voters means
     * any index is "committed" by definition); voteResult must return VoteWon.
     */
    @Property(tries = 50)
    void emptyVoterSetEdgeCase(@ForAll long seed) {
        MajorityConfig cfg = new MajorityConfig();
        assertThat(cfg.committedIndex(MajorityConfigPropertyTestSupport.fixed(100L))).isEqualTo(Long.MAX_VALUE);
        assertThat(cfg.voteResult(new HashMap<>())).isEqualTo(VoteResult.VoteWon);
    }

    /**
     * If every voter has acked at least index k, committedIndex >= k. This
     * is a basic monotonicity / safety property.
     */
    @Property(tries = 300)
    void universalAckBoundsCommitted(@ForAll("voterSets") Set<Long> voters,
                                     @ForAll long minAck) {
        long min = Math.abs(minAck % 1_000_000L);
        MajorityConfig cfg = new MajorityConfig(voters);
        Map<Long, Long> acks = new HashMap<>();
        for (long id : voters) acks.put(id, min);
        long committed = cfg.committedIndex(MajorityConfigPropertyTestSupport.mapIndexer(acks));
        assertThat(committed).as("universal ack of %d", min).isGreaterThanOrEqualTo(min);
    }

    /**
     * JointConfig.committedIndex equals the min of the two halves'
     * committedIndex values.
     */
    @Property(tries = 200)
    void jointCommittedIsMinOfHalves(@ForAll("voterSets") Set<Long> incoming,
                                     @ForAll("voterSets") Set<Long> outgoing,
                                     @ForAll long seed) {
        java.util.Random rand = new java.util.Random(seed);
        MajorityConfig in = new MajorityConfig(incoming);
        MajorityConfig out = new MajorityConfig(outgoing);
        JointConfig joint = new JointConfig(in, out);

        Set<Long> allVoters = new HashSet<>(incoming);
        allVoters.addAll(outgoing);
        Map<Long, Long> acks = new HashMap<>();
        for (long id : allVoters) {
            if (rand.nextBoolean()) acks.put(id, (long) rand.nextInt(1000));
        }

        long jointCommit = joint.committedIndex(MajorityConfigPropertyTestSupport.mapIndexer(acks));
        long expected = Math.min(in.committedIndex(MajorityConfigPropertyTestSupport.mapIndexer(acks)),
                out.committedIndex(MajorityConfigPropertyTestSupport.mapIndexer(acks)));
        assertThat(jointCommit).isEqualTo(expected);
    }
}
