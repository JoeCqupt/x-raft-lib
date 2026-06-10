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
import io.github.xinfra.lab.raft.internal.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * MajorityConfig is a set of IDs that uses majority quorums to make decisions.
 */
public class MajorityConfig {
    private final Set<Long> ids;

    public MajorityConfig() {
        this.ids = new HashSet<>();
    }

    public MajorityConfig(Set<Long> ids) {
        this.ids = new HashSet<>(ids);
    }

    public MajorityConfig(MajorityConfig other) {
        this.ids = new HashSet<>(other.ids);
    }

    public Set<Long> ids() {
        return ids;
    }

    public int size() {
        return ids.size();
    }

    public boolean isEmpty() {
        return ids.isEmpty();
    }

    public boolean contains(long id) {
        return ids.contains(id);
    }

    public void add(long id) {
        ids.add(id);
    }

    public void remove(long id) {
        ids.remove(id);
    }

    public List<Long> slice() {
        List<Long> sl = new ArrayList<>(ids);
        Collections.sort(sl);
        return sl;
    }

    /**
     * CommittedIndex computes the committed index from those supplied via the
     * provided AckedIndexer (for the active config).
     */
    public long committedIndex(AckedIndexer l) {
        int n = ids.size();
        if (n == 0) {
            return Long.MAX_VALUE;
        }

        long[] srt = new long[n];
        int i = n - 1;
        for (long id : ids) {
            java.util.OptionalLong idx = l.ackedIndex(id);
            if (idx.isPresent()) {
                srt[i] = idx.getAsLong();
                i--;
            }
        }
        Arrays.sort(srt);

        int pos = n - (n / 2 + 1);
        return srt[pos];
    }

    /**
     * VoteResult takes a mapping of voters to yes/no votes and returns
     * a result indicating whether the vote is pending, won, or lost.
     */
    public VoteResult voteResult(Map<Long, Boolean> votes) {
        if (ids.isEmpty()) {
            return VoteResult.VoteWon;
        }

        int votedCnt = 0;
        int missing = 0;
        for (long id : ids) {
            Boolean v = votes.get(id);
            if (v == null) {
                missing++;
                continue;
            }
            if (v) {
                votedCnt++;
            }
        }

        int q = ids.size() / 2 + 1;
        if (votedCnt >= q) {
            return VoteResult.VoteWon;
        }
        if (votedCnt + missing >= q) {
            return VoteResult.VotePending;
        }
        return VoteResult.VoteLost;
    }

    @Override
    public String toString() {
        List<Long> sl = slice();
        return "(" + sl.stream().map(id -> Long.toHexString(id)).collect(Collectors.joining(" ")) + ")";
    }
}
