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

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * JointConfig is a configuration of two groups of (possibly overlapping)
 * majority configurations. Decisions require the support of both majorities.
 */
public class JointConfig {
    private final MajorityConfig[] configs;

    public JointConfig() {
        this.configs = new MajorityConfig[]{new MajorityConfig(), new MajorityConfig()};
    }

    public JointConfig(MajorityConfig incoming, MajorityConfig outgoing) {
        this.configs = new MajorityConfig[]{
                incoming != null ? incoming : new MajorityConfig(),
                outgoing != null ? outgoing : new MajorityConfig()
        };
    }

    public MajorityConfig[] getConfigs() {
        return configs;
    }

    public MajorityConfig incoming() {
        return configs[0];
    }

    public MajorityConfig outgoing() {
        return configs[1];
    }

    /**
     * Returns a newly initialized set representing the set of voters present
     * in the joint configuration.
     */
    public Set<Long> ids() {
        Set<Long> m = new HashSet<>();
        for (MajorityConfig cc : configs) {
            m.addAll(cc.ids());
        }
        return m;
    }

    /**
     * CommittedIndex returns the largest committed index for the given joint
     * quorum. An index is jointly committed if it is committed in both constituent
     * majorities.
     */
    public long committedIndex(AckedIndexer l) {
        long idx0 = configs[0].committedIndex(l);
        long idx1 = configs[1].committedIndex(l);
        return Math.min(idx0, idx1);
    }

    /**
     * VoteResult takes a mapping of voters to yes/no votes and returns
     * a result indicating whether the vote is pending, lost, or won.
     */
    public VoteResult voteResult(Map<Long, Boolean> votes) {
        VoteResult r1 = configs[0].voteResult(votes);
        VoteResult r2 = configs[1].voteResult(votes);

        if (r1 == r2) {
            return r1;
        }
        if (r1 == VoteResult.VoteLost || r2 == VoteResult.VoteLost) {
            return VoteResult.VoteLost;
        }
        return VoteResult.VotePending;
    }

    @Override
    public String toString() {
        if (configs[1].size() > 0) {
            return configs[0].toString() + "&&" + configs[1].toString();
        }
        return configs[0].toString();
    }
}
