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
package io.github.xinfra.lab.raft.internal.tracker;
import io.github.xinfra.lab.raft.*;
import io.github.xinfra.lab.raft.internal.*;

import io.github.xinfra.lab.raft.proto.Eraftpb;
import io.github.xinfra.lab.raft.internal.quorum.JointConfig;
import io.github.xinfra.lab.raft.internal.quorum.MajorityConfig;
import io.github.xinfra.lab.raft.internal.quorum.VoteResult;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;

/**
 * ProgressTracker tracks the currently active configuration and the information
 * known about the nodes and learners in it.
 */
public class ProgressTracker {
    private Config config;
    private Map<Long, Progress> progress;
    private Map<Long, Boolean> votes;
    private int maxInflight;
    private long maxInflightBytes;

    public ProgressTracker(int maxInflight, long maxInflightBytes) {
        this.maxInflight = maxInflight;
        this.maxInflightBytes = maxInflightBytes;
        this.config = new Config();
        this.votes = new HashMap<>();
        this.progress = new HashMap<>();
    }

    public static ProgressTracker make(int maxInflight, long maxInflightBytes) {
        return new ProgressTracker(maxInflight, maxInflightBytes);
    }

    public Config getConfig() { return config; }
    public void setConfig(Config config) { this.config = config; }
    public Map<Long, Progress> getProgress() { return progress; }
    public void setProgress(Map<Long, Progress> progress) { this.progress = progress; }
    public Map<Long, Boolean> getVotes() { return votes; }
    public int getMaxInflight() { return maxInflight; }
    public long getMaxInflightBytes() { return maxInflightBytes; }

    public JointConfig getVoters() { return config.getVoters(); }

    public Eraftpb.ConfState confState() {
        Eraftpb.ConfState.Builder builder = Eraftpb.ConfState.newBuilder();
        builder.addAllVoters(config.getVoters().incoming().slice());
        builder.addAllVotersOutgoing(config.getVoters().outgoing().slice());
        if (config.getLearners() != null) {
            builder.addAllLearners(new MajorityConfig(config.getLearners()).slice());
        }
        if (config.getLearnersNext() != null) {
            builder.addAllLearnersNext(new MajorityConfig(config.getLearnersNext()).slice());
        }
        builder.setAutoLeave(config.isAutoLeave());
        return builder.build();
    }

    public boolean isSingleton() {
        return config.getVoters().incoming().size() == 1 && config.getVoters().outgoing().size() == 0;
    }

    public long committed() {
        return config.getVoters().committedIndex(id -> {
            Progress pr = progress.get(id);
            return pr == null ? java.util.OptionalLong.empty() : java.util.OptionalLong.of(pr.getMatch());
        });
    }

    public void visit(BiConsumer<Long, Progress> f) {
        List<Long> ids = new ArrayList<>(progress.keySet());
        Collections.sort(ids);
        for (long id : ids) {
            f.accept(id, progress.get(id));
        }
    }

    public boolean quorumActive() {
        Map<Long, Boolean> activeVotes = new HashMap<>();
        visit((id, pr) -> {
            if (pr.isLearner()) return;
            activeVotes.put(id, pr.isRecentActive());
        });
        return config.getVoters().voteResult(activeVotes) == VoteResult.VoteWon;
    }

    public List<Long> voterNodes() {
        Set<Long> m = config.getVoters().ids();
        List<Long> nodes = new ArrayList<>(m);
        Collections.sort(nodes);
        return nodes;
    }

    public List<Long> learnerNodes() {
        if (config.getLearners() == null || config.getLearners().isEmpty()) {
            return Collections.emptyList();
        }
        List<Long> nodes = new ArrayList<>(config.getLearners());
        Collections.sort(nodes);
        return nodes;
    }

    public void resetVotes() {
        votes = new HashMap<>();
    }

    public void recordVote(long id, boolean v) {
        votes.putIfAbsent(id, v);
    }

    public TallyResult tallyVotes() {
        int granted = 0;
        int rejected = 0;
        for (Map.Entry<Long, Progress> entry : progress.entrySet()) {
            if (entry.getValue().isLearner()) continue;
            Boolean v = votes.get(entry.getKey());
            if (v == null) continue;
            if (v) {
                granted++;
            } else {
                rejected++;
            }
        }
        VoteResult result = config.getVoters().voteResult(votes);
        return new TallyResult(granted, rejected, result);
    }

    public record TallyResult(int granted, int rejected, VoteResult result) {}

    /**
     * Config reflects the configuration tracked in a ProgressTracker.
     */
    public static class Config {
        private JointConfig voters;
        private boolean autoLeave;
        private Set<Long> learners;
        private Set<Long> learnersNext;

        public Config() {
            this.voters = new JointConfig(new MajorityConfig(), null);
        }

        public JointConfig getVoters() { return voters; }
        public void setVoters(JointConfig voters) { this.voters = voters; }
        public boolean isAutoLeave() { return autoLeave; }
        public void setAutoLeave(boolean autoLeave) { this.autoLeave = autoLeave; }
        public Set<Long> getLearners() { return learners; }
        public void setLearners(Set<Long> learners) { this.learners = learners; }
        public Set<Long> getLearnersNext() { return learnersNext; }
        public void setLearnersNext(Set<Long> learnersNext) { this.learnersNext = learnersNext; }

        @Override
        public Config clone() {
            Config c = new Config();
            c.voters = new JointConfig(
                    new MajorityConfig(voters.incoming()),
                    new MajorityConfig(voters.outgoing()));
            c.autoLeave = autoLeave;
            c.learners = learners != null ? new HashSet<>(learners) : null;
            c.learnersNext = learnersNext != null ? new HashSet<>(learnersNext) : null;
            return c;
        }

        @Override
        public String toString() {
            StringBuilder buf = new StringBuilder();
            buf.append("voters=").append(voters);
            if (learners != null && !learners.isEmpty()) {
                buf.append(" learners=").append(new MajorityConfig(learners));
            }
            if (learnersNext != null && !learnersNext.isEmpty()) {
                buf.append(" learners_next=").append(new MajorityConfig(learnersNext));
            }
            if (autoLeave) {
                buf.append(" autoleave");
            }
            return buf.toString();
        }
    }
}
