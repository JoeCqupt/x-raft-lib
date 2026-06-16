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

import static io.github.xinfra.lab.raft.internal.TestUtil.*;
import static org.assertj.core.api.Assertions.assertThat;

class StepDownOnRemovalTest {

    @Test
    void leaderStepsDownWhenRemovedAndFlagEnabled() throws RaftException {
        MemoryStorage s = newTestMemoryStorage(withPeers(1, 2, 3));
        Config cfg = newTestConfigBuilder(1, 10, 1, s)
                .stepDownOnRemoval(true)
                .build();
        Raft r = Raft.newRaft(cfg);
        r.becomeCandidate();
        r.becomeLeader();
        assertThat(r.state).isEqualTo(RaftStateType.StateLeader);

        r.applyConfChange(Eraftpb.ConfChangeV2.newBuilder()
                .addChanges(Eraftpb.ConfChangeSingle.newBuilder()
                        .setType(Eraftpb.ConfChangeType.ConfChangeRemoveNode)
                        .setNodeId(1))
                .build());

        assertThat(r.state).isEqualTo(RaftStateType.StateFollower);
        assertThat(r.lead).isEqualTo(Util.NONE);
    }

    @Test
    void leaderStaysLeaderWhenRemovedAndFlagDisabled() throws RaftException {
        MemoryStorage s = newTestMemoryStorage(withPeers(1, 2, 3));
        Raft r = newTestRaft(1, 10, 1, s);
        r.becomeCandidate();
        r.becomeLeader();
        assertThat(r.state).isEqualTo(RaftStateType.StateLeader);

        r.applyConfChange(Eraftpb.ConfChangeV2.newBuilder()
                .addChanges(Eraftpb.ConfChangeSingle.newBuilder()
                        .setType(Eraftpb.ConfChangeType.ConfChangeRemoveNode)
                        .setNodeId(1))
                .build());

        assertThat(r.state).isEqualTo(RaftStateType.StateLeader);
    }

    @Test
    void leaderStepsDownWhenDemotedToLearnerAndFlagEnabled() throws RaftException {
        MemoryStorage s = newTestMemoryStorage(withPeers(1, 2, 3));
        Config cfg = newTestConfigBuilder(1, 10, 1, s)
                .stepDownOnRemoval(true)
                .build();
        Raft r = Raft.newRaft(cfg);
        r.becomeCandidate();
        r.becomeLeader();
        assertThat(r.state).isEqualTo(RaftStateType.StateLeader);

        r.applyConfChange(Eraftpb.ConfChangeV2.newBuilder()
                .addChanges(Eraftpb.ConfChangeSingle.newBuilder()
                        .setType(Eraftpb.ConfChangeType.ConfChangeAddLearnerNode)
                        .setNodeId(1))
                .build());

        assertThat(r.state).isEqualTo(RaftStateType.StateFollower);
        assertThat(r.isLearner).isTrue();
    }

    @Test
    void followerUnaffectedByStepDownOnRemoval() throws RaftException {
        MemoryStorage s = newTestMemoryStorage(withPeers(1, 2, 3));
        Config cfg = newTestConfigBuilder(1, 10, 1, s)
                .stepDownOnRemoval(true)
                .build();
        Raft r = Raft.newRaft(cfg);
        assertThat(r.state).isEqualTo(RaftStateType.StateFollower);

        r.applyConfChange(Eraftpb.ConfChangeV2.newBuilder()
                .addChanges(Eraftpb.ConfChangeSingle.newBuilder()
                        .setType(Eraftpb.ConfChangeType.ConfChangeRemoveNode)
                        .setNodeId(1))
                .build());

        assertThat(r.state).isEqualTo(RaftStateType.StateFollower);
    }

    @Test
    void removingOtherNodeDoesNotTriggerStepDown() throws RaftException {
        MemoryStorage s = newTestMemoryStorage(withPeers(1, 2, 3));
        Config cfg = newTestConfigBuilder(1, 10, 1, s)
                .stepDownOnRemoval(true)
                .build();
        Raft r = Raft.newRaft(cfg);
        r.becomeCandidate();
        r.becomeLeader();

        r.applyConfChange(Eraftpb.ConfChangeV2.newBuilder()
                .addChanges(Eraftpb.ConfChangeSingle.newBuilder()
                        .setType(Eraftpb.ConfChangeType.ConfChangeRemoveNode)
                        .setNodeId(2))
                .build());

        assertThat(r.state).isEqualTo(RaftStateType.StateLeader);
        assertThat(r.trk.voterNodes()).containsExactly(1L, 3L);
    }
}
