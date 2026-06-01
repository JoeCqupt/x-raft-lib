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
package io.github.xinfra.lab.raft.internal.confchange;
import io.github.xinfra.lab.raft.*;

import io.github.xinfra.lab.raft.internal.tracker.Inflights;
import io.github.xinfra.lab.raft.internal.tracker.Progress;
import io.github.xinfra.lab.raft.proto.Eraftpb;
import io.github.xinfra.lab.raft.internal.tracker.ProgressTracker;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for confchange Changer, mirroring etcd-raft confchange/restore_test.go.
 */
class ChangerTest {

    @Test
    void testRestore_empty() throws RaftException {
        Eraftpb.ConfState cs = Eraftpb.ConfState.getDefaultInstance();
        assertRestore(cs);
    }

    @Test
    void testRestore_voters() throws RaftException {
        Eraftpb.ConfState cs = Eraftpb.ConfState.newBuilder()
                .addVoters(1).addVoters(2).addVoters(3)
                .build();
        assertRestore(cs);
    }

    @Test
    void testRestore_votersAndLearners() throws RaftException {
        Eraftpb.ConfState cs = Eraftpb.ConfState.newBuilder()
                .addVoters(1).addVoters(2).addVoters(3)
                .addLearners(4).addLearners(5).addLearners(6)
                .build();
        assertRestore(cs);
    }

    @Test
    void testRestore_jointConfig() throws RaftException {
        Eraftpb.ConfState cs = Eraftpb.ConfState.newBuilder()
                .addVoters(1).addVoters(2).addVoters(3)
                .addLearners(5)
                .addVotersOutgoing(1).addVotersOutgoing(2).addVotersOutgoing(4).addVotersOutgoing(6)
                .addLearnersNext(4)
                .build();
        assertRestore(cs);
    }

    @Test
    void testSimpleChange_addVoter() throws RaftException {
        ProgressTracker tracker = new ProgressTracker(20, 0);
        tracker.getConfig().getVoters().incoming().add(1);
        tracker.getProgress().put(1L, new Progress(0, 1, new Inflights(20, 0), false));

        Changer chg = new Changer(tracker, 10);
        Changer.Result result = chg.simple(List.of(
                Eraftpb.ConfChangeSingle.newBuilder()
                        .setType(Eraftpb.ConfChangeType.ConfChangeAddNode)
                        .setNodeId(2)
                        .build()));

        assertThat(result).isNotNull();
        assertThat(result.config().getVoters().incoming().ids()).contains(1L, 2L);
        assertThat(result.progress()).containsKey(2L);
    }

    @Test
    void testSimpleChange_removeVoter() throws RaftException {
        ProgressTracker tracker = new ProgressTracker(20, 0);
        tracker.getConfig().getVoters().incoming().add(1);
        tracker.getConfig().getVoters().incoming().add(2);
        tracker.getProgress().put(1L, new Progress(0, 1, new Inflights(20, 0), false));
        tracker.getProgress().put(2L, new Progress(0, 1, new Inflights(20, 0), false));

        Changer chg = new Changer(tracker, 10);
        Changer.Result result = chg.simple(List.of(
                Eraftpb.ConfChangeSingle.newBuilder()
                        .setType(Eraftpb.ConfChangeType.ConfChangeRemoveNode)
                        .setNodeId(2)
                        .build()));

        assertThat(result).isNotNull();
        assertThat(result.config().getVoters().incoming().ids()).contains(1L);
        assertThat(result.config().getVoters().incoming().ids()).doesNotContain(2L);
        assertThat(result.progress()).doesNotContainKey(2L);
    }

    @Test
    void testSimpleChange_addLearner() throws RaftException {
        ProgressTracker tracker = new ProgressTracker(20, 0);
        tracker.getConfig().getVoters().incoming().add(1);
        tracker.getProgress().put(1L, new Progress(0, 1, new Inflights(20, 0), false));

        Changer chg = new Changer(tracker, 10);
        Changer.Result result = chg.simple(List.of(
                Eraftpb.ConfChangeSingle.newBuilder()
                        .setType(Eraftpb.ConfChangeType.ConfChangeAddLearnerNode)
                        .setNodeId(2)
                        .build()));

        assertThat(result).isNotNull();
        assertThat(result.config().getLearners()).contains(2L);
        assertThat(result.progress()).containsKey(2L);
        assertThat(result.progress().get(2L).isLearner()).isTrue();
    }

    private void assertRestore(Eraftpb.ConfState cs) throws RaftException {
        ProgressTracker tracker = new ProgressTracker(20, 0);
        Changer chg = new Changer(tracker, 10);
        Changer.Result result = Changer.restore(chg, cs);
        assertThat(result).isNotNull();

        // Apply the result
        tracker.setConfig(result.config());
        tracker.setProgress(result.progress());

        // Get the conf state back and compare
        Eraftpb.ConfState cs2 = tracker.confState();

        // The lists should match (sorted)
        assertThat(cs2.getVotersList().stream().sorted().toList())
                .isEqualTo(cs.getVotersList().stream().sorted().toList());
        assertThat(cs2.getLearnersList().stream().sorted().toList())
                .isEqualTo(cs.getLearnersList().stream().sorted().toList());
        assertThat(cs2.getVotersOutgoingList().stream().sorted().toList())
                .isEqualTo(cs.getVotersOutgoingList().stream().sorted().toList());
        assertThat(cs2.getLearnersNextList().stream().sorted().toList())
                .isEqualTo(cs.getLearnersNextList().stream().sorted().toList());
    }
}
