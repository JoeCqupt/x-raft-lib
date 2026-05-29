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

import com.google.protobuf.ByteString;
import io.github.xinfra.lab.raft.proto.Eraftpb;
import io.github.xinfra.lab.raft.internal.tracker.Progress;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.github.xinfra.lab.raft.internal.TestUtil.*;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for Raft snapshot handling, mirroring etcd-raft raft_snap_test.go.
 */
class RaftSnapTest {

    private static final Eraftpb.Snapshot TESTING_SNAP = Eraftpb.Snapshot.newBuilder()
            .setMetadata(Eraftpb.SnapshotMetadata.newBuilder()
                    .setIndex(11)
                    .setTerm(11)
                    .setConfState(Eraftpb.ConfState.newBuilder()
                            .addVoters(1).addVoters(2)))
            .build();

    @Test
    void testSendingSnapshotSetPendingSnapshot() {
        MemoryStorage storage = newTestMemoryStorage(withPeers(1));
        Raft sm = newTestRaft(1, 10, 1, storage);
        sm.restore(TESTING_SNAP);

        sm.becomeCandidate();
        sm.becomeLeader();

        // force set the next of node 2, so that node 2 needs a snapshot
        sm.trk.getProgress().get(2L).setNext(sm.raftLog.firstIndex());

        sm.step(Eraftpb.Message.newBuilder()
                .setFrom(2).setTo(1)
                .setMsgType(Eraftpb.MessageType.MsgAppendResponse)
                .setIndex(sm.trk.getProgress().get(2L).getNext() - 1)
                .setReject(true)
                .build());

        assertThat(sm.trk.getProgress().get(2L).getPendingSnapshot()).isEqualTo(11);
    }

    @Test
    void testPendingSnapshotPauseReplication() {
        MemoryStorage storage = newTestMemoryStorage(withPeers(1, 2));
        Raft sm = newTestRaft(1, 10, 1, storage);
        sm.restore(TESTING_SNAP);

        sm.becomeCandidate();
        sm.becomeLeader();

        sm.trk.getProgress().get(2L).becomeSnapshot(11);

        sm.step(Eraftpb.Message.newBuilder()
                .setFrom(1).setTo(1)
                .setMsgType(Eraftpb.MessageType.MsgPropose)
                .addEntries(Eraftpb.Entry.newBuilder().setData(ByteString.copyFromUtf8("somedata")))
                .build());
        List<Eraftpb.Message> msgs = sm.readMessages();
        assertThat(msgs).isEmpty();
    }

    @Test
    void testSnapshotFailure() {
        MemoryStorage storage = newTestMemoryStorage(withPeers(1, 2));
        Raft sm = newTestRaft(1, 10, 1, storage);
        sm.restore(TESTING_SNAP);

        sm.becomeCandidate();
        sm.becomeLeader();

        sm.trk.getProgress().get(2L).setNext(1);
        sm.trk.getProgress().get(2L).becomeSnapshot(11);

        sm.step(Eraftpb.Message.newBuilder()
                .setFrom(2).setTo(1)
                .setMsgType(Eraftpb.MessageType.MsgSnapStatus)
                .setReject(true)
                .build());

        Progress pr2 = sm.trk.getProgress().get(2L);
        assertThat(pr2.getPendingSnapshot()).isZero();
        assertThat(pr2.getNext()).isEqualTo(1);
        assertThat(pr2.isMsgAppFlowPaused()).isTrue();
    }

    @Test
    void testSnapshotSucceed() {
        MemoryStorage storage = newTestMemoryStorage(withPeers(1, 2));
        Raft sm = newTestRaft(1, 10, 1, storage);
        sm.restore(TESTING_SNAP);

        sm.becomeCandidate();
        sm.becomeLeader();

        sm.trk.getProgress().get(2L).setNext(1);
        sm.trk.getProgress().get(2L).becomeSnapshot(11);

        sm.step(Eraftpb.Message.newBuilder()
                .setFrom(2).setTo(1)
                .setMsgType(Eraftpb.MessageType.MsgSnapStatus)
                .setReject(false)
                .build());

        Progress pr2 = sm.trk.getProgress().get(2L);
        assertThat(pr2.getPendingSnapshot()).isZero();
        assertThat(pr2.getNext()).isEqualTo(12);
        assertThat(pr2.isMsgAppFlowPaused()).isTrue();
    }

    @Test
    void testSnapshotAbort() {
        MemoryStorage storage = newTestMemoryStorage(withPeers(1, 2));
        Raft sm = newTestRaft(1, 10, 1, storage);
        sm.restore(TESTING_SNAP);

        sm.becomeCandidate();
        sm.becomeLeader();

        sm.trk.getProgress().get(2L).setNext(1);
        sm.trk.getProgress().get(2L).becomeSnapshot(11);

        // A successful msgAppResp that has a higher/equal index than the
        // pending snapshot should abort the pending snapshot.
        sm.step(Eraftpb.Message.newBuilder()
                .setFrom(2).setTo(1)
                .setMsgType(Eraftpb.MessageType.MsgAppendResponse)
                .setIndex(11)
                .build());

        Progress pr2 = sm.trk.getProgress().get(2L);
        assertThat(pr2.getPendingSnapshot()).isZero();
        // The follower entered StateReplicate and the leader sent an append
        // and optimistically updated the progress (so we see 13 instead of 12).
        assertThat(pr2.getNext()).isEqualTo(13);
        assertThat(pr2.getInflights().count()).isEqualTo(1);
    }
}
