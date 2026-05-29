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
 * Tests for Raft flow control, mirroring etcd-raft raft_flow_control_test.go.
 */
class RaftFlowControlTest {

    @Test
    void testMsgAppFlowControlFull() {
        Raft r = newTestRaft(1, 5, 1, newTestMemoryStorage(withPeers(1, 2)));
        r.becomeCandidate();
        r.becomeLeader();

        Progress pr2 = r.trk.getProgress().get(2L);
        // force the progress to be in replicate state
        pr2.becomeReplicate();

        // fill in the inflights window
        for (int i = 0; i < r.trk.getMaxInflight(); i++) {
            r.step(Eraftpb.Message.newBuilder()
                    .setFrom(1).setTo(1)
                    .setMsgType(Eraftpb.MessageType.MsgPropose)
                    .addEntries(Eraftpb.Entry.newBuilder().setData(ByteString.copyFromUtf8("somedata")))
                    .build());
            List<Eraftpb.Message> ms = r.readMessages();
            assertThat(ms).hasSize(1);
            assertThat(ms.get(0).getMsgType()).isEqualTo(Eraftpb.MessageType.MsgAppend);
        }

        // ensure 1: paused
        assertThat(pr2.isPaused()).isTrue();

        // ensure 2: no more msgApp can be sent
        for (int i = 0; i < 10; i++) {
            r.step(Eraftpb.Message.newBuilder()
                    .setFrom(1).setTo(1)
                    .setMsgType(Eraftpb.MessageType.MsgPropose)
                    .addEntries(Eraftpb.Entry.newBuilder().setData(ByteString.copyFromUtf8("somedata")))
                    .build());
            List<Eraftpb.Message> ms = r.readMessages();
            assertThat(ms).isEmpty();
        }
    }

    @Test
    void testMsgAppFlowControlMoveForward() {
        Raft r = newTestRaft(1, 5, 1, newTestMemoryStorage(withPeers(1, 2)));
        r.becomeCandidate();
        r.becomeLeader();

        Progress pr2 = r.trk.getProgress().get(2L);
        pr2.becomeReplicate();

        // fill in the inflights window
        for (int i = 0; i < r.trk.getMaxInflight(); i++) {
            r.step(Eraftpb.Message.newBuilder()
                    .setFrom(1).setTo(1)
                    .setMsgType(Eraftpb.MessageType.MsgPropose)
                    .addEntries(Eraftpb.Entry.newBuilder().setData(ByteString.copyFromUtf8("somedata")))
                    .build());
            r.readMessages();
        }

        // 1 is noop, 2 is the first proposal we just sent, so we start with 2.
        for (int tt = 2; tt < r.trk.getMaxInflight(); tt++) {
            // move forward the window
            r.step(Eraftpb.Message.newBuilder()
                    .setFrom(2).setTo(1)
                    .setMsgType(Eraftpb.MessageType.MsgAppendResponse)
                    .setIndex(tt)
                    .build());
            r.readMessages();

            // fill in the inflights window again
            r.step(Eraftpb.Message.newBuilder()
                    .setFrom(1).setTo(1)
                    .setMsgType(Eraftpb.MessageType.MsgPropose)
                    .addEntries(Eraftpb.Entry.newBuilder().setData(ByteString.copyFromUtf8("somedata")))
                    .build());
            List<Eraftpb.Message> ms = r.readMessages();
            assertThat(ms).hasSize(1);
            assertThat(ms.get(0).getMsgType()).isEqualTo(Eraftpb.MessageType.MsgAppend);

            // ensure 1: inflights full ⇒ no further MsgApp can be emitted
            assertThat(pr2.getInflights().full()).isTrue();

            // ensure 2: out-of-dated msgAppResp has no effect — match/next don't
            // advance, inflights stays full, no new message is emitted. (Asserting
            // isPaused() alone would not have caught a regression that silently
            // advances match on stale resp; the unconditional MsgAppFlowPaused
            // clear in stepLeader makes the flag a poor proxy.)
            long preMatch = pr2.getMatch();
            long preNext = pr2.getNext();
            int preInflightCount = pr2.getInflights().count();
            for (int i = 0; i < tt; i++) {
                r.step(Eraftpb.Message.newBuilder()
                        .setFrom(2).setTo(1)
                        .setMsgType(Eraftpb.MessageType.MsgAppendResponse)
                        .setIndex(i)
                        .build());
                assertThat(pr2.getMatch()).as("match unchanged on OOD resp").isEqualTo(preMatch);
                assertThat(pr2.getNext()).as("next unchanged on OOD resp").isEqualTo(preNext);
                assertThat(pr2.getInflights().count()).as("inflights unchanged on OOD resp").isEqualTo(preInflightCount);
                assertThat(r.readMessages()).as("no MsgApp emitted on OOD resp").isEmpty();
            }
        }
    }

    @Test
    void testMsgAppFlowControlRecvHeartbeat() {
        Raft r = newTestRaft(1, 5, 1, newTestMemoryStorage(withPeers(1, 2)));
        r.becomeCandidate();
        r.becomeLeader();

        Progress pr2 = r.trk.getProgress().get(2L);
        pr2.becomeReplicate();

        // fill in the inflights window
        for (int i = 0; i < r.trk.getMaxInflight(); i++) {
            r.step(Eraftpb.Message.newBuilder()
                    .setFrom(1).setTo(1)
                    .setMsgType(Eraftpb.MessageType.MsgPropose)
                    .addEntries(Eraftpb.Entry.newBuilder().setData(ByteString.copyFromUtf8("somedata")))
                    .build());
            r.readMessages();
        }

        for (int tt = 1; tt < 5; tt++) {
            // recv tt msgHeartbeatResp and expect one free slot
            for (int i = 0; i < tt; i++) {
                assertThat(pr2.isPaused()).isTrue();
                // Unpauses the progress, sends an empty MsgApp, and pauses it again.
                r.step(Eraftpb.Message.newBuilder()
                        .setFrom(2).setTo(1)
                        .setMsgType(Eraftpb.MessageType.MsgHeartbeatResponse)
                        .build());
                List<Eraftpb.Message> ms = r.readMessages();
                assertThat(ms).hasSize(1);
                assertThat(ms.get(0).getMsgType()).isEqualTo(Eraftpb.MessageType.MsgAppend);
                assertThat(ms.get(0).getEntriesList()).isEmpty();
            }

            // No more appends are sent if there are no heartbeats.
            for (int i = 0; i < 10; i++) {
                assertThat(pr2.isPaused()).isTrue();
                r.step(Eraftpb.Message.newBuilder()
                        .setFrom(1).setTo(1)
                        .setMsgType(Eraftpb.MessageType.MsgPropose)
                        .addEntries(Eraftpb.Entry.newBuilder().setData(ByteString.copyFromUtf8("somedata")))
                        .build());
                List<Eraftpb.Message> ms = r.readMessages();
                assertThat(ms).isEmpty();
            }

            // clear all pending messages.
            r.step(Eraftpb.Message.newBuilder()
                    .setFrom(2).setTo(1)
                    .setMsgType(Eraftpb.MessageType.MsgHeartbeatResponse)
                    .build());
            r.readMessages();
        }
    }
}
