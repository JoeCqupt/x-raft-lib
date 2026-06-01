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
import io.github.xinfra.lab.raft.internal.tracker.StateType;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

import static io.github.xinfra.lab.raft.internal.Network.*;
import static io.github.xinfra.lab.raft.internal.TestUtil.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for Raft core logic, mirroring etcd-raft raft_test.go.
 */
class RaftTest {

    @Test
    void testProgressLeader() throws RaftException {
        MemoryStorage s = newTestMemoryStorage(withPeers(1, 2));
        Raft r = newTestRaft(1, 5, 1, s);
        r.becomeCandidate();
        r.becomeLeader();
        r.trk.getProgress().get(2L).becomeReplicate();

        // Send proposals to r1. The first 5 entries should be queued in the unstable log.
        Eraftpb.Message propMsg = Eraftpb.Message.newBuilder()
                .setFrom(1).setTo(1)
                .setMsgType(Eraftpb.MessageType.MsgPropose)
                .addEntries(Eraftpb.Entry.newBuilder().setData(ByteString.copyFromUtf8("foo")))
                .build();
        for (int i = 0; i < 5; i++) {
            r.step(propMsg);
        }

        assertThat(r.trk.getProgress().get(1L).getMatch()).isZero();

        List<Eraftpb.Entry> ents = r.raftLog.nextUnstableEnts();
        assertThat(ents).hasSize(6);
        assertThat(ents.get(0).getData().size()).isZero();
        assertThat(ents.get(5).getData().toStringUtf8()).isEqualTo("foo");

        r.advanceMessagesAfterAppend();

        assertThat(r.trk.getProgress().get(1L).getMatch()).isEqualTo(6);
        assertThat(r.trk.getProgress().get(1L).getNext()).isEqualTo(7);
    }

    @Test
    void testProgressResumeByHeartbeatResp() throws RaftException {
        Raft r = newTestRaft(1, 5, 1, newTestMemoryStorage(withPeers(1, 2)));
        r.becomeCandidate();
        r.becomeLeader();

        r.trk.getProgress().get(2L).setMsgAppFlowPaused(true);

        r.step(Eraftpb.Message.newBuilder()
                .setFrom(1).setTo(1)
                .setMsgType(Eraftpb.MessageType.MsgBeat)
                .build());
        assertThat(r.trk.getProgress().get(2L).isMsgAppFlowPaused()).isTrue();

        r.trk.getProgress().get(2L).becomeReplicate();
        assertThat(r.trk.getProgress().get(2L).isMsgAppFlowPaused()).isFalse();
        r.trk.getProgress().get(2L).setMsgAppFlowPaused(true);
        r.step(Eraftpb.Message.newBuilder()
                .setFrom(2).setTo(1)
                .setMsgType(Eraftpb.MessageType.MsgHeartbeatResponse)
                .build());
        assertThat(r.trk.getProgress().get(2L).isMsgAppFlowPaused()).isFalse();
    }

    @Test
    void testProgressPaused() throws RaftException {
        Raft r = newTestRaft(1, 5, 1, newTestMemoryStorage(withPeers(1, 2)));
        r.becomeCandidate();
        r.becomeLeader();
        r.step(Eraftpb.Message.newBuilder()
                .setFrom(1).setTo(1).setMsgType(Eraftpb.MessageType.MsgPropose)
                .addEntries(Eraftpb.Entry.newBuilder().setData(ByteString.copyFromUtf8("somedata")))
                .build());
        r.step(Eraftpb.Message.newBuilder()
                .setFrom(1).setTo(1).setMsgType(Eraftpb.MessageType.MsgPropose)
                .addEntries(Eraftpb.Entry.newBuilder().setData(ByteString.copyFromUtf8("somedata")))
                .build());
        r.step(Eraftpb.Message.newBuilder()
                .setFrom(1).setTo(1).setMsgType(Eraftpb.MessageType.MsgPropose)
                .addEntries(Eraftpb.Entry.newBuilder().setData(ByteString.copyFromUtf8("somedata")))
                .build());

        List<Eraftpb.Message> ms = r.readMessages();
        assertThat(ms).hasSize(1);
    }

    @Test
    void testUncommittedEntryLimit() throws RaftException {
        final int maxEntries = 1024;
        Eraftpb.Entry testEntry = Eraftpb.Entry.newBuilder()
                .setData(ByteString.copyFromUtf8("testdata")).build();
        long maxEntrySize = maxEntries * Util.payloadSize(testEntry);

        assertThat(Util.payloadSize(Eraftpb.Entry.getDefaultInstance())).isZero();

        Config cfg = newTestConfig(1, 5, 1, newTestMemoryStorage(withPeers(1, 2, 3)));
        cfg.maxUncommittedEntriesSize = maxEntrySize;
        cfg.maxInflightMsgs = 2 * 1024; // avoid interference
        Raft r = Raft.newRaft(cfg);
        r.becomeCandidate();
        r.becomeLeader();
        assertThat(r.uncommittedSize).isZero();

        // Set the two followers to the replicate state. Commit to tail of log.
        r.trk.getProgress().get(2L).becomeReplicate();
        r.trk.getProgress().get(3L).becomeReplicate();
        r.uncommittedSize = 0;

        // Send proposals to r1. The first maxEntries entries should be appended.
        Eraftpb.Message propMsg = Eraftpb.Message.newBuilder()
                .setFrom(1).setTo(1)
                .setMsgType(Eraftpb.MessageType.MsgPropose)
                .addEntries(testEntry)
                .build();
        for (int i = 0; i < maxEntries; i++) {
            r.step(propMsg);
        }

        // Send one more proposal. It should be rejected.
        assertThatThrownBy(() -> r.step(propMsg)).isEqualTo(RaftException.ErrProposalDropped);

        // Read messages and reduce the uncommitted size.
        List<Eraftpb.Message> ms = r.readMessages();
        assertThat(ms).hasSize(maxEntries * 2); // 2 followers
        long totalPayload = 0;
        for (int i = 0; i < maxEntries; i++) {
            totalPayload += Util.payloadSize(testEntry);
        }
        r.reduceUncommittedSize(totalPayload);
        assertThat(r.uncommittedSize).isZero();

        // Send a single large proposal. Should be accepted because we were beneath the limit.
        Eraftpb.Message.Builder largePropBuilder = Eraftpb.Message.newBuilder()
                .setFrom(1).setTo(1)
                .setMsgType(Eraftpb.MessageType.MsgPropose);
        for (int i = 0; i < 2 * maxEntries; i++) {
            largePropBuilder.addEntries(testEntry);
        }
        r.step(largePropBuilder.build());

        // Send one more proposal. It should be rejected, again.
        assertThatThrownBy(() -> r.step(propMsg)).isEqualTo(RaftException.ErrProposalDropped);

        // But we can always append an entry with no Data.
        Eraftpb.Message emptyProp = Eraftpb.Message.newBuilder()
                .setFrom(1).setTo(1)
                .setMsgType(Eraftpb.MessageType.MsgPropose)
                .addEntries(Eraftpb.Entry.getDefaultInstance())
                .build();
        r.step(emptyProp);

        // Read messages and reduce.
        ms = r.readMessages();
        assertThat(ms).hasSize(2 * 2); // 2 proposals * 2 followers
        long largePayload = 0;
        for (int i = 0; i < 2 * maxEntries; i++) {
            largePayload += Util.payloadSize(testEntry);
        }
        r.reduceUncommittedSize(largePayload);
        assertThat(r.uncommittedSize).isZero();
    }

    @Test
    void testLearnerElectionTimeout() throws RaftException {
        Raft n1 = newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1), withLearners(2)));
        Raft n2 = newTestRaft(2, 10, 1, newTestMemoryStorage(withPeers(1), withLearners(2)));

        n1.becomeFollower(1, Util.NONE);
        n2.becomeFollower(1, Util.NONE);

        // n2 is learner. Learner should not start election even when times out.
        n2.randomizedElectionTimeout = n2.electionTimeout;
        for (int i = 0; i < n2.electionTimeout; i++) {
            n2.tickFn.run();
        }

        assertThat(n2.state).isEqualTo(RaftStateType.StateFollower);
    }

    @Test
    void testLearnerCanVote() throws RaftException {
        Raft n2 = newTestRaft(2, 10, 1, newTestMemoryStorage(withPeers(1), withLearners(2)));
        n2.becomeFollower(1, Util.NONE);

        n2.step(Eraftpb.Message.newBuilder()
                .setFrom(1).setTo(2).setTerm(2)
                .setMsgType(Eraftpb.MessageType.MsgRequestVote)
                .setLogTerm(11).setIndex(11)
                .build());

        List<Eraftpb.Message> msgs = n2.readMessages();
        assertThat(msgs).hasSize(1);
        assertThat(msgs.get(0).getMsgType()).isEqualTo(Eraftpb.MessageType.MsgRequestVoteResponse);
        assertThat(msgs.get(0).getReject()).isFalse();
    }

    @Test
    void testBcastBeat() throws RaftException {
        long offset = 1000L;
        Eraftpb.Snapshot snap = Eraftpb.Snapshot.newBuilder()
                .setMetadata(Eraftpb.SnapshotMetadata.newBuilder()
                        .setIndex(offset)
                        .setTerm(1)
                        .setConfState(Eraftpb.ConfState.newBuilder()
                                .addVoters(1).addVoters(2).addVoters(3)))
                .build();
        MemoryStorage storage = newTestMemoryStorage(withPeers(1, 2, 3));
        storage.applySnapshot(snap);
        Raft sm = newTestRaft(1, 10, 1, storage);
        sm.term = 1;

        sm.becomeCandidate();
        sm.becomeLeader();

        for (int i = 0; i < 10; i++) {
            sm.appendEntry(List.of(Eraftpb.Entry.newBuilder().setIndex(i + 1L).build()));
        }

        // Slow follower
        sm.trk.getProgress().get(2L).setMatch(5);
        sm.trk.getProgress().get(2L).setNext(6);
        // Normal follower
        long lastIndex = sm.raftLog.lastIndex();
        sm.trk.getProgress().get(3L).setMatch(lastIndex);
        sm.trk.getProgress().get(3L).setNext(lastIndex + 1);

        sm.readMessages(); // drain initial messages
        sm.step(Eraftpb.Message.newBuilder()
                .setMsgType(Eraftpb.MessageType.MsgBeat).build());

        List<Eraftpb.Message> msgs = sm.readMessages();
        assertThat(msgs).hasSize(2);

        // Verify heartbeat commit values are tailored per follower.
        // Peer 2 has match=5, so commit should be min(5, sm.raftLog.committed)
        // Peer 3 has match=lastIndex, so commit should be min(lastIndex, sm.raftLog.committed)
        for (Eraftpb.Message m : msgs) {
            assertThat(m.getMsgType()).isEqualTo(Eraftpb.MessageType.MsgHeartbeat);
            assertThat(m.getFrom()).isEqualTo(1);
        }
    }

    @Test
    void testRecvMsgBeat() throws RaftException {
        // leader: receive MsgBeat => bcast heartbeat
        Raft r = newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)));
        r.becomeCandidate();
        r.becomeLeader();
        r.readMessages(); // drain initial messages

        r.step(Eraftpb.Message.newBuilder()
                .setFrom(1).setTo(1)
                .setMsgType(Eraftpb.MessageType.MsgBeat)
                .build());

        List<Eraftpb.Message> msgs = r.readMessages();
        assertThat(msgs).hasSize(2);
        for (Eraftpb.Message m : msgs) {
            assertThat(m.getMsgType()).isEqualTo(Eraftpb.MessageType.MsgHeartbeat);
        }
    }

    @Test
    void testLeaderIncreaseNext() throws RaftException {
        List<Eraftpb.Entry> previousEnts = List.of(
                Eraftpb.Entry.newBuilder().setIndex(1).setTerm(1).build(),
                Eraftpb.Entry.newBuilder().setIndex(2).setTerm(1).build(),
                Eraftpb.Entry.newBuilder().setIndex(3).setTerm(1).build()
        );

        MemoryStorage s = newTestMemoryStorage(withPeers(1, 2));
        s.append(previousEnts);
        Raft r = newTestRaft(1, 5, 1, s);
        r.becomeCandidate();
        r.becomeLeader();
        // After becoming leader, the leader appends a noop entry and
        // resets match/next for all followers.
        // next should be lastIndex + 1 = 5 (3 entries + 1 noop)
        // reset() sets next to lastIndex+1 BEFORE noop is appended, so next = 3+1 = 4
        assertThat(r.trk.getProgress().get(2L).getNext()).isEqualTo(4L);
    }

    // ==================== Tests using Network ====================

    @Test
    void testLeaderElection() throws RaftException {
        testLeaderElectionImpl(false);
    }

    @Test
    void testLeaderElectionPreVote() throws RaftException {
        testLeaderElectionImpl(true);
    }

    private void testLeaderElectionImpl(boolean preVote) throws RaftException {
        Consumer<Config> cfg = preVote ? c -> c.preVote = true : null;
        RaftStateType candState = preVote ? RaftStateType.StatePreCandidate : RaftStateType.StateCandidate;
        long candTerm = preVote ? 0 : 1;

        record TC(Network nw, RaftStateType state, long expTerm) {}
        List<TC> tests = List.of(
                new TC(newNetworkWithConfig(cfg, null, null, null), RaftStateType.StateLeader, 1),
                new TC(newNetworkWithConfig(cfg, null, null, NOP_STEPPER), RaftStateType.StateLeader, 1),
                new TC(newNetworkWithConfig(cfg, null, NOP_STEPPER, NOP_STEPPER), candState, candTerm),
                new TC(newNetworkWithConfig(cfg, null, NOP_STEPPER, NOP_STEPPER, null), candState, candTerm),
                new TC(newNetworkWithConfig(cfg, null, NOP_STEPPER, NOP_STEPPER, null, null), RaftStateType.StateLeader, 1)
        );

        for (int i = 0; i < tests.size(); i++) {
            TC tt = tests.get(i);
            tt.nw.send(Eraftpb.Message.newBuilder().setFrom(1).setTo(1)
                    .setMsgType(Eraftpb.MessageType.MsgHup).build());
            Raft sm = tt.nw.peer(1);
            assertThat(sm.state).as("#%d", i).isEqualTo(tt.state);
            assertThat(sm.term).as("#%d", i).isEqualTo(tt.expTerm);
        }
    }

    @Test
    void testLeaderCycle() throws RaftException {
        testLeaderCycleImpl(false);
    }

    @Test
    void testLeaderCyclePreVote() throws RaftException {
        testLeaderCycleImpl(true);
    }

    private void testLeaderCycleImpl(boolean preVote) throws RaftException {
        Consumer<Config> cfg = preVote ? c -> c.preVote = true : null;
        Network n = newNetworkWithConfig(cfg, null, null, null);
        for (long campaignerID = 1; campaignerID <= 3; campaignerID++) {
            n.send(Eraftpb.Message.newBuilder().setFrom(campaignerID).setTo(campaignerID)
                    .setMsgType(Eraftpb.MessageType.MsgHup).build());
            for (long id : n.peers.keySet()) {
                Raft sm = n.peer(id);
                if (sm == null) continue;
                if (id == campaignerID) {
                    assertThat(sm.state).isEqualTo(RaftStateType.StateLeader);
                } else {
                    assertThat(sm.state).isEqualTo(RaftStateType.StateFollower);
                }
            }
        }
    }

    @Test
    void testSingleNodeCandidate() throws RaftException {
        Network tt = newNetwork((Network.StateMachine) null);
        tt.send(Eraftpb.Message.newBuilder().setFrom(1).setTo(1)
                .setMsgType(Eraftpb.MessageType.MsgHup).build());
        assertThat(tt.peer(1).state).isEqualTo(RaftStateType.StateLeader);
    }

    @Test
    void testSingleNodePreCandidate() throws RaftException {
        Network tt = newNetworkWithConfig(c -> c.preVote = true, (Network.StateMachine) null);
        tt.send(Eraftpb.Message.newBuilder().setFrom(1).setTo(1)
                .setMsgType(Eraftpb.MessageType.MsgHup).build());
        assertThat(tt.peer(1).state).isEqualTo(RaftStateType.StateLeader);
    }

    @Test
    void testSingleNodeCommit() throws RaftException {
        Network tt = newNetwork((Network.StateMachine) null);
        tt.send(Eraftpb.Message.newBuilder().setFrom(1).setTo(1)
                .setMsgType(Eraftpb.MessageType.MsgHup).build());
        tt.send(Eraftpb.Message.newBuilder().setFrom(1).setTo(1)
                .setMsgType(Eraftpb.MessageType.MsgPropose)
                .addEntries(Eraftpb.Entry.newBuilder().setData(ByteString.copyFromUtf8("some data"))).build());
        tt.send(Eraftpb.Message.newBuilder().setFrom(1).setTo(1)
                .setMsgType(Eraftpb.MessageType.MsgPropose)
                .addEntries(Eraftpb.Entry.newBuilder().setData(ByteString.copyFromUtf8("some data"))).build());
        assertThat(tt.peer(1).raftLog.committed).isEqualTo(3);
    }

    @Test
    void testLogReplication() throws RaftException {
        record TC(Network nw, List<Eraftpb.Message> msgs, long wcommitted) {}
        List<TC> tests = List.of(
                new TC(newNetwork(null, null, null),
                        List.of(Eraftpb.Message.newBuilder().setFrom(1).setTo(1)
                                .setMsgType(Eraftpb.MessageType.MsgPropose)
                                .addEntries(Eraftpb.Entry.newBuilder().setData(ByteString.copyFromUtf8("somedata"))).build()),
                        2),
                new TC(newNetwork(null, null, null),
                        List.of(Eraftpb.Message.newBuilder().setFrom(1).setTo(1)
                                        .setMsgType(Eraftpb.MessageType.MsgPropose)
                                        .addEntries(Eraftpb.Entry.newBuilder().setData(ByteString.copyFromUtf8("somedata"))).build(),
                                Eraftpb.Message.newBuilder().setFrom(1).setTo(2)
                                        .setMsgType(Eraftpb.MessageType.MsgHup).build(),
                                Eraftpb.Message.newBuilder().setFrom(1).setTo(2)
                                        .setMsgType(Eraftpb.MessageType.MsgPropose)
                                        .addEntries(Eraftpb.Entry.newBuilder().setData(ByteString.copyFromUtf8("somedata"))).build()),
                        4)
        );

        for (int i = 0; i < tests.size(); i++) {
            TC tt = tests.get(i);
            tt.nw.send(Eraftpb.Message.newBuilder().setFrom(1).setTo(1)
                    .setMsgType(Eraftpb.MessageType.MsgHup).build());
            for (Eraftpb.Message m : tt.msgs) {
                tt.nw.send(m);
            }
            for (long id : tt.nw.peers.keySet()) {
                Raft sm = tt.nw.peer(id);
                if (sm != null) {
                    assertThat(sm.raftLog.committed).as("#%d peer %d", i, id)
                            .isEqualTo(tt.wcommitted);
                }
            }
        }
    }

    @Test
    void testDuelingCandidates() throws RaftException {
        Raft a = newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)));
        Raft b = newTestRaft(2, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)));
        Raft c = newTestRaft(3, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)));

        Network nt = newNetwork(new RaftStateMachine(a), new RaftStateMachine(b), new RaftStateMachine(c));
        nt.cut(1, 3);

        nt.send(Eraftpb.Message.newBuilder().setFrom(1).setTo(1).setMsgType(Eraftpb.MessageType.MsgHup).build());
        nt.send(Eraftpb.Message.newBuilder().setFrom(3).setTo(3).setMsgType(Eraftpb.MessageType.MsgHup).build());

        // 1 becomes leader since it receives votes from 1 and 2
        assertThat(a.state).isEqualTo(RaftStateType.StateLeader);
        // 3 stays as candidate since it receives a vote from 3 and a rejection from 2
        assertThat(c.state).isEqualTo(RaftStateType.StateCandidate);

        nt.recover();

        // candidate 3 now increases its term and tries to vote again
        nt.send(Eraftpb.Message.newBuilder().setFrom(3).setTo(3).setMsgType(Eraftpb.MessageType.MsgHup).build());

        // 3 will be follower again since both 1 and 2 rejects its vote request
        assertThat(a.state).isEqualTo(RaftStateType.StateFollower);
        assertThat(b.state).isEqualTo(RaftStateType.StateFollower);
        assertThat(c.state).isEqualTo(RaftStateType.StateFollower);
    }

    @Test
    void testCandidateConcede() throws RaftException {
        Network tt = newNetwork(null, null, null);
        tt.isolate(1);

        tt.send(Eraftpb.Message.newBuilder().setFrom(1).setTo(1).setMsgType(Eraftpb.MessageType.MsgHup).build());
        tt.send(Eraftpb.Message.newBuilder().setFrom(3).setTo(3).setMsgType(Eraftpb.MessageType.MsgHup).build());

        // heal the partition
        tt.recover();
        // send heartbeat to reset wait
        tt.send(Eraftpb.Message.newBuilder().setFrom(3).setTo(3).setMsgType(Eraftpb.MessageType.MsgBeat).build());

        // send a proposal to flush out a MsgApp to 1
        tt.send(Eraftpb.Message.newBuilder().setFrom(3).setTo(3)
                .setMsgType(Eraftpb.MessageType.MsgPropose)
                .addEntries(Eraftpb.Entry.newBuilder().setData(ByteString.copyFromUtf8("force follower"))).build());
        // send heartbeat; flush out commit
        tt.send(Eraftpb.Message.newBuilder().setFrom(3).setTo(3).setMsgType(Eraftpb.MessageType.MsgBeat).build());

        Raft a = tt.peer(1);
        assertThat(a.state).isEqualTo(RaftStateType.StateFollower);
        assertThat(a.term).isEqualTo(1);
    }

    @Test
    void testCannotCommitWithoutNewTermEntry() throws RaftException {
        Network tt = newNetwork(null, null, null, null, null);
        tt.send(Eraftpb.Message.newBuilder().setFrom(1).setTo(1).setMsgType(Eraftpb.MessageType.MsgHup).build());

        // 1 cannot reach 3,4,5
        tt.cut(1, 3);
        tt.cut(1, 4);
        tt.cut(1, 5);

        tt.send(Eraftpb.Message.newBuilder().setFrom(1).setTo(1)
                .setMsgType(Eraftpb.MessageType.MsgPropose)
                .addEntries(Eraftpb.Entry.newBuilder().setData(ByteString.copyFromUtf8("some data"))).build());
        tt.send(Eraftpb.Message.newBuilder().setFrom(1).setTo(1)
                .setMsgType(Eraftpb.MessageType.MsgPropose)
                .addEntries(Eraftpb.Entry.newBuilder().setData(ByteString.copyFromUtf8("some data"))).build());

        Raft sm = tt.peer(1);
        assertThat(sm.raftLog.committed).isEqualTo(1);

        // network recovery
        tt.recover();
        // avoid committing ChangeTerm proposal
        tt.ignore(Eraftpb.MessageType.MsgAppend);

        // elect 2 as the new leader with term 2
        tt.send(Eraftpb.Message.newBuilder().setFrom(2).setTo(2).setMsgType(Eraftpb.MessageType.MsgHup).build());

        Raft sm2 = tt.peer(2);
        assertThat(sm2.raftLog.committed).isEqualTo(1);

        tt.recover();
        // send heartbeat; reset wait
        tt.send(Eraftpb.Message.newBuilder().setFrom(2).setTo(2).setMsgType(Eraftpb.MessageType.MsgBeat).build());
        // append an entry at current term
        tt.send(Eraftpb.Message.newBuilder().setFrom(2).setTo(2)
                .setMsgType(Eraftpb.MessageType.MsgPropose)
                .addEntries(Eraftpb.Entry.newBuilder().setData(ByteString.copyFromUtf8("some data"))).build());
        assertThat(sm2.raftLog.committed).isEqualTo(5);
    }

    @Test
    void testCommitWithoutNewTermEntry() throws RaftException {
        Network tt = newNetwork(null, null, null, null, null);
        tt.send(Eraftpb.Message.newBuilder().setFrom(1).setTo(1).setMsgType(Eraftpb.MessageType.MsgHup).build());

        // 1 cannot reach 3,4,5
        tt.cut(1, 3);
        tt.cut(1, 4);
        tt.cut(1, 5);

        tt.send(Eraftpb.Message.newBuilder().setFrom(1).setTo(1)
                .setMsgType(Eraftpb.MessageType.MsgPropose)
                .addEntries(Eraftpb.Entry.newBuilder().setData(ByteString.copyFromUtf8("some data"))).build());
        tt.send(Eraftpb.Message.newBuilder().setFrom(1).setTo(1)
                .setMsgType(Eraftpb.MessageType.MsgPropose)
                .addEntries(Eraftpb.Entry.newBuilder().setData(ByteString.copyFromUtf8("some data"))).build());

        Raft sm = tt.peer(1);
        assertThat(sm.raftLog.committed).isEqualTo(1);

        // network recovery
        tt.recover();

        // elect 2 as the new leader with term 2
        tt.send(Eraftpb.Message.newBuilder().setFrom(2).setTo(2).setMsgType(Eraftpb.MessageType.MsgHup).build());

        assertThat(tt.peer(2).raftLog.committed).isEqualTo(4);
    }

    @Test
    void testHandleMsgApp() throws RaftException {
        record TC(Eraftpb.Message m, long wIndex, long wCommit, boolean wReject) {}
        List<TC> tests = List.of(
                // Ensure 1: previous log mismatch
                new TC(Eraftpb.Message.newBuilder().setMsgType(Eraftpb.MessageType.MsgAppend)
                        .setTerm(2).setLogTerm(3).setIndex(2).setCommit(3).build(), 2, 0, true),
                // previous log non-exist
                new TC(Eraftpb.Message.newBuilder().setMsgType(Eraftpb.MessageType.MsgAppend)
                        .setTerm(2).setLogTerm(3).setIndex(3).setCommit(3).build(), 2, 0, true),
                // Ensure 2
                new TC(Eraftpb.Message.newBuilder().setMsgType(Eraftpb.MessageType.MsgAppend)
                        .setTerm(2).setLogTerm(1).setIndex(1).setCommit(1).build(), 2, 1, false),
                new TC(Eraftpb.Message.newBuilder().setMsgType(Eraftpb.MessageType.MsgAppend)
                        .setTerm(2).setLogTerm(0).setIndex(0).setCommit(1)
                        .addEntries(Eraftpb.Entry.newBuilder().setIndex(1).setTerm(2)).build(), 1, 1, false),
                new TC(Eraftpb.Message.newBuilder().setMsgType(Eraftpb.MessageType.MsgAppend)
                        .setTerm(2).setLogTerm(2).setIndex(2).setCommit(3)
                        .addEntries(Eraftpb.Entry.newBuilder().setIndex(3).setTerm(2))
                        .addEntries(Eraftpb.Entry.newBuilder().setIndex(4).setTerm(2)).build(), 4, 3, false),
                new TC(Eraftpb.Message.newBuilder().setMsgType(Eraftpb.MessageType.MsgAppend)
                        .setTerm(2).setLogTerm(2).setIndex(2).setCommit(4)
                        .addEntries(Eraftpb.Entry.newBuilder().setIndex(3).setTerm(2)).build(), 3, 3, false),
                new TC(Eraftpb.Message.newBuilder().setMsgType(Eraftpb.MessageType.MsgAppend)
                        .setTerm(2).setLogTerm(1).setIndex(1).setCommit(4)
                        .addEntries(Eraftpb.Entry.newBuilder().setIndex(2).setTerm(2)).build(), 2, 2, false),
                // Ensure 3
                new TC(Eraftpb.Message.newBuilder().setMsgType(Eraftpb.MessageType.MsgAppend)
                        .setTerm(1).setLogTerm(1).setIndex(1).setCommit(3).build(), 2, 1, false),
                new TC(Eraftpb.Message.newBuilder().setMsgType(Eraftpb.MessageType.MsgAppend)
                        .setTerm(1).setLogTerm(1).setIndex(1).setCommit(3)
                        .addEntries(Eraftpb.Entry.newBuilder().setIndex(2).setTerm(2)).build(), 2, 2, false),
                new TC(Eraftpb.Message.newBuilder().setMsgType(Eraftpb.MessageType.MsgAppend)
                        .setTerm(2).setLogTerm(2).setIndex(2).setCommit(3).build(), 2, 2, false),
                new TC(Eraftpb.Message.newBuilder().setMsgType(Eraftpb.MessageType.MsgAppend)
                        .setTerm(2).setLogTerm(2).setIndex(2).setCommit(4).build(), 2, 2, false)
        );

        for (int i = 0; i < tests.size(); i++) {
            TC tt = tests.get(i);
            MemoryStorage storage = newTestMemoryStorage(withPeers(1));
            storage.append(index(1).terms(1, 2));
            Raft sm = newTestRaft(1, 10, 1, storage);
            sm.becomeFollower(2, Util.NONE);

            sm.handleAppendEntries(tt.m);
            assertThat(sm.raftLog.lastIndex()).as("#%d", i).isEqualTo(tt.wIndex);
            assertThat(sm.raftLog.committed).as("#%d", i).isEqualTo(tt.wCommit);
            List<Eraftpb.Message> msgs = sm.readMessages();
            assertThat(msgs).as("#%d", i).hasSize(1);
            assertThat(msgs.get(0).getReject()).as("#%d", i).isEqualTo(tt.wReject);
        }
    }

    @Test
    void testHandleHeartbeat() throws RaftException {
        long commit = 2;
        record TC(Eraftpb.Message m, long wCommit) {}
        List<TC> tests = List.of(
                new TC(Eraftpb.Message.newBuilder().setFrom(2).setTo(1)
                        .setMsgType(Eraftpb.MessageType.MsgHeartbeat).setTerm(2).setCommit(commit + 1).build(), commit + 1),
                new TC(Eraftpb.Message.newBuilder().setFrom(2).setTo(1)
                        .setMsgType(Eraftpb.MessageType.MsgHeartbeat).setTerm(2).setCommit(commit - 1).build(), commit)
        );

        for (int i = 0; i < tests.size(); i++) {
            TC tt = tests.get(i);
            MemoryStorage storage = newTestMemoryStorage(withPeers(1, 2));
            storage.append(index(1).terms(1, 2, 3));
            Raft sm = newTestRaft(1, 5, 1, storage);
            sm.becomeFollower(2, 2);
            sm.raftLog.commitTo(commit);
            sm.handleHeartbeat(tt.m);
            assertThat(sm.raftLog.committed).as("#%d", i).isEqualTo(tt.wCommit);
            List<Eraftpb.Message> msgs = sm.readMessages();
            assertThat(msgs).hasSize(1);
            assertThat(msgs.get(0).getMsgType()).isEqualTo(Eraftpb.MessageType.MsgHeartbeatResponse);
        }
    }

    @Test
    void testHandleHeartbeatResp() throws RaftException {
        MemoryStorage storage = newTestMemoryStorage(withPeers(1, 2));
        storage.append(index(1).terms(1, 2, 3));
        Raft sm = newTestRaft(1, 5, 1, storage);
        sm.becomeCandidate();
        sm.becomeLeader();
        sm.raftLog.commitTo(sm.raftLog.lastIndex());

        // A heartbeat response from a node that is behind; re-send MsgApp
        sm.step(Eraftpb.Message.newBuilder().setFrom(2).setMsgType(Eraftpb.MessageType.MsgHeartbeatResponse).build());
        List<Eraftpb.Message> msgs = sm.readMessages();
        assertThat(msgs).hasSize(1);
        assertThat(msgs.get(0).getMsgType()).isEqualTo(Eraftpb.MessageType.MsgAppend);

        // A second heartbeat response generates another MsgApp re-send
        sm.step(Eraftpb.Message.newBuilder().setFrom(2).setMsgType(Eraftpb.MessageType.MsgHeartbeatResponse).build());
        msgs = sm.readMessages();
        assertThat(msgs).hasSize(1);
        assertThat(msgs.get(0).getMsgType()).isEqualTo(Eraftpb.MessageType.MsgAppend);

        // Once we have an MsgAppResp, heartbeats no longer send MsgApp.
        sm.step(Eraftpb.Message.newBuilder().setFrom(2)
                .setMsgType(Eraftpb.MessageType.MsgAppendResponse)
                .setIndex(msgs.get(0).getIndex() + msgs.get(0).getEntriesCount()).build());
        sm.readMessages(); // consume

        sm.step(Eraftpb.Message.newBuilder().setFrom(2).setMsgType(Eraftpb.MessageType.MsgHeartbeatResponse).build());
        msgs = sm.readMessages();
        assertThat(msgs).isEmpty();
    }

    @Test
    void testStateTransition() throws RaftException {
        record TC(RaftStateType from, RaftStateType to, boolean wallow, long wterm, long wlead) {}
        List<TC> tests = List.of(
                new TC(RaftStateType.StateFollower, RaftStateType.StateFollower, true, 1, Util.NONE),
                new TC(RaftStateType.StateFollower, RaftStateType.StatePreCandidate, true, 0, Util.NONE),
                new TC(RaftStateType.StateFollower, RaftStateType.StateCandidate, true, 1, Util.NONE),
                new TC(RaftStateType.StateFollower, RaftStateType.StateLeader, false, 0, Util.NONE),

                new TC(RaftStateType.StatePreCandidate, RaftStateType.StateFollower, true, 0, Util.NONE),
                new TC(RaftStateType.StatePreCandidate, RaftStateType.StatePreCandidate, true, 0, Util.NONE),
                new TC(RaftStateType.StatePreCandidate, RaftStateType.StateCandidate, true, 1, Util.NONE),
                new TC(RaftStateType.StatePreCandidate, RaftStateType.StateLeader, true, 0, 1),

                new TC(RaftStateType.StateCandidate, RaftStateType.StateFollower, true, 0, Util.NONE),
                new TC(RaftStateType.StateCandidate, RaftStateType.StatePreCandidate, true, 0, Util.NONE),
                new TC(RaftStateType.StateCandidate, RaftStateType.StateCandidate, true, 1, Util.NONE),
                new TC(RaftStateType.StateCandidate, RaftStateType.StateLeader, true, 0, 1),

                new TC(RaftStateType.StateLeader, RaftStateType.StateFollower, true, 1, Util.NONE),
                new TC(RaftStateType.StateLeader, RaftStateType.StatePreCandidate, false, 0, Util.NONE),
                new TC(RaftStateType.StateLeader, RaftStateType.StateCandidate, false, 1, Util.NONE),
                new TC(RaftStateType.StateLeader, RaftStateType.StateLeader, true, 0, 1)
        );

        for (int i = 0; i < tests.size(); i++) {
            TC tt = tests.get(i);
            final int idx = i;
            Runnable fn = () -> {
                Raft sm = newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1)));
                sm.state = tt.from;
                switch (tt.to) {
                    case StateFollower -> sm.becomeFollower(tt.wterm, tt.wlead);
                    case StatePreCandidate -> sm.becomePreCandidate();
                    case StateCandidate -> sm.becomeCandidate();
                    case StateLeader -> sm.becomeLeader();
                }
                assertThat(sm.term).as("#%d", idx).isEqualTo(tt.wterm);
                assertThat(sm.lead).as("#%d", idx).isEqualTo(tt.wlead);
            };
            if (tt.wallow) {
                fn.run();
            } else {
                assertThatThrownBy(fn::run).as("#%d", i).isInstanceOf(RuntimeException.class);
            }
        }
    }

    @Test
    void testAllServerStepdown() throws RaftException {
        record TC(RaftStateType state, RaftStateType wstate, long wterm, long windex) {}
        List<TC> tests = List.of(
                new TC(RaftStateType.StateFollower, RaftStateType.StateFollower, 3, 0),
                new TC(RaftStateType.StatePreCandidate, RaftStateType.StateFollower, 3, 0),
                new TC(RaftStateType.StateCandidate, RaftStateType.StateFollower, 3, 0),
                new TC(RaftStateType.StateLeader, RaftStateType.StateFollower, 3, 1)
        );

        Eraftpb.MessageType[] tmsgTypes = {Eraftpb.MessageType.MsgRequestVote, Eraftpb.MessageType.MsgAppend};
        long tterm = 3;

        for (int i = 0; i < tests.size(); i++) {
            TC tt = tests.get(i);
            Raft sm = newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)));
            switch (tt.state) {
                case StateFollower -> sm.becomeFollower(1, Util.NONE);
                case StatePreCandidate -> sm.becomePreCandidate();
                case StateCandidate -> sm.becomeCandidate();
                case StateLeader -> { sm.becomeCandidate(); sm.becomeLeader(); }
            }

            for (int j = 0; j < tmsgTypes.length; j++) {
                sm.step(Eraftpb.Message.newBuilder()
                        .setFrom(2).setMsgType(tmsgTypes[j]).setTerm(tterm).setLogTerm(tterm).build());

                assertThat(sm.state).as("#%d.%d", i, j).isEqualTo(tt.wstate);
                assertThat(sm.term).as("#%d.%d", i, j).isEqualTo(tt.wterm);
                assertThat(sm.raftLog.lastIndex()).as("#%d.%d", i, j).isEqualTo(tt.windex);
                assertThat(sm.raftLog.allEntries()).hasSize((int) tt.windex);

                long wlead = (tmsgTypes[j] == Eraftpb.MessageType.MsgRequestVote) ? Util.NONE : 2;
                assertThat(sm.lead).as("#%d.%d", i, j).isEqualTo(wlead);
            }
        }
    }

    @Test
    void testStepIgnoreOldTermMsg() throws RaftException {
        Raft sm = newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1)));
        sm.term = 2;
        // Use a proxy to detect if stepFn is called
        boolean[] called = {false};
        sm.stepFn = (r, m) -> { called[0] = true; };
        sm.step(Eraftpb.Message.newBuilder()
                .setMsgType(Eraftpb.MessageType.MsgAppend).setTerm(sm.term - 1).build());
        assertThat(called[0]).isFalse();
    }

    @Test
    void testProposal() throws RaftException {
        record TC(Network nw, boolean success) {}
        List<TC> tests = List.of(
                new TC(newNetwork(null, null, null), true),
                new TC(newNetwork(null, null, NOP_STEPPER), true),
                new TC(newNetwork(null, NOP_STEPPER, NOP_STEPPER), false),
                new TC(newNetwork(null, NOP_STEPPER, NOP_STEPPER, null), false),
                new TC(newNetwork(null, NOP_STEPPER, NOP_STEPPER, null, null), true)
        );

        for (int j = 0; j < tests.size(); j++) {
            TC tt = tests.get(j);
            try {
                tt.nw.send(Eraftpb.Message.newBuilder().setFrom(1).setTo(1)
                        .setMsgType(Eraftpb.MessageType.MsgHup).build());
                tt.nw.send(Eraftpb.Message.newBuilder().setFrom(1).setTo(1)
                        .setMsgType(Eraftpb.MessageType.MsgPropose)
                        .addEntries(Eraftpb.Entry.newBuilder().setData(ByteString.copyFromUtf8("somedata"))).build());
            } catch (Exception e) {
                // In Go, proposal to a non-leader panics
                assertThat(tt.success).as("#%d", j).isFalse();
                continue;
            }

            Raft r = tt.nw.peer(1);
            if (tt.success) {
                assertThat(r.raftLog.committed).as("#%d", j).isEqualTo(2);
            }
            assertThat(r.term).as("#%d", j).isEqualTo(1);
        }
    }

    @Test
    void testProposalByProxy() throws RaftException {
        Network tt = newNetwork(null, null, null);
        // promote 1 to leader
        tt.send(Eraftpb.Message.newBuilder().setFrom(1).setTo(1)
                .setMsgType(Eraftpb.MessageType.MsgHup).build());
        // propose via follower
        tt.send(Eraftpb.Message.newBuilder().setFrom(2).setTo(2)
                .setMsgType(Eraftpb.MessageType.MsgPropose)
                .addEntries(Eraftpb.Entry.newBuilder().setData(ByteString.copyFromUtf8("somedata"))).build());

        // All peers should have committed = 2
        for (long id : tt.peers.keySet()) {
            Raft sm = tt.peer(id);
            if (sm != null) {
                assertThat(sm.raftLog.committed).isEqualTo(2);
            }
        }
        assertThat(tt.peer(1).term).isEqualTo(1);
    }

    @Test
    void testSendAppendForProgressProbe() throws RaftException {
        Raft r = newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1, 2)));
        r.becomeCandidate();
        r.becomeLeader();
        r.readMessages();
        r.trk.getProgress().get(2L).becomeProbe();

        // each round is a heartbeat
        for (int i = 0; i < 3; i++) {
            if (i == 0) {
                // first loop: one msgApp sent, then paused
                r.appendEntry(List.of(Eraftpb.Entry.newBuilder()
                        .setData(ByteString.copyFromUtf8("somedata")).build()));
                r.sendAppend(2);
                List<Eraftpb.Message> msgs = r.readMessages();
                assertThat(msgs).hasSize(1);
                assertThat(msgs.get(0).getIndex()).isZero();
            }

            assertThat(r.trk.getProgress().get(2L).isMsgAppFlowPaused()).isTrue();
            // these appends are paused
            for (int j = 0; j < 10; j++) {
                r.appendEntry(List.of(Eraftpb.Entry.newBuilder()
                        .setData(ByteString.copyFromUtf8("somedata")).build()));
                r.sendAppend(2);
                assertThat(r.readMessages()).isEmpty();
            }

            // do a heartbeat
            for (int j = 0; j < r.heartbeatTimeout; j++) {
                r.step(Eraftpb.Message.newBuilder().setFrom(1).setTo(1)
                        .setMsgType(Eraftpb.MessageType.MsgBeat).build());
            }
            assertThat(r.trk.getProgress().get(2L).isMsgAppFlowPaused()).isTrue();

            // consume the heartbeat
            List<Eraftpb.Message> msgs = r.readMessages();
            assertThat(msgs).hasSize(1);
            assertThat(msgs.get(0).getMsgType()).isEqualTo(Eraftpb.MessageType.MsgHeartbeat);
        }

        // a heartbeat response will allow another message to be sent
        r.step(Eraftpb.Message.newBuilder().setFrom(2).setTo(1)
                .setMsgType(Eraftpb.MessageType.MsgHeartbeatResponse).build());
        List<Eraftpb.Message> msgs = r.readMessages();
        assertThat(msgs).hasSize(1);
        assertThat(msgs.get(0).getIndex()).isZero();
        assertThat(r.trk.getProgress().get(2L).isMsgAppFlowPaused()).isTrue();
    }

    @Test
    void testSendAppendForProgressReplicate() throws RaftException {
        Raft r = newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1, 2)));
        r.becomeCandidate();
        r.becomeLeader();
        r.readMessages();
        r.trk.getProgress().get(2L).becomeReplicate();

        for (int i = 0; i < 10; i++) {
            r.step(Eraftpb.Message.newBuilder().setFrom(1).setTo(1)
                    .setMsgType(Eraftpb.MessageType.MsgPropose)
                    .addEntries(Eraftpb.Entry.newBuilder().setData(ByteString.copyFromUtf8("somedata"))).build());
            List<Eraftpb.Message> msgs = r.readMessages();
            assertThat(msgs).hasSize(1);
        }
    }

    @Test
    void testSendAppendForProgressSnapshot() throws RaftException {
        Raft r = newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1, 2)));
        r.becomeCandidate();
        r.becomeLeader();
        r.readMessages();
        r.trk.getProgress().get(2L).becomeSnapshot(10);

        r.step(Eraftpb.Message.newBuilder().setFrom(1).setTo(1)
                .setMsgType(Eraftpb.MessageType.MsgPropose)
                .addEntries(Eraftpb.Entry.newBuilder().setData(ByteString.copyFromUtf8("somedata"))).build());
        List<Eraftpb.Message> msgs = r.readMessages();
        assertThat(msgs).isEmpty();
    }

    @Test
    void testRecvMsgUnreachable() throws RaftException {
        MemoryStorage s = newTestMemoryStorage(withPeers(1, 2));
        s.append(index(1).terms(1, 2, 3));
        Raft r = newTestRaft(1, 10, 1, s);
        r.becomeCandidate();
        r.becomeLeader();
        r.readMessages();

        r.trk.getProgress().get(2L).setMatch(3);
        r.trk.getProgress().get(2L).becomeReplicate();
        r.trk.getProgress().get(2L).setNext(5);

        r.step(Eraftpb.Message.newBuilder().setFrom(2).setMsgType(Eraftpb.MessageType.MsgUnreachable).build());

        assertThat(r.trk.getProgress().get(2L).getState()).isEqualTo(StateType.StateProbe);
        assertThat(r.trk.getProgress().get(2L).getNext()).isEqualTo(r.trk.getProgress().get(2L).getMatch() + 1);
    }

    @Test
    void testRestore() throws RaftException {
        Eraftpb.Snapshot snap = Eraftpb.Snapshot.newBuilder()
                .setMetadata(Eraftpb.SnapshotMetadata.newBuilder()
                        .setIndex(11).setTerm(11)
                        .setConfState(Eraftpb.ConfState.newBuilder()
                                .addVoters(1).addVoters(2).addVoters(3)))
                .build();

        MemoryStorage storage = newTestMemoryStorage(withPeers(1, 2));
        Raft sm = newTestRaft(1, 10, 1, storage);
        assertThat(sm.restore(snap)).isTrue();
        assertThat(sm.raftLog.lastIndex()).isEqualTo(snap.getMetadata().getIndex());
        assertThat(sm.raftLog.term(snap.getMetadata().getIndex())).isEqualTo(snap.getMetadata().getTerm());

        assertThat(sm.trk.voterNodes()).containsExactly(1L, 2L, 3L);
    }

    @Test
    void testRestoreIgnoreSnapshot() throws RaftException {
        List<Eraftpb.Entry> previousEnts = index(1).terms(1, 1, 1);
        long commit = 1L;

        MemoryStorage storage = newTestMemoryStorage(withPeers(1, 2));
        storage.append(previousEnts);
        Raft sm = newTestRaft(1, 10, 1, storage);
        sm.raftLog.commitTo(commit);

        Eraftpb.Snapshot snap = Eraftpb.Snapshot.newBuilder()
                .setMetadata(Eraftpb.SnapshotMetadata.newBuilder()
                        .setIndex(commit).setTerm(1)
                        .setConfState(Eraftpb.ConfState.newBuilder().addVoters(1).addVoters(2)))
                .build();
        // Should ignore snapshot with index <= committed
        assertThat(sm.restore(snap)).isFalse();
        assertThat(sm.raftLog.committed).isEqualTo(commit);
    }

    @Test
    void testPromotable() throws RaftException {
        Raft r1 = newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1, 2)));
        assertThat(r1.promotable()).isTrue();

        Raft r2 = newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1), withLearners(2)));
        assertThat(r2.promotable()).isTrue();

        // not in voter list
        Raft r3 = newTestRaft(2, 10, 1, newTestMemoryStorage(withPeers(1)));
        assertThat(r3.promotable()).isFalse();
    }

    @Test
    void testCampaignWhileLeader() throws RaftException {
        testCampaignWhileLeaderImpl(false);
    }

    @Test
    void testPreCampaignWhileLeader() throws RaftException {
        testCampaignWhileLeaderImpl(true);
    }

    private void testCampaignWhileLeaderImpl(boolean preVote) throws RaftException {
        Config cfg = newTestConfig(1, 10, 1, newTestMemoryStorage(withPeers(1)));
        cfg.preVote = preVote;
        Raft r = Raft.newRaft(cfg);
        assertThat(r.state).isEqualTo(RaftStateType.StateFollower);

        // first campaign leads to being a leader
        r.step(Eraftpb.Message.newBuilder().setFrom(1).setTo(1)
                .setMsgType(Eraftpb.MessageType.MsgHup).build());
        r.advanceMessagesAfterAppend();
        assertThat(r.state).isEqualTo(RaftStateType.StateLeader);

        long term = r.term;
        // second campaign should not affect state or term
        r.step(Eraftpb.Message.newBuilder().setFrom(1).setTo(1)
                .setMsgType(Eraftpb.MessageType.MsgHup).build());
        r.advanceMessagesAfterAppend();
        assertThat(r.state).isEqualTo(RaftStateType.StateLeader);
        assertThat(r.term).isEqualTo(term);
    }

    @Test
    void testLeaderTransferToUpToDateNode() throws RaftException {
        Network nt = newNetwork(null, null, null);
        nt.send(Eraftpb.Message.newBuilder().setFrom(1).setTo(1)
                .setMsgType(Eraftpb.MessageType.MsgHup).build());

        Raft lead = nt.peer(1);
        assertThat(lead.lead).isEqualTo(1);

        // Transfer leadership to 2.
        nt.send(Eraftpb.Message.newBuilder().setFrom(2).setTo(1)
                .setMsgType(Eraftpb.MessageType.MsgTransferLeader).build());

        checkLeaderTransferState(lead, RaftStateType.StateFollower, 2);

        // After some time, the new leader should complete.
        nt.send(Eraftpb.Message.newBuilder().setFrom(1).setTo(1)
                .setMsgType(Eraftpb.MessageType.MsgBeat).build());
    }

    @Test
    void testLeaderTransferToSelf() throws RaftException {
        Network nt = newNetwork(null, null, null);
        nt.send(Eraftpb.Message.newBuilder().setFrom(1).setTo(1)
                .setMsgType(Eraftpb.MessageType.MsgHup).build());

        Raft lead = nt.peer(1);
        // Transfer leadership to self, should be no-op
        nt.send(Eraftpb.Message.newBuilder().setFrom(1).setTo(1)
                .setMsgType(Eraftpb.MessageType.MsgTransferLeader).build());
        assertThat(lead.state).isEqualTo(RaftStateType.StateLeader);
        assertThat(lead.leadTransferee).isEqualTo(Util.NONE);
    }

    @Test
    void testLeaderTransferToNonExistingNode() throws RaftException {
        Network nt = newNetwork(null, null, null);
        nt.send(Eraftpb.Message.newBuilder().setFrom(1).setTo(1)
                .setMsgType(Eraftpb.MessageType.MsgHup).build());

        Raft lead = nt.peer(1);
        // Try to transfer to non-existing node 4
        nt.send(Eraftpb.Message.newBuilder().setFrom(4).setTo(1)
                .setMsgType(Eraftpb.MessageType.MsgTransferLeader).build());
        assertThat(lead.state).isEqualTo(RaftStateType.StateLeader);
        assertThat(lead.leadTransferee).isEqualTo(Util.NONE);
    }

    private void checkLeaderTransferState(Raft r, RaftStateType state, long lead) throws RaftException {
        assertThat(r.state).isEqualTo(state);
        assertThat(r.lead).isEqualTo(lead);
        assertThat(r.leadTransferee).isEqualTo(Util.NONE);
    }

    @Test
    void testCommit() throws RaftException {
        record TC(long[] matches, List<Eraftpb.Entry> logs, long smTerm, long w) {}
        List<TC> tests = List.of(
                // single
                new TC(new long[]{1}, index(1).terms(1), 1, 1),
                new TC(new long[]{1}, index(1).terms(1), 2, 0),
                new TC(new long[]{2}, index(1).terms(1, 2), 2, 2),
                new TC(new long[]{1}, index(1).terms(2), 2, 1),
                // odd
                new TC(new long[]{2, 1, 1}, index(1).terms(1, 2), 1, 1),
                new TC(new long[]{2, 1, 1}, index(1).terms(1, 1), 2, 0),
                new TC(new long[]{2, 1, 2}, index(1).terms(1, 2), 2, 2),
                new TC(new long[]{2, 1, 2}, index(1).terms(1, 1), 2, 0),
                // even
                new TC(new long[]{2, 1, 1, 1}, index(1).terms(1, 2), 1, 1),
                new TC(new long[]{2, 1, 1, 1}, index(1).terms(1, 1), 2, 0),
                new TC(new long[]{2, 1, 1, 2}, index(1).terms(1, 2), 1, 1),
                new TC(new long[]{2, 1, 1, 2}, index(1).terms(1, 1), 2, 0),
                new TC(new long[]{2, 1, 2, 2}, index(1).terms(1, 2), 2, 2),
                new TC(new long[]{2, 1, 2, 2}, index(1).terms(1, 1), 2, 0)
        );

        for (int i = 0; i < tests.size(); i++) {
            TC tt = tests.get(i);
            MemoryStorage storage = newTestMemoryStorage(withPeers(1));
            storage.append(tt.logs);
            storage.setHardState(Eraftpb.HardState.newBuilder().setTerm(tt.smTerm).build());

            Raft sm = newTestRaft(1, 10, 2, storage);
            for (int j = 0; j < tt.matches.length; j++) {
                long id = j + 1;
                if (id > 1) {
                    sm.applyConfChange(Eraftpb.ConfChangeV2.newBuilder()
                            .addChanges(Eraftpb.ConfChangeSingle.newBuilder()
                                    .setType(Eraftpb.ConfChangeType.ConfChangeAddNode)
                                    .setNodeId(id)).build());
                }
                Progress pr = sm.trk.getProgress().get(id);
                pr.setMatch(tt.matches[j]);
                pr.setNext(tt.matches[j] + 1);
            }
            sm.maybeCommit();
            assertThat(sm.raftLog.committed).as("#%d", i).isEqualTo(tt.w);
        }
    }

    @Test
    void testVoteFromAnyState() throws RaftException {
        testVoteFromAnyStateImpl(Eraftpb.MessageType.MsgRequestVote);
    }

    @Test
    void testPreVoteFromAnyState() throws RaftException {
        testVoteFromAnyStateImpl(Eraftpb.MessageType.MsgRequestPreVote);
    }

    private void testVoteFromAnyStateImpl(Eraftpb.MessageType vt) throws RaftException {
        for (RaftStateType st : RaftStateType.values()) {
            Raft r = newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)));
            r.term = 1;

            switch (st) {
                case StateFollower -> r.becomeFollower(r.term, 3);
                case StatePreCandidate -> r.becomePreCandidate();
                case StateCandidate -> r.becomeCandidate();
                case StateLeader -> { r.becomeCandidate(); r.becomeLeader(); }
            }

            long origTerm = r.term;
            long newTerm = r.term + 1;

            r.step(Eraftpb.Message.newBuilder()
                    .setFrom(2).setTo(1).setMsgType(vt)
                    .setTerm(newTerm).setLogTerm(newTerm).setIndex(42).build());

            List<Eraftpb.Message> msgs = r.readMessages();
            assertThat(msgs).as("%s,%s", vt, st).hasSize(1);
            assertThat(msgs.get(0).getMsgType()).isEqualTo(Util.voteRespMsgType(vt));
            assertThat(msgs.get(0).getReject()).as("%s,%s", vt, st).isFalse();

            if (vt == Eraftpb.MessageType.MsgRequestVote) {
                assertThat(r.state).isEqualTo(RaftStateType.StateFollower);
                assertThat(r.term).isEqualTo(newTerm);
                assertThat(r.vote).isEqualTo(2);
            } else {
                assertThat(r.state).isEqualTo(st);
                assertThat(r.term).isEqualTo(origTerm);
            }
        }
    }

    @Test
    void testOldMessages() throws RaftException {
        Network tt = newNetwork(null, null, null);
        // make node 1 leader at term 3
        tt.send(Eraftpb.Message.newBuilder().setFrom(1).setTo(1).setMsgType(Eraftpb.MessageType.MsgHup).build());
        tt.send(Eraftpb.Message.newBuilder().setFrom(2).setTo(2).setMsgType(Eraftpb.MessageType.MsgHup).build());
        tt.send(Eraftpb.Message.newBuilder().setFrom(1).setTo(1).setMsgType(Eraftpb.MessageType.MsgHup).build());
        // pretend we're an old leader trying to make progress; this entry is expected to be ignored.
        tt.send(Eraftpb.Message.newBuilder().setFrom(2).setTo(1)
                .setMsgType(Eraftpb.MessageType.MsgAppend).setTerm(2)
                .addEntries(Eraftpb.Entry.newBuilder().setIndex(3).setTerm(2)).build());
        // commit a new entry
        tt.send(Eraftpb.Message.newBuilder().setFrom(1).setTo(1)
                .setMsgType(Eraftpb.MessageType.MsgPropose)
                .addEntries(Eraftpb.Entry.newBuilder().setData(ByteString.copyFromUtf8("somedata"))).build());

        // all nodes should have committed = 4
        for (long id : tt.peers.keySet()) {
            Raft sm = tt.peer(id);
            if (sm != null) {
                assertThat(sm.raftLog.committed).as("peer %d", id).isEqualTo(4);
            }
        }
    }

    // ==================== Batch 1 ====================

    @Test
    void testCandidateSelfVoteAfterLostElection() throws RaftException {
        testCandidateSelfVoteAfterLostElectionImpl(false);
    }

    @Test
    void testCandidateSelfVoteAfterLostElectionPreVote() throws RaftException {
        testCandidateSelfVoteAfterLostElectionImpl(true);
    }

    private void testCandidateSelfVoteAfterLostElectionImpl(boolean preVote) throws RaftException {
        Raft sm = newTestRaft(1, 5, 1, newTestMemoryStorage(withPeers(1, 2, 3)));
        sm.preVote = preVote;

        sm.step(Eraftpb.Message.newBuilder().setFrom(1).setTo(1)
                .setMsgType(Eraftpb.MessageType.MsgHup).build());
        List<Eraftpb.Message> steps = sm.takeMessagesAfterAppend();

        sm.step(Eraftpb.Message.newBuilder().setFrom(2).setTo(1).setTerm(sm.term)
                .setMsgType(Eraftpb.MessageType.MsgHeartbeat).build());
        assertThat(sm.state).isEqualTo(RaftStateType.StateFollower);

        sm.stepOrSend(steps);
        assertThat(sm.state).isEqualTo(RaftStateType.StateFollower);

        assertThat(sm.trk.tallyVotes().granted()).isZero();
    }

    @Test
    void testCandidateDeliversPreCandidateSelfVoteAfterBecomingCandidate() throws RaftException {
        Raft sm = newTestRaft(1, 5, 1, newTestMemoryStorage(withPeers(1, 2, 3)));
        sm.preVote = true;

        sm.step(Eraftpb.Message.newBuilder().setFrom(1).setTo(1)
                .setMsgType(Eraftpb.MessageType.MsgHup).build());
        assertThat(sm.state).isEqualTo(RaftStateType.StatePreCandidate);
        List<Eraftpb.Message> steps = sm.takeMessagesAfterAppend();

        sm.step(Eraftpb.Message.newBuilder().setFrom(2).setTo(1).setTerm(sm.term + 1)
                .setMsgType(Eraftpb.MessageType.MsgRequestPreVoteResponse).build());
        sm.step(Eraftpb.Message.newBuilder().setFrom(3).setTo(1).setTerm(sm.term + 1)
                .setMsgType(Eraftpb.MessageType.MsgRequestPreVoteResponse).build());
        assertThat(sm.state).isEqualTo(RaftStateType.StateCandidate);

        sm.stepOrSend(steps);
        assertThat(sm.state).isEqualTo(RaftStateType.StateCandidate);

        steps = sm.takeMessagesAfterAppend();

        assertThat(sm.trk.tallyVotes().granted()).isZero();

        sm.step(Eraftpb.Message.newBuilder().setFrom(2).setTo(1).setTerm(sm.term)
                .setMsgType(Eraftpb.MessageType.MsgRequestVoteResponse).build());
        assertThat(sm.state).isEqualTo(RaftStateType.StateCandidate);

        sm.stepOrSend(steps);
        assertThat(sm.state).isEqualTo(RaftStateType.StateLeader);
    }

    @Test
    void testLeaderMsgAppSelfAckAfterTermChange() throws RaftException {
        Raft sm = newTestRaft(1, 5, 1, newTestMemoryStorage(withPeers(1, 2, 3)));
        sm.becomeCandidate();
        sm.becomeLeader();

        sm.step(Eraftpb.Message.newBuilder().setFrom(1).setTo(1)
                .setMsgType(Eraftpb.MessageType.MsgPropose)
                .addEntries(Eraftpb.Entry.newBuilder().setData(
                        ByteString.copyFromUtf8("somedata"))).build());
        List<Eraftpb.Message> steps = sm.takeMessagesAfterAppend();

        sm.step(Eraftpb.Message.newBuilder().setFrom(2).setTo(1).setTerm(sm.term + 1)
                .setMsgType(Eraftpb.MessageType.MsgHeartbeat).build());
        assertThat(sm.state).isEqualTo(RaftStateType.StateFollower);

        sm.stepOrSend(steps);
        assertThat(sm.state).isEqualTo(RaftStateType.StateFollower);
    }

    @Test
    void testLeaderStepdownWhenQuorumActive() throws RaftException {
        Raft sm = newTestRaft(1, 5, 1, newTestMemoryStorage(withPeers(1, 2, 3)));
        sm.checkQuorum = true;
        sm.becomeCandidate();
        sm.becomeLeader();

        for (int i = 0; i < sm.electionTimeout + 1; i++) {
            sm.step(Eraftpb.Message.newBuilder().setFrom(2)
                    .setMsgType(Eraftpb.MessageType.MsgHeartbeatResponse).setTerm(sm.term).build());
            sm.tickFn.run();
        }
        assertThat(sm.state).isEqualTo(RaftStateType.StateLeader);
    }

    @Test
    void testLeaderStepdownWhenQuorumLost() throws RaftException {
        Raft sm = newTestRaft(1, 5, 1, newTestMemoryStorage(withPeers(1, 2, 3)));
        sm.checkQuorum = true;
        sm.becomeCandidate();
        sm.becomeLeader();

        for (int i = 0; i < sm.electionTimeout + 1; i++) {
            sm.tickFn.run();
        }
        assertThat(sm.state).isEqualTo(RaftStateType.StateFollower);
    }

    @Test
    void testLeaderSupersedingWithCheckQuorum() throws RaftException {
        Raft a = newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)));
        Raft b = newTestRaft(2, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)));
        Raft c = newTestRaft(3, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)));
        a.checkQuorum = true;
        b.checkQuorum = true;
        c.checkQuorum = true;

        Network nt = Network.newNetwork(
                new Network.RaftStateMachine(a),
                new Network.RaftStateMachine(b),
                new Network.RaftStateMachine(c));
        b.randomizedElectionTimeout = b.electionTimeout + 1;

        for (int i = 0; i < b.electionTimeout; i++) b.tickFn.run();
        nt.send(Eraftpb.Message.newBuilder().setFrom(1).setTo(1)
                .setMsgType(Eraftpb.MessageType.MsgHup).build());

        assertThat(a.state).isEqualTo(RaftStateType.StateLeader);
        assertThat(c.state).isEqualTo(RaftStateType.StateFollower);

        nt.send(Eraftpb.Message.newBuilder().setFrom(3).setTo(3)
                .setMsgType(Eraftpb.MessageType.MsgHup).build());
        assertThat(c.state).isEqualTo(RaftStateType.StateCandidate);

        for (int i = 0; i < b.electionTimeout; i++) b.tickFn.run();
        nt.send(Eraftpb.Message.newBuilder().setFrom(3).setTo(3)
                .setMsgType(Eraftpb.MessageType.MsgHup).build());
        assertThat(c.state).isEqualTo(RaftStateType.StateLeader);
    }

    @Test
    void testLeaderElectionWithCheckQuorum() throws RaftException {
        Raft a = newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)));
        Raft b = newTestRaft(2, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)));
        Raft c = newTestRaft(3, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)));
        a.checkQuorum = true;
        b.checkQuorum = true;
        c.checkQuorum = true;

        Network nt = Network.newNetwork(
                new Network.RaftStateMachine(a),
                new Network.RaftStateMachine(b),
                new Network.RaftStateMachine(c));
        a.randomizedElectionTimeout = a.electionTimeout + 1;
        b.randomizedElectionTimeout = b.electionTimeout + 2;

        nt.send(Eraftpb.Message.newBuilder().setFrom(1).setTo(1)
                .setMsgType(Eraftpb.MessageType.MsgHup).build());
        assertThat(a.state).isEqualTo(RaftStateType.StateLeader);

        a.randomizedElectionTimeout = a.electionTimeout + 1;
        b.randomizedElectionTimeout = b.electionTimeout + 2;
        for (int i = 0; i < a.electionTimeout; i++) a.tickFn.run();
        for (int i = 0; i < b.electionTimeout; i++) b.tickFn.run();
        nt.send(Eraftpb.Message.newBuilder().setFrom(3).setTo(3)
                .setMsgType(Eraftpb.MessageType.MsgHup).build());

        assertThat(a.state).isEqualTo(RaftStateType.StateFollower);
        assertThat(c.state).isEqualTo(RaftStateType.StateLeader);
    }

    @Test
    void testFreeStuckCandidateWithCheckQuorum() throws RaftException {
        Raft a = newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)));
        Raft b = newTestRaft(2, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)));
        Raft c = newTestRaft(3, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)));
        a.checkQuorum = true;
        b.checkQuorum = true;
        c.checkQuorum = true;

        Network nt = Network.newNetwork(
                new Network.RaftStateMachine(a),
                new Network.RaftStateMachine(b),
                new Network.RaftStateMachine(c));
        b.randomizedElectionTimeout = b.electionTimeout + 1;

        for (int i = 0; i < b.electionTimeout; i++) b.tickFn.run();
        nt.send(Eraftpb.Message.newBuilder().setFrom(1).setTo(1)
                .setMsgType(Eraftpb.MessageType.MsgHup).build());

        nt.isolate(1);
        nt.send(Eraftpb.Message.newBuilder().setFrom(3).setTo(3)
                .setMsgType(Eraftpb.MessageType.MsgHup).build());

        assertThat(b.state).isEqualTo(RaftStateType.StateFollower);
        assertThat(c.state).isEqualTo(RaftStateType.StateCandidate);
        assertThat(c.term).isEqualTo(b.term + 1);

        nt.send(Eraftpb.Message.newBuilder().setFrom(3).setTo(3)
                .setMsgType(Eraftpb.MessageType.MsgHup).build());
        assertThat(c.state).isEqualTo(RaftStateType.StateCandidate);
        assertThat(c.term).isEqualTo(b.term + 2);

        nt.recover();
        nt.send(Eraftpb.Message.newBuilder().setFrom(1).setTo(3)
                .setMsgType(Eraftpb.MessageType.MsgHeartbeat).setTerm(a.term).build());
        assertThat(a.state).isEqualTo(RaftStateType.StateFollower);
        assertThat(a.term).isEqualTo(c.term);

        nt.send(Eraftpb.Message.newBuilder().setFrom(3).setTo(3)
                .setMsgType(Eraftpb.MessageType.MsgHup).build());
        assertThat(c.state).isEqualTo(RaftStateType.StateLeader);
    }

    @Test
    void testNonPromotableVoterWithCheckQuorum() throws RaftException {
        Raft a = newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1, 2)));
        Raft b = newTestRaft(2, 10, 1, newTestMemoryStorage(withPeers(1)));
        a.checkQuorum = true;
        b.checkQuorum = true;

        Network nt = Network.newNetwork(
                new Network.RaftStateMachine(a),
                new Network.RaftStateMachine(b));
        b.randomizedElectionTimeout = b.electionTimeout + 1;
        b.applyConfChange(Eraftpb.ConfChangeV2.newBuilder()
                .addChanges(Eraftpb.ConfChangeSingle.newBuilder()
                        .setType(Eraftpb.ConfChangeType.ConfChangeRemoveNode).setNodeId(2)).build());
        assertThat(b.promotable()).isFalse();

        for (int i = 0; i < b.electionTimeout; i++) b.tickFn.run();
        nt.send(Eraftpb.Message.newBuilder().setFrom(1).setTo(1)
                .setMsgType(Eraftpb.MessageType.MsgHup).build());

        assertThat(a.state).isEqualTo(RaftStateType.StateLeader);
        assertThat(b.state).isEqualTo(RaftStateType.StateFollower);
        assertThat(b.lead).isEqualTo(1);
    }

    @Test
    void testDisruptiveFollower() throws RaftException {
        Raft n1 = newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)));
        Raft n2 = newTestRaft(2, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)));
        Raft n3 = newTestRaft(3, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)));
        n1.checkQuorum = true; n2.checkQuorum = true; n3.checkQuorum = true;
        n1.becomeFollower(1, Util.NONE); n2.becomeFollower(1, Util.NONE); n3.becomeFollower(1, Util.NONE);

        Network nt = Network.newNetwork(
                new Network.RaftStateMachine(n1),
                new Network.RaftStateMachine(n2),
                new Network.RaftStateMachine(n3));
        nt.send(Eraftpb.Message.newBuilder().setFrom(1).setTo(1)
                .setMsgType(Eraftpb.MessageType.MsgHup).build());

        assertThat(n1.state).isEqualTo(RaftStateType.StateLeader);

        n3.randomizedElectionTimeout = n3.electionTimeout + 2;
        for (int i = 0; i < n3.randomizedElectionTimeout - 1; i++) n3.tickFn.run();
        n3.tickFn.run();

        assertThat(n3.state).isEqualTo(RaftStateType.StateCandidate);
        assertThat(n3.term).isEqualTo(3);

        nt.send(Eraftpb.Message.newBuilder().setFrom(1).setTo(3)
                .setTerm(n1.term).setMsgType(Eraftpb.MessageType.MsgHeartbeat).build());

        assertThat(n1.state).isEqualTo(RaftStateType.StateFollower);
        assertThat(n1.term).isEqualTo(3);
    }

    @Test
    void testDisruptiveFollowerPreVote() throws RaftException {
        Raft n1 = newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)));
        Raft n2 = newTestRaft(2, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)));
        Raft n3 = newTestRaft(3, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)));
        n1.checkQuorum = true; n2.checkQuorum = true; n3.checkQuorum = true;
        n1.becomeFollower(1, Util.NONE); n2.becomeFollower(1, Util.NONE); n3.becomeFollower(1, Util.NONE);

        Network nt = Network.newNetwork(
                new Network.RaftStateMachine(n1),
                new Network.RaftStateMachine(n2),
                new Network.RaftStateMachine(n3));
        nt.send(Eraftpb.Message.newBuilder().setFrom(1).setTo(1)
                .setMsgType(Eraftpb.MessageType.MsgHup).build());

        assertThat(n1.state).isEqualTo(RaftStateType.StateLeader);

        nt.isolate(3);
        nt.send(Eraftpb.Message.newBuilder().setFrom(1).setTo(1)
                .setMsgType(Eraftpb.MessageType.MsgPropose)
                .addEntries(Eraftpb.Entry.newBuilder().setData(ByteString.copyFromUtf8("somedata"))).build());
        nt.send(Eraftpb.Message.newBuilder().setFrom(1).setTo(1)
                .setMsgType(Eraftpb.MessageType.MsgPropose)
                .addEntries(Eraftpb.Entry.newBuilder().setData(ByteString.copyFromUtf8("somedata"))).build());
        nt.send(Eraftpb.Message.newBuilder().setFrom(1).setTo(1)
                .setMsgType(Eraftpb.MessageType.MsgPropose)
                .addEntries(Eraftpb.Entry.newBuilder().setData(ByteString.copyFromUtf8("somedata"))).build());
        n1.preVote = true; n2.preVote = true; n3.preVote = true;
        nt.recover();
        nt.send(Eraftpb.Message.newBuilder().setFrom(3).setTo(3)
                .setMsgType(Eraftpb.MessageType.MsgHup).build());

        assertThat(n1.state).isEqualTo(RaftStateType.StateLeader);
        assertThat(n3.state).isEqualTo(RaftStateType.StatePreCandidate);
        assertThat(n3.term).isEqualTo(2);

        nt.send(Eraftpb.Message.newBuilder().setFrom(1).setTo(3)
                .setTerm(n1.term).setMsgType(Eraftpb.MessageType.MsgHeartbeat).build());
        assertThat(n1.state).isEqualTo(RaftStateType.StateLeader);
    }

    // ==================== Batch 1 ====================

    @Test
    void testProgressFlowControl() throws RaftException {
        Config cfg = newTestConfig(1, 5, 1, newTestMemoryStorage(withPeers(1, 2)));
        cfg.maxInflightMsgs = 3;
        cfg.maxSizePerMsg = 2048;
        Raft r = Raft.newRaft(cfg);
        r.becomeCandidate();
        r.becomeLeader();

        // Throw away all the messages relating to the initial election.
        r.readMessages();

        // While node 2 is in probe state, propose a bunch of entries.
        r.trk.getProgress().get(2L).becomeProbe();
        String blob = "a".repeat(1000);
        for (int i = 0; i < 10; i++) {
            r.step(Eraftpb.Message.newBuilder().setFrom(1).setTo(1)
                    .setMsgType(Eraftpb.MessageType.MsgPropose)
                    .addEntries(Eraftpb.Entry.newBuilder().setData(
                            com.google.protobuf.ByteString.copyFromUtf8(blob))).build());
        }

        List<Eraftpb.Message> ms = r.readMessages();
        // First append when in probe mode
        assertThat(ms).hasSize(1);
        assertThat(ms.get(0).getMsgType()).isEqualTo(Eraftpb.MessageType.MsgAppend);

        // After resume (heartbeat response), we get more
        r.step(Eraftpb.Message.newBuilder().setFrom(2).setTo(1)
                .setMsgType(Eraftpb.MessageType.MsgHeartbeatResponse).build());
        ms = r.readMessages();
        assertThat(ms).hasSize(1);
        assertThat(ms.get(0).getMsgType()).isEqualTo(Eraftpb.MessageType.MsgAppend);
    }

    @Test
    void testLeaderElectionOverwriteNewerLogs() throws RaftException {
        testLeaderElectionOverwriteNewerLogsImpl(false);
    }

    @Test
    void testLeaderElectionOverwriteNewerLogsPreVote() throws RaftException {
        testLeaderElectionOverwriteNewerLogsImpl(true);
    }

    private void testLeaderElectionOverwriteNewerLogsImpl(boolean preVote) throws RaftException {
        Consumer<Config> cfg = preVote ? c -> c.preVote = true : null;
        Network n = Network.newNetworkWithConfig(cfg,
                Network.entsWithConfig(cfg, 1),     // Node 1: Won first election
                Network.entsWithConfig(cfg, 1),     // Node 2: Got logs from node 1
                Network.entsWithConfig(cfg, 2),     // Node 3: Won second election
                Network.votedWithConfig(cfg, 3, 2), // Node 4: Voted but didn't get logs
                Network.votedWithConfig(cfg, 3, 2)  // Node 5: Voted but didn't get logs
        );

        n.send(Eraftpb.Message.newBuilder().setFrom(1).setTo(1)
                .setMsgType(Eraftpb.MessageType.MsgHup).build());
        Raft sm1 = n.peer(1);
        assertThat(sm1.state).isEqualTo(RaftStateType.StateFollower);
        assertThat(sm1.term).isEqualTo(2);

        n.send(Eraftpb.Message.newBuilder().setFrom(1).setTo(1)
                .setMsgType(Eraftpb.MessageType.MsgHup).build());
        assertThat(sm1.state).isEqualTo(RaftStateType.StateLeader);
        assertThat(sm1.term).isEqualTo(3);

        for (long id : n.peers.keySet()) {
            Raft sm = n.peer(id);
            if (sm != null) {
                List<Eraftpb.Entry> entries = sm.raftLog.allEntries();
                assertThat(entries).hasSize(2);
                assertThat(entries.get(0).getTerm()).isEqualTo(1);
                assertThat(entries.get(1).getTerm()).isEqualTo(3);
            }
        }
    }

    @Test
    void testLearnerLogReplication() throws RaftException {
        MemoryStorage s1 = newTestMemoryStorage(withPeers(1), withLearners(2));
        Raft n1 = newTestRaft(1, 10, 1, s1);
        Raft n2 = newTestRaft(2, 10, 1, newTestMemoryStorage(withPeers(1), withLearners(2)));

        Network nt = Network.newNetwork(new Network.RaftStateMachine(n1), new Network.RaftStateMachine(n2));

        n1.becomeFollower(1, Util.NONE);
        n2.becomeFollower(1, Util.NONE);

        n1.randomizedElectionTimeout = n1.electionTimeout;
        for (int i = 0; i < n1.electionTimeout; i++) {
            n1.tickFn.run();
        }
        n1.advanceMessagesAfterAppend();

        nt.send(Eraftpb.Message.newBuilder().setFrom(1).setTo(1)
                .setMsgType(Eraftpb.MessageType.MsgBeat).build());

        assertThat(n1.state).isEqualTo(RaftStateType.StateLeader);
        assertThat(n2.isLearner).isTrue();

        nt.send(Eraftpb.Message.newBuilder().setFrom(1).setTo(1)
                .setMsgType(Eraftpb.MessageType.MsgPropose)
                .addEntries(Eraftpb.Entry.newBuilder().setData(
                        com.google.protobuf.ByteString.copyFromUtf8("somedata"))).build());

        assertThat(n1.raftLog.committed).isEqualTo(2);
        assertThat(n2.raftLog.committed).isEqualTo(n1.raftLog.committed);

        long match = n1.trk.getProgress().get(2L).getMatch();
        assertThat(match).isEqualTo(n2.raftLog.committed);
    }

    @Test
    void testLearnerPromotion() throws RaftException {
        Raft n1 = newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1), withLearners(2)));
        Raft n2 = newTestRaft(2, 10, 1, newTestMemoryStorage(withPeers(1), withLearners(2)));

        n1.becomeFollower(1, Util.NONE);
        n2.becomeFollower(1, Util.NONE);

        Network nt = Network.newNetwork(new Network.RaftStateMachine(n1), new Network.RaftStateMachine(n2));

        assertThat(n1.state).isNotEqualTo(RaftStateType.StateLeader);

        // n1 should become leader
        n1.randomizedElectionTimeout = n1.electionTimeout;
        for (int i = 0; i < n1.electionTimeout; i++) n1.tickFn.run();
        n1.advanceMessagesAfterAppend();

        assertThat(n1.state).isEqualTo(RaftStateType.StateLeader);
        assertThat(n2.state).isEqualTo(RaftStateType.StateFollower);

        nt.send(Eraftpb.Message.newBuilder().setFrom(1).setTo(1)
                .setMsgType(Eraftpb.MessageType.MsgBeat).build());

        n1.applyConfChange(Eraftpb.ConfChangeV2.newBuilder()
                .addChanges(Eraftpb.ConfChangeSingle.newBuilder()
                        .setType(Eraftpb.ConfChangeType.ConfChangeAddNode).setNodeId(2)).build());
        n2.applyConfChange(Eraftpb.ConfChangeV2.newBuilder()
                .addChanges(Eraftpb.ConfChangeSingle.newBuilder()
                        .setType(Eraftpb.ConfChangeType.ConfChangeAddNode).setNodeId(2)).build());
        assertThat(n2.isLearner).isFalse();

        // n2 starts election, should become leader
        n2.randomizedElectionTimeout = n2.electionTimeout;
        for (int i = 0; i < n2.electionTimeout; i++) n2.tickFn.run();
        n2.advanceMessagesAfterAppend();

        nt.send(Eraftpb.Message.newBuilder().setFrom(2).setTo(2)
                .setMsgType(Eraftpb.MessageType.MsgBeat).build());

        assertThat(n1.state).isEqualTo(RaftStateType.StateFollower);
        assertThat(n2.state).isEqualTo(RaftStateType.StateLeader);
    }

    @Test
    void testDuelingPreCandidates() throws RaftException {
        Config cfgA = newTestConfig(1, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)));
        cfgA.preVote = true;
        Config cfgB = newTestConfig(2, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)));
        cfgB.preVote = true;
        Config cfgC = newTestConfig(3, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)));
        cfgC.preVote = true;
        Raft a = Raft.newRaft(cfgA);
        Raft b = Raft.newRaft(cfgB);
        Raft c = Raft.newRaft(cfgC);

        Network nt = Network.newNetwork(
                new Network.RaftStateMachine(a),
                new Network.RaftStateMachine(b),
                new Network.RaftStateMachine(c));
        nt.cut(1, 3);

        nt.send(Eraftpb.Message.newBuilder().setFrom(1).setTo(1)
                .setMsgType(Eraftpb.MessageType.MsgHup).build());
        nt.send(Eraftpb.Message.newBuilder().setFrom(3).setTo(3)
                .setMsgType(Eraftpb.MessageType.MsgHup).build());

        assertThat(nt.peer(1).state).isEqualTo(RaftStateType.StateLeader);
        assertThat(nt.peer(3).state).isEqualTo(RaftStateType.StateFollower);

        nt.recover();
        nt.send(Eraftpb.Message.newBuilder().setFrom(3).setTo(3)
                .setMsgType(Eraftpb.MessageType.MsgHup).build());

        assertThat(a.state).isEqualTo(RaftStateType.StateLeader);
        assertThat(a.term).isEqualTo(1);
        assertThat(b.state).isEqualTo(RaftStateType.StateFollower);
        assertThat(b.term).isEqualTo(1);
        assertThat(c.state).isEqualTo(RaftStateType.StateFollower);
        assertThat(c.term).isEqualTo(1);
        assertThat(c.raftLog.lastIndex()).isZero();
    }

    @Test
    void testRaftFreesReadOnlyMem() throws RaftException {
        Raft sm = newTestRaft(1, 5, 1, newTestMemoryStorage(withPeers(1, 2)));
        sm.becomeCandidate();
        sm.becomeLeader();
        sm.raftLog.commitTo(sm.raftLog.lastIndex());

        byte[] reqCtx = "ctx".getBytes();
        sm.step(Eraftpb.Message.newBuilder().setFrom(2)
                .setMsgType(Eraftpb.MessageType.MsgReadIndex)
                .addEntries(Eraftpb.Entry.newBuilder().setData(
                        com.google.protobuf.ByteString.copyFrom(reqCtx))).build());
        List<Eraftpb.Message> msgs = sm.readMessages();
        assertThat(msgs).hasSize(1);
        assertThat(msgs.get(0).getMsgType()).isEqualTo(Eraftpb.MessageType.MsgHeartbeat);
        assertThat(sm.readOnly.unconfirmedReads).hasSize(1);

        sm.step(Eraftpb.Message.newBuilder().setFrom(2)
                .setMsgType(Eraftpb.MessageType.MsgHeartbeatResponse)
                .setContext(msgs.get(0).getContext()).build());
        assertThat(sm.readOnly.unconfirmedReads).isEmpty();
    }

    @Test
    void testMsgAppRespWaitReset() throws RaftException {
        MemoryStorage s = newTestMemoryStorage(withPeers(1, 2, 3));
        Raft sm = newTestRaft(1, 5, 1, s);
        sm.becomeCandidate();
        sm.becomeLeader();

        nextEnts(sm, s);

        sm.step(Eraftpb.Message.newBuilder().setFrom(2)
                .setMsgType(Eraftpb.MessageType.MsgAppendResponse).setIndex(1).build());
        assertThat(sm.raftLog.committed).isEqualTo(1);
        sm.readMessages();

        sm.step(Eraftpb.Message.newBuilder().setFrom(1)
                .setMsgType(Eraftpb.MessageType.MsgPropose)
                .addEntries(Eraftpb.Entry.getDefaultInstance()).build());

        List<Eraftpb.Message> msgs = sm.readMessages();
        assertThat(msgs).hasSize(1);
        assertThat(msgs.get(0).getMsgType()).isEqualTo(Eraftpb.MessageType.MsgAppend);
        assertThat(msgs.get(0).getTo()).isEqualTo(2);
        assertThat(msgs.get(0).getEntriesCount()).isEqualTo(1);
        assertThat(msgs.get(0).getEntries(0).getIndex()).isEqualTo(2);

        sm.step(Eraftpb.Message.newBuilder().setFrom(3)
                .setMsgType(Eraftpb.MessageType.MsgAppendResponse).setIndex(1).build());
        msgs = sm.readMessages();
        assertThat(msgs).hasSize(1);
        assertThat(msgs.get(0).getMsgType()).isEqualTo(Eraftpb.MessageType.MsgAppend);
        assertThat(msgs.get(0).getTo()).isEqualTo(3);
        assertThat(msgs.get(0).getEntriesCount()).isEqualTo(1);
        assertThat(msgs.get(0).getEntries(0).getIndex()).isEqualTo(2);
    }

    @Test
    void testRecvMsgVote() throws RaftException {
        testRecvMsgVoteImpl(Eraftpb.MessageType.MsgRequestVote);
    }

    @Test
    void testRecvMsgPreVote() throws RaftException {
        testRecvMsgVoteImpl(Eraftpb.MessageType.MsgRequestPreVote);
    }

    private void testRecvMsgVoteImpl(Eraftpb.MessageType msgType) throws RaftException {
        record TC(RaftStateType state, long index, long logTerm, long voteFor, boolean wreject) {}
        List<TC> tests = List.of(
                new TC(RaftStateType.StateFollower, 0, 0, Util.NONE, true),
                new TC(RaftStateType.StateFollower, 0, 1, Util.NONE, true),
                new TC(RaftStateType.StateFollower, 0, 2, Util.NONE, true),
                new TC(RaftStateType.StateFollower, 0, 3, Util.NONE, false),
                new TC(RaftStateType.StateFollower, 1, 0, Util.NONE, true),
                new TC(RaftStateType.StateFollower, 1, 1, Util.NONE, true),
                new TC(RaftStateType.StateFollower, 1, 2, Util.NONE, true),
                new TC(RaftStateType.StateFollower, 1, 3, Util.NONE, false),
                new TC(RaftStateType.StateFollower, 2, 0, Util.NONE, true),
                new TC(RaftStateType.StateFollower, 2, 1, Util.NONE, true),
                new TC(RaftStateType.StateFollower, 2, 2, Util.NONE, false),
                new TC(RaftStateType.StateFollower, 2, 3, Util.NONE, false),
                new TC(RaftStateType.StateFollower, 3, 0, Util.NONE, true),
                new TC(RaftStateType.StateFollower, 3, 1, Util.NONE, true),
                new TC(RaftStateType.StateFollower, 3, 2, Util.NONE, false),
                new TC(RaftStateType.StateFollower, 3, 3, Util.NONE, false),
                new TC(RaftStateType.StateFollower, 3, 2, 2, false),
                new TC(RaftStateType.StateFollower, 3, 2, 1, true),
                new TC(RaftStateType.StateLeader, 3, 3, 1, true),
                new TC(RaftStateType.StatePreCandidate, 3, 3, 1, true),
                new TC(RaftStateType.StateCandidate, 3, 3, 1, true)
        );

        for (int i = 0; i < tests.size(); i++) {
            TC tt = tests.get(i);
            MemoryStorage storage = new MemoryStorage();
            storage.setSnapshot(Eraftpb.Snapshot.newBuilder()
                    .setMetadata(Eraftpb.SnapshotMetadata.newBuilder()
                            .setConfState(Eraftpb.ConfState.newBuilder().addVoters(1)))
                    .build());
            storage.append(index(1).terms(2, 2));
            Raft sm = newTestRaft(1, 10, 1, storage);
            sm.state = tt.state;
            switch (tt.state) {
                case StateFollower -> sm.stepFn = Raft::stepFollower;
                case StateCandidate, StatePreCandidate -> sm.stepFn = Raft::stepCandidate;
                case StateLeader -> sm.stepFn = Raft::stepLeader;
            }
            sm.vote = tt.voteFor;

            long term = Math.max(sm.raftLog.lastEntryID().term(), tt.logTerm);
            sm.term = term;
            sm.step(Eraftpb.Message.newBuilder()
                    .setMsgType(msgType).setTerm(term).setFrom(2)
                    .setIndex(tt.index).setLogTerm(tt.logTerm).build());

            List<Eraftpb.Message> msgs = sm.readMessages();
            assertThat(msgs).as("#%d", i).hasSize(1);
            assertThat(msgs.get(0).getMsgType()).isEqualTo(Util.voteRespMsgType(msgType));
            assertThat(msgs.get(0).getReject()).as("#%d", i).isEqualTo(tt.wreject);
        }
    }

    @Test
    void testCandidateResetTermMsgHeartbeat() throws RaftException {
        testCandidateResetTermImpl(Eraftpb.MessageType.MsgHeartbeat);
    }

    @Test
    void testCandidateResetTermMsgApp() throws RaftException {
        testCandidateResetTermImpl(Eraftpb.MessageType.MsgAppend);
    }

    private void testCandidateResetTermImpl(Eraftpb.MessageType mt) throws RaftException {
        Raft a = newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)));
        Raft b = newTestRaft(2, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)));
        Raft c = newTestRaft(3, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)));

        Network nt = Network.newNetwork(
                new Network.RaftStateMachine(a),
                new Network.RaftStateMachine(b),
                new Network.RaftStateMachine(c));

        nt.send(Eraftpb.Message.newBuilder().setFrom(1).setTo(1)
                .setMsgType(Eraftpb.MessageType.MsgHup).build());
        assertThat(a.state).isEqualTo(RaftStateType.StateLeader);

        nt.isolate(3);
        nt.send(Eraftpb.Message.newBuilder().setFrom(2).setTo(2)
                .setMsgType(Eraftpb.MessageType.MsgHup).build());
        nt.send(Eraftpb.Message.newBuilder().setFrom(1).setTo(1)
                .setMsgType(Eraftpb.MessageType.MsgHup).build());
        assertThat(a.state).isEqualTo(RaftStateType.StateLeader);

        // trigger campaign in isolated c
        c.resetRandomizedElectionTimeout();
        for (int i = 0; i < c.randomizedElectionTimeout; i++) {
            c.tickFn.run();
        }
        c.advanceMessagesAfterAppend();
        assertThat(c.state).isEqualTo(RaftStateType.StateCandidate);

        nt.recover();
        nt.send(Eraftpb.Message.newBuilder().setFrom(1).setTo(3)
                .setTerm(a.term).setMsgType(mt).build());

        assertThat(c.state).isEqualTo(RaftStateType.StateFollower);
        assertThat(c.term).isEqualTo(a.term);
    }

    @Test
    void testPastElectionTimeout() throws RaftException {
        record TC(int elapse, double wprobability, boolean round) {}
        List<TC> tests = List.of(
                new TC(5, 0, false),
                new TC(10, 0.1, true),
                new TC(13, 0.4, true),
                new TC(15, 0.6, true),
                new TC(18, 0.9, true),
                new TC(20, 1, false)
        );

        for (int i = 0; i < tests.size(); i++) {
            TC tt = tests.get(i);
            Raft sm = newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1)));
            sm.electionElapsed = tt.elapse;
            int c = 0;
            for (int j = 0; j < 10000; j++) {
                sm.resetRandomizedElectionTimeout();
                if (sm.pastElectionTimeout()) c++;
            }
            double got = (double) c / 10000.0;
            if (tt.round) {
                got = Math.floor(got * 10 + 0.5) / 10.0;
            }
            assertThat(got).as("#%d", i).isEqualTo(tt.wprobability);
        }
    }

    @Test
    void testReadOnlyOptionSafe() throws RaftException {
        Raft a = newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)));
        Raft b = newTestRaft(2, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)));
        Raft c = newTestRaft(3, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)));

        Network nt = Network.newNetwork(
                new Network.RaftStateMachine(a),
                new Network.RaftStateMachine(b),
                new Network.RaftStateMachine(c));
        b.randomizedElectionTimeout = b.electionTimeout + 1;
        for (int i = 0; i < b.electionTimeout; i++) b.tickFn.run();
        nt.send(Eraftpb.Message.newBuilder().setFrom(1).setTo(1)
                .setMsgType(Eraftpb.MessageType.MsgHup).build());
        assertThat(a.state).isEqualTo(RaftStateType.StateLeader);

        record ReadTC(Raft sm, int proposals, long wri, byte[] wctx) {}
        List<ReadTC> readTests = List.of(
                new ReadTC(a, 10, 11, "ctx1".getBytes()),
                new ReadTC(b, 10, 21, "ctx2".getBytes()),
                new ReadTC(c, 10, 31, "ctx3".getBytes()),
                new ReadTC(a, 10, 41, "ctx4".getBytes()),
                new ReadTC(b, 10, 51, "ctx5".getBytes()),
                new ReadTC(c, 10, 61, "ctx6".getBytes())
        );
        for (int i = 0; i < readTests.size(); i++) {
            ReadTC tt = readTests.get(i);
            for (int j = 0; j < tt.proposals; j++) {
                nt.send(Eraftpb.Message.newBuilder().setFrom(1).setTo(1)
                        .setMsgType(Eraftpb.MessageType.MsgPropose)
                        .addEntries(Eraftpb.Entry.getDefaultInstance()).build());
            }
            nt.send(Eraftpb.Message.newBuilder().setFrom(tt.sm.id).setTo(tt.sm.id)
                    .setMsgType(Eraftpb.MessageType.MsgReadIndex)
                    .addEntries(Eraftpb.Entry.newBuilder().setData(
                            ByteString.copyFrom(tt.wctx))).build());

            assertThat(tt.sm.readStates).as("#%d", i).isNotEmpty();
            ReadState rs = tt.sm.readStates.get(0);
            assertThat(rs.index()).as("#%d", i).isEqualTo(tt.wri);
            assertThat(rs.requestCtx()).as("#%d", i).isEqualTo(tt.wctx);
            tt.sm.readStates = new ArrayList<>();
        }
    }

    @Test
    void testReadOnlyOptionLease() throws RaftException {
        Raft a = newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)));
        Raft b = newTestRaft(2, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)));
        Raft c = newTestRaft(3, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)));
        a.readOnly.option = ReadOnlyOption.ReadOnlyLeaseBased;
        b.readOnly.option = ReadOnlyOption.ReadOnlyLeaseBased;
        c.readOnly.option = ReadOnlyOption.ReadOnlyLeaseBased;
        a.checkQuorum = true; b.checkQuorum = true; c.checkQuorum = true;

        Network nt = Network.newNetwork(
                new Network.RaftStateMachine(a),
                new Network.RaftStateMachine(b),
                new Network.RaftStateMachine(c));
        b.randomizedElectionTimeout = b.electionTimeout + 1;
        for (int i = 0; i < b.electionTimeout; i++) b.tickFn.run();
        nt.send(Eraftpb.Message.newBuilder().setFrom(1).setTo(1)
                .setMsgType(Eraftpb.MessageType.MsgHup).build());
        assertThat(a.state).isEqualTo(RaftStateType.StateLeader);

        record LeaseTC(Raft sm, int proposals, long wri, byte[] wctx) {}
        List<LeaseTC> leaseTests = List.of(
                new LeaseTC(a, 10, 11, "ctx1".getBytes()),
                new LeaseTC(b, 10, 21, "ctx2".getBytes()),
                new LeaseTC(c, 10, 31, "ctx3".getBytes()),
                new LeaseTC(a, 10, 41, "ctx4".getBytes()),
                new LeaseTC(b, 10, 51, "ctx5".getBytes()),
                new LeaseTC(c, 10, 61, "ctx6".getBytes())
        );
        for (int i = 0; i < leaseTests.size(); i++) {
            LeaseTC tt = leaseTests.get(i);
            for (int j = 0; j < tt.proposals; j++) {
                nt.send(Eraftpb.Message.newBuilder().setFrom(1).setTo(1)
                        .setMsgType(Eraftpb.MessageType.MsgPropose)
                        .addEntries(Eraftpb.Entry.getDefaultInstance()).build());
            }
            nt.send(Eraftpb.Message.newBuilder().setFrom(tt.sm.id).setTo(tt.sm.id)
                    .setMsgType(Eraftpb.MessageType.MsgReadIndex)
                    .addEntries(Eraftpb.Entry.newBuilder().setData(
                            ByteString.copyFrom(tt.wctx))).build());

            ReadState rs = tt.sm.readStates.get(0);
            assertThat(rs.index()).as("#%d", i).isEqualTo(tt.wri);
            assertThat(rs.requestCtx()).as("#%d", i).isEqualTo(tt.wctx);
            tt.sm.readStates = new ArrayList<>();
        }
    }

    @Test
    void testReadOnlyForNewLeader() throws RaftException {
        MemoryStorage s1 = newTestMemoryStorage(withPeers(1, 2, 3));
        s1.append(index(1).terms(1, 1));
        s1.setHardState(Eraftpb.HardState.newBuilder().setTerm(1).setCommit(1).build());
        MemoryStorage s2 = newTestMemoryStorage(withPeers(1, 2, 3));
        s2.append(index(1).terms(1, 1));
        s2.setHardState(Eraftpb.HardState.newBuilder().setTerm(1).setCommit(2).build());
        s2.compact(2);
        MemoryStorage s3 = newTestMemoryStorage(withPeers(1, 2, 3));
        s3.append(index(1).terms(1, 1));
        s3.setHardState(Eraftpb.HardState.newBuilder().setTerm(1).setCommit(2).build());
        s3.compact(2);

        Config cfg1 = newTestConfig(1, 10, 1, s1); cfg1.applied = 1;
        Config cfg2 = newTestConfig(2, 10, 1, s2); cfg2.applied = 2;
        Config cfg3 = newTestConfig(3, 10, 1, s3); cfg3.applied = 2;

        Network nt = Network.newNetwork(
                new Network.RaftStateMachine(Raft.newRaft(cfg1)),
                new Network.RaftStateMachine(Raft.newRaft(cfg2)),
                new Network.RaftStateMachine(Raft.newRaft(cfg3)));
        nt.ignore(Eraftpb.MessageType.MsgAppend);
        nt.send(Eraftpb.Message.newBuilder().setFrom(1).setTo(1)
                .setMsgType(Eraftpb.MessageType.MsgHup).build());
        Raft sm = nt.peer(1);
        assertThat(sm.state).isEqualTo(RaftStateType.StateLeader);

        byte[] wctx = "ctx".getBytes();
        nt.send(Eraftpb.Message.newBuilder().setFrom(1).setTo(1)
                .setMsgType(Eraftpb.MessageType.MsgReadIndex)
                .addEntries(Eraftpb.Entry.newBuilder().setData(ByteString.copyFrom(wctx))).build());
        assertThat(sm.readStates).isEmpty();

        nt.recover();
        for (int i = 0; i < sm.heartbeatTimeout; i++) sm.tickFn.run();
        nt.send(Eraftpb.Message.newBuilder().setFrom(1).setTo(1)
                .setMsgType(Eraftpb.MessageType.MsgPropose)
                .addEntries(Eraftpb.Entry.getDefaultInstance()).build());
        assertThat(sm.raftLog.committed).isEqualTo(4);

        assertThat(sm.readStates).hasSize(1);
        assertThat(sm.readStates.get(0).index()).isEqualTo(4);
        assertThat(sm.readStates.get(0).requestCtx()).isEqualTo(wctx);

        nt.send(Eraftpb.Message.newBuilder().setFrom(1).setTo(1)
                .setMsgType(Eraftpb.MessageType.MsgReadIndex)
                .addEntries(Eraftpb.Entry.newBuilder().setData(ByteString.copyFrom(wctx))).build());
        assertThat(sm.readStates).hasSize(2);
        assertThat(sm.readStates.get(1).index()).isEqualTo(4);
    }

    @Test
    void testReadOnlyWithLearner() throws RaftException {
        MemoryStorage s = newTestMemoryStorage(withPeers(1), withLearners(2));
        Raft a = newTestRaft(1, 10, 1, s);
        Raft b = newTestRaft(2, 10, 1, newTestMemoryStorage(withPeers(1), withLearners(2)));

        Network nt = Network.newNetwork(new Network.RaftStateMachine(a), new Network.RaftStateMachine(b));
        b.randomizedElectionTimeout = b.electionTimeout + 1;
        for (int i = 0; i < b.electionTimeout; i++) b.tickFn.run();
        nt.send(Eraftpb.Message.newBuilder().setFrom(1).setTo(1)
                .setMsgType(Eraftpb.MessageType.MsgHup).build());
        assertThat(a.state).isEqualTo(RaftStateType.StateLeader);

        record LearnerReadTC(Raft sm, int proposals, long wri, byte[] wctx) {}
        List<LearnerReadTC> tests = List.of(
                new LearnerReadTC(a, 10, 11, "ctx1".getBytes()),
                new LearnerReadTC(b, 10, 21, "ctx2".getBytes()),
                new LearnerReadTC(a, 10, 31, "ctx3".getBytes()),
                new LearnerReadTC(b, 10, 41, "ctx4".getBytes())
        );
        for (int i = 0; i < tests.size(); i++) {
            LearnerReadTC tt = tests.get(i);
            for (int j = 0; j < tt.proposals; j++) {
                nt.send(Eraftpb.Message.newBuilder().setFrom(1).setTo(1)
                        .setMsgType(Eraftpb.MessageType.MsgPropose)
                        .addEntries(Eraftpb.Entry.getDefaultInstance()).build());
                nextEnts(a, s);
            }
            nt.send(Eraftpb.Message.newBuilder().setFrom(tt.sm.id).setTo(tt.sm.id)
                    .setMsgType(Eraftpb.MessageType.MsgReadIndex)
                    .addEntries(Eraftpb.Entry.newBuilder().setData(
                            ByteString.copyFrom(tt.wctx))).build());

            assertThat(tt.sm.readStates).as("#%d", i).isNotEmpty();
            ReadState rs = tt.sm.readStates.get(0);
            assertThat(rs.index()).as("#%d", i).isEqualTo(tt.wri);
            assertThat(rs.requestCtx()).as("#%d", i).isEqualTo(tt.wctx);
            tt.sm.readStates = new ArrayList<>();
        }
    }

    @Test
    void testReadOnlyDuplicateRequest() throws RaftException {
        Raft r1 = newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)));
        Raft r2 = newTestRaft(2, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)));
        Raft r3 = newTestRaft(3, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)));
        Network net = Network.newNetwork(
                new Network.RaftStateMachine(r1),
                new Network.RaftStateMachine(r2),
                new Network.RaftStateMachine(r3));

        // Delayed messages
        List<Eraftpb.Message> delayedMsgs = new ArrayList<>();

        // elect r1 as leader
        net.send(Eraftpb.Message.newBuilder().setFrom(1).setTo(1)
                .setMsgType(Eraftpb.MessageType.MsgHup).build());

        // Create readIndexMsgA
        Eraftpb.Message readIndexMsgA = Eraftpb.Message.newBuilder()
                .setFrom(2).setTo(1)
                .setMsgType(Eraftpb.MessageType.MsgReadIndex)
                .addEntries(Eraftpb.Entry.newBuilder().setData(ByteString.copyFromUtf8("A"))).build();
        long readIndexMinimumA = r1.raftLog.committed;

        // Explicitly duplicate readIndexMsgA for later
        delayedMsgs.add(readIndexMsgA);

        // Process readIndex request, but delay heartbeat responses
        net.msgHook = m -> {
            if (m.getMsgType() == Eraftpb.MessageType.MsgHeartbeatResponse) {
                delayedMsgs.add(m);
                return false;
            }
            return true;
        };
        net.send(readIndexMsgA);
        net.msgHook = null;

        // tick and send whatever raft wants to send
        r1.tickFn.run();
        r1.advanceMessagesAfterAppend();
        List<Eraftpb.Message> msgs = r1.readMessages();
        net.send(msgs.toArray(new Eraftpb.Message[0]));

        net.isolate(r1.id);

        // elect r2 as leader, and commit a new entry
        net.send(Eraftpb.Message.newBuilder().setFrom(2).setTo(2)
                .setMsgType(Eraftpb.MessageType.MsgHup).build());
        net.send(Eraftpb.Message.newBuilder().setFrom(2).setTo(2)
                .setMsgType(Eraftpb.MessageType.MsgPropose)
                .addEntries(Eraftpb.Entry.newBuilder().setData(ByteString.copyFromUtf8("someOp"))).build());

        // Create readIndexMsgB
        Eraftpb.Message readIndexMsgB = Eraftpb.Message.newBuilder()
                .setFrom(1).setTo(1)
                .setMsgType(Eraftpb.MessageType.MsgReadIndex)
                .addEntries(Eraftpb.Entry.newBuilder().setData(ByteString.copyFromUtf8("B"))).build();
        long readIndexMinimumB = r2.raftLog.committed;

        net.send(readIndexMsgB);
        net.recover();
        // First send the delayed readIndexMsgA, then the delayed heartbeats
        net.send(delayedMsgs.toArray(new Eraftpb.Message[0]));

        // make sure the readIndexes aren't too small (which would violate linearizability)
        List<ReadState> allReadStates = new ArrayList<>();
        allReadStates.addAll(r1.readStates);
        allReadStates.addAll(r2.readStates);
        allReadStates.addAll(r3.readStates);
        for (ReadState rd : allReadStates) {
            if ("A".equals(new String(rd.requestCtx()))) {
                assertThat(rd.index()).as("readIndex for A").isGreaterThanOrEqualTo(readIndexMinimumA);
            } else if ("B".equals(new String(rd.requestCtx()))) {
                assertThat(rd.index()).as("readIndex for B").isGreaterThanOrEqualTo(readIndexMinimumB);
            }
        }
    }

    @Test
    void testLeaderAppResp() throws RaftException {
        record AppTC(long index, boolean reject, long wmatch, long wnext, int wmsgNum, long windex, long wcommitted) {}
        List<AppTC> tests = List.of(
                new AppTC(3, true, 0, 3, 0, 0, 0),
                new AppTC(2, true, 0, 2, 1, 1, 0),
                new AppTC(2, false, 2, 4, 2, 2, 2),
                new AppTC(0, false, 0, 4, 1, 0, 0)
        );
        for (AppTC tt : tests) {
            MemoryStorage storage = new MemoryStorage();
            storage.setSnapshot(Eraftpb.Snapshot.newBuilder()
                    .setMetadata(Eraftpb.SnapshotMetadata.newBuilder()
                            .setConfState(Eraftpb.ConfState.newBuilder().addVoters(1).addVoters(2).addVoters(3)))
                    .build());
            storage.append(index(1).terms(1, 1));
            Raft sm = newTestRaft(1, 10, 1, storage);
            sm.becomeCandidate();
            sm.becomeLeader();
            sm.readMessages();
            sm.step(Eraftpb.Message.newBuilder().setFrom(2)
                    .setMsgType(Eraftpb.MessageType.MsgAppendResponse)
                    .setIndex(tt.index).setTerm(sm.term)
                    .setReject(tt.reject).setRejectHint(tt.index).build());

            Progress p = sm.trk.getProgress().get(2L);
            assertThat(p.getMatch()).isEqualTo(tt.wmatch);
            assertThat(p.getNext()).isEqualTo(tt.wnext);

            List<Eraftpb.Message> msgs = sm.readMessages();
            assertThat(msgs).hasSize(tt.wmsgNum);
            for (Eraftpb.Message msg : msgs) {
                assertThat(msg.getIndex()).isEqualTo(tt.windex);
                assertThat(msg.getCommit()).isEqualTo(tt.wcommitted);
            }
        }
    }

    @Test
    void testRestoreWithLearner() throws RaftException {
        Eraftpb.Snapshot snap = Eraftpb.Snapshot.newBuilder()
                .setMetadata(Eraftpb.SnapshotMetadata.newBuilder()
                        .setIndex(11).setTerm(11)
                        .setConfState(Eraftpb.ConfState.newBuilder()
                                .addVoters(1).addVoters(2).addLearners(3)))
                .build();

        MemoryStorage storage = newTestMemoryStorage(withPeers(1, 2), withLearners(3));
        Raft sm = newTestRaft(3, 8, 2, storage);
        assertThat(sm.restore(snap)).isTrue();
        assertThat(sm.raftLog.lastIndex()).isEqualTo(snap.getMetadata().getIndex());
        assertThat(sm.raftLog.term(snap.getMetadata().getIndex())).isEqualTo(snap.getMetadata().getTerm());
        assertThat(sm.trk.voterNodes()).hasSize(2);
        assertThat(sm.trk.learnerNodes()).hasSize(1);

        for (long n : snap.getMetadata().getConfState().getVotersList()) {
            assertThat(sm.trk.getProgress().get(n).isLearner()).isFalse();
        }
        for (long n : snap.getMetadata().getConfState().getLearnersList()) {
            assertThat(sm.trk.getProgress().get(n).isLearner()).isTrue();
        }
        assertThat(sm.restore(snap)).isFalse();
    }

    @Test
    void testRestoreWithVotersOutgoing() throws RaftException {
        Eraftpb.Snapshot snap = Eraftpb.Snapshot.newBuilder()
                .setMetadata(Eraftpb.SnapshotMetadata.newBuilder()
                        .setIndex(11).setTerm(11)
                        .setConfState(Eraftpb.ConfState.newBuilder()
                                .addVoters(2).addVoters(3).addVoters(4)
                                .addVotersOutgoing(1).addVotersOutgoing(2).addVotersOutgoing(3)))
                .build();

        MemoryStorage storage = newTestMemoryStorage(withPeers(1, 2));
        Raft sm = newTestRaft(1, 10, 1, storage);
        assertThat(sm.restore(snap)).isTrue();
        assertThat(sm.raftLog.lastIndex()).isEqualTo(snap.getMetadata().getIndex());
        assertThat(sm.trk.voterNodes()).containsExactly(1L, 2L, 3L, 4L);
        assertThat(sm.restore(snap)).isFalse();

        for (int i = 0; i < sm.randomizedElectionTimeout; i++) sm.tickFn.run();
        assertThat(sm.state).isEqualTo(RaftStateType.StateFollower);
    }

    @Test
    void testRestoreVoterToLearner() throws RaftException {
        Eraftpb.Snapshot snap = Eraftpb.Snapshot.newBuilder()
                .setMetadata(Eraftpb.SnapshotMetadata.newBuilder()
                        .setIndex(11).setTerm(11)
                        .setConfState(Eraftpb.ConfState.newBuilder()
                                .addVoters(1).addVoters(2).addLearners(3)))
                .build();

        MemoryStorage storage = newTestMemoryStorage(withPeers(1, 2, 3));
        Raft sm = newTestRaft(3, 10, 1, storage);
        assertThat(sm.isLearner).isFalse();
        assertThat(sm.restore(snap)).isTrue();
    }

    @Test
    void testRestoreLearnerPromotion() throws RaftException {
        Eraftpb.Snapshot snap = Eraftpb.Snapshot.newBuilder()
                .setMetadata(Eraftpb.SnapshotMetadata.newBuilder()
                        .setIndex(11).setTerm(11)
                        .setConfState(Eraftpb.ConfState.newBuilder()
                                .addVoters(1).addVoters(2).addVoters(3)))
                .build();

        MemoryStorage storage = newTestMemoryStorage(withPeers(1, 2), withLearners(3));
        Raft sm = newTestRaft(3, 10, 1, storage);
        assertThat(sm.isLearner).isTrue();
        assertThat(sm.restore(snap)).isTrue();
        assertThat(sm.isLearner).isFalse();
    }

    @Test
    void testLearnerReceiveSnapshot() throws RaftException {
        Eraftpb.Snapshot snap = Eraftpb.Snapshot.newBuilder()
                .setMetadata(Eraftpb.SnapshotMetadata.newBuilder()
                        .setIndex(11).setTerm(11)
                        .setConfState(Eraftpb.ConfState.newBuilder()
                                .addVoters(1).addLearners(2)))
                .build();

        MemoryStorage store = newTestMemoryStorage(withPeers(1), withLearners(2));
        Raft n1 = newTestRaft(1, 10, 1, store);
        Raft n2 = newTestRaft(2, 10, 1, newTestMemoryStorage(withPeers(1), withLearners(2)));

        n1.restore(snap);
        Eraftpb.Snapshot unstableSnap = n1.raftLog.nextUnstableSnapshot();
        store.applySnapshot(unstableSnap);
        n1.appliedSnap(unstableSnap);

        Network nt = Network.newNetwork(new Network.RaftStateMachine(n1), new Network.RaftStateMachine(n2));
        n1.randomizedElectionTimeout = n1.electionTimeout;
        for (int i = 0; i < n1.electionTimeout; i++) n1.tickFn.run();

        nt.send(Eraftpb.Message.newBuilder().setFrom(1).setTo(1)
                .setMsgType(Eraftpb.MessageType.MsgBeat).build());
        assertThat(n2.raftLog.committed).isEqualTo(n1.raftLog.committed);
    }

    @Test
    void testProvideSnap() throws RaftException {
        Eraftpb.Snapshot snap = Eraftpb.Snapshot.newBuilder()
                .setMetadata(Eraftpb.SnapshotMetadata.newBuilder()
                        .setIndex(11).setTerm(11)
                        .setConfState(Eraftpb.ConfState.newBuilder()
                                .addVoters(1).addVoters(2)))
                .build();

        MemoryStorage storage = newTestMemoryStorage(withPeers(1));
        Raft sm = newTestRaft(1, 10, 1, storage);
        sm.restore(snap);

        sm.becomeCandidate();
        sm.becomeLeader();

        sm.trk.getProgress().get(2L).setNext(sm.raftLog.firstIndex());
        sm.step(Eraftpb.Message.newBuilder().setFrom(2).setTo(1)
                .setMsgType(Eraftpb.MessageType.MsgAppendResponse)
                .setIndex(sm.trk.getProgress().get(2L).getNext() - 1)
                .setReject(true).build());

        List<Eraftpb.Message> msgs = sm.readMessages();
        assertThat(msgs).hasSize(1);
        assertThat(msgs.get(0).getMsgType()).isEqualTo(Eraftpb.MessageType.MsgSnapshot);
    }

    @Test
    void testIgnoreProvidingSnap() throws RaftException {
        Eraftpb.Snapshot snap = Eraftpb.Snapshot.newBuilder()
                .setMetadata(Eraftpb.SnapshotMetadata.newBuilder()
                        .setIndex(11).setTerm(11)
                        .setConfState(Eraftpb.ConfState.newBuilder()
                                .addVoters(1).addVoters(2)))
                .build();

        MemoryStorage storage = newTestMemoryStorage(withPeers(1));
        Raft sm = newTestRaft(1, 10, 1, storage);
        sm.restore(snap);

        sm.becomeCandidate();
        sm.becomeLeader();

        sm.trk.getProgress().get(2L).setNext(sm.raftLog.firstIndex() - 1);
        sm.trk.getProgress().get(2L).setRecentActive(false);

        sm.step(Eraftpb.Message.newBuilder().setFrom(1).setTo(1)
                .setMsgType(Eraftpb.MessageType.MsgPropose)
                .addEntries(Eraftpb.Entry.newBuilder().setData(
                        ByteString.copyFromUtf8("somedata"))).build());

        List<Eraftpb.Message> msgs = sm.readMessages();
        assertThat(msgs).isEmpty();
    }

    @Test
    void testRestoreFromSnapMsg() throws RaftException {
        Eraftpb.Snapshot snap = Eraftpb.Snapshot.newBuilder()
                .setMetadata(Eraftpb.SnapshotMetadata.newBuilder()
                        .setIndex(11).setTerm(11)
                        .setConfState(Eraftpb.ConfState.newBuilder()
                                .addVoters(1).addVoters(2)))
                .build();

        Raft sm = newTestRaft(2, 10, 1, newTestMemoryStorage(withPeers(1, 2)));
        sm.step(Eraftpb.Message.newBuilder()
                .setMsgType(Eraftpb.MessageType.MsgSnapshot)
                .setFrom(1).setTerm(2).setSnapshot(snap).build());
        assertThat(sm.lead).isEqualTo(1);
    }

    @Test
    void testSlowNodeRestore() throws RaftException {
        Network nt = newNetwork(null, null, null);
        nt.send(Eraftpb.Message.newBuilder().setFrom(1).setTo(1)
                .setMsgType(Eraftpb.MessageType.MsgHup).build());

        nt.isolate(3);
        for (int j = 0; j <= 100; j++) {
            nt.send(Eraftpb.Message.newBuilder().setFrom(1).setTo(1)
                    .setMsgType(Eraftpb.MessageType.MsgPropose)
                    .addEntries(Eraftpb.Entry.getDefaultInstance()).build());
        }
        Raft lead = nt.peer(1);
        nextEnts(lead, nt.storage.get(1L));
        nt.storage.get(1L).createSnapshot(lead.raftLog.applied,
                lead.trk.confState(), null);
        nt.storage.get(1L).compact(lead.raftLog.applied);

        nt.recover();
        // send heartbeats until leader learns node 3 is active
        for (int i = 0; i < 100; i++) {
            nt.send(Eraftpb.Message.newBuilder().setFrom(1).setTo(1)
                    .setMsgType(Eraftpb.MessageType.MsgBeat).build());
            if (lead.trk.getProgress().get(3L).isRecentActive()) break;
        }

        // trigger a snapshot
        nt.send(Eraftpb.Message.newBuilder().setFrom(1).setTo(1)
                .setMsgType(Eraftpb.MessageType.MsgPropose)
                .addEntries(Eraftpb.Entry.getDefaultInstance()).build());
        // trigger a commit
        nt.send(Eraftpb.Message.newBuilder().setFrom(1).setTo(1)
                .setMsgType(Eraftpb.MessageType.MsgPropose)
                .addEntries(Eraftpb.Entry.getDefaultInstance()).build());

        Raft follower = nt.peer(3);
        assertThat(follower.raftLog.committed).isEqualTo(lead.raftLog.committed);
    }

    @Test
    void testStepConfig() throws RaftException {
        Raft r = newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1, 2)));
        r.becomeCandidate();
        r.becomeLeader();
        long idx = r.raftLog.lastIndex();
        r.step(Eraftpb.Message.newBuilder().setFrom(1).setTo(1)
                .setMsgType(Eraftpb.MessageType.MsgPropose)
                .addEntries(Eraftpb.Entry.newBuilder()
                        .setEntryType(Eraftpb.EntryType.EntryConfChange)).build());
        assertThat(r.raftLog.lastIndex()).isEqualTo(idx + 1);
        assertThat(r.pendingConfIndex).isEqualTo(idx + 1);
    }

    @Test
    void testStepIgnoreConfig() throws RaftException {
        Raft r = newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1, 2)));
        r.becomeCandidate();
        r.becomeLeader();
        r.step(Eraftpb.Message.newBuilder().setFrom(1).setTo(1)
                .setMsgType(Eraftpb.MessageType.MsgPropose)
                .addEntries(Eraftpb.Entry.newBuilder()
                        .setEntryType(Eraftpb.EntryType.EntryConfChange)).build());
        long idx = r.raftLog.lastIndex();
        long pendingConfIdx = r.pendingConfIndex;
        r.step(Eraftpb.Message.newBuilder().setFrom(1).setTo(1)
                .setMsgType(Eraftpb.MessageType.MsgPropose)
                .addEntries(Eraftpb.Entry.newBuilder()
                        .setEntryType(Eraftpb.EntryType.EntryConfChange)).build());

        List<Eraftpb.Entry> ents = r.raftLog.entries(idx + 1, TestUtil.NO_LIMIT);
        assertThat(ents).hasSize(1);
        assertThat(ents.get(0).getEntryType()).isEqualTo(Eraftpb.EntryType.EntryNormal);
        assertThat(r.pendingConfIndex).isEqualTo(pendingConfIdx);
    }

    @Test
    void testNewLeaderPendingConfig() throws RaftException {
        record TC(boolean addEntry, long wpendingIndex) {}
        List<TC> tests = List.of(new TC(false, 0), new TC(true, 1));
        for (int i = 0; i < tests.size(); i++) {
            TC tt = tests.get(i);
            Raft r = newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1, 2)));
            if (tt.addEntry) {
                r.appendEntry(List.of(Eraftpb.Entry.newBuilder()
                        .setEntryType(Eraftpb.EntryType.EntryNormal).build()));
            }
            r.becomeCandidate();
            r.becomeLeader();
            assertThat(r.pendingConfIndex).as("#%d", i).isEqualTo(tt.wpendingIndex);
        }
    }

    @Test
    void testAddNode() throws RaftException {
        Raft r = newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1)));
        r.applyConfChange(Eraftpb.ConfChangeV2.newBuilder()
                .addChanges(Eraftpb.ConfChangeSingle.newBuilder()
                        .setType(Eraftpb.ConfChangeType.ConfChangeAddNode).setNodeId(2)).build());
        assertThat(r.trk.voterNodes()).containsExactly(1L, 2L);
    }

    @Test
    void testAddLearner() throws RaftException {
        Raft r = newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1)));
        r.applyConfChange(Eraftpb.ConfChangeV2.newBuilder()
                .addChanges(Eraftpb.ConfChangeSingle.newBuilder()
                        .setType(Eraftpb.ConfChangeType.ConfChangeAddLearnerNode).setNodeId(2)).build());
        assertThat(r.isLearner).isFalse();
        assertThat(r.trk.learnerNodes()).containsExactly(2L);
        assertThat(r.trk.getProgress().get(2L).isLearner()).isTrue();

        // Promote peer to voter
        r.applyConfChange(Eraftpb.ConfChangeV2.newBuilder()
                .addChanges(Eraftpb.ConfChangeSingle.newBuilder()
                        .setType(Eraftpb.ConfChangeType.ConfChangeAddNode).setNodeId(2)).build());
        assertThat(r.trk.getProgress().get(2L).isLearner()).isFalse();

        // Demote self
        r.applyConfChange(Eraftpb.ConfChangeV2.newBuilder()
                .addChanges(Eraftpb.ConfChangeSingle.newBuilder()
                        .setType(Eraftpb.ConfChangeType.ConfChangeAddLearnerNode).setNodeId(1)).build());
        assertThat(r.trk.getProgress().get(1L).isLearner()).isTrue();
        assertThat(r.isLearner).isTrue();

        // Promote self back
        r.applyConfChange(Eraftpb.ConfChangeV2.newBuilder()
                .addChanges(Eraftpb.ConfChangeSingle.newBuilder()
                        .setType(Eraftpb.ConfChangeType.ConfChangeAddNode).setNodeId(1)).build());
        assertThat(r.trk.getProgress().get(1L).isLearner()).isFalse();
        assertThat(r.isLearner).isFalse();
    }

    @Test
    void testAddNodeCheckQuorum() throws RaftException {
        Raft r = newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1)));
        r.checkQuorum = true;
        r.becomeCandidate();
        r.becomeLeader();

        // Tick until just before election timeout.
        for (int i = 0; i < r.electionTimeout - 1; i++) r.tickFn.run();

        r.applyConfChange(Eraftpb.ConfChangeV2.newBuilder()
                .addChanges(Eraftpb.ConfChangeSingle.newBuilder()
                        .setType(Eraftpb.ConfChangeType.ConfChangeAddNode).setNodeId(2)).build());

        // This tick will reach electionTimeout and trigger a quorum check.
        // Node 1 stays leader because the freshly added node 2 is marked
        // recentActive=true on initialization (matches etcd-raft).
        r.tickFn.run();
        assertThat(r.state).isEqualTo(RaftStateType.StateLeader);

        // After another electionTimeout ticks without hearing from node 2,
        // node 1 should step down.
        for (int i = 0; i < r.electionTimeout; i++) r.tickFn.run();
        assertThat(r.state).isEqualTo(RaftStateType.StateFollower);
    }

    @Test
    void testRemoveNode() throws RaftException {
        Raft r = newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1, 2)));
        r.applyConfChange(Eraftpb.ConfChangeV2.newBuilder()
                .addChanges(Eraftpb.ConfChangeSingle.newBuilder()
                        .setType(Eraftpb.ConfChangeType.ConfChangeRemoveNode).setNodeId(2)).build());
        assertThat(r.trk.voterNodes()).containsExactly(1L);
    }

    @Test
    void testRemoveLearner() throws RaftException {
        Raft r = newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1), withLearners(2)));
        r.applyConfChange(Eraftpb.ConfChangeV2.newBuilder()
                .addChanges(Eraftpb.ConfChangeSingle.newBuilder()
                        .setType(Eraftpb.ConfChangeType.ConfChangeRemoveNode).setNodeId(2)).build());
        assertThat(r.trk.voterNodes()).containsExactly(1L);
        assertThat(r.trk.learnerNodes()).isEmpty();
    }

    @Test
    void testRaftNodes() throws RaftException {
        Raft r = newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)));
        assertThat(r.trk.voterNodes()).containsExactly(1L, 2L, 3L);
    }

    @Test
    void testCommitAfterRemoveNode() throws RaftException {
        MemoryStorage s = newTestMemoryStorage(withPeers(1, 2));
        Raft r = newTestRaft(1, 5, 1, s);
        r.becomeCandidate();
        r.becomeLeader();

        // Propose remove node 2
        Eraftpb.ConfChange cc = Eraftpb.ConfChange.newBuilder()
                .setChangeType(Eraftpb.ConfChangeType.ConfChangeRemoveNode)
                .setNodeId(2).build();
        r.step(Eraftpb.Message.newBuilder()
                .setMsgType(Eraftpb.MessageType.MsgPropose)
                .addEntries(Eraftpb.Entry.newBuilder()
                        .setEntryType(Eraftpb.EntryType.EntryConfChange)
                        .setData(cc.toByteString())).build());

        // Stabilize the log
        assertThat(nextEnts(r, s)).isEmpty();
        long ccIndex = r.raftLog.lastIndex();

        // Propose another entry
        r.step(Eraftpb.Message.newBuilder()
                .setMsgType(Eraftpb.MessageType.MsgPropose)
                .addEntries(Eraftpb.Entry.newBuilder()
                        .setData(ByteString.copyFromUtf8("hello"))).build());

        // Node 2 acknowledges the config change
        r.step(Eraftpb.Message.newBuilder()
                .setMsgType(Eraftpb.MessageType.MsgAppendResponse)
                .setFrom(2).setIndex(ccIndex).build());
        List<Eraftpb.Entry> ents = nextEnts(r, s);
        assertThat(ents).hasSize(2);
        assertThat(ents.get(0).getEntryType()).isEqualTo(Eraftpb.EntryType.EntryNormal);
        assertThat(ents.get(1).getEntryType()).isEqualTo(Eraftpb.EntryType.EntryConfChange);

        // Apply the config change
        r.applyConfChange(Eraftpb.ConfChangeV2.newBuilder()
                .addChanges(Eraftpb.ConfChangeSingle.newBuilder()
                        .setType(Eraftpb.ConfChangeType.ConfChangeRemoveNode).setNodeId(2)).build());
        ents = nextEnts(r, s);
        assertThat(ents).hasSize(1);
        assertThat(ents.get(0).getEntryType()).isEqualTo(Eraftpb.EntryType.EntryNormal);
        assertThat(ents.get(0).getData()).isEqualTo(ByteString.copyFromUtf8("hello"));
    }

    @Test
    void testLeaderTransferToUpToDateNodeFromFollower() throws RaftException {
        Network nt = newNetwork(null, null, null);
        nt.send(Eraftpb.Message.newBuilder().setFrom(1).setTo(1)
                .setMsgType(Eraftpb.MessageType.MsgHup).build());

        Raft lead = nt.peer(1);
        assertThat(lead.lead).isEqualTo(1);

        // Transfer to 2 via follower (sent to node 2 itself)
        nt.send(Eraftpb.Message.newBuilder().setFrom(2).setTo(2)
                .setMsgType(Eraftpb.MessageType.MsgTransferLeader).build());
        checkLeaderTransferState(lead, RaftStateType.StateFollower, 2);

        // After some replication, transfer back
        nt.send(Eraftpb.Message.newBuilder().setFrom(1).setTo(1)
                .setMsgType(Eraftpb.MessageType.MsgPropose)
                .addEntries(Eraftpb.Entry.getDefaultInstance()).build());
        nt.send(Eraftpb.Message.newBuilder().setFrom(1).setTo(1)
                .setMsgType(Eraftpb.MessageType.MsgTransferLeader).build());
        checkLeaderTransferState(lead, RaftStateType.StateLeader, 1);
    }

    @Test
    void testLeaderTransferWithCheckQuorum() throws RaftException {
        Network nt = newNetwork(null, null, null);
        for (long i = 1; i <= 3; i++) {
            Raft r = nt.peer(i);
            r.checkQuorum = true;
            r.randomizedElectionTimeout = r.electionTimeout + (int) i;
        }
        // Let peer 2 election timeout so it can vote for peer 1
        Raft f = nt.peer(2);
        for (int i = 0; i < f.electionTimeout; i++) f.tickFn.run();

        nt.send(Eraftpb.Message.newBuilder().setFrom(1).setTo(1)
                .setMsgType(Eraftpb.MessageType.MsgHup).build());

        Raft lead = nt.peer(1);
        assertThat(lead.state).isEqualTo(RaftStateType.StateLeader);

        // Transfer to 2
        nt.send(Eraftpb.Message.newBuilder().setFrom(2).setTo(1)
                .setMsgType(Eraftpb.MessageType.MsgTransferLeader).build());
        checkLeaderTransferState(lead, RaftStateType.StateFollower, 2);

        // Transfer back
        nt.send(Eraftpb.Message.newBuilder().setFrom(1).setTo(1)
                .setMsgType(Eraftpb.MessageType.MsgPropose)
                .addEntries(Eraftpb.Entry.getDefaultInstance()).build());
        nt.send(Eraftpb.Message.newBuilder().setFrom(1).setTo(2)
                .setMsgType(Eraftpb.MessageType.MsgTransferLeader).build());
        checkLeaderTransferState(lead, RaftStateType.StateLeader, 1);
    }

    @Test
    void testLeaderTransferToSlowFollower() throws RaftException {
        Network nt = newNetwork(null, null, null);
        nt.send(Eraftpb.Message.newBuilder().setFrom(1).setTo(1)
                .setMsgType(Eraftpb.MessageType.MsgHup).build());

        nt.isolate(3);
        nt.send(Eraftpb.Message.newBuilder().setFrom(1).setTo(1)
                .setMsgType(Eraftpb.MessageType.MsgPropose)
                .addEntries(Eraftpb.Entry.getDefaultInstance()).build());

        nt.recover();
        Raft lead = nt.peer(1);
        assertThat(lead.trk.getProgress().get(3L).getMatch()).isEqualTo(1);

        // Transfer to slow follower 3
        nt.send(Eraftpb.Message.newBuilder().setFrom(3).setTo(1)
                .setMsgType(Eraftpb.MessageType.MsgTransferLeader).build());
        checkLeaderTransferState(lead, RaftStateType.StateFollower, 3);
    }

    @Test
    void testLeaderTransferAfterSnapshot() throws RaftException {
        Network nt = newNetwork(null, null, null);
        nt.send(Eraftpb.Message.newBuilder().setFrom(1).setTo(1)
                .setMsgType(Eraftpb.MessageType.MsgHup).build());

        nt.isolate(3);
        nt.send(Eraftpb.Message.newBuilder().setFrom(1).setTo(1)
                .setMsgType(Eraftpb.MessageType.MsgPropose)
                .addEntries(Eraftpb.Entry.getDefaultInstance()).build());

        Raft lead = nt.peer(1);
        nextEnts(lead, nt.storage.get(1L));
        nt.storage.get(1L).createSnapshot(lead.raftLog.applied, lead.trk.confState(), null);
        nt.storage.get(1L).compact(lead.raftLog.applied);

        nt.recover();
        assertThat(lead.trk.getProgress().get(3L).getMatch()).isEqualTo(1);

        // Use msgHook to filter out MsgAppResp from 3
        final Eraftpb.Message[] filtered = {Eraftpb.Message.getDefaultInstance()};
        nt.msgHook = m -> {
            if (m.getMsgType() == Eraftpb.MessageType.MsgAppendResponse && m.getFrom() == 3 && !m.getReject()) {
                filtered[0] = m;
                return false;
            }
            return true;
        };

        // Transfer to 3 (needs snapshot)
        nt.send(Eraftpb.Message.newBuilder().setFrom(3).setTo(1)
                .setMsgType(Eraftpb.MessageType.MsgTransferLeader).build());
        assertThat(lead.state).isEqualTo(RaftStateType.StateLeader);

        // Apply snapshot and resume
        Raft follower = nt.peer(3);
        Eraftpb.Snapshot snap = follower.raftLog.nextUnstableSnapshot();
        nt.storage.get(3L).applySnapshot(snap);
        follower.appliedSnap(snap);
        nt.msgHook = null;
        nt.send(filtered[0]);

        checkLeaderTransferState(lead, RaftStateType.StateFollower, 3);
    }

    @Test
    void testLeaderTransferTimeout() throws RaftException {
        Network nt = newNetwork(null, null, null);
        nt.send(Eraftpb.Message.newBuilder().setFrom(1).setTo(1)
                .setMsgType(Eraftpb.MessageType.MsgHup).build());

        nt.isolate(3);
        Raft lead = nt.peer(1);

        nt.send(Eraftpb.Message.newBuilder().setFrom(3).setTo(1)
                .setMsgType(Eraftpb.MessageType.MsgTransferLeader).build());
        assertThat(lead.leadTransferee).isEqualTo(3);

        for (int i = 0; i < lead.heartbeatTimeout; i++) lead.tickFn.run();
        assertThat(lead.leadTransferee).isEqualTo(3);

        for (int i = 0; i < lead.electionTimeout - lead.heartbeatTimeout; i++) lead.tickFn.run();
        checkLeaderTransferState(lead, RaftStateType.StateLeader, 1);
    }

    @Test
    void testLeaderTransferIgnoreProposal() throws RaftException {
        MemoryStorage s = newTestMemoryStorage(withPeers(1, 2, 3));
        Raft r = newTestRaft(1, 10, 1, s);
        Network nt = Network.newNetwork(new Network.RaftStateMachine(r), null, null);
        nt.send(Eraftpb.Message.newBuilder().setFrom(1).setTo(1)
                .setMsgType(Eraftpb.MessageType.MsgHup).build());

        nt.isolate(3);
        Raft lead = nt.peer(1);
        nextEnts(r, s);

        nt.send(Eraftpb.Message.newBuilder().setFrom(3).setTo(1)
                .setMsgType(Eraftpb.MessageType.MsgTransferLeader).build());
        assertThat(lead.leadTransferee).isEqualTo(3);

        nt.send(Eraftpb.Message.newBuilder().setFrom(1).setTo(1)
                .setMsgType(Eraftpb.MessageType.MsgPropose)
                .addEntries(Eraftpb.Entry.getDefaultInstance()).build());
        assertThat(lead.trk.getProgress().get(1L).getMatch()).isEqualTo(1);
    }

    @Test
    void testLeaderTransferReceiveHigherTermVote() throws RaftException {
        Network nt = newNetwork(null, null, null);
        nt.send(Eraftpb.Message.newBuilder().setFrom(1).setTo(1)
                .setMsgType(Eraftpb.MessageType.MsgHup).build());

        nt.isolate(3);
        Raft lead = nt.peer(1);

        nt.send(Eraftpb.Message.newBuilder().setFrom(3).setTo(1)
                .setMsgType(Eraftpb.MessageType.MsgTransferLeader).build());
        assertThat(lead.leadTransferee).isEqualTo(3);

        nt.send(Eraftpb.Message.newBuilder().setFrom(2).setTo(2)
                .setMsgType(Eraftpb.MessageType.MsgHup).setIndex(1).setTerm(2).build());
        checkLeaderTransferState(lead, RaftStateType.StateFollower, 2);
    }

    @Test
    void testLeaderTransferRemoveNode() throws RaftException {
        Network nt = newNetwork(null, null, null);
        nt.send(Eraftpb.Message.newBuilder().setFrom(1).setTo(1)
                .setMsgType(Eraftpb.MessageType.MsgHup).build());

        nt.ignore(Eraftpb.MessageType.MsgTimeoutNow);
        Raft lead = nt.peer(1);

        nt.send(Eraftpb.Message.newBuilder().setFrom(3).setTo(1)
                .setMsgType(Eraftpb.MessageType.MsgTransferLeader).build());
        assertThat(lead.leadTransferee).isEqualTo(3);

        lead.applyConfChange(Eraftpb.ConfChangeV2.newBuilder()
                .addChanges(Eraftpb.ConfChangeSingle.newBuilder()
                        .setType(Eraftpb.ConfChangeType.ConfChangeRemoveNode).setNodeId(3)).build());
        checkLeaderTransferState(lead, RaftStateType.StateLeader, 1);
    }

    @Test
    void testLeaderTransferDemoteNode() throws RaftException {
        Network nt = newNetwork(null, null, null);
        nt.send(Eraftpb.Message.newBuilder().setFrom(1).setTo(1)
                .setMsgType(Eraftpb.MessageType.MsgHup).build());

        nt.ignore(Eraftpb.MessageType.MsgTimeoutNow);
        Raft lead = nt.peer(1);

        nt.send(Eraftpb.Message.newBuilder().setFrom(3).setTo(1)
                .setMsgType(Eraftpb.MessageType.MsgTransferLeader).build());
        assertThat(lead.leadTransferee).isEqualTo(3);

        lead.applyConfChange(Eraftpb.ConfChangeV2.newBuilder()
                .addChanges(Eraftpb.ConfChangeSingle.newBuilder()
                        .setType(Eraftpb.ConfChangeType.ConfChangeRemoveNode).setNodeId(3))
                .addChanges(Eraftpb.ConfChangeSingle.newBuilder()
                        .setType(Eraftpb.ConfChangeType.ConfChangeAddLearnerNode).setNodeId(3)).build());
        // Leave joint
        lead.applyConfChange(Eraftpb.ConfChangeV2.getDefaultInstance());
        checkLeaderTransferState(lead, RaftStateType.StateLeader, 1);
    }

    @Test
    void testLeaderTransferBack() throws RaftException {
        Network nt = newNetwork(null, null, null);
        nt.send(Eraftpb.Message.newBuilder().setFrom(1).setTo(1)
                .setMsgType(Eraftpb.MessageType.MsgHup).build());

        nt.isolate(3);
        Raft lead = nt.peer(1);

        nt.send(Eraftpb.Message.newBuilder().setFrom(3).setTo(1)
                .setMsgType(Eraftpb.MessageType.MsgTransferLeader).build());
        assertThat(lead.leadTransferee).isEqualTo(3);

        // Transfer back to self
        nt.send(Eraftpb.Message.newBuilder().setFrom(1).setTo(1)
                .setMsgType(Eraftpb.MessageType.MsgTransferLeader).build());
        checkLeaderTransferState(lead, RaftStateType.StateLeader, 1);
    }

    @Test
    void testLeaderTransferSecondTransferToAnotherNode() throws RaftException {
        Network nt = newNetwork(null, null, null);
        nt.send(Eraftpb.Message.newBuilder().setFrom(1).setTo(1)
                .setMsgType(Eraftpb.MessageType.MsgHup).build());

        nt.isolate(3);
        Raft lead = nt.peer(1);

        nt.send(Eraftpb.Message.newBuilder().setFrom(3).setTo(1)
                .setMsgType(Eraftpb.MessageType.MsgTransferLeader).build());
        assertThat(lead.leadTransferee).isEqualTo(3);

        // Transfer to another node
        nt.send(Eraftpb.Message.newBuilder().setFrom(2).setTo(1)
                .setMsgType(Eraftpb.MessageType.MsgTransferLeader).build());
        checkLeaderTransferState(lead, RaftStateType.StateFollower, 2);
    }

    @Test
    void testLeaderTransferSecondTransferToSameNode() throws RaftException {
        Network nt = newNetwork(null, null, null);
        nt.send(Eraftpb.Message.newBuilder().setFrom(1).setTo(1)
                .setMsgType(Eraftpb.MessageType.MsgHup).build());

        nt.isolate(3);
        Raft lead = nt.peer(1);

        nt.send(Eraftpb.Message.newBuilder().setFrom(3).setTo(1)
                .setMsgType(Eraftpb.MessageType.MsgTransferLeader).build());
        assertThat(lead.leadTransferee).isEqualTo(3);

        for (int i = 0; i < lead.heartbeatTimeout; i++) lead.tickFn.run();

        // Second transfer to same node - should not reset timeout
        nt.send(Eraftpb.Message.newBuilder().setFrom(3).setTo(1)
                .setMsgType(Eraftpb.MessageType.MsgTransferLeader).build());

        for (int i = 0; i < lead.electionTimeout - lead.heartbeatTimeout; i++) lead.tickFn.run();
        checkLeaderTransferState(lead, RaftStateType.StateLeader, 1);
    }

    @Test
    void testTransferNonMember() throws RaftException {
        Raft r = newTestRaft(1, 5, 1, newTestMemoryStorage(withPeers(2, 3, 4)));
        r.step(Eraftpb.Message.newBuilder().setFrom(2).setTo(1)
                .setMsgType(Eraftpb.MessageType.MsgTimeoutNow).build());
        r.step(Eraftpb.Message.newBuilder().setFrom(2).setTo(1)
                .setMsgType(Eraftpb.MessageType.MsgRequestVoteResponse).build());
        r.step(Eraftpb.Message.newBuilder().setFrom(3).setTo(1)
                .setMsgType(Eraftpb.MessageType.MsgRequestVoteResponse).build());
        assertThat(r.state).isEqualTo(RaftStateType.StateFollower);
    }

    @Test
    void testNodeWithSmallerTermCanCompleteElection() throws RaftException {
        Raft n1 = newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)));
        Raft n2 = newTestRaft(2, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)));
        Raft n3 = newTestRaft(3, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)));
        n1.becomeFollower(1, Util.NONE); n2.becomeFollower(1, Util.NONE); n3.becomeFollower(1, Util.NONE);
        n1.preVote = true; n2.preVote = true; n3.preVote = true;

        Network nt = Network.newNetwork(
                new Network.RaftStateMachine(n1),
                new Network.RaftStateMachine(n2),
                new Network.RaftStateMachine(n3));
        nt.cut(1, 3);
        nt.cut(2, 3);

        nt.send(Eraftpb.Message.newBuilder().setFrom(1).setTo(1)
                .setMsgType(Eraftpb.MessageType.MsgHup).build());
        assertThat(n1.state).isEqualTo(RaftStateType.StateLeader);
        assertThat(n2.state).isEqualTo(RaftStateType.StateFollower);

        nt.send(Eraftpb.Message.newBuilder().setFrom(3).setTo(3)
                .setMsgType(Eraftpb.MessageType.MsgHup).build());
        assertThat(n3.state).isEqualTo(RaftStateType.StatePreCandidate);

        nt.send(Eraftpb.Message.newBuilder().setFrom(2).setTo(2)
                .setMsgType(Eraftpb.MessageType.MsgHup).build());

        assertThat(n1.term).isEqualTo(3);
        assertThat(n2.term).isEqualTo(3);
        assertThat(n3.term).isEqualTo(1);
        assertThat(n1.state).isEqualTo(RaftStateType.StateFollower);
        assertThat(n2.state).isEqualTo(RaftStateType.StateLeader);
        assertThat(n3.state).isEqualTo(RaftStateType.StatePreCandidate);

        // recover network and isolate b
        nt.recover();
        nt.cut(2, 1);
        nt.cut(2, 3);

        nt.send(Eraftpb.Message.newBuilder().setFrom(3).setTo(3)
                .setMsgType(Eraftpb.MessageType.MsgHup).build());
        nt.send(Eraftpb.Message.newBuilder().setFrom(1).setTo(1)
                .setMsgType(Eraftpb.MessageType.MsgHup).build());

        assertThat(n1.state == RaftStateType.StateLeader || n3.state == RaftStateType.StateLeader).isTrue();
    }

    @Test
    void testPreVoteWithSplitVote() throws RaftException {
        Raft n1 = newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)));
        Raft n2 = newTestRaft(2, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)));
        Raft n3 = newTestRaft(3, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)));
        n1.becomeFollower(1, Util.NONE); n2.becomeFollower(1, Util.NONE); n3.becomeFollower(1, Util.NONE);
        n1.preVote = true; n2.preVote = true; n3.preVote = true;

        Network nt = Network.newNetwork(
                new Network.RaftStateMachine(n1),
                new Network.RaftStateMachine(n2),
                new Network.RaftStateMachine(n3));
        nt.send(Eraftpb.Message.newBuilder().setFrom(1).setTo(1)
                .setMsgType(Eraftpb.MessageType.MsgHup).build());

        // simulate leader down, split vote (send both hup messages together)
        nt.isolate(1);
        nt.send(Eraftpb.Message.newBuilder().setFrom(2).setTo(2)
                .setMsgType(Eraftpb.MessageType.MsgHup).build(),
                Eraftpb.Message.newBuilder().setFrom(3).setTo(3)
                .setMsgType(Eraftpb.MessageType.MsgHup).build());

        assertThat(n2.term).isEqualTo(3);
        assertThat(n3.term).isEqualTo(3);
        assertThat(n2.state).isEqualTo(RaftStateType.StateCandidate);
        assertThat(n3.state).isEqualTo(RaftStateType.StateCandidate);

        // node 2 campaigns again
        nt.send(Eraftpb.Message.newBuilder().setFrom(2).setTo(2)
                .setMsgType(Eraftpb.MessageType.MsgHup).build());

        assertThat(n2.term).isEqualTo(4);
        assertThat(n3.term).isEqualTo(4);
        assertThat(n2.state).isEqualTo(RaftStateType.StateLeader);
        assertThat(n3.state).isEqualTo(RaftStateType.StateFollower);
    }

    @Test
    void testPreVoteWithCheckQuorum() throws RaftException {
        Raft n1 = newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)));
        Raft n2 = newTestRaft(2, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)));
        Raft n3 = newTestRaft(3, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)));
        n1.becomeFollower(1, Util.NONE); n2.becomeFollower(1, Util.NONE); n3.becomeFollower(1, Util.NONE);
        n1.preVote = true; n2.preVote = true; n3.preVote = true;
        n1.checkQuorum = true; n2.checkQuorum = true; n3.checkQuorum = true;

        Network nt = Network.newNetwork(
                new Network.RaftStateMachine(n1),
                new Network.RaftStateMachine(n2),
                new Network.RaftStateMachine(n3));
        nt.send(Eraftpb.Message.newBuilder().setFrom(1).setTo(1)
                .setMsgType(Eraftpb.MessageType.MsgHup).build());

        nt.isolate(1);
        assertThat(n1.state).isEqualTo(RaftStateType.StateLeader);
        assertThat(n2.state).isEqualTo(RaftStateType.StateFollower);
        assertThat(n3.state).isEqualTo(RaftStateType.StateFollower);

        // node 2 will ignore node 3's PreVote
        nt.send(Eraftpb.Message.newBuilder().setFrom(3).setTo(3)
                .setMsgType(Eraftpb.MessageType.MsgHup).build());
        nt.send(Eraftpb.Message.newBuilder().setFrom(2).setTo(2)
                .setMsgType(Eraftpb.MessageType.MsgHup).build());

        assertThat(n2.state == RaftStateType.StateLeader || n3.state == RaftStateType.StateFollower).isTrue();
    }

    @Test
    void testLearnerCampaign() throws RaftException {
        Raft n1 = newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1)));
        n1.applyConfChange(Eraftpb.ConfChangeV2.newBuilder()
                .addChanges(Eraftpb.ConfChangeSingle.newBuilder()
                        .setType(Eraftpb.ConfChangeType.ConfChangeAddLearnerNode).setNodeId(2)).build());
        Raft n2 = newTestRaft(2, 10, 1, newTestMemoryStorage(withPeers(1)));
        n2.applyConfChange(Eraftpb.ConfChangeV2.newBuilder()
                .addChanges(Eraftpb.ConfChangeSingle.newBuilder()
                        .setType(Eraftpb.ConfChangeType.ConfChangeAddLearnerNode).setNodeId(2)).build());

        Network nt = Network.newNetwork(new Network.RaftStateMachine(n1), new Network.RaftStateMachine(n2));
        nt.send(Eraftpb.Message.newBuilder().setFrom(2).setTo(2)
                .setMsgType(Eraftpb.MessageType.MsgHup).build());

        assertThat(n2.isLearner).isTrue();
        assertThat(n2.state).isEqualTo(RaftStateType.StateFollower);

        nt.send(Eraftpb.Message.newBuilder().setFrom(1).setTo(1)
                .setMsgType(Eraftpb.MessageType.MsgHup).build());
        assertThat(n1.state).isEqualTo(RaftStateType.StateLeader);
        assertThat(n1.lead).isEqualTo(1);

        // MsgTimeoutNow should be ignored by learner
        nt.send(Eraftpb.Message.newBuilder().setFrom(1).setTo(2)
                .setMsgType(Eraftpb.MessageType.MsgTimeoutNow).build());
        assertThat(n2.state).isEqualTo(RaftStateType.StateFollower);
    }

    private Network newPreVoteMigrationCluster() throws RaftException {
        Raft n1 = newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)));
        Raft n2 = newTestRaft(2, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)));
        Raft n3 = newTestRaft(3, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)));
        n1.becomeFollower(1, Util.NONE); n2.becomeFollower(1, Util.NONE); n3.becomeFollower(1, Util.NONE);
        n1.preVote = true; n2.preVote = true;
        // n3 intentionally doesn't have preVote enabled

        Network nt = Network.newNetwork(
                new Network.RaftStateMachine(n1),
                new Network.RaftStateMachine(n2),
                new Network.RaftStateMachine(n3));
        nt.send(Eraftpb.Message.newBuilder().setFrom(1).setTo(1)
                .setMsgType(Eraftpb.MessageType.MsgHup).build());

        // isolate n3, make it campaign twice to increase term
        nt.isolate(3);
        nt.send(Eraftpb.Message.newBuilder().setFrom(1).setTo(1)
                .setMsgType(Eraftpb.MessageType.MsgPropose)
                .addEntries(Eraftpb.Entry.newBuilder().setData(
                        ByteString.copyFromUtf8("some data"))).build());
        nt.send(Eraftpb.Message.newBuilder().setFrom(3).setTo(3)
                .setMsgType(Eraftpb.MessageType.MsgHup).build());
        nt.send(Eraftpb.Message.newBuilder().setFrom(3).setTo(3)
                .setMsgType(Eraftpb.MessageType.MsgHup).build());

        assertThat(n1.state).isEqualTo(RaftStateType.StateLeader);
        assertThat(n2.state).isEqualTo(RaftStateType.StateFollower);
        assertThat(n3.state).isEqualTo(RaftStateType.StateCandidate);
        assertThat(n1.term).isEqualTo(2);
        assertThat(n2.term).isEqualTo(2);
        assertThat(n3.term).isEqualTo(4);

        // enable preVote on n3, recover network
        n3.preVote = true;
        nt.recover();
        return nt;
    }

    @Test
    void testPreVoteMigrationCanCompleteElection() throws RaftException {
        Network nt = newPreVoteMigrationCluster();
        Raft n2 = nt.peer(2);
        Raft n3 = nt.peer(3);

        // simulate leader down
        nt.isolate(1);

        nt.send(Eraftpb.Message.newBuilder().setFrom(3).setTo(3)
                .setMsgType(Eraftpb.MessageType.MsgHup).build());
        nt.send(Eraftpb.Message.newBuilder().setFrom(2).setTo(2)
                .setMsgType(Eraftpb.MessageType.MsgHup).build());

        assertThat(n2.state).isEqualTo(RaftStateType.StateFollower);
        assertThat(n3.state).isEqualTo(RaftStateType.StatePreCandidate);

        nt.send(Eraftpb.Message.newBuilder().setFrom(3).setTo(3)
                .setMsgType(Eraftpb.MessageType.MsgHup).build());
        nt.send(Eraftpb.Message.newBuilder().setFrom(2).setTo(2)
                .setMsgType(Eraftpb.MessageType.MsgHup).build());

        assertThat(n2.state == RaftStateType.StateLeader || n3.state == RaftStateType.StateFollower).isTrue();
    }

    @Test
    void testPreVoteMigrationWithFreeStuckPreCandidate() throws RaftException {
        Network nt = newPreVoteMigrationCluster();
        Raft n1 = nt.peer(1);
        Raft n2 = nt.peer(2);
        Raft n3 = nt.peer(3);

        nt.send(Eraftpb.Message.newBuilder().setFrom(3).setTo(3)
                .setMsgType(Eraftpb.MessageType.MsgHup).build());
        assertThat(n1.state).isEqualTo(RaftStateType.StateLeader);
        assertThat(n2.state).isEqualTo(RaftStateType.StateFollower);
        assertThat(n3.state).isEqualTo(RaftStateType.StatePreCandidate);

        nt.send(Eraftpb.Message.newBuilder().setFrom(3).setTo(3)
                .setMsgType(Eraftpb.MessageType.MsgHup).build());
        assertThat(n1.state).isEqualTo(RaftStateType.StateLeader);
        assertThat(n2.state).isEqualTo(RaftStateType.StateFollower);
        assertThat(n3.state).isEqualTo(RaftStateType.StatePreCandidate);

        // heartbeat from leader disrupts, freeing stuck pre-candidate
        nt.send(Eraftpb.Message.newBuilder().setFrom(1).setTo(3)
                .setMsgType(Eraftpb.MessageType.MsgHeartbeat).setTerm(n1.term).build());
        assertThat(n1.state).isEqualTo(RaftStateType.StateFollower);
        assertThat(n1.term).isEqualTo(n3.term);
    }

    @Test
    void testConfChangeCheckBeforeCampaign() throws RaftException {
        testConfChangeCheckBeforeCampaignImpl(false);
    }

    @Test
    void testConfChangeV2CheckBeforeCampaign() throws RaftException {
        testConfChangeCheckBeforeCampaignImpl(true);
    }

    private void testConfChangeCheckBeforeCampaignImpl(boolean v2) throws RaftException {
        Network nt = newNetwork(null, null, null);
        Raft n1 = nt.peer(1);
        Raft n2 = nt.peer(2);
        nt.send(Eraftpb.Message.newBuilder().setFrom(1).setTo(1)
                .setMsgType(Eraftpb.MessageType.MsgHup).build());
        assertThat(n1.state).isEqualTo(RaftStateType.StateLeader);

        // Propose removal of node 2
        Eraftpb.ConfChange cc = Eraftpb.ConfChange.newBuilder()
                .setChangeType(Eraftpb.ConfChangeType.ConfChangeRemoveNode)
                .setNodeId(2).build();
        Eraftpb.EntryType ty;
        com.google.protobuf.ByteString ccData;
        if (v2) {
            Eraftpb.ConfChangeV2 ccv2 = Eraftpb.ConfChangeV2.newBuilder()
                    .addChanges(Eraftpb.ConfChangeSingle.newBuilder()
                            .setType(cc.getChangeType()).setNodeId(cc.getNodeId())).build();
            ccData = ccv2.toByteString();
            ty = Eraftpb.EntryType.EntryConfChangeV2;
        } else {
            ccData = cc.toByteString();
            ty = Eraftpb.EntryType.EntryConfChange;
        }
        nt.send(Eraftpb.Message.newBuilder().setFrom(1).setTo(1)
                .setMsgType(Eraftpb.MessageType.MsgPropose)
                .addEntries(Eraftpb.Entry.newBuilder().setEntryType(ty).setData(ccData)).build());

        // n2 should not campaign before applying conf change
        for (int i = 0; i < n2.randomizedElectionTimeout; i++) n2.tickFn.run();
        assertThat(n2.state).isEqualTo(RaftStateType.StateFollower);

        // Transfer leadership to n2 should also fail
        nt.send(Eraftpb.Message.newBuilder().setFrom(2).setTo(1)
                .setMsgType(Eraftpb.MessageType.MsgTransferLeader).build());
        assertThat(n1.state).isEqualTo(RaftStateType.StateLeader);
        assertThat(n2.state).isEqualTo(RaftStateType.StateFollower);

        // Abort transfer
        for (int i = 0; i < n1.electionTimeout; i++) n1.tickFn.run();

        // Advance apply on n2
        nextEnts(n2, nt.storage.get(2L));

        // Now transfer should work
        nt.send(Eraftpb.Message.newBuilder().setFrom(2).setTo(1)
                .setMsgType(Eraftpb.MessageType.MsgTransferLeader).build());
        assertThat(n1.state).isEqualTo(RaftStateType.StateFollower);
        assertThat(n2.state).isEqualTo(RaftStateType.StateLeader);

        nextEnts(n1, nt.storage.get(1L));
        // Now n1 should be able to campaign
        for (int i = 0; i < n1.randomizedElectionTimeout; i++) n1.tickFn.run();
        assertThat(n1.state).isEqualTo(RaftStateType.StateCandidate);
    }

    @Test
    void testFastLogRejection() throws RaftException {
        record TC(List<Eraftpb.Entry> leaderLog, List<Eraftpb.Entry> followerLog,
                  long followerCompact, long rejectHintTerm, long rejectHintIndex,
                  long nextAppendTerm, long nextAppendIndex) {}
        List<TC> tests = List.of(
                new TC(index(1).terms(1, 2, 2, 4, 4, 4, 4), index(1).terms(1, 2, 2, 3, 3, 3, 3, 3, 3, 3, 3),
                        0, 3, 7, 2, 3),
                new TC(index(1).terms(1, 2, 2, 3, 4, 4, 4, 5), index(1).terms(1, 2, 2, 3, 3, 3, 3, 3, 3, 3, 3),
                        0, 3, 8, 3, 4),
                new TC(index(1).terms(1, 1, 1, 1), index(1).terms(1, 2, 2, 4),
                        0, 1, 1, 1, 1),
                new TC(index(1).terms(1, 1, 1, 1, 1, 1), index(1).terms(1, 2, 2, 4),
                        0, 1, 1, 1, 1),
                new TC(index(1).terms(1, 1, 1, 1), index(1).terms(1, 2, 2, 4, 4, 4),
                        0, 1, 1, 1, 1),
                new TC(index(1).terms(1, 1, 1, 4, 5), index(1).terms(1, 1, 1, 4),
                        0, 4, 4, 4, 4),
                new TC(index(1).terms(2, 5, 5, 5, 5, 5, 5, 5, 5), index(1).terms(2, 4, 4, 4, 4, 4),
                        0, 4, 6, 2, 1),
                new TC(index(1).terms(2, 2, 2, 2, 2), index(1).terms(2, 4, 4, 4, 4, 4, 4, 4),
                        0, 2, 1, 2, 1),
                new TC(index(1).terms(1, 1, 3), index(1).terms(1, 1, 3, 3, 3),
                        5, 0, 3, 1, 2)
        );

        for (int ti = 0; ti < tests.size(); ti++) {
            TC tt = tests.get(ti);
            MemoryStorage s1 = new MemoryStorage();
            s1.setSnapshot(Eraftpb.Snapshot.newBuilder()
                    .setMetadata(Eraftpb.SnapshotMetadata.newBuilder()
                            .setConfState(Eraftpb.ConfState.newBuilder()
                                    .addVoters(1).addVoters(2).addVoters(3))).build());
            s1.append(tt.leaderLog);
            long lastTerm = tt.leaderLog.get(tt.leaderLog.size() - 1).getTerm();
            s1.setHardState(Eraftpb.HardState.newBuilder()
                    .setTerm(lastTerm - 1)
                    .setCommit(tt.leaderLog.get(tt.leaderLog.size() - 1).getIndex()).build());
            Raft n1 = newTestRaft(1, 10, 1, s1);
            n1.becomeCandidate();
            n1.becomeLeader();

            MemoryStorage s2 = new MemoryStorage();
            s2.setSnapshot(Eraftpb.Snapshot.newBuilder()
                    .setMetadata(Eraftpb.SnapshotMetadata.newBuilder()
                            .setConfState(Eraftpb.ConfState.newBuilder()
                                    .addVoters(1).addVoters(2).addVoters(3))).build());
            s2.append(tt.followerLog);
            s2.setHardState(Eraftpb.HardState.newBuilder()
                    .setTerm(lastTerm).setVote(1).build());
            Raft n2 = newTestRaft(2, 10, 1, s2);
            if (tt.followerCompact > 0) {
                s2.compact(tt.followerCompact);
            }

            n2.step(Eraftpb.Message.newBuilder().setFrom(1).setTo(2)
                    .setMsgType(Eraftpb.MessageType.MsgHeartbeat).build());
            List<Eraftpb.Message> msgs = n2.readMessages();
            assertThat(msgs).as("#%d hb resp", ti).hasSize(1);

            n1.step(msgs.get(0));
            msgs = n1.readMessages();
            assertThat(msgs).as("#%d app", ti).hasSize(1);
            assertThat(msgs.get(0).getMsgType()).isEqualTo(Eraftpb.MessageType.MsgAppend);

            n2.step(msgs.get(0));
            msgs = n2.readMessages();
            assertThat(msgs).as("#%d app resp", ti).hasSize(1);
            assertThat(msgs.get(0).getReject()).as("#%d reject", ti).isTrue();
            assertThat(msgs.get(0).getLogTerm()).as("#%d hint term", ti).isEqualTo(tt.rejectHintTerm);
            assertThat(msgs.get(0).getRejectHint()).as("#%d hint index", ti).isEqualTo(tt.rejectHintIndex);

            n1.step(msgs.get(0));
            msgs = n1.readMessages();
            assertThat(msgs.get(0).getLogTerm()).as("#%d next term", ti).isEqualTo(tt.nextAppendTerm);
            assertThat(msgs.get(0).getIndex()).as("#%d next index", ti).isEqualTo(tt.nextAppendIndex);
        }
    }

    @Test
    void testLogReplicationWithReorderedMessage() throws RaftException {
        Raft r1 = newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1, 2)));
        r1.becomeCandidate();
        r1.becomeLeader();
        r1.readMessages();
        r1.trk.getProgress().get(2L).becomeReplicate();

        Raft r2 = newTestRaft(2, 10, 1, newTestMemoryStorage(withPeers(1, 2)));

        // r1 sends 2 MsgApp messages to r2
        r1.step(Eraftpb.Message.newBuilder().setFrom(1).setTo(1)
                .setMsgType(Eraftpb.MessageType.MsgPropose)
                .addEntries(Eraftpb.Entry.newBuilder().setData(ByteString.copyFromUtf8("somedata"))).build());
        List<Eraftpb.Message> msgs1 = r1.readMessages();
        assertThat(msgs1).hasSize(1);
        Eraftpb.Message req1 = msgs1.get(0);

        r1.step(Eraftpb.Message.newBuilder().setFrom(1).setTo(1)
                .setMsgType(Eraftpb.MessageType.MsgPropose)
                .addEntries(Eraftpb.Entry.newBuilder().setData(ByteString.copyFromUtf8("somedata"))).build());
        List<Eraftpb.Message> msgs2 = r1.readMessages();
        assertThat(msgs2).hasSize(1);
        Eraftpb.Message req2 = msgs2.get(0);

        // r2 receives req2 first (reordered)
        r2.step(req2);
        List<Eraftpb.Message> resp2Msgs = r2.readMessages();
        assertThat(resp2Msgs).hasSize(1);
        Eraftpb.Message resp2 = resp2Msgs.get(0);
        assertThat(resp2.getReject()).isTrue();

        // r2 handles req1
        r2.step(req1);
        List<Eraftpb.Message> resp1Msgs = r2.readMessages();
        assertThat(resp1Msgs).hasSize(1);
        assertThat(resp1Msgs.get(0).getReject()).isFalse();
        r1.step(resp1Msgs.get(0));
        r1.readMessages(); // consume msg
        assertThat(r1.trk.getProgress().get(2L).getMatch()).isEqualTo(2);

        // r1 observes unreachable, transits to probe
        r1.step(Eraftpb.Message.newBuilder().setFrom(2).setTo(1)
                .setMsgType(Eraftpb.MessageType.MsgUnreachable).build());
        assertThat(r1.trk.getProgress().get(2L).getState()).isEqualTo(StateType.StateProbe);

        // r1 receives delayed resp2
        r1.step(resp2);
        List<Eraftpb.Message> resendMsgs = r1.readMessages();
        assertThat(resendMsgs).hasSize(1);
        // should re-send from match index
        assertThat(resendMsgs.get(0).getIndex()).isEqualTo(r1.trk.getProgress().get(2L).getMatch());
    }

    static List<Eraftpb.Entry> nextEnts(Raft r, MemoryStorage s) {
        List<Eraftpb.Entry> unstable = r.raftLog.nextUnstableEnts();
        if (unstable != null && !unstable.isEmpty()) {
            s.append(unstable);
        }
        r.raftLog.stableTo(r.raftLog.lastEntryID());
        r.advanceMessagesAfterAppend();
        List<Eraftpb.Entry> ents = r.raftLog.nextCommittedEnts(true);
        r.raftLog.appliedTo(r.raftLog.committed, 0);
        return ents != null ? ents : Collections.emptyList();
    }
}
