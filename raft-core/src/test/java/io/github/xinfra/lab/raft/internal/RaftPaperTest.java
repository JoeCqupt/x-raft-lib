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
import org.junit.jupiter.api.Test;

import java.util.*;

import static io.github.xinfra.lab.raft.internal.TestUtil.*;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests which verify that the scenarios described in the Raft paper
 * (https://raft.github.io/raft.pdf) are handled correctly.
 * Corresponds to etcd-raft's raft_paper_test.go.
 */
class RaftPaperTest {

    // Section 5.1: testUpdateTermFromMessage
    @Test
    void testFollowerUpdateTermFromMessage() {
        testUpdateTermFromMessage(RaftStateType.StateFollower);
    }

    @Test
    void testCandidateUpdateTermFromMessage() {
        testUpdateTermFromMessage(RaftStateType.StateCandidate);
    }

    @Test
    void testLeaderUpdateTermFromMessage() {
        testUpdateTermFromMessage(RaftStateType.StateLeader);
    }

    private void testUpdateTermFromMessage(RaftStateType state) {
        Raft r = newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)));
        switch (state) {
            case StateFollower -> r.becomeFollower(1, 2);
            case StateCandidate -> r.becomeCandidate();
            case StateLeader -> { r.becomeCandidate(); r.becomeLeader(); }
        }

        r.step(Eraftpb.Message.newBuilder()
                .setMsgType(Eraftpb.MessageType.MsgAppend).setTerm(2).build());

        assertThat(r.term).isEqualTo(2);
        assertThat(r.state).isEqualTo(RaftStateType.StateFollower);
    }

    // Section 5.1: TestRejectStaleTermMessage
    @Test
    void testRejectStaleTermMessage() {
        Raft r = newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)));
        r.loadState(Eraftpb.HardState.newBuilder().setTerm(2).build());

        r.step(Eraftpb.Message.newBuilder()
                .setMsgType(Eraftpb.MessageType.MsgAppend).setTerm(r.term - 1).build());
        // Should be ignored - no state change
        assertThat(r.term).isEqualTo(2);
    }

    // Section 5.2: TestStartAsFollower
    @Test
    void testStartAsFollower() {
        Raft r = newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)));
        assertThat(r.state).isEqualTo(RaftStateType.StateFollower);
    }

    // Section 5.2: TestLeaderBcastBeat
    @Test
    void testLeaderBcastBeat() {
        int hi = 1;
        Raft r = newTestRaft(1, 10, hi, newTestMemoryStorage(withPeers(1, 2, 3)));
        r.becomeCandidate();
        r.becomeLeader();

        for (int i = 0; i < hi; i++) {
            r.tickFn.run();
        }

        List<Eraftpb.Message> msgs = r.readMessages();
        msgs.sort(Comparator.comparingLong(Eraftpb.Message::getTo));
        assertThat(msgs).hasSize(2);
        assertThat(msgs.get(0).getTo()).isEqualTo(2);
        assertThat(msgs.get(0).getMsgType()).isEqualTo(Eraftpb.MessageType.MsgHeartbeat);
        assertThat(msgs.get(1).getTo()).isEqualTo(3);
        assertThat(msgs.get(1).getMsgType()).isEqualTo(Eraftpb.MessageType.MsgHeartbeat);
    }

    // Section 5.2: testNonleaderStartElection
    @Test
    void testFollowerStartElection() {
        testNonleaderStartElection(RaftStateType.StateFollower);
    }

    @Test
    void testCandidateStartNewElection() {
        testNonleaderStartElection(RaftStateType.StateCandidate);
    }

    private void testNonleaderStartElection(RaftStateType state) {
        int et = 10;
        Raft r = newTestRaft(1, et, 1, newTestMemoryStorage(withPeers(1, 2, 3)));
        switch (state) {
            case StateFollower -> r.becomeFollower(1, 2);
            case StateCandidate -> r.becomeCandidate();
        }

        for (int i = 1; i < 2 * et; i++) {
            r.tickFn.run();
        }
        r.advanceMessagesAfterAppend();

        assertThat(r.term).isEqualTo(2);
        assertThat(r.state).isEqualTo(RaftStateType.StateCandidate);
        assertThat(r.trk.getVotes().get(r.id)).isTrue();

        List<Eraftpb.Message> msgs = r.readMessages();
        msgs.sort(Comparator.comparingLong(Eraftpb.Message::getTo));
        assertThat(msgs).hasSize(2);
        assertThat(msgs.get(0).getMsgType()).isEqualTo(Eraftpb.MessageType.MsgRequestVote);
        assertThat(msgs.get(0).getTo()).isEqualTo(2);
        assertThat(msgs.get(1).getMsgType()).isEqualTo(Eraftpb.MessageType.MsgRequestVote);
        assertThat(msgs.get(1).getTo()).isEqualTo(3);
    }

    // Section 5.2: TestLeaderElectionInOneRoundRPC
    @Test
    void testLeaderElectionInOneRoundRPC() {
        record TC(int size, Map<Long, Boolean> votes, RaftStateType state) {}
        List<TC> tests = List.of(
                new TC(1, Map.of(), RaftStateType.StateLeader),
                new TC(3, Map.of(2L, true, 3L, true), RaftStateType.StateLeader),
                new TC(3, Map.of(2L, true), RaftStateType.StateLeader),
                new TC(5, Map.of(2L, true, 3L, true, 4L, true, 5L, true), RaftStateType.StateLeader),
                new TC(5, Map.of(2L, true, 3L, true, 4L, true), RaftStateType.StateLeader),
                new TC(5, Map.of(2L, true, 3L, true), RaftStateType.StateLeader),
                // return to follower state
                new TC(3, Map.of(2L, false, 3L, false), RaftStateType.StateFollower),
                new TC(5, Map.of(2L, false, 3L, false, 4L, false, 5L, false), RaftStateType.StateFollower),
                new TC(5, Map.of(2L, true, 3L, false, 4L, false, 5L, false), RaftStateType.StateFollower),
                // stay in candidate
                new TC(3, Map.of(), RaftStateType.StateCandidate),
                new TC(5, Map.of(2L, true), RaftStateType.StateCandidate),
                new TC(5, Map.of(2L, false, 3L, false), RaftStateType.StateCandidate),
                new TC(5, Map.of(), RaftStateType.StateCandidate)
        );

        for (int i = 0; i < tests.size(); i++) {
            TC tt = tests.get(i);
            Raft r = newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(idsBySize(tt.size))));
            r.step(Eraftpb.Message.newBuilder().setFrom(1).setTo(1)
                    .setMsgType(Eraftpb.MessageType.MsgHup).build());
            r.advanceMessagesAfterAppend();

            for (Map.Entry<Long, Boolean> entry : tt.votes.entrySet()) {
                r.step(Eraftpb.Message.newBuilder()
                        .setFrom(entry.getKey()).setTo(1).setTerm(r.term)
                        .setMsgType(Eraftpb.MessageType.MsgRequestVoteResponse)
                        .setReject(!entry.getValue()).build());
            }

            assertThat(r.state).as("test #%d", i).isEqualTo(tt.state);
            assertThat(r.term).as("test #%d", i).isEqualTo(1);
        }
    }

    // Section 5.2: TestFollowerVote
    @Test
    void testFollowerVote() {
        record TC(long vote, long nvote, boolean wreject) {}
        List<TC> tests = List.of(
                new TC(Util.NONE, 2, false),
                new TC(Util.NONE, 3, false),
                new TC(2, 2, false),
                new TC(3, 3, false),
                new TC(2, 3, true),
                new TC(3, 2, true)
        );

        for (int i = 0; i < tests.size(); i++) {
            TC tt = tests.get(i);
            Raft r = newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)));
            r.loadState(Eraftpb.HardState.newBuilder().setTerm(1).setVote(tt.vote).build());

            r.step(Eraftpb.Message.newBuilder()
                    .setFrom(tt.nvote).setTo(1).setTerm(1)
                    .setMsgType(Eraftpb.MessageType.MsgRequestVote).build());

            assertThat(r.msgsAfterAppend).as("test #%d", i).hasSize(1);
            Eraftpb.Message resp = r.msgsAfterAppend.get(0);
            assertThat(resp.getMsgType()).isEqualTo(Eraftpb.MessageType.MsgRequestVoteResponse);
            assertThat(resp.getReject()).as("test #%d", i).isEqualTo(tt.wreject);
        }
    }

    // Section 5.2: TestCandidateFallback
    @Test
    void testCandidateFallback() {
        List<Eraftpb.Message> tests = List.of(
                Eraftpb.Message.newBuilder().setFrom(2).setTo(1).setTerm(1)
                        .setMsgType(Eraftpb.MessageType.MsgAppend).build(),
                Eraftpb.Message.newBuilder().setFrom(2).setTo(1).setTerm(2)
                        .setMsgType(Eraftpb.MessageType.MsgAppend).build()
        );

        for (int i = 0; i < tests.size(); i++) {
            Eraftpb.Message tt = tests.get(i);
            Raft r = newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)));
            r.step(Eraftpb.Message.newBuilder().setFrom(1).setTo(1)
                    .setMsgType(Eraftpb.MessageType.MsgHup).build());
            assertThat(r.state).as("test #%d", i).isEqualTo(RaftStateType.StateCandidate);

            r.step(tt);

            assertThat(r.state).as("test #%d", i).isEqualTo(RaftStateType.StateFollower);
            assertThat(r.term).as("test #%d", i).isEqualTo(tt.getTerm());
        }
    }

    // Section 5.3: TestLeaderStartReplication
    @Test
    void testLeaderStartReplication() {
        MemoryStorage s = newTestMemoryStorage(withPeers(1, 2, 3));
        Raft r = newTestRaft(1, 10, 1, s);
        r.becomeCandidate();
        r.becomeLeader();
        commitNoopEntry(r, s);
        long li = r.raftLog.lastIndex();

        r.step(Eraftpb.Message.newBuilder().setFrom(1).setTo(1)
                .setMsgType(Eraftpb.MessageType.MsgPropose)
                .addEntries(Eraftpb.Entry.newBuilder().setData(ByteString.copyFromUtf8("some data")))
                .build());

        assertThat(r.raftLog.lastIndex()).isEqualTo(li + 1);
        assertThat(r.raftLog.committed).isEqualTo(li);

        List<Eraftpb.Message> msgs = r.readMessages();
        msgs.sort(Comparator.comparingLong(Eraftpb.Message::getTo));
        assertThat(msgs).hasSize(2);
        for (Eraftpb.Message m : msgs) {
            assertThat(m.getMsgType()).isEqualTo(Eraftpb.MessageType.MsgAppend);
            assertThat(m.getEntriesCount()).isEqualTo(1);
            assertThat(m.getEntries(0).getData().toStringUtf8()).isEqualTo("some data");
        }
    }

    // Section 5.3: TestLeaderCommitEntry
    @Test
    void testLeaderCommitEntry() {
        MemoryStorage s = newTestMemoryStorage(withPeers(1, 2, 3));
        Raft r = newTestRaft(1, 10, 1, s);
        r.becomeCandidate();
        r.becomeLeader();
        commitNoopEntry(r, s);
        long li = r.raftLog.lastIndex();

        r.step(Eraftpb.Message.newBuilder().setFrom(1).setTo(1)
                .setMsgType(Eraftpb.MessageType.MsgPropose)
                .addEntries(Eraftpb.Entry.newBuilder().setData(ByteString.copyFromUtf8("some data")))
                .build());

        for (Eraftpb.Message m : r.readMessages()) {
            r.step(acceptAndReply(m));
        }

        assertThat(r.raftLog.committed).isEqualTo(li + 1);
        List<Eraftpb.Entry> committed = r.raftLog.nextCommittedEnts(true);
        assertThat(committed).hasSize(1);
        assertThat(committed.get(0).getData().toStringUtf8()).isEqualTo("some data");
    }

    // Section 5.3: TestLeaderAcknowledgeCommit
    @Test
    void testLeaderAcknowledgeCommit() {
        record TC(int size, Set<Long> acceptors, boolean wack) {}
        List<TC> tests = List.of(
                new TC(1, Set.of(), true),
                new TC(3, Set.of(), false),
                new TC(3, Set.of(2L), true),
                new TC(3, Set.of(2L, 3L), true),
                new TC(5, Set.of(), false),
                new TC(5, Set.of(2L), false),
                new TC(5, Set.of(2L, 3L), true),
                new TC(5, Set.of(2L, 3L, 4L), true)
        );

        for (int i = 0; i < tests.size(); i++) {
            TC tt = tests.get(i);
            MemoryStorage s = newTestMemoryStorage(withPeers(idsBySize(tt.size)));
            Raft r = newTestRaft(1, 10, 1, s);
            r.becomeCandidate();
            r.becomeLeader();
            commitNoopEntry(r, s);
            long li = r.raftLog.lastIndex();

            r.step(Eraftpb.Message.newBuilder().setFrom(1).setTo(1)
                    .setMsgType(Eraftpb.MessageType.MsgPropose)
                    .addEntries(Eraftpb.Entry.newBuilder().setData(ByteString.copyFromUtf8("some data")))
                    .build());
            r.advanceMessagesAfterAppend();

            for (Eraftpb.Message m : new ArrayList<>(r.msgs)) {
                if (tt.acceptors.contains(m.getTo())) {
                    r.step(acceptAndReply(m));
                }
            }

            assertThat(r.raftLog.committed > li).as("test #%d", i).isEqualTo(tt.wack);
        }
    }

    // Section 5.3: TestFollowerCommitEntry
    @Test
    void testFollowerCommitEntry() {
        record TC(List<Eraftpb.Entry> ents, long commit) {}
        List<TC> tests = List.of(
                new TC(List.of(Eraftpb.Entry.newBuilder().setTerm(1).setIndex(1)
                        .setData(ByteString.copyFromUtf8("some data")).build()), 1),
                new TC(List.of(
                        Eraftpb.Entry.newBuilder().setTerm(1).setIndex(1).setData(ByteString.copyFromUtf8("some data")).build(),
                        Eraftpb.Entry.newBuilder().setTerm(1).setIndex(2).setData(ByteString.copyFromUtf8("some data2")).build()
                ), 2),
                new TC(List.of(
                        Eraftpb.Entry.newBuilder().setTerm(1).setIndex(1).setData(ByteString.copyFromUtf8("some data")).build(),
                        Eraftpb.Entry.newBuilder().setTerm(1).setIndex(2).setData(ByteString.copyFromUtf8("some data2")).build()
                ), 1)
        );

        for (int i = 0; i < tests.size(); i++) {
            TC tt = tests.get(i);
            Raft r = newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)));
            r.becomeFollower(1, 2);

            r.step(Eraftpb.Message.newBuilder()
                    .setFrom(2).setTo(1).setTerm(1)
                    .setMsgType(Eraftpb.MessageType.MsgAppend)
                    .addAllEntries(tt.ents).setCommit(tt.commit).build());

            assertThat(r.raftLog.committed).as("test #%d", i).isEqualTo(tt.commit);
            List<Eraftpb.Entry> committed = r.raftLog.nextCommittedEnts(true);
            assertThat(committed).as("test #%d", i).hasSize((int) tt.commit);
        }
    }

    // Section 5.3: TestFollowerCheckMsgApp
    @Test
    void testFollowerCheckMsgApp() {
        List<Eraftpb.Entry> ents = List.of(
                Eraftpb.Entry.newBuilder().setTerm(1).setIndex(1).build(),
                Eraftpb.Entry.newBuilder().setTerm(2).setIndex(2).build()
        );

        record TC(long term, long index, long windex, boolean wreject, long wrejectHint, long wlogterm) {}
        List<TC> tests = List.of(
                new TC(0, 0, 1, false, 0, 0),
                new TC(1, 1, 1, false, 0, 0),
                new TC(2, 2, 2, false, 0, 0),
                new TC(1, 2, 2, true, 1, 1),
                new TC(3, 3, 3, true, 2, 2)
        );

        for (int i = 0; i < tests.size(); i++) {
            TC tt = tests.get(i);
            MemoryStorage storage = newTestMemoryStorage(withPeers(1, 2, 3));
            storage.append(ents);
            Raft r = newTestRaft(1, 10, 1, storage);
            r.loadState(Eraftpb.HardState.newBuilder().setCommit(1).build());
            r.becomeFollower(2, 2);

            r.step(Eraftpb.Message.newBuilder()
                    .setFrom(2).setTo(1).setTerm(2)
                    .setMsgType(Eraftpb.MessageType.MsgAppend)
                    .setLogTerm(tt.term).setIndex(tt.index).build());

            List<Eraftpb.Message> msgs = r.readMessages();
            assertThat(msgs).as("test #%d", i).hasSize(1);
            Eraftpb.Message resp = msgs.get(0);
            assertThat(resp.getMsgType()).isEqualTo(Eraftpb.MessageType.MsgAppendResponse);
            assertThat(resp.getReject()).as("test #%d reject", i).isEqualTo(tt.wreject);
            assertThat(resp.getIndex()).as("test #%d index", i).isEqualTo(tt.windex);
            assertThat(resp.getRejectHint()).as("test #%d rejectHint", i).isEqualTo(tt.wrejectHint);
            assertThat(resp.getLogTerm()).as("test #%d logTerm", i).isEqualTo(tt.wlogterm);
        }
    }

    // Section 5.4.1: TestVoter
    @Test
    void testVoter() {
        record TC(List<Eraftpb.Entry> ents, long logterm, long index, boolean wreject) {}
        List<TC> tests = List.of(
                new TC(index(1).terms(1), 1, 1, false),
                new TC(index(1).terms(1), 1, 2, false),
                new TC(index(1).terms(1, 1), 1, 1, true),
                new TC(index(1).terms(1), 2, 1, false),
                new TC(index(1).terms(1), 2, 2, false),
                new TC(index(1).terms(1, 1), 2, 1, false),
                new TC(index(1).terms(2), 1, 1, true),
                new TC(index(1).terms(2), 1, 2, true),
                new TC(index(1).terms(2, 2), 1, 1, true),
                new TC(index(1).terms(1, 1), 1, 1, true)
        );

        for (int i = 0; i < tests.size(); i++) {
            TC tt = tests.get(i);
            MemoryStorage storage = newTestMemoryStorage(withPeers(1, 2));
            storage.append(tt.ents);
            Raft r = newTestRaft(1, 10, 1, storage);

            r.step(Eraftpb.Message.newBuilder()
                    .setFrom(2).setTo(1).setTerm(3)
                    .setMsgType(Eraftpb.MessageType.MsgRequestVote)
                    .setLogTerm(tt.logterm).setIndex(tt.index).build());

            List<Eraftpb.Message> msgs = r.readMessages();
            assertThat(msgs).as("test #%d", i).hasSize(1);
            assertThat(msgs.get(0).getMsgType()).isEqualTo(Eraftpb.MessageType.MsgRequestVoteResponse);
            assertThat(msgs.get(0).getReject()).as("test #%d", i).isEqualTo(tt.wreject);
        }
    }

    // Section 5.4.2: TestLeaderOnlyCommitsLogFromCurrentTerm
    @Test
    void testLeaderOnlyCommitsLogFromCurrentTerm() {
        List<Eraftpb.Entry> ents = List.of(
                Eraftpb.Entry.newBuilder().setTerm(1).setIndex(1).build(),
                Eraftpb.Entry.newBuilder().setTerm(2).setIndex(2).build()
        );
        record TC(long index, long wcommit) {}
        List<TC> tests = List.of(
                new TC(1, 0),
                new TC(2, 0),
                new TC(3, 3)
        );

        for (int i = 0; i < tests.size(); i++) {
            TC tt = tests.get(i);
            MemoryStorage storage = newTestMemoryStorage(withPeers(1, 2));
            storage.append(ents);
            Raft r = newTestRaft(1, 10, 1, storage);
            r.loadState(Eraftpb.HardState.newBuilder().setTerm(2).build());
            r.becomeCandidate();
            r.becomeLeader();
            r.readMessages();

            r.step(Eraftpb.Message.newBuilder().setFrom(1).setTo(1)
                    .setMsgType(Eraftpb.MessageType.MsgPropose)
                    .addEntries(Eraftpb.Entry.newBuilder()).build());

            r.step(Eraftpb.Message.newBuilder()
                    .setFrom(2).setTo(1).setTerm(r.term)
                    .setMsgType(Eraftpb.MessageType.MsgAppendResponse)
                    .setIndex(tt.index).build());
            r.advanceMessagesAfterAppend();

            assertThat(r.raftLog.committed).as("test #%d", i).isEqualTo(tt.wcommit);
        }
    }

    // Section 5.2: TestFollowerElectionTimeoutRandomized
    @Test
    void testFollowerElectionTimeoutRandomized() {
        testNonleaderElectionTimeoutRandomized(RaftStateType.StateFollower);
    }

    @Test
    void testCandidateElectionTimeoutRandomized() {
        testNonleaderElectionTimeoutRandomized(RaftStateType.StateCandidate);
    }

    private void testNonleaderElectionTimeoutRandomized(RaftStateType state) {
        int et = 10;
        Raft r = newTestRaft(1, et, 1, newTestMemoryStorage(withPeers(1, 2, 3)));
        Set<Integer> timeouts = new HashSet<>();
        for (int round = 0; round < 50 * et; round++) {
            switch (state) {
                case StateFollower -> r.becomeFollower(r.term + 1, 2);
                case StateCandidate -> r.becomeCandidate();
            }

            int time = 0;
            while (r.readMessages().isEmpty()) {
                r.tickFn.run();
                time++;
            }
            timeouts.add(time);
        }

        for (int d = et; d < 2 * et; d++) {
            assertThat(timeouts).as("timeout in %d ticks should happen", d).contains(d);
        }
    }

    // Section 5.2: TestFollowersElectionTimeoutNonconflict
    @Test
    void testFollowersElectionTimeoutNonconflict() {
        testNonleadersElectionTimeoutNonconflict(RaftStateType.StateFollower);
    }

    @Test
    void testCandidatesElectionTimeoutNonconflict() {
        testNonleadersElectionTimeoutNonconflict(RaftStateType.StateCandidate);
    }

    private void testNonleadersElectionTimeoutNonconflict(RaftStateType state) {
        int et = 10;
        int size = 5;
        long[] ids = idsBySize(size);
        Raft[] rs = new Raft[size];
        for (int k = 0; k < size; k++) {
            rs[k] = newTestRaft(ids[k], et, 1, newTestMemoryStorage(withPeers(ids)));
        }
        int conflicts = 0;
        for (int round = 0; round < 1000; round++) {
            for (Raft r : rs) {
                switch (state) {
                    case StateFollower -> r.becomeFollower(r.term + 1, Util.NONE);
                    case StateCandidate -> r.becomeCandidate();
                }
            }

            int timeoutNum = 0;
            while (timeoutNum == 0) {
                for (Raft r : rs) {
                    r.tickFn.run();
                    if (!r.readMessages().isEmpty()) {
                        timeoutNum++;
                    }
                }
            }
            if (timeoutNum > 1) {
                conflicts++;
            }
        }

        assertThat((double) conflicts / 1000).isLessThanOrEqualTo(0.3);
    }

    // Section 5.3: TestLeaderCommitPrecedingEntries
    @Test
    void testLeaderCommitPrecedingEntries() {
        List<List<Eraftpb.Entry>> tests = List.of(
                List.of(),
                List.of(Eraftpb.Entry.newBuilder().setTerm(2).setIndex(1).build()),
                List.of(Eraftpb.Entry.newBuilder().setTerm(1).setIndex(1).build(),
                        Eraftpb.Entry.newBuilder().setTerm(2).setIndex(2).build()),
                List.of(Eraftpb.Entry.newBuilder().setTerm(1).setIndex(1).build())
        );

        for (int i = 0; i < tests.size(); i++) {
            List<Eraftpb.Entry> tt = tests.get(i);
            MemoryStorage storage = newTestMemoryStorage(withPeers(1, 2, 3));
            storage.append(tt);
            Raft r = newTestRaft(1, 10, 1, storage);
            r.loadState(Eraftpb.HardState.newBuilder().setTerm(2).build());
            r.becomeCandidate();
            r.becomeLeader();
            r.step(Eraftpb.Message.newBuilder().setFrom(1).setTo(1)
                    .setMsgType(Eraftpb.MessageType.MsgPropose)
                    .addEntries(Eraftpb.Entry.newBuilder().setData(ByteString.copyFromUtf8("some data"))).build());

            for (Eraftpb.Message m : r.readMessages()) {
                r.step(acceptAndReply(m));
            }

            long li = tt.size();
            List<Eraftpb.Entry> wents = new ArrayList<>(tt);
            wents.add(Eraftpb.Entry.newBuilder().setTerm(3).setIndex(li + 1).build());
            wents.add(Eraftpb.Entry.newBuilder().setTerm(3).setIndex(li + 2)
                    .setData(ByteString.copyFromUtf8("some data")).build());
            assertThat(r.raftLog.nextCommittedEnts(true)).as("#%d", i).isEqualTo(wents);
        }
    }

    // Section 5.3: TestFollowerAppendEntries
    @Test
    void testFollowerAppendEntries() {
        record TC(long index, long term, List<Eraftpb.Entry> ents, List<Eraftpb.Entry> wents, List<Eraftpb.Entry> wunstable) {}
        List<TC> tests = List.of(
                new TC(2, 2,
                        index(3).terms(3),
                        index(1).terms(1, 2, 3),
                        index(3).terms(3)),
                new TC(1, 1,
                        index(2).terms(3, 4),
                        index(1).terms(1, 3, 4),
                        index(2).terms(3, 4)),
                new TC(0, 0,
                        index(1).terms(1),
                        index(1).terms(1, 2),
                        null),
                new TC(0, 0,
                        index(1).terms(3),
                        index(1).terms(3),
                        index(1).terms(3))
        );

        for (int i = 0; i < tests.size(); i++) {
            TC tt = tests.get(i);
            MemoryStorage storage = newTestMemoryStorage(withPeers(1, 2, 3));
            storage.append(List.of(
                    Eraftpb.Entry.newBuilder().setTerm(1).setIndex(1).build(),
                    Eraftpb.Entry.newBuilder().setTerm(2).setIndex(2).build()));
            Raft r = newTestRaft(1, 10, 1, storage);
            r.becomeFollower(2, 2);

            r.step(Eraftpb.Message.newBuilder().setFrom(2).setTo(1)
                    .setMsgType(Eraftpb.MessageType.MsgAppend).setTerm(2)
                    .setLogTerm(tt.term).setIndex(tt.index)
                    .addAllEntries(tt.ents).build());

            assertThat(r.raftLog.allEntries()).as("#%d", i).isEqualTo(tt.wents);
            List<Eraftpb.Entry> unstable = r.raftLog.nextUnstableEnts();
            assertThat(unstable).as("#%d unstable", i).isEqualTo(tt.wunstable);
        }
    }

    // Section 5.3: TestLeaderSyncFollowerLog
    @Test
    void testLeaderSyncFollowerLog() {
        List<Eraftpb.Entry> ents = index(0).terms(0, 1, 1, 1, 4, 4, 5, 5, 6, 6, 6);
        long term = 8;

        List<List<Eraftpb.Entry>> followerLogs = List.of(
                index(0).terms(0, 1, 1, 1, 4, 4, 5, 5, 6, 6),
                index(0).terms(0, 1, 1, 1, 4, 4),
                index(0).terms(0, 1, 1, 1, 4, 4, 5, 5, 6, 6, 6, 6),
                index(0).terms(0, 1, 1, 1, 4, 4, 5, 5, 6, 6, 6, 7, 7),
                index(0).terms(0, 1, 1, 1, 4, 4, 4, 4),
                index(0).terms(0, 1, 1, 1, 2, 2, 2, 3, 3, 3, 3, 3)
        );

        for (int i = 0; i < followerLogs.size(); i++) {
            List<Eraftpb.Entry> tt = followerLogs.get(i);
            MemoryStorage leadStorage = newTestMemoryStorage(withPeers(1, 2, 3));
            leadStorage.append(ents);
            Raft lead = newTestRaft(1, 10, 1, leadStorage);
            lead.loadState(Eraftpb.HardState.newBuilder()
                    .setCommit(lead.raftLog.lastIndex()).setTerm(term).build());

            MemoryStorage followerStorage = newTestMemoryStorage(withPeers(1, 2, 3));
            followerStorage.append(tt);
            Raft follower = newTestRaft(2, 10, 1, followerStorage);
            follower.loadState(Eraftpb.HardState.newBuilder().setTerm(term - 1).build());

            Network n = Network.newNetwork(
                    new Network.RaftStateMachine(lead),
                    new Network.RaftStateMachine(follower),
                    Network.NOP_STEPPER);
            n.send(Eraftpb.Message.newBuilder().setFrom(1).setTo(1)
                    .setMsgType(Eraftpb.MessageType.MsgHup).build());
            // The election occurs in the term after the one we loaded with
            n.send(Eraftpb.Message.newBuilder().setFrom(3).setTo(1)
                    .setMsgType(Eraftpb.MessageType.MsgRequestVoteResponse).setTerm(term + 1).build());

            n.send(Eraftpb.Message.newBuilder().setFrom(1).setTo(1)
                    .setMsgType(Eraftpb.MessageType.MsgPropose)
                    .addEntries(Eraftpb.Entry.newBuilder()).build());

            assertThat(lead.raftLog.allEntries()).as("#%d", i)
                    .isEqualTo(follower.raftLog.allEntries());
        }
    }

    // Section 5.4.1: TestVoteRequest
    @Test
    void testVoteRequest() {
        record TC(List<Eraftpb.Entry> ents, long wterm) {}
        List<TC> tests = List.of(
                new TC(index(1).terms(1), 2),
                new TC(index(1).terms(1, 2), 3)
        );

        for (int j = 0; j < tests.size(); j++) {
            TC tt = tests.get(j);
            Raft r = newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)));
            r.step(Eraftpb.Message.newBuilder()
                    .setFrom(2).setTo(1).setMsgType(Eraftpb.MessageType.MsgAppend)
                    .setTerm(tt.wterm - 1).setLogTerm(0).setIndex(0)
                    .addAllEntries(tt.ents).build());
            r.readMessages();

            for (int i = 1; i < r.electionTimeout * 2; i++) {
                r.tickElection();
            }

            List<Eraftpb.Message> msgs = r.readMessages();
            msgs.sort(Comparator.comparingLong(Eraftpb.Message::getTo));
            assertThat(msgs).as("#%d", j).hasSize(2);
            for (int i = 0; i < msgs.size(); i++) {
                Eraftpb.Message m = msgs.get(i);
                assertThat(m.getMsgType()).isEqualTo(Eraftpb.MessageType.MsgRequestVote);
                assertThat(m.getTo()).isEqualTo(i + 2);
                assertThat(m.getTerm()).as("#%d.%d", j, i).isEqualTo(tt.wterm);

                long wIndex = tt.ents.get(tt.ents.size() - 1).getIndex();
                long wLogTerm = tt.ents.get(tt.ents.size() - 1).getTerm();
                assertThat(m.getIndex()).as("#%d.%d", j, i).isEqualTo(wIndex);
                assertThat(m.getLogTerm()).as("#%d.%d", j, i).isEqualTo(wLogTerm);
            }
        }
    }

    // ============= Helper methods =============

    static long[] idsBySize(int size) {
        long[] ids = new long[size];
        for (int i = 0; i < size; i++) {
            ids[i] = i + 1;
        }
        return ids;
    }

    static void commitNoopEntry(Raft r, MemoryStorage s) {
        if (r.state != RaftStateType.StateLeader) {
            throw new IllegalStateException("it should only be used when it is the leader");
        }
        r.bcastAppend();
        List<Eraftpb.Message> msgs = r.readMessages();
        for (Eraftpb.Message m : msgs) {
            if (m.getMsgType() != Eraftpb.MessageType.MsgAppend || m.getEntriesCount() != 1 ||
                    m.getEntries(0).getData().size() != 0) {
                throw new IllegalStateException("not a message to append noop entry");
            }
            r.step(acceptAndReply(m));
        }
        r.readMessages();
        List<Eraftpb.Entry> unstable = r.raftLog.nextUnstableEnts();
        if (unstable != null && !unstable.isEmpty()) {
            s.append(unstable);
        }
        r.raftLog.appliedTo(r.raftLog.committed, 0);
        r.raftLog.stableTo(r.raftLog.lastEntryID());
    }

    static Eraftpb.Message acceptAndReply(Eraftpb.Message m) {
        if (m.getMsgType() != Eraftpb.MessageType.MsgAppend) {
            throw new IllegalArgumentException("type should be MsgAppend");
        }
        return Eraftpb.Message.newBuilder()
                .setFrom(m.getTo())
                .setTo(m.getFrom())
                .setTerm(m.getTerm())
                .setMsgType(Eraftpb.MessageType.MsgAppendResponse)
                .setIndex(m.getIndex() + m.getEntriesCount())
                .build();
    }
}
