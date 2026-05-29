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

import java.util.ArrayList;
import java.util.List;

import static io.github.xinfra.lab.raft.internal.TestUtil.*;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for RawNode, mirroring etcd-raft rawnode_test.go.
 */
class RawNodeTest {

    @Test
    void testRawNodeStep() {
        // Test that RawNode.step ignores local messages
        for (Eraftpb.MessageType mt : Eraftpb.MessageType.values()) {
            if (mt == Eraftpb.MessageType.UNRECOGNIZED) continue;
            MemoryStorage s = newTestMemoryStorage(withPeers(1));
            s.setHardState(Eraftpb.HardState.newBuilder().setTerm(1).setCommit(1).build());
            s.append(List.of(Eraftpb.Entry.newBuilder().setTerm(1).setIndex(1).build()));
            RawNode rn = RawNode.newRawNode(newTestConfig(1, 10, 1, s));
            RaftException err = rn.step(Eraftpb.Message.newBuilder().setMsgType(mt).build());
            if (Util.isLocalMsg(mt)) {
                assertThat(err).as("%s", mt).isEqualTo(RaftException.ErrStepLocalMsg);
            }
        }
    }

    @Test
    void testRawNodeRestart() {
        List<Eraftpb.Entry> entries = List.of(
                Eraftpb.Entry.newBuilder().setTerm(1).setIndex(1).build(),
                Eraftpb.Entry.newBuilder().setTerm(1).setIndex(2)
                        .setData(com.google.protobuf.ByteString.copyFromUtf8("foo")).build()
        );
        Eraftpb.HardState st = Eraftpb.HardState.newBuilder().setTerm(1).setCommit(1).build();

        MemoryStorage storage = newTestMemoryStorage(withPeers(1));
        storage.setHardState(st);
        storage.append(entries);
        RawNode rn = RawNode.newRawNode(newTestConfig(1, 10, 1, storage));
        Ready rd = rn.ready();
        // Should commit up to commit index in st
        assertThat(rd.committedEntries).hasSize(1);
        assertThat(rd.committedEntries.get(0).getIndex()).isEqualTo(1);
        rn.advance(rd);
        assertThat(rn.hasReady()).isFalse();
    }

    @Test
    void testRawNodeRestartFromSnapshot() throws RaftException {
        Eraftpb.Snapshot snap = Eraftpb.Snapshot.newBuilder()
                .setMetadata(Eraftpb.SnapshotMetadata.newBuilder()
                        .setConfState(Eraftpb.ConfState.newBuilder().addVoters(1).addVoters(2))
                        .setIndex(2).setTerm(1))
                .build();
        List<Eraftpb.Entry> entries = List.of(
                Eraftpb.Entry.newBuilder().setTerm(1).setIndex(3)
                        .setData(com.google.protobuf.ByteString.copyFromUtf8("foo")).build()
        );
        Eraftpb.HardState st = Eraftpb.HardState.newBuilder().setTerm(1).setCommit(3).build();

        MemoryStorage s = new MemoryStorage();
        s.setHardState(st);
        s.applySnapshot(snap);
        s.append(entries);
        RawNode rn = RawNode.newRawNode(newTestConfig(1, 10, 1, s));
        Ready rd = rn.ready();
        assertThat(rd.committedEntries).hasSize(1);
        assertThat(rd.committedEntries.get(0).getIndex()).isEqualTo(3);
        rn.advance(rd);
        assertThat(rn.hasReady()).isFalse();
    }

    @Test
    void testRawNodeStatus() {
        MemoryStorage s = newTestMemoryStorage(withPeers(1));
        RawNode rn = RawNode.newRawNode(newTestConfig(1, 10, 1, s));
        assertThat(rn.campaign()).isNull();

        Ready rd = rn.ready();
        s.append(rd.entries);
        rn.advance(rd);

        assertThat(rn.raft.state).isEqualTo(RaftStateType.StateLeader);
        assertThat(rn.raft.lead).isEqualTo(1);
    }

    @Test
    void testRawNodeProposeAndConfChange() throws Exception {
        MemoryStorage s = newTestMemoryStorage(withPeers(1));
        RawNode rn = RawNode.newRawNode(newTestConfig(1, 10, 1, s));
        assertThat(rn.campaign()).isNull();

        // Become leader
        Ready rd = rn.ready();
        s.append(rd.entries);
        rn.advance(rd);

        // Drain committed entries from election
        while (rn.hasReady()) {
            rd = rn.ready();
            if (rd.entries != null && !rd.entries.isEmpty()) s.append(rd.entries);
            rn.advance(rd);
        }

        // Propose a conf change to add node 2
        Eraftpb.ConfChange cc = Eraftpb.ConfChange.newBuilder()
                .setChangeType(Eraftpb.ConfChangeType.ConfChangeAddNode)
                .setNodeId(2).build();
        rn.proposeConfChange(cc);

        // Drain until we find the conf change entry
        boolean found = false;
        while (rn.hasReady()) {
            rd = rn.ready();
            if (rd.entries != null && !rd.entries.isEmpty()) s.append(rd.entries);
            for (Eraftpb.Entry e : rd.committedEntries) {
                if (e.getEntryType() == Eraftpb.EntryType.EntryConfChange) {
                    found = true;
                    Eraftpb.ConfChange parsedCC = Eraftpb.ConfChange.parseFrom(e.getData());
                    Eraftpb.ConfState cs = rn.applyConfChange(Eraftpb.ConfChangeV2.newBuilder()
                            .addChanges(Eraftpb.ConfChangeSingle.newBuilder()
                                    .setType(parsedCC.getChangeType())
                                    .setNodeId(parsedCC.getNodeId())).build());
                    assertThat(cs.getVotersList()).contains(1L, 2L);
                }
            }
            rn.advance(rd);
        }
        assertThat(found).isTrue();
    }

    @Test
    void testRawNodeProposeDuplicateNode() throws Exception {
        MemoryStorage s = newTestMemoryStorage(withPeers(1));
        RawNode rn = RawNode.newRawNode(newTestConfig(1, 10, 1, s));
        rn.campaign();
        // Become leader and drain
        while (rn.hasReady()) {
            Ready rd = rn.ready();
            if (rd.entries != null && !rd.entries.isEmpty()) s.append(rd.entries);
            rn.advance(rd);
        }

        // Add node 1 again (duplicate) - should not crash
        rn.proposeConfChange(Eraftpb.ConfChange.newBuilder()
                .setChangeType(Eraftpb.ConfChangeType.ConfChangeAddNode)
                .setNodeId(1).build());
        // Drain
        while (rn.hasReady()) {
            Ready rd = rn.ready();
            if (rd.entries != null && !rd.entries.isEmpty()) s.append(rd.entries);
            for (Eraftpb.Entry e : rd.committedEntries) {
                if (e.getEntryType() == Eraftpb.EntryType.EntryConfChange) {
                    Eraftpb.ConfChange cc2 = Eraftpb.ConfChange.parseFrom(e.getData());
                    rn.applyConfChange(Eraftpb.ConfChangeV2.newBuilder()
                            .addChanges(Eraftpb.ConfChangeSingle.newBuilder()
                                    .setType(cc2.getChangeType())
                                    .setNodeId(cc2.getNodeId())).build());
                }
            }
            rn.advance(rd);
        }

        // Propose add node 2
        rn.proposeConfChange(Eraftpb.ConfChange.newBuilder()
                .setChangeType(Eraftpb.ConfChangeType.ConfChangeAddNode)
                .setNodeId(2).build());
        // Drain
        while (rn.hasReady()) {
            Ready rd = rn.ready();
            if (rd.entries != null && !rd.entries.isEmpty()) s.append(rd.entries);
            for (Eraftpb.Entry e : rd.committedEntries) {
                if (e.getEntryType() == Eraftpb.EntryType.EntryConfChange) {
                    Eraftpb.ConfChange cc3 = Eraftpb.ConfChange.parseFrom(e.getData());
                    rn.applyConfChange(Eraftpb.ConfChangeV2.newBuilder()
                            .addChanges(Eraftpb.ConfChangeSingle.newBuilder()
                                    .setType(cc3.getChangeType())
                                    .setNodeId(cc3.getNodeId())).build());
                }
            }
            rn.advance(rd);
        }

        assertThat(rn.raft.trk.voterNodes()).containsExactly(1L, 2L);
    }

    @Test
    void testRawNodeReadIndex() {
        MemoryStorage s = newTestMemoryStorage(withPeers(1));
        Config cfg = newTestConfig(1, 10, 1, s);
        cfg.maxSizePerMsg = NO_LIMIT;
        cfg.maxInflightMsgs = 256;
        RawNode rn = RawNode.newRawNode(cfg);
        rn.campaign();

        // Become leader & drain
        while (rn.hasReady()) {
            Ready rd = rn.ready();
            if (rd.entries != null && !rd.entries.isEmpty()) s.append(rd.entries);
            rn.advance(rd);
        }

        byte[] ctx = "ctx".getBytes();
        rn.readIndex(ctx);
        assertThat(rn.hasReady()).isTrue();
        Ready rd = rn.ready();
        assertThat(rd.readStates).isNotNull();
        assertThat(rd.readStates).isNotEmpty();
        assertThat(rd.readStates.get(0).requestCtx()).isEqualTo(ctx);
        rn.advance(rd);
    }

    @Test
    void testRawNodeWithProgress() {
        MemoryStorage s = newTestMemoryStorage(withPeers(1, 2));
        RawNode rn = RawNode.newRawNode(newTestConfig(1, 10, 1, s));
        rn.campaign();
        // Drain
        while (rn.hasReady()) {
            Ready rd = rn.ready();
            if (rd.entries != null && !rd.entries.isEmpty()) s.append(rd.entries);
            rn.advance(rd);
        }

        int[] count = {0};
        rn.withProgress((id, type, pr) -> {
            count[0]++;
        });
        assertThat(count[0]).isEqualTo(2);
    }

    /**
     * TestRawNodeJointAutoLeave ensures that Joint Consensus with auto-leave
     * works: the config change enters a joint state, and when the node becomes
     * leader again it automatically proposes the leave-joint change.
     */
    @Test
    void testRawNodeJointAutoLeave() throws Exception {
        Eraftpb.ConfChangeV2 testCc = Eraftpb.ConfChangeV2.newBuilder()
                .addChanges(Eraftpb.ConfChangeSingle.newBuilder()
                        .setType(Eraftpb.ConfChangeType.ConfChangeAddLearnerNode)
                        .setNodeId(2))
                .setTransition(Eraftpb.ConfChangeTransition.ConfChangeTransitionJointImplicit)
                .build();
        Eraftpb.ConfState expCs = Eraftpb.ConfState.newBuilder()
                .addVoters(1).addVotersOutgoing(1).addLearners(2)
                .setAutoLeave(true)
                .build();
        Eraftpb.ConfState exp2Cs = Eraftpb.ConfState.newBuilder()
                .addVoters(1).addLearners(2).build();

        MemoryStorage s = newTestMemoryStorage(withPeers(1));
        RawNode rawNode = RawNode.newRawNode(newTestConfig(1, 10, 1, s));

        rawNode.campaign();
        boolean proposed = false;
        byte[] ccdata = testCc.toByteArray();

        // Propose the ConfChange, wait until it applies, save the resulting ConfState.
        Eraftpb.ConfState cs = null;
        while (cs == null) {
            Ready rd = rawNode.ready();
            s.append(rd.entries);
            for (Eraftpb.Entry ent : rd.committedEntries) {
                if (ent.getEntryType() == Eraftpb.EntryType.EntryConfChangeV2) {
                    Eraftpb.ConfChangeV2 ccc = Eraftpb.ConfChangeV2.parseFrom(ent.getData());
                    // Force it to step down.
                    rawNode.raft.step(Eraftpb.Message.newBuilder()
                            .setMsgType(Eraftpb.MessageType.MsgHeartbeatResponse)
                            .setFrom(1)
                            .setTerm(rawNode.raft.term + 1)
                            .build());
                    cs = rawNode.applyConfChange(ccc);
                }
            }
            rawNode.advance(rd);
            // Once we are the leader, propose a command and a ConfChange.
            if (!proposed && rawNode.raft.state == RaftStateType.StateLeader) {
                rawNode.propose("somedata".getBytes());
                rawNode.proposeConfChange(testCc);
                proposed = true;
            }
        }

        // Check the resulting conf state
        assertThat(cs).isEqualTo(expCs);
        assertThat(rawNode.raft.pendingConfIndex).isZero();

        // Move the RawNode along. It should not leave joint because it's follower.
        Ready rd = rawNode.readyWithoutAccept();
        assertThat(rd.entries).isEmpty();

        // Make it leader again. It should leave joint automatically after moving apply index.
        rawNode.campaign();
        // Drain multiple readies until we find the auto-leave conf change
        boolean foundAutoLeave = false;
        for (int i = 0; i < 10 && !foundAutoLeave; i++) {
            if (!rawNode.hasReady()) break;
            rd = rawNode.ready();
            s.append(rd.entries);
            for (Eraftpb.Entry ent : rd.entries) {
                if (ent.getEntryType() == Eraftpb.EntryType.EntryConfChangeV2) {
                    Eraftpb.ConfChangeV2 cc = Eraftpb.ConfChangeV2.parseFrom(ent.getData());
                    // Auto-leave: empty ConfChangeV2
                    cs = rawNode.applyConfChange(cc);
                    foundAutoLeave = true;
                }
            }
            rawNode.advance(rd);
        }
        assertThat(foundAutoLeave).isTrue();
        assertThat(cs).isEqualTo(exp2Cs);
    }

    /**
     * TestRawNodeStart tests the bootstrapping flow using ApplySnapshot-based bootstrap.
     */
    @Test
    void testRawNodeStart() {
        MemoryStorage storage = new MemoryStorage();
        // Set first index to 2 by applying a snapshot at index 1
        Eraftpb.Snapshot snap = Eraftpb.Snapshot.newBuilder()
                .setMetadata(Eraftpb.SnapshotMetadata.newBuilder()
                        .setIndex(1).setTerm(0)
                        .setConfState(Eraftpb.ConfState.newBuilder().addVoters(1)))
                .build();
        try {
            storage.applySnapshot(snap);
        } catch (RaftException e) {
            throw new RuntimeException(e);
        }

        RawNode rawNode = RawNode.newRawNode(newTestConfig(1, 10, 1, storage));
        assertThat(rawNode.hasReady()).isFalse();

        rawNode.campaign();
        Ready rd = rawNode.ready();
        storage.append(rd.entries);
        rawNode.advance(rd);

        rawNode.propose("foo".getBytes());
        assertThat(rawNode.hasReady()).isTrue();

        rd = rawNode.ready();
        // Should have the empty entry + "foo" entry
        assertThat(rd.entries).hasSize(2);
        storage.append(rd.entries);
        rawNode.advance(rd);

        assertThat(rawNode.hasReady()).isTrue();
        rd = rawNode.ready();
        assertThat(rd.entries).isEmpty();
        assertThat(rd.mustSync).isFalse();
        // Committed entries should contain the two entries
        assertThat(rd.committedEntries).hasSize(2);
        rawNode.advance(rd);
        assertThat(rawNode.hasReady()).isFalse();
    }

    /**
     * TestRawNodeCommitPaginationAfterRestart regression tests a scenario where
     * the Storage's Entries size limitation is more permissive than Raft's internal one.
     */
    @Test
    void testRawNodeCommitPaginationAfterRestart() {
        // Create a storage that ignores the maxSize hint
        MemoryStorage base = newTestMemoryStorage(withPeers(1));
        IgnoreSizeHintMemStorage s = new IgnoreSizeHintMemStorage(base);

        Eraftpb.HardState persistedHardState = Eraftpb.HardState.newBuilder()
                .setTerm(1).setVote(1).setCommit(10).build();
        s.setHardState(persistedHardState);

        // Create 10 entries
        List<Eraftpb.Entry> ents = new ArrayList<>();
        long size = 0;
        for (int i = 0; i < 10; i++) {
            Eraftpb.Entry ent = Eraftpb.Entry.newBuilder()
                    .setTerm(1).setIndex(i + 1)
                    .setEntryType(Eraftpb.EntryType.EntryNormal)
                    .setData(ByteString.copyFromUtf8("a"))
                    .build();
            ents.add(ent);
            size += ent.getSerializedSize();
        }
        s.setEntries(ents);

        Config cfg = newTestConfig(1, 10, 1, s);
        // Set a MaxSizePerMsg that would normally exclude the last entry
        cfg.maxSizePerMsg = size - ents.get(ents.size() - 1).getSerializedSize() - 1;

        // Add an 11th entry
        ents.add(Eraftpb.Entry.newBuilder()
                .setTerm(1).setIndex(11)
                .setEntryType(Eraftpb.EntryType.EntryNormal)
                .setData(ByteString.copyFromUtf8("boom"))
                .build());
        s.setEntries(ents);

        RawNode rawNode = RawNode.newRawNode(cfg);

        long highestApplied = 0;
        while (highestApplied != 11) {
            Ready rd = rawNode.ready();
            int n = rd.committedEntries.size();
            assertThat(n).as("stopped applying entries at index %d", highestApplied).isGreaterThan(0);
            long next = rd.committedEntries.get(0).getIndex();
            if (highestApplied != 0) {
                assertThat(next).as("attempting to apply index %d after index %d, leaving a gap", next, highestApplied)
                        .isEqualTo(highestApplied + 1);
            }
            highestApplied = rd.committedEntries.get(n - 1).getIndex();
            rawNode.advance(rd);
            rawNode.raft.step(Eraftpb.Message.newBuilder()
                    .setMsgType(Eraftpb.MessageType.MsgHeartbeat)
                    .setTo(1).setFrom(2).setTerm(1).setCommit(11)
                    .build());
        }
    }

    /**
     * TestRawNodeBoundedLogGrowthWithPartition tests that a leader's log is
     * protected from unbounded growth by MaxUncommittedEntriesSize even when
     * partitioned.
     */
    @Test
    void testRawNodeBoundedLogGrowthWithPartition() {
        int maxEntries = 16;
        byte[] data = "testdata".getBytes();
        Eraftpb.Entry testEntry = Eraftpb.Entry.newBuilder()
                .setData(ByteString.copyFrom(data)).build();
        long maxEntrySize = maxEntries * Util.payloadSize(testEntry);

        MemoryStorage s = newTestMemoryStorage(withPeers(1));
        Config cfg = newTestConfig(1, 10, 1, s);
        cfg.maxUncommittedEntriesSize = maxEntrySize;
        RawNode rawNode = RawNode.newRawNode(cfg);

        // Become the leader and apply empty entry.
        rawNode.campaign();
        while (true) {
            Ready rd = rawNode.ready();
            s.append(rd.entries);
            rawNode.advance(rd);
            if (!rd.committedEntries.isEmpty()) {
                break;
            }
        }

        // Simulate a network partition - never committing anything.
        for (int i = 0; i < 1024; i++) {
            rawNode.propose(data);
        }

        // Check the size of leader's uncommitted log tail.
        assertThat(rawNode.raft.uncommittedSize).isEqualTo(maxEntrySize);

        // Recover from the partition.
        Ready rd = rawNode.ready();
        assertThat(rd.entries).hasSize(maxEntries);
        s.append(rd.entries);
        rawNode.advance(rd);

        // Entries are appended, but not applied.
        assertThat(rawNode.raft.uncommittedSize).isEqualTo(maxEntrySize);

        rd = rawNode.ready();
        assertThat(rd.entries).isEmpty();
        assertThat(rd.committedEntries).hasSize(maxEntries);
        rawNode.advance(rd);

        assertThat(rawNode.raft.uncommittedSize).isZero();
    }

    /**
     * TestRawNodeConsumeReady checks that readyWithoutAccept() does not call
     * acceptReady (which resets the messages) but Ready() does.
     */
    @Test
    void testRawNodeConsumeReady() {
        MemoryStorage s = newTestMemoryStorage(withPeers(1));
        Config cfg = newTestConfig(1, 3, 1, s);
        RawNode rn = RawNode.newRawNode(cfg);
        Eraftpb.Message m1 = Eraftpb.Message.newBuilder()
                .setContext(ByteString.copyFromUtf8("foo")).build();
        Eraftpb.Message m2 = Eraftpb.Message.newBuilder()
                .setContext(ByteString.copyFromUtf8("bar")).build();

        // Inject first message, make sure it's visible via readyWithoutAccept.
        rn.raft.msgs.add(m1);
        Ready rd = rn.readyWithoutAccept();
        assertThat(rd.messages).hasSize(1);
        assertThat(rd.messages.get(0)).isEqualTo(m1);
        assertThat(rn.raft.msgs).hasSize(1);
        assertThat(rn.raft.msgs.get(0)).isEqualTo(m1);

        // Now call Ready() which should move the message into the Ready
        rd = rn.ready();
        assertThat(rn.raft.msgs).isEmpty();
        assertThat(rd.messages).hasSize(1);
        assertThat(rd.messages.get(0)).isEqualTo(m1);

        // Add a message to raft to make sure that Advance() doesn't drop it.
        rn.raft.msgs.add(m2);
        rn.advance(rd);
        assertThat(rn.raft.msgs).hasSize(1);
        assertThat(rn.raft.msgs.get(0)).isEqualTo(m2);
    }

    /**
     * A MemoryStorage wrapper that ignores the maxSize hint and returns all entries.
     */
    private static class IgnoreSizeHintMemStorage implements Storage {
        private final MemoryStorage inner;

        IgnoreSizeHintMemStorage(MemoryStorage inner) {
            this.inner = inner;
        }

        @Override
        public void setHardState(Eraftpb.HardState hs) {
            inner.setHardState(hs);
        }

        void setEntries(List<Eraftpb.Entry> ents) {
            inner.setEntries(ents);
        }

        @Override
        public InitialStateResult initialState() {
            return inner.initialState();
        }

        @Override
        public List<Eraftpb.Entry> entries(long lo, long hi, long maxSize) throws RaftException {
            // Ignore maxSize hint - return all entries in range
            return inner.entries(lo, hi, Long.MAX_VALUE);
        }

        @Override
        public long term(long i) throws RaftException {
            return inner.term(i);
        }

        @Override
        public long lastIndex() {
            return inner.lastIndex();
        }

        @Override
        public long firstIndex() {
            return inner.firstIndex();
        }

        @Override
        public Eraftpb.Snapshot snapshot() {
            return inner.snapshot();
        }
    }
}
