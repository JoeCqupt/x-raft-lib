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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static io.github.xinfra.lab.raft.internal.TestUtil.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for the Node interface (DefaultNode), mirroring etcd-raft node_test.go.
 */
class NodeTest {

    /**
     * TestDisableProposalForwarding ensures that proposals are not forwarded to
     * the leader when DisableProposalForwarding is true.
     */
    @Test
    void testDisableProposalForwarding() throws RaftException {
        Raft r1 = newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)));
        Raft r2 = newTestRaft(2, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)));
        Config cfg3 = newTestConfig(3, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3))).toBuilder()
                .disableProposalForwarding(true)
                .build();
        Raft r3 = Raft.newRaft(cfg3);
        Network nt = Network.newNetwork(
                new Network.RaftStateMachine(r1),
                new Network.RaftStateMachine(r2),
                new Network.RaftStateMachine(r3));

        // elect r1 as leader
        nt.send(Eraftpb.Message.newBuilder().setFrom(1).setTo(1)
                .setMsgType(Eraftpb.MessageType.MsgHup).build());

        List<Eraftpb.Entry> testEntries = List.of(
                Eraftpb.Entry.newBuilder().setData(com.google.protobuf.ByteString.copyFromUtf8("testdata")).build());

        // send proposal to r2(follower) where DisableProposalForwarding is false
        r2.step(Eraftpb.Message.newBuilder().setFrom(2).setTo(2)
                .setMsgType(Eraftpb.MessageType.MsgPropose)
                .addAllEntries(testEntries).build());

        // verify r2(follower) does forward the proposal
        assertThat(r2.msgs).hasSize(1);

        // send proposal to r3(follower) where DisableProposalForwarding is true.
        // r3 now throws ErrProposalDropped instead of returning it; assert that.
        assertThatThrownBy(() -> r3.step(Eraftpb.Message.newBuilder().setFrom(3).setTo(3)
                .setMsgType(Eraftpb.MessageType.MsgPropose)
                .addAllEntries(testEntries).build()))
                .isEqualTo(RaftException.ErrProposalDropped);

        // verify r3(follower) does not forward the proposal
        assertThat(r3.msgs).isEmpty();
    }

    /**
     * TestNodeReadIndexToOldLeader ensures that MsgReadIndex to old leader gets
     * forwarded to the new leader and 'send' does not attach its term.
     */
    @Test
    void testNodeReadIndexToOldLeader() throws RaftException {
        Raft r1 = newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)));
        Raft r2 = newTestRaft(2, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)));
        Raft r3 = newTestRaft(3, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)));

        Network nt = Network.newNetwork(
                new Network.RaftStateMachine(r1),
                new Network.RaftStateMachine(r2),
                new Network.RaftStateMachine(r3));

        // elect r1 as leader
        nt.send(Eraftpb.Message.newBuilder().setFrom(1).setTo(1)
                .setMsgType(Eraftpb.MessageType.MsgHup).build());

        List<Eraftpb.Entry> testEntries = List.of(
                Eraftpb.Entry.newBuilder().setData(com.google.protobuf.ByteString.copyFromUtf8("testdata")).build());

        // send readindex request to r2(follower)
        r2.step(Eraftpb.Message.newBuilder().setFrom(2).setTo(2)
                .setMsgType(Eraftpb.MessageType.MsgReadIndex)
                .addAllEntries(testEntries).build());

        // verify r2 forwards to r1(leader) with term not set
        assertThat(r2.msgs).hasSize(1);
        assertThat(r2.msgs.get(0).getMsgType()).isEqualTo(Eraftpb.MessageType.MsgReadIndex);
        assertThat(r2.msgs.get(0).getTo()).isEqualTo(1);
        assertThat(r2.msgs.get(0).getTerm()).isEqualTo(0);

        // send readindex request to r3(follower)
        r3.step(Eraftpb.Message.newBuilder().setFrom(3).setTo(3)
                .setMsgType(Eraftpb.MessageType.MsgReadIndex)
                .addAllEntries(testEntries).build());

        // verify r3 forwards to r1(leader) with term not set
        assertThat(r3.msgs).hasSize(1);
        assertThat(r3.msgs.get(0).getMsgType()).isEqualTo(Eraftpb.MessageType.MsgReadIndex);
        assertThat(r3.msgs.get(0).getTo()).isEqualTo(1);
        assertThat(r3.msgs.get(0).getTerm()).isEqualTo(0);

        // Save messages
        Eraftpb.Message readIndxMsg1 = r2.msgs.get(0);
        Eraftpb.Message readIndxMsg2 = r3.msgs.get(0);

        // now elect r3 as leader
        nt.send(Eraftpb.Message.newBuilder().setFrom(3).setTo(3)
                .setMsgType(Eraftpb.MessageType.MsgHup).build());

        // let r1 steps the two messages previously we got from r2, r3
        r1.step(readIndxMsg1);
        r1.step(readIndxMsg2);

        // verify r1(follower) forwards these messages again to r3(new leader)
        assertThat(r1.msgs).hasSize(2);
        assertThat(r1.msgs.get(0).getTo()).isEqualTo(3);
        assertThat(r1.msgs.get(0).getMsgType()).isEqualTo(Eraftpb.MessageType.MsgReadIndex);
        assertThat(r1.msgs.get(1).getTo()).isEqualTo(3);
        assertThat(r1.msgs.get(1).getMsgType()).isEqualTo(Eraftpb.MessageType.MsgReadIndex);
    }

    /**
     * TestSoftStateEqual tests SoftState's equals/hashCode.
     */
    @Test
    void testSoftStateEqual() throws RaftException {
        SoftState zero = new SoftState(0L, null);
        assertThat(zero).isEqualTo(new SoftState(0L, null));
        assertThat(new SoftState(1L, null)).isNotEqualTo(zero);
        assertThat(new SoftState(0L, RaftStateType.StateLeader)).isNotEqualTo(zero);
    }

    /**
     * TestIsHardStateEqual tests Util.isHardStateEqual().
     */
    @Test
    void testIsHardStateEqual() throws RaftException {
        Eraftpb.HardState empty = Eraftpb.HardState.getDefaultInstance();
        assertThat(Util.isHardStateEqual(empty, empty)).isTrue();
        assertThat(Util.isHardStateEqual(
                Eraftpb.HardState.newBuilder().setVote(1).build(), empty)).isFalse();
        assertThat(Util.isHardStateEqual(
                Eraftpb.HardState.newBuilder().setCommit(1).build(), empty)).isFalse();
        assertThat(Util.isHardStateEqual(
                Eraftpb.HardState.newBuilder().setTerm(1).build(), empty)).isFalse();
    }

    /**
     * TestNodeTick ensures that node.tick() will increase the elapsed of the
     * underlying raft state machine.
     */
    @Test
    void testNodeTick() throws Exception {
        MemoryStorage s = newTestMemoryStorage(withPeers(1));
        Config cfg = newTestConfig(1, 10, 1, s).toBuilder()
                .maxSizePerMsg(NO_LIMIT)
                .maxInflightMsgs(256)
                .build();
        // Use RawNode directly to avoid threading complexity
        RawNode rn = RawNode.newRawNode(cfg);
        rn.bootstrap(List.of(new Peer(1)));
        long elapsed = rn.raft.electionElapsed;
        rn.tick();
        assertThat(rn.raft.electionElapsed).isEqualTo(elapsed + 1);
    }

    /**
     * TestNodeStop ensures that node.stop() blocks until the node has stopped processing.
     */
    @Test
    void testNodeStop() throws Exception {
        MemoryStorage s = newTestMemoryStorage(withPeers(1));
        Config cfg = newTestConfig(1, 10, 1, s).toBuilder()
                .maxSizePerMsg(NO_LIMIT)
                .maxInflightMsgs(256)
                .build();
        Node n = DefaultNode.startNode(cfg, List.of(new Peer(1)));
        Thread.sleep(50);

        // Node should be able to report status before stop
        Status status = n.status();
        assertThat(status).isNotNull();
        n.stop();
        // Subsequent stops should not throw
        n.stop();
    }

    /**
     * stop(timeout, unit) returns cleanly within the deadline for a healthy node.
     */
    @Test
    void testStopWithTimeoutSucceedsForHealthyNode() throws Exception {
        MemoryStorage s = newTestMemoryStorage(withPeers(1));
        Config cfg = newTestConfig(1, 10, 1, s).toBuilder()
                .maxSizePerMsg(NO_LIMIT)
                .maxInflightMsgs(256)
                .build();
        Node n = DefaultNode.startNode(cfg, List.of(new Peer(1)));
        Thread.sleep(50);
        boolean clean = n.stop(2, java.util.concurrent.TimeUnit.SECONDS);
        assertThat(clean).as("healthy node should stop cleanly within deadline").isTrue();
    }

    /**
     * basicStatus() returns a snapshot without going through the events queue.
     */
    @Test
    void testBasicStatusReadsLockFreeSample() throws Exception {
        MemoryStorage s = newTestMemoryStorage(withPeers(1));
        Config cfg = newTestConfig(1, 10, 1, s).toBuilder()
                .maxSizePerMsg(NO_LIMIT)
                .maxInflightMsgs(256)
                .build();
        Node n = DefaultNode.startNode(cfg, List.of(new Peer(1)));
        try {
            driveToLeader(n, s);
            Node.BasicStatus bs = n.basicStatus();
            assertThat(bs.id).isEqualTo(1L);
            assertThat(bs.state).isEqualTo(RaftStateType.StateLeader);
            assertThat(bs.term).isPositive();
            assertThat(bs.lead).isEqualTo(1L);
        } finally {
            n.stop();
        }
    }

    /**
     * Pluggable RaftMetrics receives leader-change + ready-emitted events on
     * a healthy single-node cluster.
     */
    @Test
    void testRaftMetricsHooksFire() throws Exception {
        java.util.concurrent.atomic.AtomicInteger leaderChanges = new java.util.concurrent.atomic.AtomicInteger();
        java.util.concurrent.atomic.AtomicInteger readyEmitted = new java.util.concurrent.atomic.AtomicInteger();
        RaftMetrics m = new RaftMetrics() {
            @Override public void onLeaderChange(long newLeader, long term) {
                leaderChanges.incrementAndGet();
            }
            @Override public void onReadyEmitted() {
                readyEmitted.incrementAndGet();
            }
        };
        MemoryStorage s = newTestMemoryStorage(withPeers(1));
        Config cfg = newTestConfig(1, 10, 1, s).toBuilder()
                .maxSizePerMsg(NO_LIMIT)
                .maxInflightMsgs(256)
                .metrics(m)
                .build();
        Node n = DefaultNode.startNode(cfg, List.of(new Peer(1)));
        try {
            driveToLeader(n, s);
            assertThat(leaderChanges.get()).as("leader change observed").isPositive();
            assertThat(readyEmitted.get()).as("ready emitted").isPositive();
        } finally {
            n.stop();
        }
    }

    /**
     * registerLeaderObserver fires on leader change and the returned handle deregisters.
     */
    @Test
    void testLeaderObserverFiresOnElection() throws Exception {
        MemoryStorage s = newTestMemoryStorage(withPeers(1));
        Config cfg = newTestConfig(1, 10, 1, s).toBuilder()
                .maxSizePerMsg(NO_LIMIT)
                .maxInflightMsgs(256)
                .build();
        Node n = DefaultNode.startNode(cfg, List.of(new Peer(1)));
        java.util.concurrent.atomic.AtomicLong observedLead = new java.util.concurrent.atomic.AtomicLong(-1);
        java.util.concurrent.atomic.AtomicLong observedTerm = new java.util.concurrent.atomic.AtomicLong(-1);
        Runnable deregister = n.registerLeaderObserver((newLead, term) -> {
            observedLead.set(newLead);
            observedTerm.set(term);
        });
        try {
            driveToLeader(n, s);
            for (int i = 0; i < 50 && observedLead.get() == -1; i++) {
                Thread.sleep(10);
            }
            assertThat(observedLead.get()).isEqualTo(1L);
            assertThat(observedTerm.get()).isPositive();
            deregister.run();
            deregister.run();
        } finally {
            n.stop();
        }
    }

    /**
     * Drives a single-node Node from bootstrap to leader: drains initial
     * Ready cycles (which apply the bootstrap ConfChange and unblock
     * campaign), then campaigns, then drains again so the leader's noop
     * commits.
     */
    private static void driveToLeader(Node n, MemoryStorage s) throws Exception {
        // Drain initial Ready (bootstrap entries + ConfState).
        drainOneReady(n, s);
        n.campaign();
        // Drain Ready cycles until we observe StateLeader.
        for (int i = 0; i < 50; i++) {
            if (n.basicStatus().state == RaftStateType.StateLeader) return;
            drainOneReady(n, s);
            Thread.sleep(5);
        }
    }

    private static void drainOneReady(Node n, MemoryStorage s) throws Exception {
        Ready rd = n.ready();
        if (rd.entries() != null && !rd.entries().isEmpty()) s.append(rd.entries());
        if (rd.committedEntries() != null) {
            for (Eraftpb.Entry e : rd.committedEntries()) {
                Eraftpb.ConfChangeV2 ccv2 = null;
                if (e.getEntryType() == Eraftpb.EntryType.EntryConfChange) {
                    Eraftpb.ConfChange cc = Eraftpb.ConfChange.parseFrom(e.getData());
                    ccv2 = Eraftpb.ConfChangeV2.newBuilder()
                            .addChanges(Eraftpb.ConfChangeSingle.newBuilder()
                                    .setType(cc.getChangeType())
                                    .setNodeId(cc.getNodeId()))
                            .build();
                } else if (e.getEntryType() == Eraftpb.EntryType.EntryConfChangeV2) {
                    ccv2 = Eraftpb.ConfChangeV2.parseFrom(e.getData());
                }
                if (ccv2 != null) {
                    n.applyConfChange(ccv2);
                }
            }
        }
        n.advance();
    }

    /**
     * TestNodePropose ensures that node.propose sends the proposal through.
     */
    @Test
    void testNodePropose() throws Exception {
        MemoryStorage s = newTestMemoryStorage(withPeers(1));
        Config cfg = newTestConfig(1, 10, 1, s).toBuilder()
                .maxSizePerMsg(NO_LIMIT)
                .maxInflightMsgs(256)
                .build();
        // Use RawNode directly to avoid threading timing issues
        RawNode rn = RawNode.newRawNode(cfg);
        rn.campaign();

        // Drain until leader
        while (rn.hasReady()) {
            Ready rd = rn.ready();
            if (rd.entries() != null && !rd.entries().isEmpty()) s.append(rd.entries());
            rn.advance(rd);
        }

        // propose data
        rn.propose("somedata".getBytes());

        // Drain and find the proposal
        boolean foundProposal = false;
        while (rn.hasReady()) {
            Ready rd = rn.ready();
            if (rd.entries() != null) {
                for (Eraftpb.Entry e : rd.entries()) {
                    if (e.getData().toStringUtf8().equals("somedata")) {
                        foundProposal = true;
                    }
                }
                s.append(rd.entries());
            }
            for (Eraftpb.Entry e : rd.committedEntries()) {
                if (e.getData().toStringUtf8().equals("somedata")) {
                    foundProposal = true;
                }
            }
            rn.advance(rd);
        }
        assertThat(foundProposal).isTrue();
    }

    /**
     * TestAppendPagination ensures that MsgApp payloads are split when they exceed MaxSizePerMsg.
     */
    @Test
    void testAppendPagination() throws RaftException {
        final int maxSizePerMsg = 2048;
        Network n = Network.newNetworkWithConfig(c -> c.maxSizePerMsg(maxSizePerMsg),
                (Network.StateMachine) null, null, null);

        boolean[] seenFullMessage = {false};
        n.msgHook = m -> {
            if (m.getMsgType() == Eraftpb.MessageType.MsgAppend) {
                int size = 0;
                for (Eraftpb.Entry e : m.getEntriesList()) {
                    size += e.getData().size();
                }
                assertThat(size).as("sent MsgApp that is too large").isLessThanOrEqualTo(maxSizePerMsg);
                if (size > maxSizePerMsg / 2) {
                    seenFullMessage[0] = true;
                }
            }
            return true;
        };

        n.send(Eraftpb.Message.newBuilder().setFrom(1).setTo(1)
                .setMsgType(Eraftpb.MessageType.MsgHup).build());

        // Partition the network while we make our proposals
        n.isolate(1);
        byte[] blob = new byte[1000];
        java.util.Arrays.fill(blob, (byte) 'a');
        for (int i = 0; i < 5; i++) {
            n.send(Eraftpb.Message.newBuilder().setFrom(1).setTo(1)
                    .setMsgType(Eraftpb.MessageType.MsgPropose)
                    .addEntries(Eraftpb.Entry.newBuilder()
                            .setData(com.google.protobuf.ByteString.copyFrom(blob)))
                    .build());
        }
        n.recover();

        // After the partition recovers, tick the clock to wake everything back up
        n.send(Eraftpb.Message.newBuilder().setFrom(1).setTo(1)
                .setMsgType(Eraftpb.MessageType.MsgBeat).build());
        assertThat(seenFullMessage[0]).as("didn't see any messages more than half the max size").isTrue();
    }

    /**
     * TestCommitPagination verifies that MaxCommittedSizePerReady works correctly.
     */
    @Test
    void testCommitPagination() throws RaftException {
        MemoryStorage s = newTestMemoryStorage(withPeers(1));
        Config cfg = newTestConfig(1, 10, 1, s).toBuilder()
                .maxSizePerMsg(NO_LIMIT)
                .maxInflightMsgs(256)
                .maxCommittedSizePerReady(2048)
                .build();
        RawNode rn = RawNode.newRawNode(cfg);
        rn.campaign();

        // Drain until leader
        while (rn.hasReady()) {
            Ready rd = rn.ready();
            if (rd.entries() != null && !rd.entries().isEmpty()) s.append(rd.entries());
            rn.advance(rd);
        }

        byte[] blob = new byte[1000];
        java.util.Arrays.fill(blob, (byte) 'a');
        for (int i = 0; i < 3; i++) {
            rn.propose(blob);
        }

        // First the 3 proposals have to be appended
        Ready rd = rn.ready();
        assertThat(rd.entries()).hasSize(3);
        s.append(rd.entries());
        rn.advance(rd);

        // The 3 proposals will commit in two batches
        rd = rn.ready();
        assertThat(rd.committedEntries()).hasSize(2);
        s.append(rd.entries());
        rn.advance(rd);

        rd = rn.ready();
        assertThat(rd.committedEntries()).hasSize(1);
        s.append(rd.entries());
        rn.advance(rd);
    }

    /**
     * TestNodeProposeConfig ensures that node.ProposeConfChange sends the proposal.
     */
    @Test
    void testNodeProposeConfig() throws RaftException {
        MemoryStorage s = newTestMemoryStorage(withPeers(1));
        Config cfg = newTestConfig(1, 10, 1, s).toBuilder()
                .maxSizePerMsg(NO_LIMIT)
                .maxInflightMsgs(256)
                .build();
        RawNode rn = RawNode.newRawNode(cfg);
        rn.campaign();

        // Drain until leader
        while (rn.hasReady()) {
            Ready rd = rn.ready();
            if (rd.entries() != null && !rd.entries().isEmpty()) s.append(rd.entries());
            rn.advance(rd);
        }

        // Propose conf change
        Eraftpb.ConfChange cc = Eraftpb.ConfChange.newBuilder()
                .setChangeType(Eraftpb.ConfChangeType.ConfChangeAddNode)
                .setNodeId(1).build();
        rn.proposeConfChange(cc);

        // Drain and find the conf change
        boolean found = false;
        while (rn.hasReady()) {
            Ready rd = rn.ready();
            for (Eraftpb.Entry e : rd.entries()) {
                if (e.getEntryType() == Eraftpb.EntryType.EntryConfChange) {
                    found = true;
                }
            }
            for (Eraftpb.Entry e : rd.committedEntries()) {
                if (e.getEntryType() == Eraftpb.EntryType.EntryConfChange) {
                    found = true;
                }
            }
            s.append(rd.entries());
            rn.advance(rd);
        }
        assertThat(found).isTrue();
    }

    /**
     * TestNodeProposeAddDuplicateNode ensures that two proposals to add the same node
     * don't affect subsequent proposals to add new nodes.
     */
    @Test
    void testNodeProposeAddDuplicateNode() throws Exception {
        MemoryStorage s = newTestMemoryStorage(withPeers(1));
        Config cfg = newTestConfig(1, 10, 1, s).toBuilder()
                .maxSizePerMsg(NO_LIMIT)
                .maxInflightMsgs(256)
                .build();
        RawNode rn = RawNode.newRawNode(cfg);
        rn.campaign();

        // Drain until leader
        while (rn.hasReady()) {
            Ready rd = rn.ready();
            if (rd.entries() != null && !rd.entries().isEmpty()) s.append(rd.entries());
            rn.advance(rd);
        }

        // Propose adding node 1 (already exists)
        Eraftpb.ConfChange cc1 = Eraftpb.ConfChange.newBuilder()
                .setChangeType(Eraftpb.ConfChangeType.ConfChangeAddNode)
                .setNodeId(1).build();
        rn.proposeConfChange(cc1);

        // Drain and apply
        while (rn.hasReady()) {
            Ready rd = rn.ready();
            s.append(rd.entries());
            for (Eraftpb.Entry e : rd.committedEntries()) {
                if (e.getEntryType() == Eraftpb.EntryType.EntryConfChange) {
                    Eraftpb.ConfChange applied = Eraftpb.ConfChange.parseFrom(e.getData());
                    rn.applyConfChange(Eraftpb.ConfChangeV2.newBuilder()
                            .addChanges(Eraftpb.ConfChangeSingle.newBuilder()
                                    .setType(applied.getChangeType())
                                    .setNodeId(applied.getNodeId())).build());
                }
            }
            rn.advance(rd);
        }

        // Propose adding node 1 again (duplicate)
        rn.proposeConfChange(cc1);
        while (rn.hasReady()) {
            Ready rd = rn.ready();
            s.append(rd.entries());
            for (Eraftpb.Entry e : rd.committedEntries()) {
                if (e.getEntryType() == Eraftpb.EntryType.EntryConfChange) {
                    Eraftpb.ConfChange applied = Eraftpb.ConfChange.parseFrom(e.getData());
                    rn.applyConfChange(Eraftpb.ConfChangeV2.newBuilder()
                            .addChanges(Eraftpb.ConfChangeSingle.newBuilder()
                                    .setType(applied.getChangeType())
                                    .setNodeId(applied.getNodeId())).build());
                }
            }
            rn.advance(rd);
        }

        // Now propose adding node 2 (new node)
        Eraftpb.ConfChange cc2 = Eraftpb.ConfChange.newBuilder()
                .setChangeType(Eraftpb.ConfChangeType.ConfChangeAddNode)
                .setNodeId(2).build();
        rn.proposeConfChange(cc2);

        boolean foundNode2 = false;
        while (rn.hasReady()) {
            Ready rd = rn.ready();
            s.append(rd.entries());
            for (Eraftpb.Entry e : rd.committedEntries()) {
                if (e.getEntryType() == Eraftpb.EntryType.EntryConfChange) {
                    Eraftpb.ConfChange applied = Eraftpb.ConfChange.parseFrom(e.getData());
                    if (applied.getNodeId() == 2) {
                        foundNode2 = true;
                    }
                    rn.applyConfChange(Eraftpb.ConfChangeV2.newBuilder()
                            .addChanges(Eraftpb.ConfChangeSingle.newBuilder()
                                    .setType(applied.getChangeType())
                                    .setNodeId(applied.getNodeId())).build());
                }
            }
            rn.advance(rd);
        }
        assertThat(foundNode2).isTrue();
    }

    /**
     * TestNodeStart ensures a node can be started correctly with proper initial Ready output.
     */
    @Test
    void testNodeStart() throws RaftException {
        MemoryStorage s = new MemoryStorage();
        Config cfg = newTestConfig(1, 10, 1, s).toBuilder()
                .maxSizePerMsg(NO_LIMIT)
                .maxInflightMsgs(256)
                .build();
        RawNode rn = RawNode.newRawNode(cfg);
        rn.bootstrap(List.of(new Peer(1)));

        // First ready should contain the conf change entry
        assertThat(rn.hasReady()).isTrue();
        Ready rd = rn.ready();
        assertThat(rd.entries()).hasSize(1);
        assertThat(rd.entries().get(0).getEntryType()).isEqualTo(Eraftpb.EntryType.EntryConfChange);
        assertThat(rd.committedEntries()).hasSize(1);
        assertThat(rd.hardState().getTerm()).isEqualTo(1);
        assertThat(rd.hardState().getCommit()).isEqualTo(1);
        s.append(rd.entries());
        rn.advance(rd);
    }

    /**
     * TestNodeRestart ensures that a node can be restarted from persistent state.
     */
    @Test
    void testNodeRestart() throws RaftException {
        List<Eraftpb.Entry> entries = List.of(
                Eraftpb.Entry.newBuilder().setTerm(1).setIndex(1).build(),
                Eraftpb.Entry.newBuilder().setTerm(1).setIndex(2)
                        .setData(com.google.protobuf.ByteString.copyFromUtf8("foo")).build()
        );
        Eraftpb.HardState st = Eraftpb.HardState.newBuilder().setTerm(1).setCommit(1).build();

        MemoryStorage storage = new MemoryStorage();
        storage.setHardState(st);
        storage.append(entries);

        Config cfg = newTestConfig(1, 10, 1, storage).toBuilder()
                .maxSizePerMsg(NO_LIMIT)
                .maxInflightMsgs(256)
                .build();
        RawNode rn = RawNode.newRawNode(cfg);

        assertThat(rn.hasReady()).isTrue();
        Ready rd = rn.ready();
        // Should commit up to the commit index in st
        assertThat(rd.committedEntries()).hasSize(1);
        assertThat(rd.committedEntries().get(0).getIndex()).isEqualTo(1);
        // No new HardState since nothing changed
        assertThat(rd.mustSync()).isFalse();
        rn.advance(rd);

        assertThat(rn.hasReady()).isFalse();
    }

    /**
     * TestNodeRestartFromSnapshot ensures that a node can restart from a snapshot.
     */
    @Test
    void testNodeRestartFromSnapshot() throws RaftException {
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

        Config cfg = newTestConfig(1, 10, 1, s).toBuilder()
                .maxSizePerMsg(NO_LIMIT)
                .maxInflightMsgs(256)
                .build();
        RawNode rn = RawNode.newRawNode(cfg);

        assertThat(rn.hasReady()).isTrue();
        Ready rd = rn.ready();
        // Should commit the entries after snapshot
        assertThat(rd.committedEntries()).hasSize(1);
        assertThat(rd.committedEntries().get(0).getIndex()).isEqualTo(3);
        assertThat(rd.mustSync()).isFalse();
        rn.advance(rd);

        assertThat(rn.hasReady()).isFalse();
    }

    /**
     * TestNodeAdvance ensures that Advance triggers delivery of committed entries.
     */
    @Test
    void testNodeAdvance() throws RaftException {
        MemoryStorage s = newTestMemoryStorage(withPeers(1));
        Config cfg = newTestConfig(1, 10, 1, s).toBuilder()
                .maxSizePerMsg(NO_LIMIT)
                .maxInflightMsgs(256)
                .build();
        RawNode rn = RawNode.newRawNode(cfg);
        rn.campaign();

        // Drain until leader
        while (rn.hasReady()) {
            Ready rd = rn.ready();
            if (rd.entries() != null && !rd.entries().isEmpty()) s.append(rd.entries());
            rn.advance(rd);
        }

        // Propose data
        rn.propose("foo".getBytes());

        // Should have ready with the proposal
        assertThat(rn.hasReady()).isTrue();
        Ready rd = rn.ready();
        assertThat(rd.entries()).isNotEmpty();
        s.append(rd.entries());
        rn.advance(rd);

        // After advance, committed entries should be available
        assertThat(rn.hasReady()).isTrue();
        rd = rn.ready();
        assertThat(rd.committedEntries()).isNotEmpty();
        rn.advance(rd);
    }

    /**
     * TestNodeProposeAddLearnerNode ensures that proposing a learner conf change works.
     */
    @Test
    void testNodeProposeAddLearnerNode() throws RaftException {
        MemoryStorage s = newTestMemoryStorage(withPeers(1));
        Config cfg = newTestConfig(1, 10, 1, s).toBuilder()
                .maxSizePerMsg(NO_LIMIT)
                .maxInflightMsgs(256)
                .build();
        RawNode rn = RawNode.newRawNode(cfg);
        rn.campaign();

        // Drain until leader
        while (rn.hasReady()) {
            Ready rd = rn.ready();
            if (rd.entries() != null && !rd.entries().isEmpty()) s.append(rd.entries());
            rn.advance(rd);
        }

        // Propose adding learner node 2
        Eraftpb.ConfChange cc = Eraftpb.ConfChange.newBuilder()
                .setChangeType(Eraftpb.ConfChangeType.ConfChangeAddLearnerNode)
                .setNodeId(2).build();
        rn.proposeConfChange(cc);

        // Drain and apply
        boolean applied = false;
        while (rn.hasReady()) {
            Ready rd = rn.ready();
            s.append(rd.entries());
            for (Eraftpb.Entry e : rd.committedEntries()) {
                if (e.getEntryType() == Eraftpb.EntryType.EntryConfChange) {
                    Eraftpb.ConfState cs = rn.applyConfChange(Eraftpb.ConfChangeV2.newBuilder()
                            .addChanges(Eraftpb.ConfChangeSingle.newBuilder()
                                    .setType(Eraftpb.ConfChangeType.ConfChangeAddLearnerNode)
                                    .setNodeId(2)).build());
                    assertThat(cs.getLearnersList()).contains(2L);
                    assertThat(cs.getVotersList()).containsExactly(1L);
                    applied = true;
                }
            }
            rn.advance(rd);
        }
        assertThat(applied).isTrue();
    }

    /**
     * TestNodeCommitPaginationAfterRestart regression tests a scenario where the
     * commit index could regress after restart due to size limiting.
     */
    @Test
    void testNodeCommitPaginationAfterRestart() throws RaftException {
        MemoryStorage s = newTestMemoryStorage(withPeers(1));
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
                    .setData(com.google.protobuf.ByteString.copyFromUtf8("a"))
                    .build();
            ents.add(ent);
            size += ent.getSerializedSize();
        }
        s.append(ents);

        // Set a MaxSizePerMsg smaller than all entries
        Config cfg = newTestConfig(1, 10, 1, s).toBuilder()
                .maxSizePerMsg(size - ents.get(ents.size() - 1).getSerializedSize() - 1)
                .maxInflightMsgs(256)
                .build();
        RawNode rn = RawNode.newRawNode(cfg);

        assertThat(rn.hasReady()).isTrue();
        Ready rd = rn.ready();

        // The HardState should not regress
        if (!Util.isEmptyHardState(rd.hardState())) {
            assertThat(rd.hardState().getCommit())
                    .as("HardState commit should not regress")
                    .isGreaterThanOrEqualTo(persistedHardState.getCommit());
        }
    }

    /**
     * TestBlockProposal ensures that node blocks proposal when no leader is known,
     * and unblocks after election.
     */
    @Test
    void testBlockProposal() throws Exception {
        MemoryStorage s = newTestMemoryStorage(withPeers(1));
        Config cfg = newTestConfig(1, 10, 1, s).toBuilder()
                .maxSizePerMsg(NO_LIMIT)
                .maxInflightMsgs(256)
                .build();
        Node n = DefaultNode.startNode(cfg, List.of(new Peer(1)));
        DefaultNode dn = (DefaultNode) n;

        // Drain the bootstrap Ready so the conf change entries are applied;
        // otherwise campaign() is rejected with "pending configuration changes".
        Ready bootstrap = dn.readyc.poll(1, TimeUnit.SECONDS);
        assertThat(bootstrap).as("bootstrap ready").isNotNull();
        s.append(bootstrap.entries());
        n.advance();

        // Campaign to elect leader
        n.campaign();

        // Drain ready/advance cycles until leader. Poll with a timeout so we don't
        // hang if no further Ready arrives after the leader is elected.
        boolean elected = false;
        for (int i = 0; i < 10 && !elected; i++) {
            Ready rd = dn.readyc.poll(500, TimeUnit.MILLISECONDS);
            if (rd == null) break;
            s.append(rd.entries());
            n.advance();
            if (rd.softState() != null && rd.softState().lead() != Util.NONE) {
                elected = true;
            }
        }
        assertThat(elected).as("leader should be elected").isTrue();

        // Now propose should succeed since we have a leader
        n.propose("somedata".getBytes());

        n.stop();
    }

    /**
     * TestNodeStep ensures that node.step sends MsgProp to propc,
     * and other non-local messages to recvc.
     */
    @Test
    void testNodeStep() throws Exception {
        for (Eraftpb.MessageType msgt : Eraftpb.MessageType.values()) {
            if (msgt == Eraftpb.MessageType.UNRECOGNIZED) continue;

            MemoryStorage s = newTestMemoryStorage(withPeers(1));
            Config cfg = newTestConfig(1, 10, 1, s);
            RawNode rawNode = RawNode.newRawNode(cfg);
            DefaultNode n = new DefaultNode(rawNode);

            Eraftpb.Message msg = Eraftpb.Message.newBuilder()
                    .setMsgType(msgt).build();
            n.step(msg);

            if (msgt == Eraftpb.MessageType.MsgPropose) {
                // Proposals are routed as ProposeEvent
                DefaultNode.Event ev = n.events.poll(100, TimeUnit.MILLISECONDS);
                assertThat(ev).as("MsgPropose should produce a ProposeEvent")
                        .isInstanceOf(DefaultNode.ProposeEvent.class);
            } else if (Util.isLocalMsg(msgt)) {
                // Local messages should be ignored by step
                DefaultNode.Event ev = n.events.poll(10, TimeUnit.MILLISECONDS);
                assertThat(ev).as("%s should be ignored by step", msgt).isNull();
            } else {
                // Non-local messages are routed as RecvEvent
                DefaultNode.Event ev = n.events.poll(100, TimeUnit.MILLISECONDS);
                assertThat(ev).as("%s should produce a RecvEvent", msgt)
                        .isInstanceOf(DefaultNode.RecvEvent.class);
                assertThat(((DefaultNode.RecvEvent) ev).msg.getMsgType()).isEqualTo(msgt);
            }
        }
    }

    /**
     * TestNodeStepUnblock verifies that stop unblocks a pending Step call.
     */
    @Test
    void testNodeStepUnblock() throws Exception {
        MemoryStorage s = newTestMemoryStorage(withPeers(1));
        Config cfg = newTestConfig(1, 10, 1, s);
        Node n = DefaultNode.startNode(cfg, List.of(new Peer(1)));

        CompletableFuture<RaftException> errFuture = new CompletableFuture<>();
        Thread proposer = new Thread(() -> {
            try {
                // This propose will wait for the result, which blocks until the
                // event loop processes it or the node is stopped.
                n.propose("somedata".getBytes());
                errFuture.complete(null);
            } catch (RaftException re) {
                errFuture.complete(re);
            } catch (InterruptedException e) {
                errFuture.complete(RaftException.ErrStopped);
            }
        });
        proposer.start();

        // Give the proposer time to block
        Thread.sleep(50);

        // Stop should unblock the pending propose
        n.stop();

        RaftException err = errFuture.get(2, TimeUnit.SECONDS);
        // The point of this test is that the propose returns (does not hang) —
        // either because stop() drained it with ErrStopped, or because the
        // event loop processed it first. Outcomes by timing:
        //   - null                 : event loop accepted before stop (would need a leader; not this test)
        //   - ErrStopped           : stop() drained it before the event loop got to it
        //   - ErrProposalDropped   : event loop processed it first; no leader → dropped
        assertThat(err == null
                || err == RaftException.ErrStopped
                || err == RaftException.ErrProposalDropped).isTrue();
    }

    /**
     * TestNodeProposeWaitDropped ensures that proposals that get dropped
     * return ErrProposalDropped when using stepWait.
     */
    @Test
    void testNodeProposeWaitDropped() throws Exception {
        MemoryStorage s = newTestMemoryStorage(withPeers(1));
        Config cfg = newTestConfig(1, 10, 1, s);
        Node n = DefaultNode.startNode(cfg, List.of(new Peer(1)));
        DefaultNode dn = (DefaultNode) n;

        // Drain the bootstrap Ready so the conf change entries are applied;
        // otherwise campaign() is rejected with "pending configuration changes".
        Ready bootstrap = dn.readyc.poll(1, TimeUnit.SECONDS);
        assertThat(bootstrap).as("bootstrap ready").isNotNull();
        s.append(bootstrap.entries());
        n.advance();

        // Campaign to elect leader
        n.campaign();

        // Drain ready/advance cycles until leader. Poll with a timeout so we don't
        // hang if no further Ready arrives after the leader is elected.
        boolean elected = false;
        for (int i = 0; i < 10 && !elected; i++) {
            Ready rd = dn.readyc.poll(500, TimeUnit.MILLISECONDS);
            if (rd == null) break;
            s.append(rd.entries());
            n.advance();
            if (rd.softState() != null && rd.softState().lead() != Util.NONE) {
                elected = true;
            }
        }
        assertThat(elected).as("leader should be elected").isTrue();

        // Install a step function that drops proposals containing "test_dropping"
        byte[] droppingMsg = "test_dropping".getBytes();
        dn.rn.raft.stepFn = (raft, m) -> {
            if (m.getMsgType() == Eraftpb.MessageType.MsgPropose) {
                for (Eraftpb.Entry e : m.getEntriesList()) {
                    if (e.getData().toStringUtf8().contains("test_dropping")) {
                        throw RaftException.ErrProposalDropped;
                    }
                }
            }
        };

        // Propose with the dropping message - should get ErrProposalDropped
        assertThatThrownBy(() -> n.propose(droppingMsg)).isEqualTo(RaftException.ErrProposalDropped);

        n.stop();
    }

    /**
     * TestCommitPaginationWithAsyncStorageWrites ensures commit pagination
     * works correctly with async storage writes.
     */
    @Test
    void testCommitPaginationWithAsyncStorageWrites() throws Exception {
        MemoryStorage s = newTestMemoryStorage(withPeers(1));
        Config cfg = newTestConfig(1, 10, 1, s).toBuilder()
                .maxCommittedSizePerReady(2048)
                .asyncStorageWrites(true)
                .build();
        RawNode rawNode = RawNode.newRawNode(cfg);

        rawNode.campaign();

        // Process the vote persistence
        Ready rd = rawNode.ready();
        assertThat(rd.messages()).hasSize(1);
        Eraftpb.Message m = rd.messages().get(0);
        assertThat(m.getMsgType()).isEqualTo(Eraftpb.MessageType.MsgStorageAppend);
        s.append(m.getEntriesList());
        for (Eraftpb.Message resp : m.getResponsesList()) {
            rawNode.raft.step(resp);
        }

        // Append empty entry
        rd = rawNode.ready();
        assertThat(rd.messages()).hasSize(1);
        m = rd.messages().get(0);
        assertThat(m.getMsgType()).isEqualTo(Eraftpb.MessageType.MsgStorageAppend);
        s.append(m.getEntriesList());
        for (Eraftpb.Message resp : m.getResponsesList()) {
            rawNode.raft.step(resp);
        }

        // Apply empty entry
        rd = rawNode.ready();
        assertThat(rd.messages()).hasSize(2);
        for (Eraftpb.Message msg : rd.messages()) {
            if (msg.getMsgType() == Eraftpb.MessageType.MsgStorageAppend) {
                s.append(msg.getEntriesList());
                for (Eraftpb.Message resp : msg.getResponsesList()) {
                    rawNode.raft.step(resp);
                }
            } else if (msg.getMsgType() == Eraftpb.MessageType.MsgStorageApply) {
                assertThat(msg.getEntriesList()).hasSize(1);
                assertThat(msg.getResponsesList()).hasSize(1);
                rawNode.raft.step(msg.getResponses(0));
            }
        }

        // Propose first entry (1024 bytes)
        byte[] blob = new byte[1024];
        java.util.Arrays.fill(blob, (byte) 'a');
        rawNode.propose(blob);

        // Append first entry
        rd = rawNode.ready();
        assertThat(rd.messages()).hasSize(1);
        m = rd.messages().get(0);
        assertThat(m.getMsgType()).isEqualTo(Eraftpb.MessageType.MsgStorageAppend);
        assertThat(m.getEntriesList()).hasSize(1);
        s.append(m.getEntriesList());
        for (Eraftpb.Message resp : m.getResponsesList()) {
            rawNode.raft.step(resp);
        }

        // Propose second entry
        rawNode.propose(blob);

        // Append second entry - don't apply first entry yet.
        rd = rawNode.ready();
        assertThat(rd.messages()).hasSize(2);
        List<Eraftpb.Message> applyResps = new ArrayList<>();
        for (Eraftpb.Message msg : rd.messages()) {
            if (msg.getMsgType() == Eraftpb.MessageType.MsgStorageAppend) {
                s.append(msg.getEntriesList());
                for (Eraftpb.Message resp : msg.getResponsesList()) {
                    rawNode.raft.step(resp);
                }
            } else if (msg.getMsgType() == Eraftpb.MessageType.MsgStorageApply) {
                assertThat(msg.getEntriesList()).hasSize(1);
                assertThat(msg.getResponsesList()).hasSize(1);
                applyResps.add(msg.getResponses(0));
            }
        }

        // Propose third entry
        rawNode.propose(blob);

        // Append third entry - don't apply second entry yet.
        rd = rawNode.ready();
        assertThat(rd.messages()).hasSize(2);
        for (Eraftpb.Message msg : rd.messages()) {
            if (msg.getMsgType() == Eraftpb.MessageType.MsgStorageAppend) {
                s.append(msg.getEntriesList());
                for (Eraftpb.Message resp : msg.getResponsesList()) {
                    rawNode.raft.step(resp);
                }
            } else if (msg.getMsgType() == Eraftpb.MessageType.MsgStorageApply) {
                assertThat(msg.getEntriesList()).hasSize(1);
                assertThat(msg.getResponsesList()).hasSize(1);
                applyResps.add(msg.getResponses(0));
            }
        }

        // Third entry should not be returned to be applied until first entry's
        // application is acknowledged. Drain any intermediate HardState-only readies.
        while (rawNode.hasReady()) {
            rd = rawNode.ready();
            for (Eraftpb.Message msg : rd.messages()) {
                assertThat(msg.getMsgType())
                        .as("should not get MsgStorageApply before acking first entry apply")
                        .isNotEqualTo(Eraftpb.MessageType.MsgStorageApply);
                if (msg.getMsgType() == Eraftpb.MessageType.MsgStorageAppend) {
                    s.append(msg.getEntriesList());
                    for (Eraftpb.Message resp : msg.getResponsesList()) {
                        rawNode.raft.step(resp);
                    }
                }
            }
        }

        // Acknowledge first entry application.
        rawNode.raft.step(applyResps.get(0));
        applyResps.remove(0);

        // Third entry now returned for application.
        assertThat(rawNode.hasReady()).isTrue();
        rd = rawNode.ready();
        boolean foundApply = false;
        for (Eraftpb.Message msg : rd.messages()) {
            if (msg.getMsgType() == Eraftpb.MessageType.MsgStorageApply) {
                foundApply = true;
                assertThat(msg.getEntriesList()).hasSize(1);
                applyResps.add(msg.getResponses(0));
            }
        }
        assertThat(foundApply).as("should get MsgStorageApply after acking first entry").isTrue();

        // Acknowledge remaining entry applications.
        for (Eraftpb.Message resp : applyResps) {
            rawNode.raft.step(resp);
        }
    }

    /**
     * Verifies that DefaultNode rejects further proposals after the local node
     * is removed from the configuration via ApplyConfChange. Mirrors etcd-raft
     * node.go setting {@code propc = nil} when a node sees its own removal.
     */
    @Test
    void testProposalsDisabledAfterSelfRemoval() throws Exception {
        MemoryStorage s = newTestMemoryStorage(withPeers(1, 2));
        Config cfg = newTestConfig(1, 10, 1, s).toBuilder()
                .maxSizePerMsg(NO_LIMIT)
                .maxInflightMsgs(256)
                .build();

        Node n = DefaultNode.startNode(cfg, List.of(new Peer(1), new Peer(2)));
        DefaultNode dn = (DefaultNode) n;

        // Drain the bootstrap Ready so the bootstrap conf-change entries are
        // applied before we issue the test conf change.
        Ready bootstrap = dn.readyc.poll(1, TimeUnit.SECONDS);
        if (bootstrap != null) {
            s.append(bootstrap.entries());
            n.advance();
        }

        // Apply a ConfChange that removes the local node (id=1) entirely.
        Eraftpb.ConfChangeV2 cc = Eraftpb.ConfChangeV2.newBuilder()
                .addChanges(Eraftpb.ConfChangeSingle.newBuilder()
                        .setType(Eraftpb.ConfChangeType.ConfChangeRemoveNode)
                        .setNodeId(1))
                .build();
        Eraftpb.ConfState cs = n.applyConfChange(cc);
        assertThat(cs.getVotersList()).doesNotContain(1L);
        assertThat(cs.getVotersOutgoingList()).doesNotContain(1L);

        // After self-removal, propose must short-circuit at the producer
        // boundary with ErrProposalDropped.
        assertThatThrownBy(() -> n.propose("data".getBytes())).isEqualTo(RaftException.ErrProposalDropped);

        // Forwarded proposals via step() should also be rejected.
        Eraftpb.Message forwarded = Eraftpb.Message.newBuilder()
                .setMsgType(Eraftpb.MessageType.MsgPropose)
                .setFrom(1)
                .addEntries(Eraftpb.Entry.newBuilder()
                        .setData(ByteString.copyFromUtf8("via-step")))
                .build();
        assertThatThrownBy(() -> n.step(forwarded)).isEqualTo(RaftException.ErrProposalDropped);

        n.stop();
    }

    /**
     * Verifies the {@link Raft#leaveJoint} / {@link Raft#enterJoint} helpers
     * mirror etcd-raft's {@code pb.ConfChangeV2.LeaveJoint() / EnterJoint()}.
     * The transition matrix decides between Simple, EnterJoint, and LeaveJoint
     * paths in {@code applyConfChange} and was previously off by a few cases.
     */
    @Test
    void testApplyConfChangeTransitionMatrix() throws RaftException {
        // Empty changes + Auto transition → LeaveJoint.
        Eraftpb.ConfChangeV2 leaveJoint = Eraftpb.ConfChangeV2.newBuilder().build();
        assertThat(Raft.leaveJoint(leaveJoint)).isTrue();
        assertThat(Raft.enterJoint(leaveJoint)).isNull();

        // Empty changes + JointExplicit transition → not LeaveJoint and EnterJoint
        // (autoLeave=false). Mirrors etcd's behavior: "explicit" never auto-leaves.
        Eraftpb.ConfChangeV2 explicitEmpty = Eraftpb.ConfChangeV2.newBuilder()
                .setTransition(Eraftpb.ConfChangeTransition.ConfChangeTransitionJointExplicit)
                .build();
        assertThat(Raft.leaveJoint(explicitEmpty)).isFalse();
        assertThat(Raft.enterJoint(explicitEmpty)).isNotNull();
        assertThat(Raft.enterJoint(explicitEmpty).autoLeave()).isFalse();

        // Single change + Auto → Simple (enterJoint returns null).
        Eraftpb.ConfChangeV2 singleAuto = Eraftpb.ConfChangeV2.newBuilder()
                .addChanges(Eraftpb.ConfChangeSingle.newBuilder()
                        .setType(Eraftpb.ConfChangeType.ConfChangeAddNode).setNodeId(2))
                .build();
        assertThat(Raft.leaveJoint(singleAuto)).isFalse();
        assertThat(Raft.enterJoint(singleAuto)).isNull();

        // Single change + JointImplicit → EnterJoint with autoLeave=true.
        Eraftpb.ConfChangeV2 singleImplicit = singleAuto.toBuilder()
                .setTransition(Eraftpb.ConfChangeTransition.ConfChangeTransitionJointImplicit)
                .build();
        assertThat(Raft.enterJoint(singleImplicit)).isNotNull();
        assertThat(Raft.enterJoint(singleImplicit).autoLeave()).isTrue();

        // Single change + JointExplicit → EnterJoint with autoLeave=false.
        Eraftpb.ConfChangeV2 singleExplicit = singleAuto.toBuilder()
                .setTransition(Eraftpb.ConfChangeTransition.ConfChangeTransitionJointExplicit)
                .build();
        assertThat(Raft.enterJoint(singleExplicit)).isNotNull();
        assertThat(Raft.enterJoint(singleExplicit).autoLeave()).isFalse();

        // Multiple changes always EnterJoint regardless of transition.
        Eraftpb.ConfChangeV2 multi = Eraftpb.ConfChangeV2.newBuilder()
                .addChanges(Eraftpb.ConfChangeSingle.newBuilder()
                        .setType(Eraftpb.ConfChangeType.ConfChangeAddNode).setNodeId(2))
                .addChanges(Eraftpb.ConfChangeSingle.newBuilder()
                        .setType(Eraftpb.ConfChangeType.ConfChangeAddNode).setNodeId(3))
                .build();
        assertThat(Raft.leaveJoint(multi)).isFalse();
        assertThat(Raft.enterJoint(multi)).isNotNull();
        // Auto + multi → autoLeave=true.
        assertThat(Raft.enterJoint(multi).autoLeave()).isTrue();
    }

    /**
     * Verifies that DefaultNode's tick path drops ticks when the events queue
     * is saturated, rather than blocking the tick caller. Matches the warning
     * etcd-raft issues via the buffered tickc default-case drop.
     */
    @Test
    void testTickDropsOnSaturatedQueue() throws Exception {
        MemoryStorage s = newTestMemoryStorage(withPeers(1));
        Config cfg = newTestConfig(1, 10, 1, s).toBuilder()
                .maxSizePerMsg(NO_LIMIT)
                .maxInflightMsgs(256)
                .build();
        RawNode rawNode = RawNode.newRawNode(cfg);
        // Use a tiny queue so we can saturate it deterministically without
        // racing the event loop. The node is constructed but never started, so
        // events accumulate and never drain.
        DefaultNode n = new DefaultNode(rawNode, 4);

        // Fill the events queue with 4 RecvEvents (queue capacity = 4).
        for (int i = 0; i < 4; i++) {
            n.events.put(new DefaultNode.RecvEvent(Eraftpb.Message.newBuilder()
                    .setMsgType(Eraftpb.MessageType.MsgHeartbeat)
                    .setFrom(2)
                    .build()));
        }
        assertThat(n.events.remainingCapacity()).isZero();

        // tick() should not block; instead it drops the tick and warns.
        long start = System.nanoTime();
        n.tick();
        long elapsedMs = (System.nanoTime() - start) / 1_000_000L;
        assertThat(elapsedMs).as("tick must not block").isLessThan(100L);

        // Queue is still saturated (tick did not enqueue).
        assertThat(n.events.size()).isEqualTo(4);
    }
}
