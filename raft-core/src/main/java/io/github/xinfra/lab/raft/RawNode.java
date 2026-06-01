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
package io.github.xinfra.lab.raft;
import io.github.xinfra.lab.raft.internal.*;

import com.google.protobuf.ByteString;
import io.github.xinfra.lab.raft.proto.Eraftpb;
import io.github.xinfra.lab.raft.internal.tracker.Progress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * RawNode is a thread-unsafe Node.
 */
public class RawNode {
    private static final Logger LOG = LoggerFactory.getLogger(RawNode.class);

    public Raft raft;
    public boolean asyncStorageWrites;

    SoftState prevSoftSt;
    Eraftpb.HardState prevHardSt;
    List<Eraftpb.Message> stepsOnAdvance;

    /**
     * NewRawNode instantiates a RawNode from the given configuration.
     */
    public static RawNode newRawNode(Config config) {
        Raft r = Raft.newRaft(config);
        RawNode rn = new RawNode();
        rn.raft = r;
        rn.asyncStorageWrites = config.asyncStorageWrites;
        rn.prevSoftSt = r.softState();
        rn.prevHardSt = r.hardState();
        rn.stepsOnAdvance = new ArrayList<>();
        return rn;
    }

    /** Tick advances the internal logical clock by a single tick. */
    public void tick() {
        raft.tickFn.run();
    }

    /** TickQuiesced advances the internal logical clock without other processing. */
    public void tickQuiesced() {
        raft.electionElapsed++;
    }

    /**
     * Build a local message of the given type with optional builder mutations
     * and step it through the underlying Raft. All single-line public step*
     * helpers funnel through this to avoid duplicating the
     * Message.newBuilder()/setMsgType()/build()/raft.step() boilerplate.
     */
    private void stepLocal(Eraftpb.MessageType type, java.util.function.Consumer<Eraftpb.Message.Builder> mutator) throws RaftException {
        Eraftpb.Message.Builder mb = Eraftpb.Message.newBuilder().setMsgType(type);
        if (mutator != null) mutator.accept(mb);
        raft.step(mb.build());
    }

    /** Campaign causes this RawNode to transition to candidate state. */
    public void campaign() throws RaftException {
        stepLocal(Eraftpb.MessageType.MsgHup, null);
    }

    /** Propose proposes data be appended to the raft log. */
    public void propose(byte[] data) throws RaftException {
        stepLocal(Eraftpb.MessageType.MsgPropose, mb -> mb
                .setFrom(raft.id)
                .addEntries(Eraftpb.Entry.newBuilder()
                        .setData(data != null ? ByteString.copyFrom(data) : ByteString.EMPTY)));
    }

    /** ProposeConfChange proposes a config change. */
    public void proposeConfChange(Eraftpb.ConfChangeV2 cc) throws RaftException {
        Eraftpb.Message m = io.github.xinfra.lab.raft.internal.confchange.Changer.toMessage(cc);
        raft.step(m);
    }

    /** ProposeConfChange proposes a V1 config change. */
    public void proposeConfChange(Eraftpb.ConfChange cc) throws RaftException {
        // V1 conf changes are marshalled as EntryConfChange (matching Go behavior)
        stepLocal(Eraftpb.MessageType.MsgPropose, mb -> mb
                .addEntries(Eraftpb.Entry.newBuilder()
                        .setEntryType(Eraftpb.EntryType.EntryConfChange)
                        .setData(cc.toByteString())));
    }

    /** ApplyConfChange applies a config change to the local node. */
    public Eraftpb.ConfState applyConfChange(Eraftpb.ConfChangeV2 cc) {
        return raft.applyConfChange(cc);
    }

    /** Step advances the state machine using the given message. */
    public void step(Eraftpb.Message m) throws RaftException {
        if (Util.isLocalMsg(m.getMsgType()) && !Util.isLocalMsgTarget(m.getFrom())) {
            throw RaftException.ErrStepLocalMsg;
        }
        if (Util.isResponseMsg(m.getMsgType()) && !Util.isLocalMsgTarget(m.getFrom()) &&
            raft.trk.getProgress().get(m.getFrom()) == null) {
            throw RaftException.ErrStepPeerNotFound;
        }
        raft.step(m);
    }

    /** Ready returns the outstanding work that the application needs to handle. */
    public Ready ready() {
        Ready rd = readyWithoutAccept();
        acceptReady(rd);
        return rd;
    }

    public Ready readyWithoutAccept() {
        Raft r = raft;

        Ready rd = new Ready();
        // Defensive copies: nextUnstableEnts() returns a subList view of the
        // unstable entries buffer; if the caller (or any step inside the
        // Ready cycle, e.g. async-mode MsgStorageAppendResp triggering
        // stableTo) mutates the underlying list, the view would throw
        // ConcurrentModificationException on later access. Same caveat
        // applies to nextCommittedEnts() when its entries come from the
        // unstable buffer.
        List<Eraftpb.Entry> unstableEnts = r.raftLog.nextUnstableEnts();
        rd.entries = unstableEnts != null ? new ArrayList<>(unstableEnts) : new ArrayList<>();
        List<Eraftpb.Entry> committedEnts = r.raftLog.nextCommittedEnts(applyUnstableEntries());
        rd.committedEntries = committedEnts != null ? new ArrayList<>(committedEnts) : new ArrayList<>();
        rd.messages = new ArrayList<>(r.msgs);

        SoftState softSt = r.softState();
        if (!softSt.equals(prevSoftSt)) {
            rd.softState = softSt;
        }
        Eraftpb.HardState hardSt = r.hardState();
        if (!Util.isHardStateEqual(hardSt, prevHardSt)) {
            rd.hardState = hardSt;
        }
        if (r.raftLog.hasNextUnstableSnapshot()) {
            rd.snapshot = r.raftLog.nextUnstableSnapshot();
        }
        if (!r.readStates.isEmpty()) {
            rd.readStates = new ArrayList<>(r.readStates);
        }
        rd.mustSync = Util.mustSync(r.hardState(), prevHardSt, rd.entries.size());

        if (asyncStorageWrites) {
            if (needStorageAppendMsg(r, rd)) {
                Eraftpb.Message m = newStorageAppendMsg(r, rd);
                rd.messages.add(m);
            }
            if (needStorageApplyMsg(rd)) {
                Eraftpb.Message m = newStorageApplyMsg(r, rd);
                rd.messages.add(m);
            }
        } else {
            for (Eraftpb.Message m : r.msgsAfterAppend) {
                if (m.getTo() != r.id) {
                    rd.messages.add(m);
                }
            }
        }

        return rd;
    }

    public void acceptReady(Ready rd) {
        if (rd.softState != null) {
            prevSoftSt = rd.softState;
        }
        if (!Util.isEmptyHardState(rd.hardState)) {
            prevHardSt = rd.hardState;
        }
        if (!rd.readStates.isEmpty()) {
            raft.readStates = new ArrayList<>();
        }
        if (!asyncStorageWrites) {
            if (!stepsOnAdvance.isEmpty()) {
                throw new RaftInvariantException("two accepted Ready structs without call to Advance");
            }
            for (Eraftpb.Message m : raft.msgsAfterAppend) {
                if (m.getTo() == raft.id) {
                    stepsOnAdvance.add(m);
                }
            }
            if (needStorageAppendRespMsg(raft, rd)) {
                stepsOnAdvance.add(newStorageAppendRespMsg(raft, rd));
            }
            if (needStorageApplyRespMsg(rd)) {
                stepsOnAdvance.add(newStorageApplyRespMsg(raft, rd.committedEntries));
            }
        }
        raft.msgs = new ArrayList<>();
        raft.msgsAfterAppend = new ArrayList<>();
        raft.raftLog.acceptUnstable();
        if (!rd.committedEntries.isEmpty()) {
            long index = rd.committedEntries.get(rd.committedEntries.size() - 1).getIndex();
            raft.raftLog.acceptApplying(index, Util.entsSize(rd.committedEntries), applyUnstableEntries());
        }

        StateTrace.traceReady(raft);
    }

    boolean applyUnstableEntries() {
        return !asyncStorageWrites;
    }

    /** HasReady called when RawNode user need to check if any Ready pending. */
    public boolean hasReady() {
        Raft r = raft;
        if (!r.softState().equals(prevSoftSt)) {
            return true;
        }
        Eraftpb.HardState hardSt = r.hardState();
        if (!Util.isEmptyHardState(hardSt) && !Util.isHardStateEqual(hardSt, prevHardSt)) {
            return true;
        }
        if (r.raftLog.hasNextUnstableSnapshot()) {
            return true;
        }
        if (!r.msgs.isEmpty() || !r.msgsAfterAppend.isEmpty()) {
            return true;
        }
        if (r.raftLog.hasNextUnstableEnts() || r.raftLog.hasNextCommittedEnts(applyUnstableEntries())) {
            return true;
        }
        if (!r.readStates.isEmpty()) {
            return true;
        }
        return false;
    }

    /**
     * Advance notifies the RawNode that the application has applied and saved progress.
     * NOTE: Advance must not be called when using AsyncStorageWrites.
     */
    public void advance(Ready rd) {
        if (asyncStorageWrites) {
            throw new RaftInvariantException("Advance must not be called when using AsyncStorageWrites");
        }
        for (int i = 0; i < stepsOnAdvance.size(); i++) {
            // stepsOnAdvance carries self-delivered append-resp / apply-resp
            // messages produced by acceptReady — never MsgPropose — so step
            // never raises ErrProposalDropped. Defensive catch keeps advance()
            // void; if a future change changes that, log unmistakably.
            try {
                raft.step(stepsOnAdvance.get(i));
            } catch (RaftException e) {
                LOG.error("node {} unexpected RaftException replaying {} in advance(): {}",
                        raft.id, stepsOnAdvance.get(i).getMsgType(), e);
            }
            stepsOnAdvance.set(i, Eraftpb.Message.getDefaultInstance());
        }
        stepsOnAdvance.clear();
    }

    /** Status returns the current status of the given group. */
    public Status status() {
        return Status.getStatus(raft);
    }

    /** BasicStatus returns a BasicStatus. */
    public Status.BasicStatus basicStatus() {
        return Status.getBasicStatus(raft);
    }

    /**
     * ProgressType indicates the type of replica a Progress corresponds to.
     */
    public enum ProgressType {
        ProgressTypePeer,
        ProgressTypeLearner
    }

    /**
     * WithProgress is a helper to introspect the Progress for this node and its peers.
     */
    public void withProgress(ProgressVisitor visitor) {
        raft.trk.visit((id, pr) -> {
            ProgressType typ = pr.isLearner() ? ProgressType.ProgressTypeLearner : ProgressType.ProgressTypePeer;
            visitor.visit(id, typ, pr);
        });
    }

    @FunctionalInterface
    public interface ProgressVisitor {
        void visit(long id, ProgressType type, Progress pr);
    }

    /** ReportUnreachable reports the given node is not reachable for the last send. */
    public void reportUnreachable(long id) {
        // MsgUnreachable is a local notification — never raises.
        try {
            stepLocal(Eraftpb.MessageType.MsgUnreachable, mb -> mb.setFrom(id));
        } catch (RaftException e) {
            LOG.error("node {} unexpected RaftException from MsgUnreachable: {}", raft.id, e);
        }
    }

    /** ReportSnapshot reports the status of the sent snapshot. */
    public void reportSnapshot(long id, SnapshotStatus status) {
        boolean rej = status == SnapshotStatus.SnapshotFailure;
        try {
            stepLocal(Eraftpb.MessageType.MsgSnapStatus, mb -> mb.setFrom(id).setReject(rej));
        } catch (RaftException e) {
            LOG.error("node {} unexpected RaftException from MsgSnapStatus: {}", raft.id, e);
        }
    }

    /** TransferLeader tries to transfer leadership to the given transferee. */
    public void transferLeader(long transferee) {
        try {
            stepLocal(Eraftpb.MessageType.MsgTransferLeader, mb -> mb.setFrom(transferee));
        } catch (RaftException e) {
            LOG.error("node {} unexpected RaftException from MsgTransferLeader: {}", raft.id, e);
        }
    }

    /** ForgetLeader forgets a follower's current leader. */
    public void forgetLeader() throws RaftException {
        stepLocal(Eraftpb.MessageType.MsgForgetLeader, null);
    }

    /** ReadIndex requests a read state. */
    public void readIndex(byte[] rctx) {
        try {
            stepLocal(Eraftpb.MessageType.MsgReadIndex, mb -> mb
                    .addEntries(Eraftpb.Entry.newBuilder()
                            .setData(rctx != null ? ByteString.copyFrom(rctx) : ByteString.EMPTY)));
        } catch (RaftException e) {
            LOG.error("node {} unexpected RaftException from MsgReadIndex: {}", raft.id, e);
        }
    }

    /** Bootstrap initializes the RawNode for first use. */
    public void bootstrap(List<Peer> peers) {
        if (peers.isEmpty()) {
            throw new IllegalArgumentException("must provide at least one peer to Bootstrap");
        }
        long lastIndex = raft.raftLog.storage.lastIndex();
        if (lastIndex != 0) {
            throw new IllegalStateException("can't bootstrap a nonempty Storage");
        }

        prevHardSt = Eraftpb.HardState.getDefaultInstance();

        raft.becomeFollower(1, Util.NONE);
        List<Eraftpb.Entry> ents = new ArrayList<>();
        for (int i = 0; i < peers.size(); i++) {
            Peer peer = peers.get(i);
            Eraftpb.ConfChange cc = Eraftpb.ConfChange.newBuilder()
                    .setChangeType(Eraftpb.ConfChangeType.ConfChangeAddNode)
                    .setNodeId(peer.id())
                    .setContext(peer.context() != null ? ByteString.copyFrom(peer.context()) : ByteString.EMPTY)
                    .build();
            ents.add(Eraftpb.Entry.newBuilder()
                    .setEntryType(Eraftpb.EntryType.EntryConfChange)
                    .setTerm(1)
                    .setIndex(i + 1)
                    .setData(cc.toByteString())
                    .build());
        }
        raft.raftLog.append(ents);
        raft.raftLog.committed = ents.size();
        for (Peer peer : peers) {
            Eraftpb.ConfChangeV2 ccv2 = Eraftpb.ConfChangeV2.newBuilder()
                    .addChanges(Eraftpb.ConfChangeSingle.newBuilder()
                            .setNodeId(peer.id())
                            .setType(Eraftpb.ConfChangeType.ConfChangeAddNode))
                    .build();
            raft.applyConfChange(ccv2);
        }
    }

    // ============= AsyncStorageWrites helpers =============
    static boolean needStorageAppendMsg(Raft r, Ready rd) {
        return !rd.entries.isEmpty() ||
                !Util.isEmptyHardState(rd.hardState) ||
                !Util.isEmptySnap(rd.snapshot) ||
                !r.msgsAfterAppend.isEmpty();
    }

    static boolean needStorageAppendRespMsg(Raft r, Ready rd) {
        return r.raftLog.hasNextOrInProgressUnstableEnts() ||
                !Util.isEmptySnap(rd.snapshot);
    }

    static Eraftpb.Message newStorageAppendMsg(Raft r, Ready rd) {
        Eraftpb.Message.Builder mb = Eraftpb.Message.newBuilder()
                .setMsgType(Eraftpb.MessageType.MsgStorageAppend)
                .setTo(Util.LOCAL_APPEND_THREAD)
                .setFrom(r.id)
                .addAllEntries(rd.entries);
        if (!Util.isEmptyHardState(rd.hardState)) {
            mb.setTerm(rd.hardState.getTerm());
            mb.setVote(rd.hardState.getVote());
            mb.setCommit(rd.hardState.getCommit());
        }
        if (!Util.isEmptySnap(rd.snapshot)) {
            mb.setSnapshot(rd.snapshot);
        }
        mb.addAllResponses(r.msgsAfterAppend);
        if (needStorageAppendRespMsg(r, rd)) {
            mb.addResponses(newStorageAppendRespMsg(r, rd));
        }
        return mb.build();
    }

    static Eraftpb.Message newStorageAppendRespMsg(Raft r, Ready rd) {
        Eraftpb.Message.Builder mb = Eraftpb.Message.newBuilder()
                .setMsgType(Eraftpb.MessageType.MsgStorageAppendResp)
                .setTo(r.id)
                .setFrom(Util.LOCAL_APPEND_THREAD)
                .setTerm(r.term);
        if (r.raftLog.hasNextOrInProgressUnstableEnts()) {
            EntryID last = r.raftLog.lastEntryID();
            mb.setIndex(last.index());
            mb.setLogTerm(last.term());
        }
        if (!Util.isEmptySnap(rd.snapshot)) {
            mb.setSnapshot(rd.snapshot);
        }
        return mb.build();
    }

    static boolean needStorageApplyMsg(Ready rd) {
        return !rd.committedEntries.isEmpty();
    }

    static boolean needStorageApplyRespMsg(Ready rd) {
        return needStorageApplyMsg(rd);
    }

    static Eraftpb.Message newStorageApplyMsg(Raft r, Ready rd) {
        return Eraftpb.Message.newBuilder()
                .setMsgType(Eraftpb.MessageType.MsgStorageApply)
                .setTo(Util.LOCAL_APPLY_THREAD)
                .setFrom(r.id)
                .setTerm(0)
                .addAllEntries(rd.committedEntries)
                .addResponses(newStorageApplyRespMsg(r, rd.committedEntries))
                .build();
    }

    static Eraftpb.Message newStorageApplyRespMsg(Raft r, List<Eraftpb.Entry> ents) {
        return Eraftpb.Message.newBuilder()
                .setMsgType(Eraftpb.MessageType.MsgStorageApplyResp)
                .setTo(r.id)
                .setFrom(Util.LOCAL_APPLY_THREAD)
                .setTerm(0)
                .addAllEntries(ents)
                .build();
    }
}
