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
import io.github.xinfra.lab.raft.internal.confchange.Changer;
import io.github.xinfra.lab.raft.proto.Eraftpb;
import io.github.xinfra.lab.raft.internal.tracker.Progress;
import io.github.xinfra.lab.raft.internal.tracker.ProgressTracker;
import io.github.xinfra.lab.raft.internal.tracker.StateType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

/**
 * Raft is the core raft state machine implementation.
 */
public class Raft {
    RaftLogger logger;

    // -------------------------------------------------------------------------
    // Field visibility: every field below is package-private. The public API
    // package ({@code io.github.xinfra.lab.raft}) reads them through the
    // accessor methods declared further down — never via direct field access.
    // This is the "lockdown" boundary: a host can't accidentally (or
    // maliciously) mutate Raft state by holding a reference to the internal
    // Raft instance, since the fields aren't visible from their package.
    // -------------------------------------------------------------------------

    long id;
    long term;
    long vote;

    List<ReadState> readStates;

    RaftLog raftLog;

    long maxMsgSize;
    long maxUncommittedSize;

    ProgressTracker trk;

    // Default to StateFollower so traceInitState (called before becomeFollower
    // in newRaft) doesn't NPE on r.state.name(). Mirrors Go's zero-value
    // StateType where the first enum constant doubles as the default.
    RaftStateType state = RaftStateType.StateFollower;
    boolean isLearner;

    List<Eraftpb.Message> msgs;
    List<Eraftpb.Message> msgsAfterAppend;

    long lead;
    long leadTransferee;
    long pendingConfIndex;
    boolean disableConfChangeValidation;
    long uncommittedSize;

    ReadOnly readOnly;

    // Long-typed to defend against overflow on a long-running learner that
    // never gets promoted (tickElection only resets electionElapsed on the
    // promotable + pastTimeout branch). At 100Hz tick, an int would overflow
    // in ~248 days; long is effectively unbounded.
    long electionElapsed;
    long heartbeatElapsed;

    boolean checkQuorum;
    boolean preVote;

    int heartbeatTimeout;
    int electionTimeout;
    int randomizedElectionTimeout;
    boolean disableProposalForwarding;
    boolean stepDownOnRemoval;

    Runnable tickFn;
    StepFunction stepFn;

    List<Eraftpb.Message> pendingReadIndexMessages;

    /** Caps for queues that can grow under sustained load (0 = unbounded). */
    int maxPendingReadIndexMessages;
    int maxReadStates;

    TraceLogger traceLogger;

    RaftMetrics metrics = RaftMetrics.NOOP;

    RateLimitedLog proposalDropLog;

    @FunctionalInterface
    interface StepFunction {
        void step(Raft r, Eraftpb.Message m) throws RaftException;
    }

    // ============= newRaft =============
    public static Raft newRaft(Config c) {
        // Config is already validated by Config.Builder.build(); the public
        // ctor path doesn't exist anymore, so no defensive validate() call here.
        RaftLog raftlog = RaftLog.newLogWithSize(c.storage, c.maxCommittedSizePerReady);
        Storage.InitialStateResult isr = c.storage.initialState();
        Eraftpb.HardState hs = isr.hardState();
        Eraftpb.ConfState cs = isr.confState();

        Raft r = new Raft();
        r.id = c.id;
        r.logger = c.logger;
        r.lead = Util.NONE;
        r.isLearner = false;
        r.raftLog = raftlog;
        r.maxMsgSize = c.maxSizePerMsg;
        r.maxUncommittedSize = c.maxUncommittedEntriesSize;
        r.trk = ProgressTracker.make(c.maxInflightMsgs, c.maxInflightBytes);
        r.electionTimeout = c.electionTick;
        r.heartbeatTimeout = c.heartbeatTick;
        r.checkQuorum = c.checkQuorum;
        r.preVote = c.preVote;
        r.readOnly = new ReadOnly(c.readOnlyOption);
        r.disableProposalForwarding = c.disableProposalForwarding;
        r.disableConfChangeValidation = c.disableConfChangeValidation;
        r.stepDownOnRemoval = c.stepDownOnRemoval;
        r.maxPendingReadIndexMessages = c.maxPendingReadIndexMessages;
        r.maxReadStates = c.maxReadStates;
        r.metrics = c.metrics != null ? c.metrics : RaftMetrics.NOOP;
        r.proposalDropLog = new RateLimitedLog(c.logger);
        r.msgs = new ArrayList<>();
        r.msgsAfterAppend = new ArrayList<>();
        r.readStates = new ArrayList<>();
        r.pendingReadIndexMessages = new ArrayList<>();
        r.traceLogger = c.traceLogger;

        StateTrace.traceInitState(r);

        EntryID lastID = r.raftLog.lastEntryID();
        Changer changer = new Changer(r.trk, lastID.index());
        Changer.Result rr = Changer.restore(changer, cs);
        Util.assertConfStatesEquivalent(cs, r.switchToConfig(rr.config(), rr.progress()));

        if (!Util.isEmptyHardState(hs)) {
            r.loadState(hs);
        }
        if (c.applied > 0) {
            raftlog.appliedTo(c.applied, 0);
        }
        r.becomeFollower(r.term, Util.NONE);

        r.logger.info("newRaft {:x} [peers: [{}], term: {}, commit: {}, applied: {}, lastindex: {}, lastterm: {}]",
                r.id, r.trk.voterNodes().stream().map(n -> String.format("%x", n)).collect(Collectors.joining(",")),
                r.term, r.raftLog.committed, r.raftLog.applied, lastID.index(), lastID.term());
        return r;
    }

    public boolean hasLeader() { return lead != Util.NONE; }

    // ============= Public accessors =============
    // These are the ONLY way for callers in {@code io.github.xinfra.lab.raft}
    // (RawNode, Status, ...) to reach internal Raft state. Fields above are
    // package-private; cross-package mutation must funnel through the
    // explicit operations below (e.g. {@link #incrementElectionElapsed},
    // {@link #drainMsgs}).

    public long id() { return id; }
    public long term() { return term; }
    public long lead() { return lead; }
    public long leadTransferee() { return leadTransferee; }
    public long electionElapsed() { return electionElapsed; }
    public RaftStateType state() { return state; }

    /** Read-only handle to the raft log; callers must not mutate via this. */
    public RaftLog raftLog() { return raftLog; }
    public ProgressTracker tracker() { return trk; }

    /** Tick once. Replaces direct {@code raft.tickFn.run()} access. */
    public void tick() { tickFn.run(); }

    /** Bump the election clock without running the full tick logic. */
    public void incrementElectionElapsed() { electionElapsed++; }

    public List<Eraftpb.Message> msgs() { return msgs; }
    public List<Eraftpb.Message> msgsAfterAppend() { return msgsAfterAppend; }
    public List<ReadState> readStates() { return readStates; }

    /** Atomically take and reset the outbound message buffer. */
    public List<Eraftpb.Message> drainMsgs() {
        List<Eraftpb.Message> taken = msgs;
        msgs = new ArrayList<>();
        return taken;
    }

    /** Atomically take and reset the after-append message buffer. */
    public List<Eraftpb.Message> drainMsgsAfterAppend() {
        List<Eraftpb.Message> taken = msgsAfterAppend;
        msgsAfterAppend = new ArrayList<>();
        return taken;
    }

    /** Reset the local read-state queue (caller has already consumed it). */
    public void resetReadStates() {
        readStates = new ArrayList<>();
    }

    public SoftState softState() { return new SoftState(lead, state); }

    public Eraftpb.HardState hardState() {
        return Eraftpb.HardState.newBuilder()
                .setTerm(term)
                .setVote(vote)
                .setCommit(raftLog.committed)
                .build();
    }

    // ============= send =============
    void send(Eraftpb.Message.Builder mb) {
        if (mb.getFrom() == Util.NONE) {
            mb.setFrom(id);
        }
        Eraftpb.MessageType type = mb.getMsgType();
        if (type == Eraftpb.MessageType.MsgRequestVote || type == Eraftpb.MessageType.MsgRequestVoteResponse ||
            type == Eraftpb.MessageType.MsgRequestPreVote || type == Eraftpb.MessageType.MsgRequestPreVoteResponse) {
            if (mb.getTerm() == 0) {
                throw new RaftInvariantException("term should be set when sending " + type);
            }
        } else {
            if (mb.getTerm() != 0) {
                throw new RaftInvariantException("term should not be set when sending " + type + " (was " + mb.getTerm() + ")");
            }
            if (type != Eraftpb.MessageType.MsgPropose && type != Eraftpb.MessageType.MsgReadIndex) {
                mb.setTerm(term);
            }
        }
        Eraftpb.Message m = mb.build();
        if (type == Eraftpb.MessageType.MsgAppendResponse ||
            type == Eraftpb.MessageType.MsgRequestVoteResponse ||
            type == Eraftpb.MessageType.MsgRequestPreVoteResponse) {
            msgsAfterAppend.add(m);
            StateTrace.traceSendMessage(this, m);
        } else {
            if (m.getTo() == id) {
                throw new RaftInvariantException("message should not be self-addressed when sending " + type);
            }
            msgs.add(m);
            StateTrace.traceSendMessage(this, m);
        }
    }

    // ============= message builder helpers =============
    /** Build an accepting MsgAppendResponse to {@code to} acknowledging {@code index}. */
    private static Eraftpb.Message.Builder appendRespAccept(long to, long index) {
        return Eraftpb.Message.newBuilder()
                .setTo(to)
                .setMsgType(Eraftpb.MessageType.MsgAppendResponse)
                .setIndex(index);
    }

    /** Build a rejecting MsgAppendResponse with hint info for the leader's probe. */
    private static Eraftpb.Message.Builder appendRespReject(long to, long index, long rejectHint, long logTerm) {
        return Eraftpb.Message.newBuilder()
                .setTo(to)
                .setMsgType(Eraftpb.MessageType.MsgAppendResponse)
                .setIndex(index)
                .setReject(true)
                .setRejectHint(rejectHint)
                .setLogTerm(logTerm);
    }

    /** Build a MsgRequestVoteResponse / MsgRequestPreVoteResponse. */
    private static Eraftpb.Message.Builder voteResp(long to, Eraftpb.MessageType respType, long term, boolean reject) {
        Eraftpb.Message.Builder mb = Eraftpb.Message.newBuilder()
                .setTo(to)
                .setMsgType(respType)
                .setTerm(term);
        if (reject) mb.setReject(true);
        return mb;
    }

    // ============= sendAppend / maybeSendAppend =============
    void sendAppend(long to) {
        maybeSendAppend(to, true);
    }

    boolean maybeSendAppend(long to, boolean sendIfEmpty) {
        Progress pr = trk.getProgress().get(to);
        if (pr.isPaused()) {
            return false;
        }

        long prevIndex = pr.getNext() - 1;
        RaftLog.TermResult tr = raftLog.termResult(prevIndex);
        if (tr.err() != null) {
            return maybeSendSnapshot(to, pr);
        }
        long prevTerm = tr.term();

        List<Eraftpb.Entry> ents = null;
        if (pr.getState() != StateType.StateReplicate || !pr.getInflights().full()) {
            try {
                ents = raftLog.entries(pr.getNext(), maxMsgSize);
            } catch (RaftException e) {
                // Log was compacted past pr.getNext() — the follower can't be
                // caught up via append; fall back to snapshot. Previously this
                // catch was empty, which silently produced an empty MsgAppend
                // and forced an extra reject round-trip.
                return maybeSendSnapshot(to, pr);
            }
        }
        if (ents == null) ents = new ArrayList<>();
        if (ents.isEmpty() && !sendIfEmpty) {
            return false;
        }

        send(Eraftpb.Message.newBuilder()
                .setTo(to)
                .setMsgType(Eraftpb.MessageType.MsgAppend)
                .setIndex(prevIndex)
                .setLogTerm(prevTerm)
                .addAllEntries(ents)
                .setCommit(raftLog.committed));
        pr.sentEntries(ents.size(), Util.payloadsSize(ents));
        pr.sentCommit(raftLog.committed);
        return true;
    }

    boolean maybeSendSnapshot(long to, Progress pr) {
        if (!pr.isRecentActive()) {
            logger.debug("ignore sending snapshot to {:x} since it is not recently active", to);
            return false;
        }

        Eraftpb.Snapshot snapshot;
        try {
            snapshot = raftLog.snapshot();
        } catch (RaftException e) {
            if (e.is(RaftException.Code.SNAPSHOT_TEMPORARILY_UNAVAILABLE)) {
                logger.debug("{:x} failed to send snapshot to {:x} because snapshot is temporarily unavailable", id, to);
                return false;
            }
            throw new RaftInvariantException("unexpected snapshot error", e);
        }
        if (Util.isEmptySnap(snapshot)) {
            throw new RaftInvariantException("need non-empty snapshot");
        }
        long sindex = snapshot.getMetadata().getIndex();
        long sterm = snapshot.getMetadata().getTerm();
        logger.debug("{:x} [firstindex: {}, commit: {}] sent snapshot[index: {}, term: {}] to {:x} [{}]",
                id, raftLog.firstIndex(), raftLog.committed, sindex, sterm, to, pr);
        pr.becomeSnapshot(sindex);

        send(Eraftpb.Message.newBuilder()
                .setTo(to)
                .setMsgType(Eraftpb.MessageType.MsgSnapshot)
                .setSnapshot(snapshot));
        return true;
    }

    void sendHeartbeat(long to, byte[] ctx) {
        Progress pr = trk.getProgress().get(to);
        long commit = Math.min(pr.getMatch(), raftLog.committed);
        Eraftpb.Message.Builder mb = Eraftpb.Message.newBuilder()
                .setTo(to)
                .setMsgType(Eraftpb.MessageType.MsgHeartbeat)
                .setCommit(commit);
        if (ctx != null) {
            mb.setContext(ByteString.copyFrom(ctx));
        }
        send(mb);
        pr.sentCommit(commit);
    }

    void bcastAppend() {
        trk.visit((id, pr) -> {
            if (id != this.id) {
                sendAppend(id);
            }
        });
    }

    void bcastHeartbeat() {
        bcastHeartbeatWithCtx(readOnly.heartbeatCtx());
    }

    void bcastHeartbeatWithCtx(byte[] ctx) {
        trk.visit((id, pr) -> {
            if (id != this.id) {
                sendHeartbeat(id, ctx);
            }
        });
    }

    // ============= appliedTo =============
    void appliedTo(long index, long size) {
        long oldApplied = raftLog.applied;
        long newApplied = Math.max(index, oldApplied);
        raftLog.appliedTo(newApplied, size);

        if (trk.getConfig().isAutoLeave() && newApplied >= pendingConfIndex && state == RaftStateType.StateLeader) {
            Eraftpb.Message m = io.github.xinfra.lab.raft.internal.confchange.Changer.toMessage(null);
            try {
                step(m);
                logger.info("initiating automatic transition out of joint configuration {}", trk.getConfig());
            } catch (RaftException err) {
                logger.debug("not initiating automatic transition out of joint configuration {}: {}", trk.getConfig(), err);
            }
        }
    }

    void appliedSnap(Eraftpb.Snapshot snap) {
        long index = snap.getMetadata().getIndex();
        raftLog.stableSnapTo(index);
        appliedTo(index, 0);
    }

    boolean maybeCommit() {
        boolean committed = raftLog.maybeCommit(new EntryID(term, trk.committed()));
        if (committed) {
            StateTrace.traceCommit(this);
        }
        return committed;
    }

    // ============= reset =============
    void reset(long newTerm) {
        if (term != newTerm) {
            term = newTerm;
            vote = Util.NONE;
        }
        lead = Util.NONE;
        electionElapsed = 0;
        heartbeatElapsed = 0;
        resetRandomizedElectionTimeout();
        abortLeaderTransfer();
        trk.resetVotes();
        long lastIdx = raftLog.lastIndex();
        trk.visit((id, pr) -> {
            // Reuse the existing Progress and its Inflights buffer; resetting
            // fields in-place avoids 2N allocations per term change.
            // Mirrors etcd-raft's `*pr = tracker.Progress{...}` zero-reset,
            // except the Inflights ring buffer is recycled rather than freshly
            // allocated.
            pr.setMatch(id == this.id ? lastIdx : 0);
            pr.setNext(lastIdx + 1);
            pr.setSentCommit(0);
            pr.setState(StateType.StateProbe);
            pr.setPendingSnapshot(0);
            pr.setRecentActive(false);
            pr.setMsgAppFlowPaused(false);
            if (pr.getInflights() == null) {
                // Tests / external callers may install bare Progress instances
                // (no Inflights) before reset; allocate on first reset.
                pr.setInflights(new io.github.xinfra.lab.raft.internal.tracker.Inflights(
                        trk.getMaxInflight(), trk.getMaxInflightBytes()));
            } else {
                pr.getInflights().reset();
            }
            // isLearner is preserved.
        });
        pendingConfIndex = 0;
        uncommittedSize = 0;
        readOnly = new ReadOnly(readOnly.option);
    }

    // ============= appendEntry =============
    boolean appendEntry(List<Eraftpb.Entry> es) {
        long li = raftLog.lastIndex();
        List<Eraftpb.Entry> newEnts = new ArrayList<>();
        for (int i = 0; i < es.size(); i++) {
            newEnts.add(es.get(i).toBuilder()
                    .setTerm(term)
                    .setIndex(li + 1 + i)
                    .build());
        }
        if (!increaseUncommittedSize(newEnts)) {
            proposalDropLog.warn("{:x} appending new entries to log would exceed uncommitted entry size limit; dropping proposal", id);
            return false;
        }

        StateTrace.traceReplicate(this, newEnts);

        li = raftLog.append(newEnts);
        send(Eraftpb.Message.newBuilder()
                .setTo(id)
                .setMsgType(Eraftpb.MessageType.MsgAppendResponse)
                .setIndex(li));
        return true;
    }

    // ============= tick functions =============
    void tickElection() {
        electionElapsed++;
        if (promotable() && pastElectionTimeout()) {
            electionElapsed = 0;
            // Self-step a local MsgHup. Local messages never throw
            // RaftException (ErrProposalDropped is MsgPropose-only); catch
            // defensively so a future change in step() can't propagate a
            // checked exception out of the Runnable tick contract.
            stepLocalSafely(Eraftpb.MessageType.MsgHup);
        }
    }

    void tickHeartbeat() {
        heartbeatElapsed++;
        electionElapsed++;

        if (electionElapsed >= electionTimeout) {
            electionElapsed = 0;
            if (checkQuorum) {
                stepLocalSafely(Eraftpb.MessageType.MsgCheckQuorum);
            }
            if (state == RaftStateType.StateLeader && leadTransferee != Util.NONE) {
                abortLeaderTransfer();
            }
        }

        if (state != RaftStateType.StateLeader) {
            return;
        }

        if (heartbeatElapsed >= heartbeatTimeout) {
            heartbeatElapsed = 0;
            stepLocalSafely(Eraftpb.MessageType.MsgBeat);
        }
    }

    /**
     * Self-step a local MsgHup/MsgBeat/MsgCheckQuorum. These message types
     * never raise RaftException (only MsgPropose does); the catch is purely
     * defensive so the {@link Runnable} tick contract is preserved.
     */
    private void stepLocalSafely(Eraftpb.MessageType type) {
        try {
            step(Eraftpb.Message.newBuilder().setFrom(id).setMsgType(type).build());
        } catch (RaftException e) {
            logger.error("{:x} unexpected RaftException from local {} step: {}", id, type, e);
        }
    }

    // ============= become* =============
    public void becomeFollower(long newTerm, long newLead) {
        proposalDropLog.flush();
        stepFn = Raft::stepFollower;
        reset(newTerm);
        tickFn = this::tickElection;
        lead = newLead;
        state = RaftStateType.StateFollower;
        logger.info("{:x} became follower at term {}", id, term);
        StateTrace.traceBecomeFollower(this);
    }

    void becomeCandidate() {
        if (state == RaftStateType.StateLeader) {
            throw new RaftInvariantException("invalid transition [leader -> candidate]");
        }
        proposalDropLog.flush();
        stepFn = Raft::stepCandidate;
        reset(term + 1);
        tickFn = this::tickElection;
        vote = id;
        state = RaftStateType.StateCandidate;
        logger.info("{:x} became candidate at term {}", id, term);
        StateTrace.traceBecomeCandidate(this);
    }

    void becomePreCandidate() {
        if (state == RaftStateType.StateLeader) {
            throw new RaftInvariantException("invalid transition [leader -> pre-candidate]");
        }
        proposalDropLog.flush();
        stepFn = Raft::stepCandidate;
        trk.resetVotes();
        tickFn = this::tickElection;
        lead = Util.NONE;
        state = RaftStateType.StatePreCandidate;
        logger.info("{:x} became pre-candidate at term {}", id, term);
    }

    void becomeLeader() {
        if (state == RaftStateType.StateFollower) {
            throw new RaftInvariantException("invalid transition [follower -> leader]");
        }
        proposalDropLog.flush();
        stepFn = Raft::stepLeader;
        reset(term);
        tickFn = this::tickHeartbeat;
        lead = id;
        state = RaftStateType.StateLeader;

        Progress pr = trk.getProgress().get(id);
        pr.becomeReplicate();
        pr.setRecentActive(true);

        pendingConfIndex = raftLog.lastIndex();

        StateTrace.traceBecomeLeader(this);

        if (!appendEntry(List.of(Eraftpb.Entry.getDefaultInstance()))) {
            throw new RaftInvariantException("empty entry was dropped");
        }
        logger.info("{:x} became leader at term {}", id, term);
    }

    // ============= hup / campaign =============
    void hup(CampaignType t) {
        if (state == RaftStateType.StateLeader) {
            logger.debug("{:x} ignoring MsgHup because already leader", id);
            return;
        }
        if (!promotable()) {
            logger.warn("{:x} is unpromotable and can not campaign", id);
            return;
        }
        if (hasUnappliedConfChanges()) {
            logger.warn("{:x} cannot campaign at term {} since there are still pending configuration changes to apply", id, term);
            return;
        }
        logger.info("{:x} is starting a new election at term {}", id, term);
        campaign(t);
    }

    boolean hasUnappliedConfChanges() {
        if (raftLog.applied >= raftLog.committed) {
            return false;
        }
        long lo = raftLog.applied + 1;
        long hi = raftLog.committed + 1;
        long pageSize = raftLog.maxApplyingEntsSize;
        boolean[] found = {false};
        try {
            raftLog.scan(lo, hi, pageSize, ents -> {
                for (Eraftpb.Entry e : ents) {
                    if (e.getEntryType() == Eraftpb.EntryType.EntryConfChange ||
                        e.getEntryType() == Eraftpb.EntryType.EntryConfChangeV2) {
                        found[0] = true;
                        return true; // break
                    }
                }
                return false; // continue
            });
        } catch (RaftException e) {
            throw new RaftInvariantException(String.format("error scanning unapplied entries [%d, %d)", lo, hi), e);
        }
        return found[0];
    }

    void campaign(CampaignType t) {
        long campaignTerm;
        Eraftpb.MessageType voteMsg;
        if (t == CampaignType.CampaignPreElection) {
            becomePreCandidate();
            voteMsg = Eraftpb.MessageType.MsgRequestPreVote;
            campaignTerm = term + 1;
        } else {
            becomeCandidate();
            voteMsg = Eraftpb.MessageType.MsgRequestVote;
            campaignTerm = term;
        }

        List<Long> ids = new ArrayList<>(trk.getConfig().getVoters().ids());
        ids.sort(Long::compare);

        for (long voterId : ids) {
            if (voterId == id) {
                send(Eraftpb.Message.newBuilder()
                        .setTo(voterId)
                        .setTerm(campaignTerm)
                        .setMsgType(Util.voteRespMsgType(voteMsg)));
                continue;
            }
            EntryID last = raftLog.lastEntryID();
            logger.info("{:x} [logterm: {}, index: {}] sent {} request to {:x} at term {}",
                    id, last.term(), last.index(), voteMsg, voterId, term);

            Eraftpb.Message.Builder mb = Eraftpb.Message.newBuilder()
                    .setTo(voterId)
                    .setTerm(campaignTerm)
                    .setMsgType(voteMsg)
                    .setIndex(last.index())
                    .setLogTerm(last.term());
            if (t == CampaignType.CampaignTransfer) {
                mb.setContext(ByteString.copyFromUtf8("CampaignTransfer"));
            }
            send(mb);
        }
    }

    // ============= poll =============
    ProgressTracker.TallyResult poll(long voterId, Eraftpb.MessageType t, boolean v) {
        if (v) {
            logger.info("{:x} received {} from {:x} at term {}", id, t, voterId, term);
        } else {
            logger.info("{:x} received {} rejection from {:x} at term {}", id, t, voterId, term);
        }
        trk.recordVote(voterId, v);
        return trk.tallyVotes();
    }

    // ============= Step =============
    /**
     * Trust boundary for inbound messages. Real raft peers can deliver
     * any message type with any field values — a hostile or buggy peer,
     * or fuzz-corrupted wire bytes, can drive an internal invariant to
     * trip ({@link RaftInvariantException}) or hit an unguarded code
     * path that throws an unrelated {@link RuntimeException}. Letting
     * those propagate would crash the event loop on every malformed
     * input, turning a single bad peer into a cluster-wide DoS.
     *
     * <p>Policy: an inbound message can fail this raft's processing
     * only via the declared {@link RaftException} channel (which the
     * caller is expected to handle). Anything else thrown out of the
     * dispatch tree is caught here, logged, counted via
     * {@link RaftMetrics#onMalformedMessageDropped}, and the message is
     * silently dropped — the cluster sees this peer as silent for that
     * one message.
     *
     * <p>This is the production-grade complement to the targeted
     * input-validation guards higher in this file (term=0 vote-family,
     * MsgHeartbeat self-spoof, commit clamp, applyresp validate). Those
     * give better log messages for known patterns; this catch is the
     * safety net for everything else, including new fuzz findings that
     * land before a targeted guard does.
     *
     * <p>Internal-state-corruption bugs that aren't message-driven
     * (e.g. invariant trips inside {@link #becomeLeader},
     * {@link #applyConfChange}, {@code Raft.tickFn} on internal state)
     * keep crashing — those callers don't go through {@code step()}.
     */
    public void step(Eraftpb.Message m) throws RaftException {
        try {
            stepInternal(m);
        } catch (RaftInvariantException invariant) {
            // Known pattern: input drove an invariant trip somewhere in the
            // dispatch tree. INFO-level on the raft logger (visible but not
            // alarming); WARN-level message in the metric backend if the host
            // wires it up.
            logger.info("{:x} dropped {} from {:x}: invariant violated: {}",
                    id, m.getMsgType(), m.getFrom(), invariant.getMessage());
            metrics.onMalformedMessageDropped(m.getMsgType().toString(),
                    RaftMetrics.MalformedMessageReason.INVARIANT_VIOLATION);
        } catch (RuntimeException unexpected) {
            // Less expected: NPE, IndexOutOfBounds, arithmetic overflow,
            // ClassCast, ... a path we didn't guard. ERROR-level with
            // stack trace so a real bug is visible in logs and triageable;
            // a sustained non-zero rate of this counter is a "look at the
            // logs" alert for the operator.
            logger.error("{:x} dropped {} from {:x}: unexpected exception",
                    id, m.getMsgType(), m.getFrom(), unexpected);
            metrics.onMalformedMessageDropped(m.getMsgType().toString(),
                    RaftMetrics.MalformedMessageReason.UNEXPECTED_EXCEPTION);
        }
    }

    private void stepInternal(Eraftpb.Message m) throws RaftException {
        StateTrace.traceReceiveMessage(this, m);

        // Handle the message term
        if (m.getTerm() == 0) {
            // Vote-family messages with term 0 are malformed: a real raft peer
            // never sends MsgRequestVote / MsgRequestPreVote (or their responses)
            // without a term. Letting them through would propagate the bad term
            // into voteResp() at the dispatch site below, which then trips the
            // outbound `send()` invariant ("term should be set when sending
            // MsgRequestVote*Response"). Drop early — same policy enforced
            // symmetrically on egress.
            //
            // Surfaced by the RaftStepFuzzTest harness, which constructs
            // arbitrary inbound messages a real peer wouldn't.
            if (m.getMsgType() == Eraftpb.MessageType.MsgRequestVote
                    || m.getMsgType() == Eraftpb.MessageType.MsgRequestPreVote
                    || m.getMsgType() == Eraftpb.MessageType.MsgRequestVoteResponse
                    || m.getMsgType() == Eraftpb.MessageType.MsgRequestPreVoteResponse) {
                logger.info("{:x} ignored {} with term=0 from {:x}", id, m.getMsgType(), m.getFrom());
                return;
            }
            // Other zero-term messages are local (MsgHup, MsgPropose, MsgBeat,
            // MsgCheckQuorum, MsgStorageAppendResp, etc.).
        } else if (m.getTerm() > term) {
            if (m.getMsgType() == Eraftpb.MessageType.MsgRequestVote || m.getMsgType() == Eraftpb.MessageType.MsgRequestPreVote) {
                boolean force = m.getContext().toStringUtf8().equals("CampaignTransfer");
                boolean inLease = checkQuorum && lead != Util.NONE && electionElapsed < electionTimeout;
                if (!force && inLease) {
                    return;
                }
            }
            if (m.getMsgType() == Eraftpb.MessageType.MsgRequestPreVote) {
                // Never change our term in response to a PreVote
            } else if (m.getMsgType() == Eraftpb.MessageType.MsgRequestPreVoteResponse && !m.getReject()) {
                // Don't change term for granted pre-vote
            } else {
                logger.info("{:x} [term: {}] received a {} message with higher term from {:x} [term: {}]",
                        id, term, m.getMsgType(), m.getFrom(), m.getTerm());
                if (m.getMsgType() == Eraftpb.MessageType.MsgAppend ||
                    m.getMsgType() == Eraftpb.MessageType.MsgHeartbeat ||
                    m.getMsgType() == Eraftpb.MessageType.MsgSnapshot) {
                    becomeFollower(m.getTerm(), m.getFrom());
                } else {
                    becomeFollower(m.getTerm(), Util.NONE);
                }
            }
        } else if (m.getTerm() < term) {
            if ((checkQuorum || preVote) &&
                (m.getMsgType() == Eraftpb.MessageType.MsgHeartbeat || m.getMsgType() == Eraftpb.MessageType.MsgAppend)) {
                send(Eraftpb.Message.newBuilder()
                        .setTo(m.getFrom())
                        .setMsgType(Eraftpb.MessageType.MsgAppendResponse));
            } else if (m.getMsgType() == Eraftpb.MessageType.MsgRequestPreVote) {
                send(Eraftpb.Message.newBuilder()
                        .setTo(m.getFrom())
                        .setTerm(term)
                        .setMsgType(Eraftpb.MessageType.MsgRequestPreVoteResponse)
                        .setReject(true));
            } else if (m.getMsgType() == Eraftpb.MessageType.MsgStorageAppendResp) {
                if (m.getIndex() != 0) {
                    logger.info("{:x} [term: {}] ignored entry appends from a {} message with lower term [term: {}]",
                            id, term, m.getMsgType(), m.getTerm());
                }
                if (m.hasSnapshot()) {
                    appliedSnap(m.getSnapshot());
                }
            } else {
                logger.info("{:x} [term: {}] ignored a {} message with lower term from {:x} [term: {}]",
                        id, term, m.getMsgType(), m.getFrom(), m.getTerm());
            }
            return;
        }

        switch (m.getMsgType()) {
            case MsgHup:
                if (preVote) {
                    hup(CampaignType.CampaignPreElection);
                } else {
                    hup(CampaignType.CampaignElection);
                }
                break;

            case MsgStorageAppendResp:
                if (m.getIndex() != 0) {
                    raftLog.stableTo(new EntryID(m.getLogTerm(), m.getIndex()));
                }
                if (m.hasSnapshot()) {
                    appliedSnap(m.getSnapshot());
                }
                break;

            case MsgStorageApplyResp:
                if (m.getEntriesCount() > 0) {
                    long index = m.getEntries(m.getEntriesCount() - 1).getIndex();
                    // The apply response must report indices we previously
                    // asked the apply thread to apply — i.e. inside
                    // [applied, committed]. A fuzzer (or a malfunctioning
                    // apply thread) can deliver an arbitrary index that
                    // would trip RaftLog.appliedTo's invariant. Drop the
                    // out-of-range response silently. Surfaced by
                    // RaftStepFuzzTest.
                    if (index > raftLog.committed || index < raftLog.applied) {
                        logger.info("{:x} ignored MsgStorageApplyResp with out-of-range index {} [applied={}, committed={}]",
                                id, index, raftLog.applied, raftLog.committed);
                        break;
                    }
                    appliedTo(index, Util.entsSize(m.getEntriesList()));
                    reduceUncommittedSize(Util.payloadsSize(m.getEntriesList()));
                }
                break;

            case MsgRequestVote:
            case MsgRequestPreVote: {
                boolean canVote = vote == m.getFrom() ||
                        (vote == Util.NONE && lead == Util.NONE) ||
                        (m.getMsgType() == Eraftpb.MessageType.MsgRequestPreVote && m.getTerm() > term);
                EntryID lastID = raftLog.lastEntryID();
                EntryID candLastID = new EntryID(m.getLogTerm(), m.getIndex());
                if (canVote && raftLog.isUpToDate(candLastID)) {
                    logger.info("{:x} [logterm: {}, index: {}, vote: {:x}] cast {} for {:x} [logterm: {}, index: {}] at term {}",
                            id, lastID.term(), lastID.index(), vote, m.getMsgType(), m.getFrom(), candLastID.term(), candLastID.index(), term);
                    send(voteResp(m.getFrom(), Util.voteRespMsgType(m.getMsgType()), m.getTerm(), false));
                    if (m.getMsgType() == Eraftpb.MessageType.MsgRequestVote) {
                        electionElapsed = 0;
                        vote = m.getFrom();
                    }
                } else {
                    logger.info("{:x} [logterm: {}, index: {}, vote: {:x}] rejected {} from {:x} [logterm: {}, index: {}] at term {}",
                            id, lastID.term(), lastID.index(), vote, m.getMsgType(), m.getFrom(), candLastID.term(), candLastID.index(), term);
                    send(voteResp(m.getFrom(), Util.voteRespMsgType(m.getMsgType()), term, true));
                }
                break;
            }

            default:
                stepFn.step(this, m);
        }
    }

    // ============= stepLeader =============
    static void stepLeader(Raft r, Eraftpb.Message m) throws RaftException {
        switch (m.getMsgType()) {
            case MsgBeat:
                r.bcastHeartbeat();
                return;
            case MsgCheckQuorum:
                if (!r.trk.quorumActive()) {
                    r.logger.warn("{:x} stepped down to follower since quorum is not active", r.id);
                    r.becomeFollower(r.term, Util.NONE);
                }
                r.trk.visit((id, pr) -> {
                    if (id != r.id) {
                        pr.setRecentActive(false);
                    }
                });
                return;
            case MsgPropose:
                if (m.getEntriesCount() == 0) {
                    throw new RaftInvariantException(String.format("%x stepped empty MsgProp", r.id));
                }
                if (r.trk.getProgress().get(r.id) == null) {
                    r.metrics.onProposal(RaftMetrics.ProposalResult.DROPPED);
                    throw RaftException.ErrProposalDropped;
                }
                if (r.leadTransferee != Util.NONE) {
                    r.proposalDropLog.info("{:x} [term {}] transfer leadership to {:x} is in progress; dropping proposal",
                            r.id, r.term, r.leadTransferee);
                    r.metrics.onProposal(RaftMetrics.ProposalResult.DROPPED);
                    throw RaftException.ErrProposalDropped;
                }

                List<Eraftpb.Entry> entries = new ArrayList<>(m.getEntriesList());
                for (int i = 0; i < entries.size(); i++) {
                    Eraftpb.Entry e = entries.get(i);
                    if (e.getEntryType() == Eraftpb.EntryType.EntryConfChange ||
                        e.getEntryType() == Eraftpb.EntryType.EntryConfChangeV2) {

                        // Parse both V1 and V2 conf changes to a unified V2
                        // representation so wantsLeaveJoint and the trace event
                        // can be computed consistently.
                        Eraftpb.ConfChangeV2 ccv2;
                        try {
                            if (e.getEntryType() == Eraftpb.EntryType.EntryConfChange) {
                                Eraftpb.ConfChange ccv1 = Eraftpb.ConfChange.parseFrom(e.getData());
                                ccv2 = Eraftpb.ConfChangeV2.newBuilder()
                                        .addChanges(Eraftpb.ConfChangeSingle.newBuilder()
                                                .setType(ccv1.getChangeType())
                                                .setNodeId(ccv1.getNodeId()))
                                        .build();
                            } else {
                                ccv2 = Eraftpb.ConfChangeV2.parseFrom(e.getData());
                            }
                        } catch (com.google.protobuf.InvalidProtocolBufferException ex) {
                            // The conf-change bytes in the raft log are
                            // produced by us and committed; a parse failure
                            // here means the log is corrupt or out of sync
                            // with the protobuf schema. Surface as an
                            // invariant violation rather than a generic
                            // RuntimeException so callers can distinguish.
                            throw new RaftInvariantException(
                                    "failed to parse conf change entry at index " + e.getIndex(), ex);
                        }

                        boolean alreadyPending = r.pendingConfIndex > r.raftLog.applied;
                        boolean alreadyJoint = !r.trk.getConfig().getVoters().getConfigs()[1].isEmpty();
                        boolean wantsLeaveJoint = ccv2.getChangesCount() == 0;

                        String failedCheck = null;
                        if (alreadyPending) {
                            failedCheck = String.format("possible unapplied conf change at index %d (applied to %d)",
                                    r.pendingConfIndex, r.raftLog.applied);
                        } else if (alreadyJoint && !wantsLeaveJoint) {
                            failedCheck = "must transition out of joint config first";
                        } else if (!alreadyJoint && wantsLeaveJoint) {
                            failedCheck = "not in joint state; refusing empty conf change";
                        }

                        if (failedCheck != null && !r.disableConfChangeValidation) {
                            r.logger.info("{:x} ignoring conf change at config {}: {}", r.id, r.trk.getConfig(), failedCheck);
                            entries.set(i, Eraftpb.Entry.newBuilder().setEntryType(Eraftpb.EntryType.EntryNormal).build());
                        } else {
                            r.pendingConfIndex = r.raftLog.lastIndex() + i + 1;
                            StateTrace.traceChangeConfEvent(ccv2, r);
                        }
                    }
                }

                if (!r.appendEntry(entries)) {
                    r.metrics.onProposal(RaftMetrics.ProposalResult.DROPPED);
                    throw RaftException.ErrProposalDropped;
                }
                r.metrics.onProposal(RaftMetrics.ProposalResult.ACCEPTED);
                r.bcastAppend();
                return;

            case MsgReadIndex:
                if (r.trk.isSingleton()) {
                    Eraftpb.Message resp = r.responseToReadIndexReq(m, r.raftLog.committed);
                    if (resp.getTo() != Util.NONE) {
                        r.send(resp.toBuilder());
                    }
                    return;
                }
                if (!r.committedEntryInCurrentTerm()) {
                    r.appendPendingReadIndex(m);
                    return;
                }
                sendMsgReadIndexResponse(r, m);
                return;

            case MsgForgetLeader:
                return;

            default:
                break;
        }

        // All other message types require a progress for m.From
        Progress pr = r.trk.getProgress().get(m.getFrom());
        if (pr == null) {
            r.logger.debug("{:x} no progress available for {:x}", r.id, m.getFrom());
            return;
        }

        switch (m.getMsgType()) {
            case MsgAppendResponse:
                pr.setRecentActive(true);
                // Mirror etcd-raft: unconditionally clear the flow-paused flag on
                // any MsgAppResp. Relying on maybeUpdate/maybeDecrTo to clear it
                // misses stale or duplicate responses (n <= match, rejected <= match
                // in StateReplicate), which previously stalled flow until the next
                // heartbeat tick.
                pr.setMsgAppFlowPaused(false);
                if (m.getReject()) {
                    r.logger.debug("{:x} received MsgAppResp(rejected, hint: (index {}, term {})) from {:x} for index {}",
                            r.id, m.getRejectHint(), m.getLogTerm(), m.getFrom(), m.getIndex());
                    long nextProbeIdx = m.getRejectHint();
                    if (m.getLogTerm() > 0) {
                        RaftLog.FindConflictResult fcr = r.raftLog.findConflictByTerm(m.getRejectHint(), m.getLogTerm());
                        nextProbeIdx = fcr.index();
                    }
                    if (pr.maybeDecrTo(m.getIndex(), nextProbeIdx)) {
                        r.logger.debug("{:x} decreased progress of {:x} to [{}]", r.id, m.getFrom(), pr);
                        if (pr.getState() == StateType.StateReplicate) {
                            pr.becomeProbe();
                        }
                        r.sendAppend(m.getFrom());
                    }
                } else {
                    if (pr.maybeUpdate(m.getIndex()) || (pr.getMatch() == m.getIndex() && pr.getState() == StateType.StateProbe)) {
                        switch (pr.getState()) {
                            case StateProbe:
                                pr.becomeReplicate();
                                break;
                            case StateSnapshot:
                                if (pr.getMatch() + 1 >= r.raftLog.firstIndex()) {
                                    r.logger.debug("{:x} recovered from needing snapshot, resumed sending replication messages to {:x} [{}]",
                                            r.id, m.getFrom(), pr);
                                    pr.becomeProbe();
                                    pr.becomeReplicate();
                                }
                                break;
                            case StateReplicate:
                                pr.getInflights().freeLE(m.getIndex());
                                break;
                        }

                        if (r.maybeCommit()) {
                            releasePendingReadIndexMessages(r);
                            r.bcastAppend();
                        } else if (r.id != m.getFrom() && pr.canBumpCommit(r.raftLog.committed)) {
                            r.sendAppend(m.getFrom());
                        }
                        if (r.id != m.getFrom()) {
                            while (r.maybeSendAppend(m.getFrom(), false)) {
                                // drain: send as many appends as the peer can accept
                            }
                        }
                        if (m.getFrom() == r.leadTransferee && pr.getMatch() == r.raftLog.lastIndex()) {
                            r.logger.info("{:x} sent MsgTimeoutNow to {:x} after received MsgAppResp", r.id, m.getFrom());
                            r.sendTimeoutNow(m.getFrom());
                        }
                    }
                }
                break;

            case MsgHeartbeatResponse:
                pr.setRecentActive(true);
                pr.setMsgAppFlowPaused(false);
                if (pr.getMatch() < r.raftLog.lastIndex() || pr.getState() == StateType.StateProbe) {
                    r.sendAppend(m.getFrom());
                }
                if (r.readOnly.option != ReadOnlyOption.ReadOnlySafe || m.getContext().isEmpty()) {
                    return;
                }
                r.readOnly.recvAck(m.getFrom(), m.getContext().toByteArray());
                List<ReadOnly.ReadIndexRequest> rss = r.readOnly.maybeAdvance(r.trk.getConfig().getVoters());
                if (rss != null) {
                    for (ReadOnly.ReadIndexRequest rs : rss) {
                        Eraftpb.Message resp = r.responseToReadIndexReq(rs.req(), rs.index());
                        if (resp.getTo() != Util.NONE) {
                            r.send(resp.toBuilder());
                        }
                    }
                }
                break;

            case MsgSnapStatus:
                if (pr.getState() != StateType.StateSnapshot) {
                    return;
                }
                if (!m.getReject()) {
                    pr.becomeProbe();
                    r.logger.debug("{:x} snapshot succeeded, resumed sending replication messages to {:x} [{}]", r.id, m.getFrom(), pr);
                } else {
                    pr.setPendingSnapshot(0);
                    pr.becomeProbe();
                    r.logger.debug("{:x} snapshot failed, resumed sending replication messages to {:x} [{}]", r.id, m.getFrom(), pr);
                }
                pr.setMsgAppFlowPaused(true);
                break;

            case MsgUnreachable:
                if (pr.getState() == StateType.StateReplicate) {
                    pr.becomeProbe();
                }
                r.logger.debug("{:x} failed to send message to {:x} because it is unreachable [{}]", r.id, m.getFrom(), pr);
                break;

            case MsgTransferLeader:
                if (pr.isLearner()) {
                    r.logger.debug("{:x} is learner. Ignored transferring leadership", r.id);
                    return;
                }
                long leadTransferee = m.getFrom();
                long lastLeadTransferee = r.leadTransferee;
                if (lastLeadTransferee != Util.NONE) {
                    if (lastLeadTransferee == leadTransferee) {
                        return;
                    }
                    r.abortLeaderTransfer();
                }
                if (leadTransferee == r.id) {
                    return;
                }
                r.logger.info("{:x} [term {}] starts to transfer leadership to {:x}", r.id, r.term, leadTransferee);
                r.electionElapsed = 0;
                r.leadTransferee = leadTransferee;
                if (pr.getMatch() == r.raftLog.lastIndex()) {
                    r.sendTimeoutNow(leadTransferee);
                } else {
                    r.sendAppend(leadTransferee);
                }
                break;
        }
        return;
    }

    // ============= stepCandidate =============
    static void stepCandidate(Raft r, Eraftpb.Message m) throws RaftException {
        Eraftpb.MessageType myVoteRespType = (r.state == RaftStateType.StatePreCandidate) ?
                Eraftpb.MessageType.MsgRequestPreVoteResponse :
                Eraftpb.MessageType.MsgRequestVoteResponse;

        switch (m.getMsgType()) {
            case MsgPropose:
                r.proposalDropLog.info("{:x} no leader at term {}; dropping proposal", r.id, r.term);
                throw RaftException.ErrProposalDropped;
            case MsgAppend:
                r.becomeFollower(m.getTerm(), m.getFrom());
                r.handleAppendEntries(m);
                break;
            case MsgHeartbeat:
                r.becomeFollower(m.getTerm(), m.getFrom());
                r.handleHeartbeat(m);
                break;
            case MsgSnapshot:
                r.becomeFollower(m.getTerm(), m.getFrom());
                r.handleSnapshot(m);
                break;
            case MsgTimeoutNow:
                r.logger.debug("{:x} [term {} state {}] ignored MsgTimeoutNow from {:x}", r.id, r.term, r.state, m.getFrom());
                break;
            default:
                if (m.getMsgType() == myVoteRespType) {
                    ProgressTracker.TallyResult pr = r.poll(m.getFrom(), m.getMsgType(), !m.getReject());
                    r.logger.info("{:x} has received {} {} votes and {} vote rejections", r.id, pr.granted(), m.getMsgType(), pr.rejected());
                    switch (pr.result()) {
                        case VoteWon:
                            if (r.state == RaftStateType.StatePreCandidate) {
                                r.campaign(CampaignType.CampaignElection);
                            } else {
                                r.becomeLeader();
                                r.bcastAppend();
                            }
                            break;
                        case VoteLost:
                            r.becomeFollower(r.term, Util.NONE);
                            break;
                    }
                }
                break;
        }
        return;
    }

    // ============= stepFollower =============
    static void stepFollower(Raft r, Eraftpb.Message m) throws RaftException {
        switch (m.getMsgType()) {
            case MsgPropose:
                if (r.lead == Util.NONE) {
                    r.proposalDropLog.info("{:x} no leader at term {}; dropping proposal", r.id, r.term);
                    throw RaftException.ErrProposalDropped;
                } else if (r.disableProposalForwarding) {
                    r.proposalDropLog.info("{:x} not forwarding to leader {:x} at term {}; dropping proposal", r.id, r.lead, r.term);
                    throw RaftException.ErrProposalDropped;
                }
                r.send(m.toBuilder().setTo(r.lead));
                break;
            case MsgAppend:
                r.electionElapsed = 0;
                r.lead = m.getFrom();
                r.handleAppendEntries(m);
                break;
            case MsgHeartbeat:
                r.electionElapsed = 0;
                r.lead = m.getFrom();
                r.handleHeartbeat(m);
                break;
            case MsgSnapshot:
                r.electionElapsed = 0;
                r.lead = m.getFrom();
                r.handleSnapshot(m);
                break;
            case MsgTransferLeader:
                if (r.lead == Util.NONE) {
                    return;
                }
                r.send(m.toBuilder().setTo(r.lead));
                break;
            case MsgForgetLeader:
                if (r.readOnly.option == ReadOnlyOption.ReadOnlyLeaseBased) {
                    r.logger.error("ignoring MsgForgetLeader due to ReadOnlyLeaseBased");
                    return;
                }
                if (r.lead != Util.NONE) {
                    r.logger.info("{:x} forgetting leader {:x} at term {}", r.id, r.lead, r.term);
                    r.lead = Util.NONE;
                }
                break;
            case MsgTimeoutNow:
                r.logger.info("{:x} [term {}] received MsgTimeoutNow from {:x} and starts an election to get leadership.",
                        r.id, r.term, m.getFrom());
                r.hup(CampaignType.CampaignTransfer);
                break;
            case MsgReadIndex:
                if (r.lead == Util.NONE) {
                    return;
                }
                r.send(m.toBuilder().setTo(r.lead));
                break;
            case MsgReadIndexResp:
                if (m.getEntriesCount() != 1) {
                    r.logger.error("{:x} invalid format of MsgReadIndexResp from {:x}, entries count: {}", r.id, m.getFrom(), m.getEntriesCount());
                    return;
                }
                r.appendReadState(new ReadState(m.getIndex(), m.getEntries(0).getData().toByteArray()));
                break;
        }
        return;
    }

    // ============= handleAppendEntries / handleHeartbeat / handleSnapshot =============
    void handleAppendEntries(Eraftpb.Message m) {
        LogSlice a = logSliceFromMsgApp(m);
        if (a.prev().index() < raftLog.committed) {
            send(appendRespAccept(m.getFrom(), raftLog.committed));
            return;
        }
        RaftLog.MaybeAppendResult result = raftLog.maybeAppend(a, m.getCommit());
        if (result.ok()) {
            send(appendRespAccept(m.getFrom(), result.lastNewIndex()));
            return;
        }

        long hintIndex = Math.min(m.getIndex(), raftLog.lastIndex());
        RaftLog.FindConflictResult fcr = raftLog.findConflictByTerm(hintIndex, m.getLogTerm());
        send(appendRespReject(m.getFrom(), m.getIndex(), fcr.index(), fcr.term()));
    }

    void handleHeartbeat(Eraftpb.Message m) {
        // Heartbeat with from == self is malformed — real peers don't spoof
        // our id, but a fuzzer / hostile peer can. Echoing m.getFrom() into
        // the response's `to` would build a self-addressed
        // MsgHeartbeatResponse and trip the egress invariant. Drop the
        // bogus heartbeat silently; nothing else in the handler depends on
        // it. Surfaced by RaftStepFuzzTest.
        if (m.getFrom() == id) {
            logger.info("{:x} ignored MsgHeartbeat from self", id);
            return;
        }
        // The leader's commit pointer is meaningful only up to entries we
        // actually have. A legitimate leader sends commit = min(leader.commit,
        // follower.match) so commit > our lastIndex shouldn't happen — but a
        // fuzzer / hostile peer can craft a heartbeat with arbitrary commit
        // that would trip RaftLog.commitTo's "tocommit out of range" invariant.
        // Clamp at lastIndex; the legitimate case (commit ≤ lastIndex) is
        // unchanged, and the malformed case becomes a no-op (commitTo only
        // advances). Matches the implicit clamp inside maybeAppend()'s own
        // commitTo call, so the heartbeat path is now as defensive as the
        // append path. Surfaced by RaftStepFuzzTest.
        raftLog.commitTo(Math.min(m.getCommit(), raftLog.lastIndex()));
        send(Eraftpb.Message.newBuilder()
                .setTo(m.getFrom())
                .setMsgType(Eraftpb.MessageType.MsgHeartbeatResponse)
                .setContext(m.getContext()));
    }

    void handleSnapshot(Eraftpb.Message m) {
        Eraftpb.Snapshot s = m.hasSnapshot() ? m.getSnapshot() : Eraftpb.Snapshot.getDefaultInstance();
        long sindex = s.getMetadata().getIndex();
        if (restore(s)) {
            logger.info("{:x} [commit: {}] restored snapshot [index: {}, term: {}]",
                    id, raftLog.committed, sindex, s.getMetadata().getTerm());
            send(appendRespAccept(m.getFrom(), raftLog.lastIndex()));
        } else {
            logger.info("{:x} [commit: {}] ignored snapshot [index: {}, term: {}]",
                    id, raftLog.committed, sindex, s.getMetadata().getTerm());
            send(appendRespAccept(m.getFrom(), raftLog.committed));
        }
    }

    // ============= restore =============
    boolean restore(Eraftpb.Snapshot s) {
        if (s.getMetadata().getIndex() <= raftLog.committed) {
            return false;
        }
        if (state != RaftStateType.StateFollower) {
            logger.warn("{:x} attempted to restore snapshot as leader; should never happen", id);
            becomeFollower(term + 1, Util.NONE);
            return false;
        }

        boolean found = false;
        Eraftpb.ConfState cs = s.getMetadata().getConfState();
        for (long nodeId : cs.getVotersList()) {
            if (nodeId == id) { found = true; break; }
        }
        if (!found) for (long nodeId : cs.getLearnersList()) {
            if (nodeId == id) { found = true; break; }
        }
        if (!found) for (long nodeId : cs.getVotersOutgoingList()) {
            if (nodeId == id) { found = true; break; }
        }
        if (!found) {
            logger.warn("{:x} attempted to restore snapshot but it is not in the ConfState; should never happen", id);
            return false;
        }

        EntryID snapID = new EntryID(s.getMetadata().getTerm(), s.getMetadata().getIndex());
        if (raftLog.matchTerm(snapID)) {
            logger.info("{:x} [commit: {}, lastindex: {}] fast-forwarded commit to snapshot [index: {}, term: {}]",
                    id, raftLog.committed, raftLog.lastIndex(), snapID.index(), snapID.term());
            raftLog.commitTo(s.getMetadata().getIndex());
            return false;
        }

        raftLog.restore(s);

        trk = ProgressTracker.make(trk.getMaxInflight(), trk.getMaxInflightBytes());
        Changer changer = new Changer(trk, raftLog.lastIndex());
        Changer.Result rr = Changer.restore(changer, cs);
        Util.assertConfStatesEquivalent(cs, switchToConfig(rr.config(), rr.progress()));

        logger.info("{:x} [commit: {}, lastindex: {}] restored snapshot [index: {}, term: {}]",
                id, raftLog.committed, raftLog.lastIndex(), snapID.index(), snapID.term());
        return true;
    }

    // ============= promotable =============
    boolean promotable() {
        Progress pr = trk.getProgress().get(id);
        return pr != null && !pr.isLearner() && !raftLog.hasNextOrInProgressSnapshot();
    }

    // ============= applyConfChange / switchToConfig =============
    public Eraftpb.ConfState applyConfChange(Eraftpb.ConfChangeV2 cc) {
        Changer changer = new Changer(trk, raftLog.lastIndex());
        Changer.Result cr;
        if (leaveJoint(cc)) {
            cr = changer.leaveJoint();
        } else {
            EnterJointResult ej = enterJoint(cc);
            if (ej != null) {
                cr = changer.enterJoint(ej.autoLeave, cc.getChangesList());
            } else {
                cr = changer.simple(cc.getChangesList());
            }
        }
        return switchToConfig(cr.config(), cr.progress());
    }

    /**
     * leaveJoint mirrors etcd-raft's pb.ConfChangeV2.LeaveJoint(): true iff the
     * ConfChangeV2 carries no changes and is not a deliberate explicit/implicit
     * transition request, indicating the caller wants to leave the joint config.
     */
    static boolean leaveJoint(Eraftpb.ConfChangeV2 cc) {
        return cc.getChangesCount() == 0
                && cc.getTransition() == Eraftpb.ConfChangeTransition.ConfChangeTransitionAuto;
    }

    /**
     * enterJoint mirrors etcd-raft's pb.ConfChangeV2.EnterJoint(): returns a
     * non-null result iff Joint Consensus must be used (transition is explicit,
     * or more than one change is present). The {@code autoLeave} flag captures
     * whether raft should automatically leave the joint state once committed.
     */
    static EnterJointResult enterJoint(Eraftpb.ConfChangeV2 cc) {
        if (cc.getTransition() == Eraftpb.ConfChangeTransition.ConfChangeTransitionAuto
                && cc.getChangesCount() <= 1) {
            return null;
        }
        boolean autoLeave;
        switch (cc.getTransition()) {
            case ConfChangeTransitionAuto:
            case ConfChangeTransitionJointImplicit:
                autoLeave = true;
                break;
            case ConfChangeTransitionJointExplicit:
                autoLeave = false;
                break;
            default:
                throw new IllegalArgumentException("unknown transition: " + cc.getTransition());
        }
        return new EnterJointResult(autoLeave);
    }

    record EnterJointResult(boolean autoLeave) {}

    Eraftpb.ConfState switchToConfig(ProgressTracker.Config cfg, Map<Long, Progress> progressMap) {
        // Trace before mutating trk so the recorded event reflects the
        // pre-application state (matches etcd-raft's switchToConfig).
        StateTrace.traceConfChangeEvent(cfg, this);

        trk.setConfig(cfg);
        trk.setProgress(progressMap);

        logger.info("{:x} switched to configuration {}", id, trk.getConfig());
        Eraftpb.ConfState cs = trk.confState();
        Progress pr = trk.getProgress().get(id);

        isLearner = pr != null && pr.isLearner();

        if ((pr == null || isLearner) && state == RaftStateType.StateLeader) {
            if (stepDownOnRemoval) {
                becomeFollower(term, Util.NONE);
            }
            return cs;
        }

        if (state != RaftStateType.StateLeader || cs.getVotersCount() == 0) {
            return cs;
        }

        if (maybeCommit()) {
            bcastAppend();
        } else {
            trk.visit((nodeId, p) -> {
                if (nodeId != id) {
                    maybeSendAppend(nodeId, false);
                }
            });
        }

        if (!trk.getConfig().getVoters().ids().contains(leadTransferee) && leadTransferee != 0) {
            abortLeaderTransfer();
        }

        return cs;
    }

    // ============= loadState =============
    void loadState(Eraftpb.HardState hs) {
        if (hs.getCommit() < raftLog.committed || hs.getCommit() > raftLog.lastIndex()) {
            throw new RaftInvariantException(String.format("%x state.commit %d is out of range [%d, %d]",
                    id, hs.getCommit(), raftLog.committed, raftLog.lastIndex()));
        }
        raftLog.committed = hs.getCommit();
        term = hs.getTerm();
        vote = hs.getVote();
    }

    // ============= misc =============
    boolean pastElectionTimeout() {
        return electionElapsed >= randomizedElectionTimeout;
    }

    void resetRandomizedElectionTimeout() {
        randomizedElectionTimeout = electionTimeout + ThreadLocalRandom.current().nextInt(electionTimeout);
    }

    void sendTimeoutNow(long to) {
        send(Eraftpb.Message.newBuilder()
                .setTo(to)
                .setMsgType(Eraftpb.MessageType.MsgTimeoutNow));
    }

    void abortLeaderTransfer() {
        leadTransferee = Util.NONE;
    }

    boolean committedEntryInCurrentTerm() {
        return raftLog.zeroTermOnOutOfBounds(raftLog.termResult(raftLog.committed)) == term;
    }

    /**
     * Append a pending MsgReadIndex with bounded-queue policy: drop oldest
     * when {@code maxPendingReadIndexMessages > 0} and the cap is reached.
     * Mitigates OOM when leader's current term has no committed entry yet
     * (so reads pile up) and read QPS is high.
     */
    void appendPendingReadIndex(Eraftpb.Message m) {
        if (maxPendingReadIndexMessages > 0 && pendingReadIndexMessages.size() >= maxPendingReadIndexMessages) {
            pendingReadIndexMessages.remove(0);
            logger.warn("{:x} pendingReadIndexMessages full (cap={}), dropping oldest", id, maxPendingReadIndexMessages);
            metrics.onReadIndexDropped(RaftMetrics.ReadIndexDropReason.QUEUE_FULL);
        }
        pendingReadIndexMessages.add(m);
    }

    /**
     * Append a confirmed read-state with bounded-queue policy: drop oldest
     * when {@code maxReadStates > 0} and the cap is reached. Mitigates
     * OOM when the host application is slow to drain Ready.readStates.
     */
    void appendReadState(ReadState rs) {
        if (maxReadStates > 0 && readStates.size() >= maxReadStates) {
            readStates.remove(0);
            logger.warn("{:x} readStates full (cap={}), dropping oldest", id, maxReadStates);
            metrics.onReadStateEvicted();
        }
        readStates.add(rs);
    }

    Eraftpb.Message responseToReadIndexReq(Eraftpb.Message req, long readIndex) {
        if (req.getFrom() == Util.NONE || req.getFrom() == id) {
            appendReadState(new ReadState(readIndex, req.getEntries(0).getData().toByteArray()));
            return Eraftpb.Message.getDefaultInstance();
        }
        return Eraftpb.Message.newBuilder()
                .setMsgType(Eraftpb.MessageType.MsgReadIndexResp)
                .setTo(req.getFrom())
                .setIndex(readIndex)
                .addAllEntries(req.getEntriesList())
                .build();
    }

    boolean increaseUncommittedSize(List<Eraftpb.Entry> ents) {
        long s = Util.payloadsSize(ents);
        if (uncommittedSize > 0 && s > 0 && uncommittedSize + s > maxUncommittedSize) {
            return false;
        }
        uncommittedSize += s;
        return true;
    }

    void reduceUncommittedSize(long s) {
        if (s > uncommittedSize) {
            uncommittedSize = 0;
        } else {
            uncommittedSize -= s;
        }
    }

    static void releasePendingReadIndexMessages(Raft r) {
        if (r.pendingReadIndexMessages.isEmpty()) {
            return;
        }
        if (!r.committedEntryInCurrentTerm()) {
            r.logger.error("pending MsgReadIndex should be released only after first commit in current term");
            return;
        }
        List<Eraftpb.Message> msgs = r.pendingReadIndexMessages;
        r.pendingReadIndexMessages = new ArrayList<>();
        for (Eraftpb.Message m : msgs) {
            sendMsgReadIndexResponse(r, m);
        }
    }

    static void sendMsgReadIndexResponse(Raft r, Eraftpb.Message m) {
        switch (r.readOnly.option) {
            case ReadOnlySafe:
                r.readOnly.addRequest(r.raftLog.committed, m);
                r.readOnly.recvAck(r.id, r.readOnly.heartbeatCtx());
                r.bcastHeartbeat();
                break;
            case ReadOnlyLeaseBased:
                Eraftpb.Message resp = r.responseToReadIndexReq(m, r.raftLog.committed);
                if (resp.getTo() != Util.NONE) {
                    r.send(resp.toBuilder());
                }
                break;
        }
    }

    static LogSlice logSliceFromMsgApp(Eraftpb.Message m) {
        return new LogSlice(m.getTerm(),
                new EntryID(m.getLogTerm(), m.getIndex()),
                m.getEntriesList());
    }

    // ============= test helpers (mirroring Go test methods) =============

    /**
     * readMessages drains pending messages, first advancing any after-append messages.
     * This is a test helper matching the Go raft_test.go readMessages.
     */
    List<Eraftpb.Message> readMessages() {
        advanceMessagesAfterAppend();
        List<Eraftpb.Message> result = msgs;
        msgs = new ArrayList<>();
        return result;
    }

    void advanceMessagesAfterAppend() {
        while (true) {
            List<Eraftpb.Message> after = takeMessagesAfterAppend();
            if (after.isEmpty()) {
                break;
            }
            stepOrSend(after);
        }
    }

    List<Eraftpb.Message> takeMessagesAfterAppend() {
        List<Eraftpb.Message> result = msgsAfterAppend;
        msgsAfterAppend = new ArrayList<>();
        return result;
    }

    void stepOrSend(List<Eraftpb.Message> messages) {
        for (Eraftpb.Message m : messages) {
            if (m.getTo() == id) {
                // Self-delivered messages from msgsAfterAppend (vote / append
                // responses, etc.) are never MsgPropose, so step never raises.
                // Defensive catch to keep the void signature.
                try {
                    step(m);
                } catch (RaftException e) {
                    logger.error("{:x} unexpected RaftException from self-step {}: {}",
                            id, m.getMsgType(), e);
                }
            } else {
                msgs.add(m);
            }
        }
    }

}
