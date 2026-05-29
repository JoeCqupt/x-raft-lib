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
import io.github.xinfra.lab.raft.internal.*;

import io.github.xinfra.lab.raft.proto.Eraftpb;
import io.github.xinfra.lab.raft.internal.quorum.JointConfig;
import io.github.xinfra.lab.raft.internal.quorum.MajorityConfig;
import io.github.xinfra.lab.raft.internal.tracker.Inflights;
import io.github.xinfra.lab.raft.internal.tracker.Progress;
import io.github.xinfra.lab.raft.internal.tracker.ProgressTracker;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Changer facilitates configuration changes. It exposes methods to handle
 * simple and joint consensus while performing the proper validation that allows
 * refusing invalid configuration changes before they affect the active configuration.
 */
public class Changer {
    private final ProgressTracker tracker;
    private final long lastIndex;

    public Changer(ProgressTracker tracker, long lastIndex) {
        this.tracker = tracker;
        this.lastIndex = lastIndex;
    }

    /**
     * Simple carries out a series of configuration changes that (in aggregate)
     * mutates the incoming majority config Voters[0] by at most one. Returns an
     * error if that is not the case, if the resulting quorum is zero, or if the
     * configuration is in a joint state.
     */
    public Result simple(List<Eraftpb.ConfChangeSingle> ccs) {
        Snapshot s = checkAndCopy();
        ProgressTracker.Config cfg = s.config;
        Map<Long, Progress> trk = s.progress;

        if (joint(cfg)) {
            throw new IllegalArgumentException("can't apply simple config change in joint config");
        }
        applyChanges(cfg, trk, ccs);

        if (symdiff(tracker.getConfig().getVoters().incoming().ids(),
                    cfg.getVoters().incoming().ids()) > 1) {
            throw new IllegalArgumentException(
                    "more than one voter changed without entering joint config");
        }

        return checkAndReturn(cfg, trk);
    }

    /**
     * EnterJoint verifies that the outgoing majority config of the joint config
     * is empty and initializes it with a copy of the incoming majority config.
     * Then applies the supplied changes to the incoming majority config.
     */
    public Result enterJoint(boolean autoLeave, List<Eraftpb.ConfChangeSingle> ccs) {
        Snapshot s = checkAndCopy();
        ProgressTracker.Config cfg = s.config;
        Map<Long, Progress> trk = s.progress;

        if (joint(cfg)) {
            throw new IllegalArgumentException("config is already joint");
        }
        if (cfg.getVoters().incoming().isEmpty()) {
            // We allow adding nodes to an empty config for convenience (testing
            // and bootstrap), but you can't enter a joint state.
            throw new IllegalArgumentException("can't make a zero-voter config joint");
        }
        // Clear the outgoing config and copy incoming to outgoing.
        MajorityConfig newOutgoing = new MajorityConfig(cfg.getVoters().incoming().ids());
        cfg.setVoters(new JointConfig(cfg.getVoters().incoming(), newOutgoing));

        applyChanges(cfg, trk, ccs);
        cfg.setAutoLeave(autoLeave);

        return checkAndReturn(cfg, trk);
    }

    /**
     * LeaveJoint transitions out of a joint configuration. The outgoing majority
     * config is removed; staged learners (LearnersNext) are promoted into Learners.
     */
    public Result leaveJoint() {
        Snapshot s = checkAndCopy();
        ProgressTracker.Config cfg = s.config;
        Map<Long, Progress> trk = s.progress;

        if (!joint(cfg)) {
            throw new IllegalArgumentException("can't leave a non-joint config");
        }

        if (cfg.getLearnersNext() != null) {
            for (long id : cfg.getLearnersNext()) {
                if (cfg.getLearners() == null) {
                    cfg.setLearners(new HashSet<>());
                }
                cfg.getLearners().add(id);
                Progress pr = trk.get(id);
                if (pr == null) {
                    throw new IllegalStateException("no progress for " + id + " in LearnersNext");
                }
                pr.setLearner(true);
            }
            cfg.setLearnersNext(null);
        }

        // Iterate over a snapshot of outgoing because we may modify trk.
        for (long id : new HashSet<>(cfg.getVoters().outgoing().ids())) {
            boolean isVoter = cfg.getVoters().incoming().contains(id);
            boolean isLearner = cfg.getLearners() != null && cfg.getLearners().contains(id);
            if (!isVoter && !isLearner) {
                trk.remove(id);
            }
        }

        cfg.setVoters(new JointConfig(cfg.getVoters().incoming(), null));
        cfg.setAutoLeave(false);

        return checkAndReturn(cfg, trk);
    }

    private void applyChanges(ProgressTracker.Config cfg, Map<Long, Progress> trk,
                              List<Eraftpb.ConfChangeSingle> ccs) {
        for (Eraftpb.ConfChangeSingle cc : ccs) {
            applyChange(cfg, trk, cc);
        }
        if (cfg.getVoters().incoming().isEmpty()) {
            throw new IllegalArgumentException("removed all voters");
        }
    }

    private void applyChange(ProgressTracker.Config cfg, Map<Long, Progress> trk,
                             Eraftpb.ConfChangeSingle cc) {
        long id = cc.getNodeId();
        if (id == 0) {
            // etcd replaces NodeId with zero if it decides to skip a change.
            return;
        }
        switch (cc.getType()) {
            case ConfChangeAddNode -> makeVoter(cfg, trk, id);
            case ConfChangeAddLearnerNode -> makeLearner(cfg, trk, id);
            case ConfChangeRemoveNode -> remove(cfg, trk, id);
            case ConfChangeUpdateNode -> { /* noop */ }
            default -> throw new IllegalArgumentException("unexpected conf type " + cc.getType());
        }
    }

    private void makeVoter(ProgressTracker.Config cfg, Map<Long, Progress> trk, long id) {
        Progress pr = trk.get(id);
        if (pr == null) {
            initProgress(cfg, trk, id, false);
            return;
        }
        pr.setLearner(false);
        if (cfg.getLearners() != null) cfg.getLearners().remove(id);
        if (cfg.getLearnersNext() != null) cfg.getLearnersNext().remove(id);
        cfg.getVoters().incoming().add(id);
    }

    /**
     * makeLearner makes the given ID a learner or stages it to be a learner once
     * an active joint configuration is exited. If the peer is a voter in the
     * outgoing config, we add it to LearnersNext to preserve the invariant that
     * Voters and Learners do not intersect.
     */
    private void makeLearner(ProgressTracker.Config cfg, Map<Long, Progress> trk, long id) {
        Progress pr = trk.get(id);
        if (pr == null) {
            initProgress(cfg, trk, id, true);
            return;
        }
        if (pr.isLearner()) {
            return;
        }
        // Remove any existing voter in the incoming config (and clean up
        // Learners/LearnersNext), then re-attach the saved Progress.
        remove(cfg, trk, id);
        trk.put(id, pr);

        if (cfg.getVoters().outgoing().contains(id)) {
            // The peer is still tracked as a voter in the outgoing config; stage
            // it as a LearnersNext so it will become a Learner upon LeaveJoint.
            if (cfg.getLearnersNext() == null) cfg.setLearnersNext(new HashSet<>());
            cfg.getLearnersNext().add(id);
        } else {
            pr.setLearner(true);
            if (cfg.getLearners() == null) cfg.setLearners(new HashSet<>());
            cfg.getLearners().add(id);
        }
    }

    private void remove(ProgressTracker.Config cfg, Map<Long, Progress> trk, long id) {
        if (!trk.containsKey(id)) {
            return;
        }
        cfg.getVoters().incoming().remove(id);
        if (cfg.getLearners() != null) cfg.getLearners().remove(id);
        if (cfg.getLearnersNext() != null) cfg.getLearnersNext().remove(id);
        // If the peer is still a voter in the outgoing config, keep its Progress.
        if (!cfg.getVoters().outgoing().contains(id)) {
            trk.remove(id);
        }
    }

    private void initProgress(ProgressTracker.Config cfg, Map<Long, Progress> trk,
                              long id, boolean isLearner) {
        // Match invariant: Match < Next, so Next must be at least 1.
        //
        // Note: this is a deliberate divergence from etcd-raft, which uses
        // `Next = lastIndex + 1`. With +1, switchToConfig's sendIfEmpty=false
        // path emits no probe to a freshly-added peer when the conf change
        // entry is the leader's last entry, leaving the new peer idle until a
        // heartbeat or proposal triggers send. With Next=lastIndex, the first
        // probe lands one earlier and (on a freshly-compacted leader) hits
        // termResult(prevIndex)→ErrCompacted, driving the snapshot path
        // directly without the extra append→reject round-trip that the +1
        // path requires for the empty-fresh-peer case.
        long next = Math.max(lastIndex, 1);
        Progress pr = new Progress(0, next,
                new Inflights(tracker.getMaxInflight(), tracker.getMaxInflightBytes()),
                isLearner);
        // Mark as recently active. Otherwise, CheckQuorum may cause us to step
        // down if it is invoked before the added node has had a chance to
        // communicate with us.
        pr.setRecentActive(true);
        if (isLearner) {
            if (cfg.getLearners() == null) cfg.setLearners(new HashSet<>());
            cfg.getLearners().add(id);
        } else {
            cfg.getVoters().incoming().add(id);
        }
        trk.put(id, pr);
    }

    /**
     * checkAndCopy clones the tracker's config and progress map and validates
     * the starting state.
     */
    private Snapshot checkAndCopy() {
        ProgressTracker.Config cfg = tracker.getConfig().clone();
        Map<Long, Progress> trk = cloneProgress(tracker.getProgress());
        checkInvariants(cfg, trk);
        return new Snapshot(cfg, trk);
    }

    /**
     * checkAndReturn validates the resulting state before returning.
     */
    private Result checkAndReturn(ProgressTracker.Config cfg, Map<Long, Progress> trk) {
        checkInvariants(cfg, trk);
        return new Result(cfg, trk);
    }

    /**
     * checkInvariants enforces the invariants that must hold for any
     * configuration tracked by a Changer.
     */
    private static void checkInvariants(ProgressTracker.Config cfg, Map<Long, Progress> trk) {
        // Each voter, learner, and learnersNext must have a corresponding progress.
        Set<Long> all = new HashSet<>(cfg.getVoters().ids());
        if (cfg.getLearners() != null) all.addAll(cfg.getLearners());
        if (cfg.getLearnersNext() != null) all.addAll(cfg.getLearnersNext());
        for (long id : all) {
            if (!trk.containsKey(id)) {
                throw new IllegalStateException("no progress for " + id);
            }
        }
        // Any staged learner was staged because it could not be added directly
        // due to a conflicting voter in the outgoing config.
        if (cfg.getLearnersNext() != null) {
            for (long id : cfg.getLearnersNext()) {
                if (!cfg.getVoters().outgoing().contains(id)) {
                    throw new IllegalStateException(id + " is in LearnersNext, but not Voters[1]");
                }
                Progress pr = trk.get(id);
                if (pr != null && pr.isLearner()) {
                    throw new IllegalStateException(
                            id + " is in LearnersNext, but is already marked as learner");
                }
            }
        }
        // Conversely Learners and Voters do not intersect.
        if (cfg.getLearners() != null) {
            for (long id : cfg.getLearners()) {
                if (cfg.getVoters().outgoing().contains(id)) {
                    throw new IllegalStateException(id + " is in Learners and Voters[1]");
                }
                if (cfg.getVoters().incoming().contains(id)) {
                    throw new IllegalStateException(id + " is in Learners and Voters[0]");
                }
                Progress pr = trk.get(id);
                if (pr != null && !pr.isLearner()) {
                    throw new IllegalStateException(
                            id + " is in Learners, but is not marked as learner");
                }
            }
        }
        // When not joint, outgoing must be empty, and LearnersNext/AutoLeave
        // must be unset.
        if (!joint(cfg)) {
            if (cfg.getLearnersNext() != null && !cfg.getLearnersNext().isEmpty()) {
                throw new IllegalStateException("cfg.LearnersNext must be nil when not joint");
            }
            if (cfg.isAutoLeave()) {
                throw new IllegalStateException("AutoLeave must be false when not joint");
            }
        }
    }

    private static boolean joint(ProgressTracker.Config cfg) {
        return !cfg.getVoters().outgoing().isEmpty();
    }

    private static int symdiff(Set<Long> l, Set<Long> r) {
        int n = 0;
        for (long id : l) if (!r.contains(id)) n++;
        for (long id : r) if (!l.contains(id)) n++;
        return n;
    }

    private Map<Long, Progress> cloneProgress(Map<Long, Progress> orig) {
        Map<Long, Progress> m = new HashMap<>();
        for (Map.Entry<Long, Progress> e : orig.entrySet()) {
            Progress pr = e.getValue();
            Progress clone = new Progress(pr.getMatch(), pr.getNext(),
                    pr.getInflights().clone(), pr.isLearner());
            clone.setState(pr.getState());
            clone.setRecentActive(pr.isRecentActive());
            clone.setMsgAppFlowPaused(pr.isMsgAppFlowPaused());
            clone.setPendingSnapshot(pr.getPendingSnapshot());
            m.put(e.getKey(), clone);
        }
        return m;
    }

    private record Snapshot(ProgressTracker.Config config, Map<Long, Progress> progress) {}

    public record Result(ProgressTracker.Config config, Map<Long, Progress> progress) {}

    /**
     * toConfChangeSingle translates a conf state into 1) a slice of operations creating
     * first the config that will become the outgoing one, and then the incoming one, and
     * 2) another slice that, when applied to the config resulted from 1), represents the
     * ConfState.
     */
    static ToConfChangeSingleResult toConfChangeSingle(Eraftpb.ConfState cs) {
        List<Eraftpb.ConfChangeSingle> out = new ArrayList<>();
        List<Eraftpb.ConfChangeSingle> in = new ArrayList<>();

        for (long id : cs.getVotersOutgoingList()) {
            out.add(Eraftpb.ConfChangeSingle.newBuilder()
                    .setType(Eraftpb.ConfChangeType.ConfChangeAddNode)
                    .setNodeId(id).build());
        }

        // First, remove all of the outgoing voters.
        for (long id : cs.getVotersOutgoingList()) {
            in.add(Eraftpb.ConfChangeSingle.newBuilder()
                    .setType(Eraftpb.ConfChangeType.ConfChangeRemoveNode)
                    .setNodeId(id).build());
        }
        // Then add the incoming voters and learners.
        for (long id : cs.getVotersList()) {
            in.add(Eraftpb.ConfChangeSingle.newBuilder()
                    .setType(Eraftpb.ConfChangeType.ConfChangeAddNode)
                    .setNodeId(id).build());
        }
        for (long id : cs.getLearnersList()) {
            in.add(Eraftpb.ConfChangeSingle.newBuilder()
                    .setType(Eraftpb.ConfChangeType.ConfChangeAddLearnerNode)
                    .setNodeId(id).build());
        }
        // Same for LearnersNext.
        for (long id : cs.getLearnersNextList()) {
            in.add(Eraftpb.ConfChangeSingle.newBuilder()
                    .setType(Eraftpb.ConfChangeType.ConfChangeAddLearnerNode)
                    .setNodeId(id).build());
        }
        return new ToConfChangeSingleResult(out, in);
    }

    record ToConfChangeSingleResult(List<Eraftpb.ConfChangeSingle> outgoing, List<Eraftpb.ConfChangeSingle> incoming) {}

    /**
     * chain applies a sequence of operations to the Changer, threading through
     * the config and progress map at each step.
     */
    private static Result chain(Changer chg, List<java.util.function.Function<Changer, Result>> ops) {
        for (var op : ops) {
            Result r = op.apply(chg);
            // Thread through: update tracker config and progress for next op
            ProgressTracker tempTracker = new ProgressTracker(
                    chg.tracker.getMaxInflight(),
                    chg.tracker.getMaxInflightBytes());
            tempTracker.setConfig(r.config());
            tempTracker.setProgress(r.progress());
            chg = new Changer(tempTracker, chg.lastIndex);
        }
        return new Result(chg.tracker.getConfig(), chg.tracker.getProgress());
    }

    /**
     * Restore takes a Changer (which must represent an empty configuration), and
     * runs a sequence of changes enacting the configuration described in the ConfState.
     */
    public static Result restore(Changer changer, Eraftpb.ConfState cs) {
        ToConfChangeSingleResult r = toConfChangeSingle(cs);
        List<Eraftpb.ConfChangeSingle> outgoing = r.outgoing();
        List<Eraftpb.ConfChangeSingle> incoming = r.incoming();

        List<java.util.function.Function<Changer, Result>> ops = new ArrayList<>();

        if (outgoing.isEmpty()) {
            // No outgoing config, so just apply the incoming changes one by one.
            for (Eraftpb.ConfChangeSingle cc : incoming) {
                ops.add(chg -> chg.simple(List.of(cc)));
            }
        } else {
            // The ConfState describes a joint configuration.
            // First, apply all of the changes of the outgoing config one by one.
            for (Eraftpb.ConfChangeSingle cc : outgoing) {
                ops.add(chg -> chg.simple(List.of(cc)));
            }
            // Now enter the joint state.
            ops.add(chg -> chg.enterJoint(cs.getAutoLeave(), incoming));
        }

        return chain(changer, ops);
    }

    /**
     * Wrap a ConfChangeV2 into an MsgPropose ready for {@code raft.step}.
     * Pure proto-massaging — no Raft state required. Lives here so the
     * conf-change concern stays out of Raft.java.
     */
    public static Eraftpb.Message toMessage(Eraftpb.ConfChangeV2 cc) {
        Eraftpb.Entry.Builder eb = Eraftpb.Entry.newBuilder()
                .setEntryType(Eraftpb.EntryType.EntryConfChangeV2);
        if (cc != null) {
            eb.setData(cc.toByteString());
        }
        return Eraftpb.Message.newBuilder()
                .setMsgType(Eraftpb.MessageType.MsgPropose)
                .addEntries(eb)
                .build();
    }
}
