/*
 * Copyright 2024-2026 The x-raft-lib Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * SPDX-License-Identifier: Apache-2.0
 */
package io.github.xinfra.lab.raft.tests.simulation;

import io.github.xinfra.lab.raft.MemoryStorage;
import io.github.xinfra.lab.raft.RaftException;
import io.github.xinfra.lab.raft.RaftStateType;
import io.github.xinfra.lab.raft.RawNode;
import io.github.xinfra.lab.raft.internal.Raft;
import io.github.xinfra.lab.raft.proto.Eraftpb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Checks Raft paper Figure 2 invariants and additional safety properties
 * across a simulated cluster. All violations are reported as
 * {@link InvariantViolation} exceptions with enough context to reproduce.
 */
final class RaftInvariantChecker {

    static final class InvariantViolation extends RuntimeException {
        InvariantViolation(String msg) { super(msg); }
    }

    private final Map<Long, Long> prevCommitted = new HashMap<>();
    private final Map<Long, Long> prevTerm = new HashMap<>();
    private final Map<Long, List<AppliedEntry>> appliedLogs = new HashMap<>();

    record AppliedEntry(long index, long term, com.google.protobuf.ByteString data) {}

    void recordApplied(long nodeId, Eraftpb.Entry entry) {
        appliedLogs.computeIfAbsent(nodeId, k -> new ArrayList<>())
                .add(new AppliedEntry(entry.getIndex(), entry.getTerm(), entry.getData()));
    }

    /**
     * Run all invariant checks on the current cluster state.
     *
     * @param tick current simulation tick (for error messages)
     * @param seed simulation seed (for reproducibility)
     * @param nodes map of live node id → RawNode
     * @param storages map of all node id → MemoryStorage (including crashed nodes)
     */
    void checkAll(int tick, long seed, Map<Long, RawNode> nodes, Map<Long, MemoryStorage> storages) {
        checkElectionSafety(tick, seed, nodes);
        checkMonotonicity(tick, seed, nodes);
        checkStateMachineSafety(tick, seed);
        checkLogMatching(tick, seed, nodes, storages);
    }

    /**
     * Called when a new leader is detected. Verifies that all previously
     * committed entries exist in the new leader's log (Leader Completeness).
     */
    void checkLeaderCompleteness(int tick, long seed, long leaderId,
                                 RawNode leaderNode, MemoryStorage leaderStorage) {
        for (Map.Entry<Long, List<AppliedEntry>> e : appliedLogs.entrySet()) {
            for (AppliedEntry ae : e.getValue()) {
                try {
                    long leaderTerm = leaderNode.raft.raftLog().term(ae.index);
                    if (leaderTerm != ae.term) {
                        throw new InvariantViolation(String.format(
                                "Leader Completeness violated at tick=%d seed=%d: " +
                                        "new leader %d has term %d at index %d, but committed entry has term %d",
                                tick, seed, leaderId, leaderTerm, ae.index, ae.term));
                    }
                } catch (RaftException ex) {
                    if (ex.code() == RaftException.Code.COMPACTED) {
                        continue;
                    }
                    throw new InvariantViolation(String.format(
                            "Leader Completeness check error at tick=%d seed=%d: " +
                                    "leader %d cannot read term at index %d: %s",
                            tick, seed, leaderId, ae.index, ex));
                }
            }
        }
    }

    private void checkElectionSafety(int tick, long seed, Map<Long, RawNode> nodes) {
        Map<Long, List<Long>> termLeaders = new HashMap<>();
        for (Map.Entry<Long, RawNode> e : nodes.entrySet()) {
            Raft r = e.getValue().raft;
            if (r.state() == RaftStateType.StateLeader) {
                termLeaders.computeIfAbsent(r.term(), k -> new ArrayList<>()).add(e.getKey());
            }
        }
        for (Map.Entry<Long, List<Long>> e : termLeaders.entrySet()) {
            if (e.getValue().size() > 1) {
                throw new InvariantViolation(String.format(
                        "Election Safety violated at tick=%d seed=%d: term %d has leaders %s",
                        tick, seed, e.getKey(), e.getValue()));
            }
        }
    }

    private void checkMonotonicity(int tick, long seed, Map<Long, RawNode> nodes) {
        for (Map.Entry<Long, RawNode> e : nodes.entrySet()) {
            long id = e.getKey();
            Raft r = e.getValue().raft;

            long committed = r.raftLog().committed;
            Long prev = prevCommitted.get(id);
            if (prev != null && committed < prev) {
                throw new InvariantViolation(String.format(
                        "Committed monotonicity violated at tick=%d seed=%d: " +
                                "node %d committed regressed from %d to %d",
                        tick, seed, id, prev, committed));
            }
            prevCommitted.put(id, committed);

            long term = r.term();
            Long prevT = prevTerm.get(id);
            if (prevT != null && term < prevT) {
                throw new InvariantViolation(String.format(
                        "Term monotonicity violated at tick=%d seed=%d: " +
                                "node %d term regressed from %d to %d",
                        tick, seed, id, prevT, term));
            }
            prevTerm.put(id, term);

            if (r.raftLog().applied > committed) {
                throw new InvariantViolation(String.format(
                        "Applied > Committed at tick=%d seed=%d: " +
                                "node %d applied=%d committed=%d",
                        tick, seed, id, r.raftLog().applied, committed));
            }
        }
    }

    private void checkStateMachineSafety(int tick, long seed) {
        Map<Long, AppliedEntry> indexToEntry = new HashMap<>();
        for (Map.Entry<Long, List<AppliedEntry>> e : appliedLogs.entrySet()) {
            long nodeId = e.getKey();
            for (AppliedEntry ae : e.getValue()) {
                AppliedEntry existing = indexToEntry.get(ae.index);
                if (existing == null) {
                    indexToEntry.put(ae.index, ae);
                } else if (existing.term != ae.term || !existing.data.equals(ae.data)) {
                    throw new InvariantViolation(String.format(
                            "State Machine Safety violated at tick=%d seed=%d: " +
                                    "index %d applied differently — node %d has (term=%d, data=%s) " +
                                    "but another node has (term=%d, data=%s)",
                            tick, seed, ae.index, nodeId, ae.term, ae.data.toStringUtf8(),
                            existing.term, existing.data.toStringUtf8()));
                }
            }
        }
    }

    private void checkLogMatching(int tick, long seed,
                                  Map<Long, RawNode> nodes,
                                  Map<Long, MemoryStorage> storages) {
        List<Long> nodeIds = new ArrayList<>(nodes.keySet());
        for (int i = 0; i < nodeIds.size(); i++) {
            for (int j = i + 1; j < nodeIds.size(); j++) {
                long idA = nodeIds.get(i);
                long idB = nodeIds.get(j);
                RawNode rnA = nodes.get(idA);
                RawNode rnB = nodes.get(idB);
                long minCommitted = Math.min(rnA.raft.raftLog().committed, rnB.raft.raftLog().committed);
                long firstA = rnA.raft.raftLog().firstIndex();
                long firstB = rnB.raft.raftLog().firstIndex();
                long start = Math.max(firstA, firstB);
                if (start > minCommitted) continue;

                for (long idx = start; idx <= minCommitted; idx++) {
                    try {
                        long termA = rnA.raft.raftLog().term(idx);
                        long termB = rnB.raft.raftLog().term(idx);
                        if (termA != termB) {
                            throw new InvariantViolation(String.format(
                                    "Log Matching violated at tick=%d seed=%d: " +
                                            "nodes %d and %d disagree at committed index %d — " +
                                            "terms %d vs %d",
                                    tick, seed, idA, idB, idx, termA, termB));
                        }
                    } catch (RaftException e) {
                        // Compacted past this index — can't compare, skip
                    }
                }
            }
        }
    }

    void reset() {
        prevCommitted.clear();
        prevTerm.clear();
        appliedLogs.clear();
    }
}
