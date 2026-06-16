/*
 * Copyright 2024-2026 The x-raft-lib Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * SPDX-License-Identifier: Apache-2.0
 */
package io.github.xinfra.lab.raft.internal.simulation;

import io.github.xinfra.lab.raft.Config;
import io.github.xinfra.lab.raft.MemoryStorage;
import io.github.xinfra.lab.raft.Peer;
import io.github.xinfra.lab.raft.RaftException;
import io.github.xinfra.lab.raft.RaftStateType;
import io.github.xinfra.lab.raft.RawNode;
import io.github.xinfra.lab.raft.Ready;
import io.github.xinfra.lab.raft.internal.Raft;
import io.github.xinfra.lab.raft.internal.Util;
import io.github.xinfra.lab.raft.proto.Eraftpb;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Deterministic multi-node raft simulation cluster.
 *
 * <p>Drives N {@link RawNode} instances with {@link MemoryStorage} backends
 * through a {@link SimulatedNetwork}. The entire simulation is single-threaded
 * and controlled by a seeded {@link Random}, making every execution fully
 * reproducible.
 *
 * <p>On each tick the simulation:
 * <ol>
 *   <li>Ticks a random subset of live nodes (simulating clock skew)</li>
 *   <li>Delivers due messages from the network → step into target nodes</li>
 *   <li>Processes Ready on each node (persist → route → apply → advance)</li>
 *   <li>Injects random operations (propose, fault, heal) per the scenario</li>
 *   <li>Checks Raft paper invariants via {@link RaftInvariantChecker}</li>
 * </ol>
 */
final class SimulationCluster {

    private static final Logger LOG = LoggerFactory.getLogger(SimulationCluster.class);

    private final long seed;
    private final Random random;
    private final SimulatedNetwork network;
    private final RaftInvariantChecker checker;

    private final Map<Long, RawNode> liveNodes = new LinkedHashMap<>();
    private final Map<Long, MemoryStorage> storages = new LinkedHashMap<>();
    private final List<Long> allNodeIds;

    private final Map<Long, Long> prevLeaderTerm = new LinkedHashMap<>();
    private final Map<Long, Long> nodeAppliedIndex = new LinkedHashMap<>();
    private int currentTick;

    private long totalProposed;
    private long totalApplied;

    SimulationCluster(long seed, int nodeCount) {
        this.seed = seed;
        this.random = new Random(seed);
        this.network = new SimulatedNetwork(random);
        this.checker = new RaftInvariantChecker();
        this.allNodeIds = new ArrayList<>();

        List<Peer> peers = new ArrayList<>();
        for (int i = 1; i <= nodeCount; i++) {
            allNodeIds.add((long) i);
            peers.add(new Peer(i));
        }

        for (long id : allNodeIds) {
            MemoryStorage storage = new MemoryStorage();
            Config cfg = Config.builder()
                    .id(id)
                    .electionTick(10)
                    .heartbeatTick(1)
                    .storage(storage)
                    .maxSizePerMsg(Long.MAX_VALUE)
                    .maxInflightMsgs(256)
                    .preVote(true)
                    .checkQuorum(true)
                    .build();
            RawNode rn = RawNode.newRawNode(cfg);
            rn.bootstrap(peers);
            storages.put(id, storage);
            liveNodes.put(id, rn);
        }
    }

    SimulatedNetwork network() { return network; }

    Random random() { return random; }

    int currentTick() { return currentTick; }

    long totalProposed() { return totalProposed; }

    long totalApplied() { return totalApplied; }

    /**
     * Run the simulation for {@code maxTicks} ticks. On each tick, the
     * simulation advances all live nodes, delivers messages, and checks
     * invariants.
     */
    void run(int maxTicks) {
        for (int t = 0; t < maxTicks; t++) {
            currentTick = t;
            tickNodes();
            deliverMessages();
            processAllReady();
            checker.checkAll(t, seed, liveNodes, storages);
        }
    }

    /**
     * Run one simulation tick. Callers that need to inject operations
     * (propose, fault) between ticks use this instead of {@link #run(int)}.
     */
    void stepOneTick() {
        tickNodes();
        deliverMessages();
        processAllReady();
        checker.checkAll(currentTick, seed, liveNodes, storages);
        currentTick++;
    }

    void propose(byte[] data) {
        RawNode leader = findLeader();
        if (leader == null) return;
        try {
            leader.propose(data);
            totalProposed++;
        } catch (RaftException e) {
            // Proposal dropped — not a violation, just can't propose right now
        }
    }

    void crashNode(long nodeId) {
        RawNode rn = liveNodes.remove(nodeId);
        if (rn == null) return;
        network.dropMessagesFor(nodeId);
        LOG.debug("tick={} seed={}: crashed node {}", currentTick, seed, nodeId);
    }

    void restartNode(long nodeId) {
        if (liveNodes.containsKey(nodeId)) return;
        MemoryStorage storage = storages.get(nodeId);
        if (storage == null) return;

        long applied = nodeAppliedIndex.getOrDefault(nodeId, 0L);
        Config cfg = Config.builder()
                .id(nodeId)
                .electionTick(10)
                .heartbeatTick(1)
                .storage(storage)
                .maxSizePerMsg(Long.MAX_VALUE)
                .maxInflightMsgs(256)
                .preVote(true)
                .checkQuorum(true)
                .applied(applied)
                .build();
        RawNode rn = RawNode.newRawNode(cfg);
        liveNodes.put(nodeId, rn);
        LOG.debug("tick={} seed={}: restarted node {}", currentTick, seed, nodeId);
    }

    RawNode findLeader() {
        for (RawNode rn : liveNodes.values()) {
            if (rn.raft.state() == RaftStateType.StateLeader) {
                return rn;
            }
        }
        return null;
    }

    long findLeaderId() {
        for (Map.Entry<Long, RawNode> e : liveNodes.entrySet()) {
            if (e.getValue().raft.state() == RaftStateType.StateLeader) {
                return e.getKey();
            }
        }
        return 0;
    }

    boolean hasLeader() {
        return findLeader() != null;
    }

    Map<Long, RawNode> liveNodes() {
        return Collections.unmodifiableMap(liveNodes);
    }

    List<Long> allNodeIds() {
        return allNodeIds;
    }

    private void tickNodes() {
        List<Long> ids = new ArrayList<>(liveNodes.keySet());
        Collections.shuffle(ids, random);
        for (long id : ids) {
            RawNode rn = liveNodes.get(id);
            if (rn != null) {
                rn.tick();
            }
        }
    }

    private void deliverMessages() {
        List<SimulatedNetwork.PendingMessage> due = network.deliverDue(currentTick);
        for (SimulatedNetwork.PendingMessage pm : due) {
            RawNode target = liveNodes.get(pm.to());
            if (target == null) continue;
            try {
                target.step(pm.message());
            } catch (RaftException e) {
                // Step rejection — normal (e.g., stale message)
            }
        }
    }

    private void processAllReady() {
        List<Long> ids = new ArrayList<>(liveNodes.keySet());
        for (long id : ids) {
            RawNode rn = liveNodes.get(id);
            if (rn == null || !rn.hasReady()) continue;

            Ready rd = rn.ready();

            // Persist snapshot first
            if (rd.snapshot() != null && !Util.isEmptySnap(rd.snapshot())) {
                try {
                    storages.get(id).applySnapshot(rd.snapshot());
                } catch (RaftException e) {
                    // Snapshot apply error — log and continue
                }
            }

            // Persist hard state
            if (!Util.isEmptyHardState(rd.hardState())) {
                storages.get(id).setHardState(rd.hardState());
            }

            // Persist entries
            if (!rd.entries().isEmpty()) {
                storages.get(id).append(rd.entries());
            }

            // Route outbound messages through the simulated network
            for (Eraftpb.Message m : rd.messages()) {
                if (m.getTo() != id) {
                    network.enqueue(id, m.getTo(), m, currentTick);
                }
            }

            // Apply committed entries and record for invariant checking
            long highestApplied = 0;
            for (Eraftpb.Entry entry : rd.committedEntries()) {
                highestApplied = entry.getIndex();
                if (entry.getEntryType() == Eraftpb.EntryType.EntryNormal && entry.getData().size() > 0) {
                    checker.recordApplied(id, entry);
                    totalApplied++;
                } else if (entry.getEntryType() == Eraftpb.EntryType.EntryConfChange) {
                    try {
                        Eraftpb.ConfChange cc = Eraftpb.ConfChange.parseFrom(entry.getData());
                        Eraftpb.ConfChangeV2 v2 = Eraftpb.ConfChangeV2.newBuilder()
                                .addChanges(Eraftpb.ConfChangeSingle.newBuilder()
                                        .setType(cc.getChangeType())
                                        .setNodeId(cc.getNodeId()))
                                .build();
                        rn.applyConfChange(v2);
                    } catch (com.google.protobuf.InvalidProtocolBufferException e) {
                        LOG.warn("malformed conf-change entry at index {}", entry.getIndex());
                    }
                } else if (entry.getEntryType() == Eraftpb.EntryType.EntryConfChangeV2) {
                    try {
                        rn.applyConfChange(Eraftpb.ConfChangeV2.parseFrom(entry.getData()));
                    } catch (com.google.protobuf.InvalidProtocolBufferException e) {
                        LOG.warn("malformed conf-change-v2 entry at index {}", entry.getIndex());
                    }
                }
            }

            if (highestApplied > 0) {
                nodeAppliedIndex.put(id, highestApplied);
            }

            rn.advance(rd);

            // Check leader completeness when a new leader appears
            Raft r = rn.raft;
            if (r.state() == RaftStateType.StateLeader) {
                Long prevTerm = prevLeaderTerm.get(id);
                if (prevTerm == null || prevTerm != r.term()) {
                    prevLeaderTerm.put(id, r.term());
                    checker.checkLeaderCompleteness(currentTick, seed, id, rn, storages.get(id));
                }
            }
        }
    }
}
