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
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * System-level tests for raft: deterministic multi-node simulations driven
 * over many ticks, with controlled network partitions and crashes.
 *
 * <p>Mirrors the spirit of etcd-raft's {@code rafttest/} package, simplified
 * for single-threaded determinism. Each tick advances all live nodes' logical
 * clocks; after each tick, every node's pending Ready is processed and its
 * outgoing messages are routed through {@link Sim} (which honours partitions
 * and isolation). The simulation runs for a configured number of ticks.
 *
 * <p>These tests catch regressions that unit tests miss: leader-failure
 * elections, replication after partition healing, and convergence under
 * combined faults — the kind of scenarios where a one-off unit test would
 * only check a single message exchange.
 */
class RaftSystemTest {

    /** Per-node state in the simulation. */
    static final class Node {
        final long id;
        final MemoryStorage storage;
        final RawNode rn;
        final List<Eraftpb.Message> inbox = new ArrayList<>();
        boolean alive = true;
        // Track committed entries per node so the test can assert convergence.
        final List<Eraftpb.Entry> committedHistory = new ArrayList<>();

        Node(long id, MemoryStorage storage, RawNode rn) {
            this.id = id;
            this.storage = storage;
            this.rn = rn;
        }
    }

    /**
     * Lightweight deterministic simulator for a multi-node cluster.
     * Single-threaded: every step is explicit. No goroutines, no channels.
     */
    static final class Sim {
        final Map<Long, Node> nodes = new LinkedHashMap<>();
        final Set<Long> partitionedNodes = new HashSet<>();

        Sim(long... ids) {
            for (long id : ids) {
                MemoryStorage s = new MemoryStorage();
                Eraftpb.SnapshotMetadata.Builder mb = s.getSnapshot().getMetadata().toBuilder();
                Eraftpb.ConfState.Builder cb = mb.getConfState().toBuilder();
                for (long v : ids) cb.addVoters(v);
                mb.setConfState(cb);
                s.setSnapshot(s.getSnapshot().toBuilder().setMetadata(mb).build());

                Config cfg = new Config();
                cfg.id = id;
                cfg.electionTick = 10;
                cfg.heartbeatTick = 1;
                cfg.storage = s;
                cfg.maxSizePerMsg = Long.MAX_VALUE;
                cfg.maxInflightMsgs = 256;

                nodes.put(id, new Node(id, s, RawNode.newRawNode(cfg)));
            }
        }

        /** Run a single tick across all alive nodes, then drain readies/messages. */
        void tick() throws RaftException {
            for (Node n : nodes.values()) {
                if (n.alive) n.rn.tick();
            }
            drainAll();
        }

        /** Drain all readies + inbox messages until quiescent. */
        void drainAll() throws RaftException {
            for (int iter = 0; iter < 100; iter++) {
                boolean progress = false;
                for (Node n : nodes.values()) {
                    if (!n.alive) continue;
                    while (!n.inbox.isEmpty()) {
                        n.rn.step(n.inbox.remove(0));
                        progress = true;
                    }
                    if (n.rn.hasReady()) {
                        Ready rd = n.rn.ready();
                        if (!rd.entries.isEmpty()) n.storage.append(rd.entries);
                        for (Eraftpb.Entry e : rd.committedEntries) {
                            n.committedHistory.add(e);
                        }
                        for (Eraftpb.Message m : rd.messages) {
                            deliver(n, m);
                        }
                        n.rn.advance(rd);
                        progress = true;
                    }
                }
                if (!progress) return;
            }
        }

        /** Deliver a message, respecting partitions and crashes. */
        private void deliver(Node from, Eraftpb.Message m) throws RaftException {
            if (partitionedNodes.contains(from.id)) return;
            Node target = nodes.get(m.getTo());
            if (target == null || !target.alive) return;
            if (partitionedNodes.contains(target.id)) return;
            target.inbox.add(m);
        }

        void campaign(long id) throws RaftException {
            nodes.get(id).rn.campaign();
            drainAll();
        }

        void propose(long id, byte[] data) throws RaftException {
            nodes.get(id).rn.propose(data);
            drainAll();
        }

        /** Run for {@code ticks} ticks (each = one round across all nodes). */
        void run(int ticks) throws RaftException {
            for (int i = 0; i < ticks; i++) tick();
        }

        /** Stop a node from participating until {@link #revive} is called. */
        void crash(long id) throws RaftException { nodes.get(id).alive = false; }

        void revive(long id) throws RaftException {
            nodes.get(id).alive = true;
            // Drain any pending Ready that accumulated while alive=false. We
            // didn't actually pause Ready production (rn was still ticking),
            // but a typical test path is to crash + partition together.
            drainAll();
        }

        void partition(long id) throws RaftException { partitionedNodes.add(id); }
        void heal(long id) throws RaftException { partitionedNodes.remove(id); }
        void healAll() throws RaftException { partitionedNodes.clear(); }

        Node node(long id) { return nodes.get(id); }

        /** Find the current leader, or 0 if no consensus. */
        long currentLeader() {
            for (Node n : nodes.values()) {
                if (n.alive && n.rn.raft.state == RaftStateType.StateLeader) return n.id;
            }
            return 0;
        }
    }

    /**
     * Baseline: 3 nodes elect a leader, propose, and converge on the same log.
     */
    @Test
    void threeNodeBasicReplication() throws RaftException {
        Sim s = new Sim(1, 2, 3);
        s.campaign(1);
        s.run(2);
        assertThat(s.currentLeader()).isEqualTo(1L);

        for (int i = 0; i < 5; i++) {
            s.propose(1, ("v" + i).getBytes());
        }
        s.run(5);

        // All nodes have matching committed indexes and lastIndex.
        long expectedLast = s.node(1).rn.raft.raftLog.lastIndex();
        long expectedCommit = s.node(1).rn.raft.raftLog.committed;
        for (long id : new long[]{1, 2, 3}) {
            Node n = s.node(id);
            assertThat(n.rn.raft.raftLog.lastIndex()).as("node %d lastIndex", id).isEqualTo(expectedLast);
            assertThat(n.rn.raft.raftLog.committed).as("node %d committed", id).isEqualTo(expectedCommit);
        }
    }

    /**
     * Leader failure: kill the elected leader, verify followers elect a new
     * one and continue replicating.
     */
    @Test
    void leaderFailureTriggersNewElection() throws RaftException {
        Sim s = new Sim(1, 2, 3);
        s.campaign(1);
        s.run(2);
        assertThat(s.currentLeader()).isEqualTo(1L);

        // Kill node 1.
        s.crash(1);
        s.partition(1);
        // Tick enough times to exceed the election timeout on followers (10).
        // electionTimeout is randomized to [10, 20); 30 ticks is safely past.
        s.run(30);

        long newLeader = s.currentLeader();
        assertThat(newLeader).as("a new leader should be elected").isNotZero();
        assertThat(newLeader).as("new leader is not the crashed node").isNotEqualTo(1L);

        // Propose under the new leader; it should commit on the surviving
        // quorum {2,3}.
        s.propose(newLeader, "post-failure".getBytes());
        s.run(5);

        long newCommit = s.node(newLeader).rn.raft.raftLog.committed;
        for (long id : new long[]{2, 3}) {
            assertThat(s.node(id).rn.raft.raftLog.committed)
                    .as("survivor %d commit matches new leader", id).isEqualTo(newCommit);
        }
    }

    /**
     * Partition then heal: a minority-side node falls behind, then catches up
     * once the partition is removed. The final state must be consistent.
     */
    @Test
    void partitionHealConverges() throws RaftException {
        Sim s = new Sim(1, 2, 3);
        s.campaign(1);
        s.run(2);

        // Isolate node 3.
        s.partition(3);

        // Propose a few times on the leader; only {1, 2} commit.
        for (int i = 0; i < 4; i++) {
            s.propose(1, ("during-partition-" + i).getBytes());
        }
        s.run(5);

        long leaderCommit = s.node(1).rn.raft.raftLog.committed;
        assertThat(s.node(2).rn.raft.raftLog.committed).isEqualTo(leaderCommit);
        assertThat(s.node(3).rn.raft.raftLog.committed).isLessThan(leaderCommit);

        // Heal and let raft drive replication via heartbeat retries.
        s.heal(3);
        s.run(20);

        // All three nodes converge.
        for (long id : new long[]{1, 2, 3}) {
            assertThat(s.node(id).rn.raft.raftLog.committed)
                    .as("node %d commit converges to %d", id, leaderCommit)
                    .isEqualTo(leaderCommit);
        }
    }

    /**
     * Ten quick proposals interleaved with a temporary partition: every node
     * commits the same set of entries by the end (committedHistory equality).
     */
    @Test
    void interleavedProposalsAndPartitionConverge() throws RaftException {
        Sim s = new Sim(1, 2, 3);
        s.campaign(1);
        s.run(2);

        for (int i = 0; i < 5; i++) {
            s.propose(1, ("a" + i).getBytes());
        }
        s.run(3);

        s.partition(2);
        for (int i = 0; i < 5; i++) {
            s.propose(1, ("b" + i).getBytes());
        }
        s.run(3);
        s.heal(2);
        s.run(20);

        // committedHistory of each node should contain all 10 proposals plus
        // the leader's no-op entry.
        Set<String> historyOf1 = byteValues(s.node(1).committedHistory);
        Set<String> historyOf2 = byteValues(s.node(2).committedHistory);
        Set<String> historyOf3 = byteValues(s.node(3).committedHistory);
        assertThat(historyOf1).as("node 1 committed all 10 proposals")
                .contains("a0","a1","a2","a3","a4","b0","b1","b2","b3","b4");
        assertThat(historyOf2).isEqualTo(historyOf1);
        assertThat(historyOf3).isEqualTo(historyOf1);
    }

    private static Set<String> byteValues(List<Eraftpb.Entry> ents) {
        Set<String> out = new HashSet<>();
        for (Eraftpb.Entry e : ents) {
            ByteString d = e.getData();
            if (!d.isEmpty()) out.add(d.toStringUtf8());
        }
        return out;
    }
}
