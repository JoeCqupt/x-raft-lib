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

import io.github.xinfra.lab.raft.proto.Eraftpb;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.github.xinfra.lab.raft.internal.TestUtil.*;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end tests for {@link Config#asyncStorageWrites} mode. In async mode
 * the RawNode does not return entries / committed entries directly through
 * Ready; instead it emits {@code MsgStorageAppend} and {@code MsgStorageApply}
 * messages addressed to virtual {@code LOCAL_APPEND_THREAD} /
 * {@code LOCAL_APPLY_THREAD} targets. The application is expected to:
 *
 * <ol>
 *   <li>Receive the MsgStorageAppend, persist its entries / hard state /
 *       snapshot, then deliver each of its attached {@code Responses} (which
 *       include MsgStorageAppendResp and any peer responses, e.g. MsgAppResp).</li>
 *   <li>Receive the MsgStorageApply, apply its entries to the state machine,
 *       then deliver the attached MsgStorageApplyResp.</li>
 * </ol>
 *
 * <p>This file exercises the async path on a single-node cluster (no peers,
 * no real network) so we can focus on the storage / apply message handling
 * without the noise of follower routing. Multi-node async would need network
 * routing of all 4 of MsgStorageAppend/MsgStorageApply/Resp + peer messages
 * — best left to a follow-up.
 */
class AsyncStorageWritesTest {

    /**
     * Async mode contract: Ready.entries is still populated (for backward
     * compatibility with sync mode), but the authoritative copy that the app
     * should persist is in the MsgStorageAppend message in rd.messages. They
     * carry the same payload. Mirrors etcd-raft's docstring on Ready.Entries.
     */
    @Test
    void asyncReadyAlsoEmitsMsgStorageAppend() throws Exception {
        MemoryStorage s = newTestMemoryStorage(withPeers(1));
        Config cfg = newTestConfig(1, 10, 1, s);
        cfg.maxSizePerMsg = NO_LIMIT;
        cfg.maxInflightMsgs = 256;
        cfg.asyncStorageWrites = true;

        RawNode rn = RawNode.newRawNode(cfg);
        rn.campaign();

        // Drive until the cluster is quiescent — but in async mode, advance()
        // must not be called; we route MsgStorage* responses back manually.
        runUntilLeader(rn, s);

        // Propose; verify the propose ends up in a MsgStorageAppend.
        rn.propose("payload".getBytes());
        Ready rd = drainOneReady(rn, s);

        // Verify there is exactly one MsgStorageAppend addressed to the local
        // append thread, with the propose payload in its entries.
        List<Eraftpb.Message> appends = filter(rd.messages, Eraftpb.MessageType.MsgStorageAppend);
        assertThat(appends).as("async: a MsgStorageAppend message is emitted").hasSize(1);
        Eraftpb.Message append = appends.get(0);
        assertThat(append.getTo()).isEqualTo(Util.LOCAL_APPEND_THREAD);

        // Find the "payload" entry in either rd.entries or the MsgStorageAppend.
        // Either is acceptable per etcd-raft contract; we just want to confirm
        // it isn't lost.
        boolean foundInAppend = append.getEntriesList().stream()
                .anyMatch(e -> "payload".equals(e.getData().toStringUtf8()));
        boolean foundInRd = rd.entries.stream()
                .anyMatch(e -> "payload".equals(e.getData().toStringUtf8()));
        assertThat(foundInAppend || foundInRd)
                .as("payload is reachable via either rd.entries or MsgStorageAppend").isTrue();
    }

    /**
     * Full round-trip: propose → MsgStorageAppend → app persists → app
     * delivers MsgStorageAppendResp → MsgStorageApply → app applies →
     * MsgStorageApplyResp → entry shows up as committed and applied.
     *
     * <p>Without this round-trip, the entry stays in the unstable log forever
     * and never advances {@code raftLog.applied}. Verifies the async path
     * actually closes the loop end-to-end.
     */
    @Test
    void asyncProposeAppliesViaStorageRoundTrip() throws Exception {
        MemoryStorage s = newTestMemoryStorage(withPeers(1));
        Config cfg = newTestConfig(1, 10, 1, s);
        cfg.maxSizePerMsg = NO_LIMIT;
        cfg.maxInflightMsgs = 256;
        cfg.asyncStorageWrites = true;

        RawNode rn = RawNode.newRawNode(cfg);
        rn.campaign();
        runUntilLeader(rn, s);

        long beforeApplied = rn.raft.raftLog.applied;
        rn.propose("hello".getBytes());

        // Loop: each iteration drains all current Readys, persists / applies
        // as instructed, and delivers the matching Resp messages back.
        for (int iter = 0; iter < 10; iter++) {
            if (!rn.hasReady()) break;
            Ready rd = rn.ready();
            // Persist entries / snapshot to storage.
            if (!rd.entries.isEmpty()) s.append(rd.entries);
            // Process each outgoing message: MsgStorage{Append,Apply} are
            // delivered to the simulated local append/apply threads (their
            // responses are attached, deliver them straight back to raft).
            for (Eraftpb.Message m : rd.messages) {
                if (m.getMsgType() == Eraftpb.MessageType.MsgStorageAppend) {
                    // The app "writes" then delivers each attached Response.
                    for (Eraftpb.Message resp : m.getResponsesList()) {
                        rn.step(resp);
                    }
                } else if (m.getMsgType() == Eraftpb.MessageType.MsgStorageApply) {
                    // The app "applies" then delivers each attached Response.
                    for (Eraftpb.Message resp : m.getResponsesList()) {
                        rn.step(resp);
                    }
                }
            }
            // NB: in async mode advance() must NOT be called.
        }

        // Applied must have advanced past beforeApplied (the no-op entry was
        // already applied during runUntilLeader). The "hello" entry must now
        // be applied too — beforeApplied + 1.
        assertThat(rn.raft.raftLog.applied)
                .as("applied advances by ≥1 via MsgStorageApply round-trip")
                .isGreaterThan(beforeApplied);

        // The committed log should contain our payload at the head.
        List<Eraftpb.Entry> all = s.entries(1, s.lastIndex() + 1, Long.MAX_VALUE);
        boolean found = false;
        for (Eraftpb.Entry e : all) {
            if ("hello".equals(e.getData().toStringUtf8())) found = true;
        }
        assertThat(found).as("propose payload reached storage").isTrue();
    }

    /**
     * Verifies the "advance() must not be called in async mode" guardrail. If
     * an app accidentally calls advance under async, RawNode throws — the
     * loudest failure mode is the best, since silent breakage of async mode
     * could go undetected for a long time.
     */
    @Test
    void advanceInAsyncModeThrows() throws Exception {
        MemoryStorage s = newTestMemoryStorage(withPeers(1));
        Config cfg = newTestConfig(1, 10, 1, s);
        cfg.maxSizePerMsg = NO_LIMIT;
        cfg.maxInflightMsgs = 256;
        cfg.asyncStorageWrites = true;
        RawNode rn = RawNode.newRawNode(cfg);

        Ready rd = rn.hasReady() ? rn.ready() : new Ready();
        try {
            rn.advance(rd);
            org.junit.jupiter.api.Assertions.fail("advance() must throw in async mode");
        } catch (RuntimeException expected) {
            assertThat(expected.getMessage())
                    .containsIgnoringCase("Advance must not be called");
        }
    }

    // ---- helpers ----

    /**
     * In async mode, run Ready cycles routing MsgStorageAppend responses back
     * to raft until the node settles into a leader / steady state.
     */
    private static void runUntilLeader(RawNode rn, MemoryStorage s) {
        for (int iter = 0; iter < 50; iter++) {
            if (!rn.hasReady()) break;
            Ready rd = rn.ready();
            if (!rd.entries.isEmpty()) s.append(rd.entries);
            for (Eraftpb.Message m : rd.messages) {
                if (m.getMsgType() == Eraftpb.MessageType.MsgStorageAppend
                        || m.getMsgType() == Eraftpb.MessageType.MsgStorageApply) {
                    for (Eraftpb.Message resp : m.getResponsesList()) {
                        rn.step(resp);
                    }
                }
            }
        }
    }

    /**
     * Drain at most one Ready (no fan-out routing). The caller is expected to
     * handle messages itself for inspection.
     */
    private static Ready drainOneReady(RawNode rn, MemoryStorage s) throws Exception {
        if (!rn.hasReady()) return new Ready();
        Ready rd = rn.ready();
        if (!rd.entries.isEmpty()) s.append(rd.entries);
        // Deliver MsgStorageAppend responses to keep state consistent, but
        // don't loop — caller wants to see this exact Ready.
        for (Eraftpb.Message m : rd.messages) {
            if (m.getMsgType() == Eraftpb.MessageType.MsgStorageAppend
                    || m.getMsgType() == Eraftpb.MessageType.MsgStorageApply) {
                for (Eraftpb.Message resp : m.getResponsesList()) {
                    rn.step(resp);
                }
            }
        }
        return rd;
    }

    private static List<Eraftpb.Message> filter(List<Eraftpb.Message> in, Eraftpb.MessageType type) {
        List<Eraftpb.Message> out = new ArrayList<>();
        for (Eraftpb.Message m : in) if (m.getMsgType() == type) out.add(m);
        return out;
    }

    // ============= Multi-node async tests =============
    //
    // A three-node cluster runs entirely in async mode. Each node simulates a
    // separate append/apply thread by immediately delivering the attached
    // Responses on MsgStorage{Append,Apply}. Peer messages (MsgAppend etc.)
    // are routed across nodes via a Sim-like inbox model.

    private static final class AsyncNode {
        final long id;
        final MemoryStorage storage;
        final RawNode rn;
        final List<Eraftpb.Message> inbox = new ArrayList<>();

        AsyncNode(long id, long... voters) {
            this.id = id;
            this.storage = newTestMemoryStorage(withPeers(voters));
            Config cfg = newTestConfig(id, 10, 1, storage);
            cfg.maxSizePerMsg = NO_LIMIT;
            cfg.maxInflightMsgs = 256;
            cfg.asyncStorageWrites = true;
            this.rn = RawNode.newRawNode(cfg);
        }
    }

    /**
     * 3-node async cluster: campaign on node 1, propose, verify all peers
     * converge on the same committed/applied entries via the async path.
     */
    @Test
    void threeNodeAsyncReplicationConverges() {
        Map<Long, AsyncNode> nodes = new HashMap<>();
        for (long id : new long[]{1, 2, 3}) {
            nodes.put(id, new AsyncNode(id, 1, 2, 3));
        }

        nodes.get(1L).rn.campaign();
        drainAll(nodes, 200);

        // After campaign + stabilize, node 1 should be leader on all peers.
        assertThat(nodes.get(1L).rn.raft.state).isEqualTo(RaftStateType.StateLeader);

        long beforeApplied = nodes.get(1L).rn.raft.raftLog.applied;
        nodes.get(1L).rn.propose("payload-1".getBytes());
        nodes.get(1L).rn.propose("payload-2".getBytes());
        drainAll(nodes, 200);

        // All nodes have applied the same entries.
        long leaderApplied = nodes.get(1L).rn.raft.raftLog.applied;
        assertThat(leaderApplied).as("leader applied advanced past initial").isGreaterThan(beforeApplied);
        for (long id : new long[]{2, 3}) {
            assertThat(nodes.get(id).rn.raft.raftLog.applied)
                    .as("follower %d applied matches leader", id)
                    .isEqualTo(leaderApplied);
        }

        // Verify the proposed payloads are durably stored on every node.
        for (long id : new long[]{1, 2, 3}) {
            MemoryStorage s = nodes.get(id).storage;
            try {
                List<Eraftpb.Entry> all = s.entries(1, s.lastIndex() + 1, Long.MAX_VALUE);
                boolean p1 = false, p2 = false;
                for (Eraftpb.Entry e : all) {
                    String d = e.getData().toStringUtf8();
                    if ("payload-1".equals(d)) p1 = true;
                    if ("payload-2".equals(d)) p2 = true;
                }
                assertThat(p1).as("node %d has payload-1", id).isTrue();
                assertThat(p2).as("node %d has payload-2", id).isTrue();
            } catch (RaftException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Multi-node async: a partitioned follower must catch up via async
     * MsgStorageAppend after the partition heals. Verifies the async path
     * supports the same convergence guarantees as sync mode.
     */
    @Test
    void asyncFollowerCatchesUpAfterPartition() {
        Map<Long, AsyncNode> nodes = new HashMap<>();
        for (long id : new long[]{1, 2, 3}) {
            nodes.put(id, new AsyncNode(id, 1, 2, 3));
        }
        java.util.Set<Long> partitioned = new java.util.HashSet<>();

        nodes.get(1L).rn.campaign();
        drainAll(nodes, partitioned, 200);

        // Partition node 3, propose 4 entries.
        partitioned.add(3L);
        for (int i = 0; i < 4; i++) {
            nodes.get(1L).rn.propose(("p" + i).getBytes());
        }
        drainAll(nodes, partitioned, 200);

        long leaderApplied = nodes.get(1L).rn.raft.raftLog.applied;
        assertThat(nodes.get(2L).rn.raft.raftLog.applied)
                .as("partner 2 caught up to leader")
                .isEqualTo(leaderApplied);
        assertThat(nodes.get(3L).rn.raft.raftLog.applied)
                .as("partitioned 3 stayed behind")
                .isLessThan(leaderApplied);

        // Heal partition; follower 3 must catch up.
        partitioned.clear();
        drainAll(nodes, partitioned, 200);

        assertThat(nodes.get(3L).rn.raft.raftLog.applied)
                .as("node 3 caught up after heal")
                .isEqualTo(leaderApplied);
    }

    private static void drainAll(Map<Long, AsyncNode> nodes, int maxIter) {
        drainAll(nodes, java.util.Collections.emptySet(), maxIter);
    }

    /**
     * Drain Readys + inboxes across all nodes until quiescent. {@code partitioned}
     * nodes neither send nor receive peer messages, but still process their own
     * local-storage round-trip (so their internal state remains coherent).
     */
    private static void drainAll(Map<Long, AsyncNode> nodes, java.util.Set<Long> partitioned, int maxIter) {
        // If a round of drain makes no progress we fire one tick across all
        // alive nodes — this lets the leader's heartbeat timer eventually
        // discover a recently-healed follower. Without ticking, a leader has
        // no signal to retry after partition recovery.
        int idleRounds = 0;
        for (int iter = 0; iter < maxIter; iter++) {
            boolean progress = false;
            for (AsyncNode n : nodes.values()) {
                // Drain inbox into raft (skip if partitioned — drop inbound).
                if (!partitioned.contains(n.id)) {
                    while (!n.inbox.isEmpty()) {
                        n.rn.step(n.inbox.remove(0));
                        progress = true;
                    }
                } else {
                    if (!n.inbox.isEmpty()) {
                        n.inbox.clear();
                        progress = true;
                    }
                }
                if (n.rn.hasReady()) {
                    Ready rd = n.rn.ready();
                    if (!rd.entries.isEmpty()) n.storage.append(rd.entries);
                    if (rd.snapshot != null && !Util.isEmptySnap(rd.snapshot)) {
                        try { n.storage.applySnapshot(rd.snapshot); }
                        catch (RaftException ex) { throw new RuntimeException(ex); }
                    }
                    for (Eraftpb.Message m : rd.messages) {
                        if (m.getMsgType() == Eraftpb.MessageType.MsgStorageAppend
                                || m.getMsgType() == Eraftpb.MessageType.MsgStorageApply) {
                            // Local message: persist already happened above;
                            // deliver attached Responses back to raft.
                            for (Eraftpb.Message resp : m.getResponsesList()) {
                                if (resp.getTo() == n.id) {
                                    n.rn.step(resp);
                                } else {
                                    // Peer-bound response (e.g. MsgAppResp after
                                    // entries are durable). Route via inboxes.
                                    routeToPeer(nodes, partitioned, n.id, resp);
                                }
                            }
                        } else {
                            routeToPeer(nodes, partitioned, n.id, m);
                        }
                    }
                    // NB: NO advance() in async mode.
                    progress = true;
                }
            }
            if (!progress) {
                idleRounds++;
                if (idleRounds >= 2) return; // truly quiescent
                // Idle round: tick all live nodes once to advance heartbeat
                // / election timers. This is what discovers a healed follower.
                for (AsyncNode n : nodes.values()) {
                    if (!partitioned.contains(n.id)) n.rn.tick();
                }
            } else {
                idleRounds = 0;
            }
        }
    }

    private static void routeToPeer(Map<Long, AsyncNode> nodes, java.util.Set<Long> partitioned,
                                    long fromId, Eraftpb.Message m) {
        if (partitioned.contains(fromId)) return;
        AsyncNode target = nodes.get(m.getTo());
        if (target == null || partitioned.contains(target.id)) return;
        if (target.id == fromId) {
            // Self-addressed message (e.g. own vote response): step directly.
            target.rn.step(m);
        } else {
            target.inbox.add(m);
        }
    }
}
