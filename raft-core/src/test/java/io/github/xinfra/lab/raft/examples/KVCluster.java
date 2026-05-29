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
package io.github.xinfra.lab.raft.examples;
import io.github.xinfra.lab.raft.internal.*;

import io.github.xinfra.lab.raft.Config;
import io.github.xinfra.lab.raft.MemoryStorage;
import io.github.xinfra.lab.raft.RaftException;
import io.github.xinfra.lab.raft.RaftStateType;
import io.github.xinfra.lab.raft.RawNode;
import io.github.xinfra.lab.raft.Ready;
import io.github.xinfra.lab.raft.internal.Util;
import io.github.xinfra.lab.raft.proto.Eraftpb;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A 3-node in-process replicated KV cluster, built on top of {@link RawNode}.
 *
 * <p>This is the canonical "how do I wire raft into an application" demo:
 * <ul>
 *   <li>Each node owns a {@link RawNode} + a {@link MemoryStorage} +
 *       a {@link KVStore} state machine.</li>
 *   <li>Outgoing raft messages are routed in-process via per-node inboxes
 *       (in a real app, this would be the network layer).</li>
 *   <li>Every Ready cycle drains entries to storage, applies committed
 *       entries to the KVStore, and routes outgoing peer messages.</li>
 *   <li>Reads are not strongly linearizable here — this is a demo, not a
 *       production library. In a real app you'd issue a ReadIndex on the
 *       leader and wait for the leader's commit index to be applied before
 *       answering.</li>
 * </ul>
 */
public class KVCluster {

    /** A single node in the simulated cluster. */
    public static final class Node {
        public final long id;
        public final MemoryStorage storage;
        public final RawNode rn;
        public final KVStore kvStore = new KVStore();
        final List<Eraftpb.Message> inbox = new ArrayList<>();

        Node(long id, long[] voters) {
            this.id = id;
            this.storage = new MemoryStorage();
            // Seed the storage with a ConfState containing all voters so
            // RawNode knows the cluster size from the start. We do this by
            // applying a degenerate snapshot at index=0 carrying just the
            // ConfState — the public way to set initial cluster membership
            // without writing log entries.
            Eraftpb.ConfState.Builder cs = Eraftpb.ConfState.newBuilder();
            for (long v : voters) cs.addVoters(v);
            try {
                storage.applySnapshot(Eraftpb.Snapshot.newBuilder()
                        .setMetadata(Eraftpb.SnapshotMetadata.newBuilder()
                                .setIndex(0).setTerm(0).setConfState(cs))
                        .build());
            } catch (RaftException e) {
                // applySnapshot can reject if storage already has a snapshot
                // at index >= 0. Our storage is fresh, so this can't happen,
                // but throw if it ever does — it'd be a Storage contract bug.
                throw new RuntimeException(e);
            }

            Config cfg = new Config();
            cfg.id = id;
            cfg.electionTick = 10;
            cfg.heartbeatTick = 1;
            cfg.storage = storage;
            cfg.maxSizePerMsg = Long.MAX_VALUE;
            cfg.maxInflightMsgs = 256;
            this.rn = RawNode.newRawNode(cfg);
        }

        /** Public read-only check: is this node currently the raft leader? */
        public boolean isLeader() {
            return rn.basicStatus().softState.raftState == RaftStateType.StateLeader;
        }
    }

    private final Map<Long, Node> nodes = new LinkedHashMap<>();

    public KVCluster(long... ids) {
        for (long id : ids) {
            nodes.put(id, new Node(id, ids));
        }
    }

    public Node node(long id) { return nodes.get(id); }

    public Node leader() {
        for (Node n : nodes.values()) {
            if (n.isLeader()) return n;
        }
        return null;
    }

    /** Triggers an election by node {@code id} and runs to quiescence. */
    public void electLeader(long id) {
        nodes.get(id).rn.campaign();
        run(50);
    }

    /** Proposes a KV command on the leader. Throws if there is no leader. */
    public RaftException put(String key, String value) {
        Node l = leader();
        if (l == null) throw new IllegalStateException("no leader");
        RaftException err = l.rn.propose(new KVStore.Command(
                KVStore.Command.Op.PUT, key, value).serialize());
        run(50);
        return err;
    }

    public RaftException delete(String key) {
        Node l = leader();
        if (l == null) throw new IllegalStateException("no leader");
        RaftException err = l.rn.propose(new KVStore.Command(
                KVStore.Command.Op.DELETE, key, null).serialize());
        run(50);
        return err;
    }

    /**
     * Run the cluster for up to {@code ticks} idle rounds; each idle round
     * fires a tick on every node (mirrors the AsyncStorageWritesTest harness).
     */
    public void run(int ticks) {
        int idle = 0;
        for (int i = 0; i < ticks; i++) {
            boolean progress = false;
            for (Node n : nodes.values()) {
                while (!n.inbox.isEmpty()) {
                    n.rn.step(n.inbox.remove(0));
                    progress = true;
                }
                if (n.rn.hasReady()) {
                    handleReady(n);
                    progress = true;
                }
            }
            if (!progress) {
                idle++;
                if (idle >= 2) return;
                for (Node n : nodes.values()) n.rn.tick();
            } else {
                idle = 0;
            }
        }
    }

    private void handleReady(Node n) {
        Ready rd = n.rn.ready();
        if (!rd.entries.isEmpty()) n.storage.append(rd.entries);
        if (rd.snapshot != null && !Util.isEmptySnap(rd.snapshot)) {
            try { n.storage.applySnapshot(rd.snapshot); }
            catch (RaftException e) { throw new RuntimeException(e); }
        }
        // Apply committed entries to the state machine. EntryNormal with
        // non-empty data is a KV command; empty payload (leader's no-op
        // post-election) is ignored.
        for (Eraftpb.Entry e : rd.committedEntries) {
            if (e.getEntryType() == Eraftpb.EntryType.EntryNormal && !e.getData().isEmpty()) {
                KVStore.Command cmd = KVStore.Command.deserialize(e.getData().toByteArray());
                n.kvStore.applyCommand(e.getIndex(), cmd);
            }
        }
        for (Eraftpb.Message m : rd.messages) {
            Node target = nodes.get(m.getTo());
            if (target != null && target != n) target.inbox.add(m);
        }
        n.rn.advance(rd);
    }
}
