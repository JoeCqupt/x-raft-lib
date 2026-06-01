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
import io.github.xinfra.lab.raft.internal.quorum.MajorityConfig;
import io.github.xinfra.lab.raft.internal.tracker.Progress;
import io.github.xinfra.lab.raft.internal.tracker.ProgressTracker;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static io.github.xinfra.lab.raft.internal.TestUtil.*;

/**
 * A simulated network for testing raft message exchange, mirroring Go's network struct.
 */
public class Network {

    Map<Long, StateMachine> peers;
    Map<Long, MemoryStorage> storage;
    Map<Long, Map<Long, Double>> dropm; // from -> to -> drop probability
    Set<Eraftpb.MessageType> ignorem;
    Predicate<Eraftpb.Message> msgHook;

    interface StateMachine {
        void step(Eraftpb.Message m) throws RaftException;
        List<Eraftpb.Message> readMessages();
        void advanceMessagesAfterAppend();
    }

    /** A black hole that drops all messages. */
    static final StateMachine NOP_STEPPER = new StateMachine() {
        @Override public void step(Eraftpb.Message m) {}
        @Override public List<Eraftpb.Message> readMessages() { return Collections.emptyList(); }
        @Override public void advanceMessagesAfterAppend() {}
    };

    /** Adapter wrapping a Raft instance as a StateMachine. */
    static class RaftStateMachine implements StateMachine {
        final Raft raft;
        RaftStateMachine(Raft r) { this.raft = r; }
        @Override public void step(Eraftpb.Message m) throws RaftException { raft.step(m); }
        @Override public List<Eraftpb.Message> readMessages() { return raft.readMessages(); }
        @Override public void advanceMessagesAfterAppend() { raft.advanceMessagesAfterAppend(); }
    }

    private Network() {
        this.peers = new LinkedHashMap<>();
        this.storage = new LinkedHashMap<>();
        this.dropm = new HashMap<>();
        this.ignorem = new HashSet<>();
    }

    public static Network newNetwork(StateMachine... peers) {
        return newNetworkWithConfig(null, peers);
    }

    public static Network newNetworkWithConfig(Consumer<Config.Builder> configFunc, StateMachine... peers) {
        int size = peers.length;
        long[] peerAddrs = new long[size];
        for (int i = 0; i < size; i++) peerAddrs[i] = i + 1;

        Network nw = new Network();

        for (int j = 0; j < size; j++) {
            long id = peerAddrs[j];
            StateMachine p = peers[j];
            if (p == null) {
                MemoryStorage ms = newTestMemoryStorage(withPeers(peerAddrs));
                nw.storage.put(id, ms);
                Config.Builder cb = newTestConfigBuilder(id, 10, 1, ms);
                if (configFunc != null) configFunc.accept(cb);
                Raft sm = Raft.newRaft(cb.build());
                nw.peers.put(id, new RaftStateMachine(sm));
            } else if (p instanceof RaftStateMachine rsm) {
                // Already a raft, re-initialize with proper peer list (matching Go behavior)
                Raft v = rsm.raft;
                // Save existing learners
                Set<Long> learners = new HashSet<>();
                if (v.trk.getConfig().getLearners() != null) {
                    learners.addAll(v.trk.getConfig().getLearners());
                }
                v.id = id;
                // Re-create tracker with all peers
                ProgressTracker newTrk = ProgressTracker.make(v.trk.getMaxInflight(), v.trk.getMaxInflightBytes());
                ProgressTracker.Config trkCfg = new ProgressTracker.Config();
                MajorityConfig incoming = new MajorityConfig();
                Set<Long> learnerSet = learners.isEmpty() ? null : new HashSet<>();
                for (int i = 0; i < size; i++) {
                    Progress pr = new Progress();
                    if (learners.contains(peerAddrs[i])) {
                        pr.setLearner(true);
                        learnerSet.add(peerAddrs[i]);
                    } else {
                        incoming.add(peerAddrs[i]);
                    }
                    newTrk.getProgress().put(peerAddrs[i], pr);
                }
                trkCfg.setVoters(new io.github.xinfra.lab.raft.internal.quorum.JointConfig(incoming, null));
                trkCfg.setLearners(learnerSet);
                newTrk.setConfig(trkCfg);
                v.trk = newTrk;
                v.reset(v.term);
                nw.peers.put(id, rsm);
            } else {
                // blackHole / nopStepper
                nw.peers.put(id, p);
            }
        }
        return nw;
    }

    /** Creates a raft with given entry terms (for use in network creation). */
    public static StateMachine entsWithConfig(Consumer<Config.Builder> configFunc, long... terms) {
        MemoryStorage storage = new MemoryStorage();
        for (int i = 0; i < terms.length; i++) {
            storage.append(List.of(Eraftpb.Entry.newBuilder()
                    .setIndex(i + 1).setTerm(terms[i]).build()));
        }
        Config.Builder cb = newTestConfigBuilder(1, 5, 1, storage);
        if (configFunc != null) configFunc.accept(cb);
        Raft sm = Raft.newRaft(cb.build());
        sm.reset(terms[terms.length - 1]);
        return new RaftStateMachine(sm);
    }

    /** Creates a raft that voted in the given term but has no log entries. */
    public static StateMachine votedWithConfig(Consumer<Config.Builder> configFunc, long vote, long term) {
        MemoryStorage storage = new MemoryStorage();
        storage.setHardState(Eraftpb.HardState.newBuilder().setVote(vote).setTerm(term).build());
        Config.Builder cb = newTestConfigBuilder(1, 5, 1, storage);
        if (configFunc != null) configFunc.accept(cb);
        Raft sm = Raft.newRaft(cb.build());
        sm.reset(term);
        return new RaftStateMachine(sm);
    }

    public void send(Eraftpb.Message... msgs) throws RaftException {
        LinkedList<Eraftpb.Message> queue = new LinkedList<>(Arrays.asList(msgs));
        while (!queue.isEmpty()) {
            Eraftpb.Message m = queue.poll();
            StateMachine p = peers.get(m.getTo());
            if (p == null) continue;
            try {
                p.step(m);
            } catch (RaftException ignored) {
                // Simulated network ignores per-message raft-layer rejections
                // (e.g. ErrProposalDropped) and continues delivering the rest.
            }
            p.advanceMessagesAfterAppend();
            queue.addAll(filter(p.readMessages()));
        }
    }

    public void drop(long from, long to, double perc) throws RaftException {
        dropm.computeIfAbsent(from, k -> new HashMap<>()).put(to, perc);
    }

    public void cut(long one, long other) throws RaftException {
        drop(one, other, 2.0); // always drop
        drop(other, one, 2.0);
    }

    public void isolate(long id) throws RaftException {
        for (long nid : peers.keySet()) {
            if (nid != id) {
                drop(id, nid, 1.0);
                drop(nid, id, 1.0);
            }
        }
    }

    public void ignore(Eraftpb.MessageType t) throws RaftException {
        ignorem.add(t);
    }

    public void recover() throws RaftException {
        dropm.clear();
        ignorem.clear();
    }

    List<Eraftpb.Message> filter(List<Eraftpb.Message> msgs) {
        List<Eraftpb.Message> mm = new ArrayList<>();
        for (Eraftpb.Message m : msgs) {
            if (ignorem.contains(m.getMsgType())) continue;
            if (m.getMsgType() == Eraftpb.MessageType.MsgHup) {
                throw new RuntimeException("unexpected msgHup");
            }
            Map<Long, Double> fromMap = dropm.get(m.getFrom());
            if (fromMap != null) {
                Double perc = fromMap.get(m.getTo());
                if (perc != null && Math.random() < perc) continue;
            }
            if (msgHook != null && !msgHook.test(m)) continue;
            mm.add(m);
        }
        return mm;
    }

    /** Get a peer's Raft instance. */
    public Raft peer(long id) {
        StateMachine sm = peers.get(id);
        if (sm instanceof RaftStateMachine rsm) return rsm.raft;
        return null;
    }
}
