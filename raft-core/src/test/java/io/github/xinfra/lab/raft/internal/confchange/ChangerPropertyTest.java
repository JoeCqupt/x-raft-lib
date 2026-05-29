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

import io.github.xinfra.lab.raft.proto.Eraftpb;
import io.github.xinfra.lab.raft.internal.tracker.Inflights;
import io.github.xinfra.lab.raft.internal.tracker.Progress;
import io.github.xinfra.lab.raft.internal.tracker.ProgressTracker;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property-based tests for {@link Changer} invariants. Mirrors etcd-raft's
 * {@code confchange/quick_test.go}: random sequences of conf changes applied
 * to a starting configuration should always either (a) succeed and produce a
 * config satisfying {@link Changer}'s checkInvariants, or (b) fail loudly.
 */
class ChangerPropertyTest {

    /** Generate a list of random conf changes targeting ids in 1..5. */
    @Provide
    Arbitrary<List<Eraftpb.ConfChangeSingle>> changes() {
        Arbitrary<Eraftpb.ConfChangeType> types = Arbitraries.of(
                Eraftpb.ConfChangeType.ConfChangeAddNode,
                Eraftpb.ConfChangeType.ConfChangeAddLearnerNode,
                Eraftpb.ConfChangeType.ConfChangeRemoveNode);
        Arbitrary<Long> ids = Arbitraries.longs().between(1L, 5L);
        Arbitrary<Eraftpb.ConfChangeSingle> single = Arbitraries.entries(types, ids)
                .map(e -> Eraftpb.ConfChangeSingle.newBuilder()
                        .setType(e.getKey())
                        .setNodeId(e.getValue())
                        .build());
        return single.list().ofMinSize(0).ofMaxSize(8);
    }

    /**
     * A sequence of simple conf changes (one at a time) applied to an
     * initially-1-voter cluster always preserves Changer invariants and never
     * removes the last voter.
     */
    @Property(tries = 200)
    void simpleSequenceMaintainsInvariants(@ForAll("changes") List<Eraftpb.ConfChangeSingle> ccs) {
        ProgressTracker trk = bootstrap();

        for (Eraftpb.ConfChangeSingle cc : ccs) {
            Changer changer = new Changer(trk, /*lastIndex*/ 100);
            try {
                Changer.Result r = changer.simple(List.of(cc));
                // Apply; checkInvariants is internal but throws on failure.
                trk.setConfig(r.config());
                trk.setProgress(r.progress());
            } catch (RuntimeException e) {
                // Some sequences are illegal (e.g. removing the last voter,
                // or simple changes touching > 1 voter in a row). Changer must
                // reject them; swallow and continue.
            }
            // After every step, the invariants we *guarantee* must hold:
            // - Voter set non-empty.
            // - Progress map keys equal voters ∪ learners ∪ learnersNext.
            assertThat(trk.getConfig().getVoters().incoming().isEmpty())
                    .as("voters must never be empty after applied step").isFalse();
            assertProgressKeysCoverConfig(trk);
        }
    }

    /**
     * EnterJoint then LeaveJoint round-trips through a valid joint state and
     * back to a single-config state with the expected voter set.
     */
    @Property(tries = 200)
    void enterAndLeaveJointRoundTrip(@ForAll("changes") List<Eraftpb.ConfChangeSingle> ccs) {
        ProgressTracker trk = bootstrap();

        Changer changer = new Changer(trk, 100);
        try {
            Changer.Result r = changer.enterJoint(/*autoLeave*/ false, ccs);
            trk.setConfig(r.config());
            trk.setProgress(r.progress());
        } catch (RuntimeException e) {
            // Some change sets are invalid; we don't drive further.
            return;
        }
        // In joint state, outgoing must be non-empty.
        assertThat(trk.getConfig().getVoters().outgoing().isEmpty())
                .as("outgoing should be populated after EnterJoint").isFalse();
        assertProgressKeysCoverConfig(trk);

        Changer leaver = new Changer(trk, 100);
        Changer.Result r2 = leaver.leaveJoint();
        trk.setConfig(r2.config());
        trk.setProgress(r2.progress());

        // After LeaveJoint, outgoing must be empty (unset/empty MajorityConfig).
        assertThat(trk.getConfig().getVoters().outgoing().isEmpty())
                .as("outgoing must be empty after LeaveJoint").isTrue();
        assertThat(trk.getConfig().isAutoLeave())
                .as("autoLeave must be false post-leave").isFalse();
        assertProgressKeysCoverConfig(trk);
    }

    /**
     * Restore from a generated ConfState reproduces the same config when
     * applied to a fresh tracker. This is the property etcd-raft relies on
     * to recover a node from a snapshot.
     */
    @Property(tries = 100)
    void restoreFromConfStateReproducesConfig(@ForAll("voterSets") Set<Long> voters,
                                              @ForAll("voterSets") Set<Long> learners) {
        // Make sure voters and learners are disjoint (Changer invariant).
        Set<Long> learnerOnly = new HashSet<>(learners);
        learnerOnly.removeAll(voters);
        if (voters.isEmpty()) return;

        Eraftpb.ConfState.Builder csb = Eraftpb.ConfState.newBuilder();
        for (long v : voters) csb.addVoters(v);
        for (long l : learnerOnly) csb.addLearners(l);
        Eraftpb.ConfState cs = csb.build();

        ProgressTracker trk = ProgressTracker.make(64, 0);
        Changer changer = new Changer(trk, 0);
        Changer.Result r = Changer.restore(changer, cs);
        trk.setConfig(r.config());
        trk.setProgress(r.progress());

        Eraftpb.ConfState rebuilt = trk.confState();
        // Voters must match (order-independent).
        assertThat(new HashSet<>(rebuilt.getVotersList())).isEqualTo(voters);
        assertThat(new HashSet<>(rebuilt.getLearnersList())).isEqualTo(learnerOnly);
    }

    @Provide
    Arbitrary<Set<Long>> voterSets() {
        return Arbitraries.longs().between(1L, 10L).set().ofMinSize(0).ofMaxSize(7);
    }

    // ---- Helpers ----

    /** A single-voter (id=1) ProgressTracker, mirroring a freshly-bootstrapped node. */
    private static ProgressTracker bootstrap() {
        ProgressTracker trk = ProgressTracker.make(64, 0);
        Map<Long, Progress> p = new HashMap<>();
        Progress pr = new Progress(0, 1, new Inflights(64, 0), false);
        p.put(1L, pr);
        trk.setProgress(p);
        trk.getConfig().getVoters().incoming().add(1L);
        return trk;
    }

    /**
     * Assert that the progress map's key set covers the union of voters,
     * learners, and learners_next — the invariant Changer.checkInvariants
     * enforces internally.
     */
    private static void assertProgressKeysCoverConfig(ProgressTracker trk) {
        Set<Long> needed = new HashSet<>(trk.getConfig().getVoters().ids());
        if (trk.getConfig().getLearners() != null)
            needed.addAll(trk.getConfig().getLearners());
        if (trk.getConfig().getLearnersNext() != null)
            needed.addAll(trk.getConfig().getLearnersNext());
        for (long id : needed) {
            assertThat(trk.getProgress()).containsKey(id);
        }
    }
}
