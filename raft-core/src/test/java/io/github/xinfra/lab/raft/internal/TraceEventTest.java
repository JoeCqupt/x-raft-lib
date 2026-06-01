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
import java.util.List;

import static io.github.xinfra.lab.raft.internal.TestUtil.*;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that {@link StateTrace} emits the expected sequence of
 * {@link TracingEvent}s during canonical raft transitions, mirroring etcd-raft's
 * trace event coverage. Each test attaches a recording {@link TraceLogger} to
 * the raft node, drives a scenario, and asserts the captured event types
 * (and a few salient fields like role/term).
 *
 * <p>These tests are coarse: they assert the events that <em>must</em> occur,
 * not the exact ordering of every event (e.g. SendAppendEntriesRequest may
 * fire multiple times during a single Ready cycle). That keeps them robust to
 * legitimate refactors of the state machine while still catching missing trace
 * calls — the failure mode that motivated etcd-raft's StateTrace abstraction.
 */
class TraceEventTest {

    /** Collects every traced event in order. */
    static final class RecordingTraceLogger implements TraceLogger {
        final List<TracingEvent> events = new ArrayList<>();
        @Override
        public void traceEvent(TracingEvent event) { events.add(event); }

        List<TracingEvent.EventType> types() {
            List<TracingEvent.EventType> ts = new ArrayList<>(events.size());
            for (TracingEvent e : events) ts.add(e.getName());
            return ts;
        }
    }

    /**
     * Single-node bootstrap: must emit InitState then BecomeFollower.
     */
    @Test
    void initStateAndFollowerEmittedOnBoot() throws RaftException {
        RecordingTraceLogger trace = new RecordingTraceLogger();
        MemoryStorage s = newTestMemoryStorage(withPeers(1));
        Config cfg = newTestConfig(1, 10, 1, s);
        cfg.maxSizePerMsg = NO_LIMIT;
        cfg.maxInflightMsgs = 256;
        cfg.traceLogger = trace;

        Raft.newRaft(cfg);

        // InitState is emitted before the conf change is replayed and the
        // initial becomeFollower runs.
        assertThat(trace.types())
                .as("first events should be InitState followed by ApplyConfChange and BecomeFollower")
                .startsWith(TracingEvent.EventType.InitState)
                .contains(TracingEvent.EventType.ApplyConfChange,
                        TracingEvent.EventType.BecomeFollower);
        // Final state must be follower.
        TracingEvent last = trace.events.get(trace.events.size() - 1);
        assertThat(last.getRole()).isEqualTo("StateFollower");
    }

    /**
     * Single-node election: campaign → BecomeCandidate → BecomeLeader → Replicate.
     * Drives a propose to verify the Replicate event fires for normal entries.
     */
    @Test
    void electionAndProposalEmitsExpectedTraceSequence() throws Exception {
        RecordingTraceLogger trace = new RecordingTraceLogger();
        MemoryStorage s = newTestMemoryStorage(withPeers(1));
        Config cfg = newTestConfig(1, 10, 1, s);
        cfg.maxSizePerMsg = NO_LIMIT;
        cfg.maxInflightMsgs = 256;
        cfg.traceLogger = trace;

        RawNode rn = RawNode.newRawNode(cfg);
        // Clear bootstrap events.
        trace.events.clear();

        rn.campaign();

        // After campaign in a single-node config, BecomeCandidate fires
        // immediately. The self-vote response is queued in msgsAfterAppend and
        // processed when we drain the Ready cycle below, which is when
        // BecomeLeader fires.
        List<TracingEvent.EventType> types = trace.types();
        assertThat(types).as("after campaign, candidate state recorded")
                .contains(TracingEvent.EventType.BecomeCandidate);

        // Drain Ready cycle — this flushes the self-vote response, which
        // triggers BecomeLeader and the leader's no-op append.
        while (rn.hasReady()) {
            Ready rd = rn.ready();
            if (!rd.entries.isEmpty()) s.append(rd.entries);
            rn.advance(rd);
        }
        types = trace.types();
        assertThat(types).as("Ready/Commit/BecomeLeader after drain")
                .contains(TracingEvent.EventType.BecomeLeader,
                        TracingEvent.EventType.Ready);

        // Propose a normal entry → must emit Replicate (for the EntryNormal).
        trace.events.clear();
        rn.propose("hello".getBytes());
        types = trace.types();
        assertThat(types).as("propose triggers Replicate").contains(TracingEvent.EventType.Replicate);
    }

    /**
     * Three-node cluster: leader's bcastAppend must emit
     * SendAppendEntriesRequest events for each follower.
     */
    @Test
    void leaderEmitsSendAppendForEachFollower() throws Exception {
        // Build three rafts manually and route messages.
        RecordingTraceLogger trace1 = new RecordingTraceLogger();
        RecordingTraceLogger trace2 = new RecordingTraceLogger();
        RecordingTraceLogger trace3 = new RecordingTraceLogger();

        Raft r1 = makeRaft(1, trace1, 1, 2, 3);
        Raft r2 = makeRaft(2, trace2, 1, 2, 3);
        Raft r3 = makeRaft(3, trace3, 1, 2, 3);

        Network nt = Network.newNetwork(
                new Network.RaftStateMachine(r1),
                new Network.RaftStateMachine(r2),
                new Network.RaftStateMachine(r3));

        // Trigger election on r1.
        trace1.events.clear(); trace2.events.clear(); trace3.events.clear();
        nt.send(Eraftpb.Message.newBuilder()
                .setFrom(1).setTo(1)
                .setMsgType(Eraftpb.MessageType.MsgHup).build());

        // r1 must emit BecomeCandidate then BecomeLeader.
        assertThat(trace1.types())
                .contains(TracingEvent.EventType.BecomeCandidate,
                        TracingEvent.EventType.BecomeLeader);

        // After becoming leader, r1 broadcasts an initial MsgAppend to 2 and 3.
        long sendAppendCount = trace1.events.stream()
                .filter(e -> e.getName() == TracingEvent.EventType.SendAppendEntriesRequest)
                .count();
        assertThat(sendAppendCount).as("at least one SendAppendEntriesRequest per follower")
                .isGreaterThanOrEqualTo(2L);

        // Followers must record ReceiveAppendEntriesRequest.
        assertThat(trace2.types()).contains(TracingEvent.EventType.ReceiveAppendEntriesRequest);
        assertThat(trace3.types()).contains(TracingEvent.EventType.ReceiveAppendEntriesRequest);
    }

    /**
     * ConfChange must emit ChangeConf at propose time and ApplyConfChange at
     * apply time. Verifies both halves of the conf-change trace coverage.
     */
    @Test
    void confChangeEmitsChangeConfAndApplyConfChange() throws Exception {
        RecordingTraceLogger trace = new RecordingTraceLogger();
        MemoryStorage s = newTestMemoryStorage(withPeers(1));
        Config cfg = newTestConfig(1, 10, 1, s);
        cfg.maxSizePerMsg = NO_LIMIT;
        cfg.maxInflightMsgs = 256;
        cfg.traceLogger = trace;

        RawNode rn = RawNode.newRawNode(cfg);
        rn.campaign();
        while (rn.hasReady()) {
            Ready rd = rn.ready();
            if (!rd.entries.isEmpty()) s.append(rd.entries);
            rn.advance(rd);
        }

        trace.events.clear();

        // Propose a V2 conf change adding node 2.
        Eraftpb.ConfChangeV2 cc = Eraftpb.ConfChangeV2.newBuilder()
                .addChanges(Eraftpb.ConfChangeSingle.newBuilder()
                        .setType(Eraftpb.ConfChangeType.ConfChangeAddNode)
                        .setNodeId(2))
                .build();
        rn.proposeConfChange(cc);
        assertThat(trace.types()).contains(TracingEvent.EventType.ChangeConf);

        // Drain Ready then apply: ApplyConfChange must fire.
        while (rn.hasReady()) {
            Ready rd = rn.ready();
            if (!rd.entries.isEmpty()) s.append(rd.entries);
            rn.advance(rd);
        }
        trace.events.clear();
        rn.applyConfChange(cc);
        assertThat(trace.types()).contains(TracingEvent.EventType.ApplyConfChange);
    }

    // ---- Helpers ----

    private static Raft makeRaft(long id, TraceLogger trace, long... voters) {
        MemoryStorage s = newTestMemoryStorage(withPeers(voters));
        Config cfg = newTestConfig(id, 10, 1, s);
        cfg.maxSizePerMsg = NO_LIMIT;
        cfg.maxInflightMsgs = 256;
        cfg.traceLogger = trace;
        return Raft.newRaft(cfg);
    }
}
