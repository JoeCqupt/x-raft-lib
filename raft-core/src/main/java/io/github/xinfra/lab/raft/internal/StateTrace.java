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
import io.github.xinfra.lab.raft.internal.tracker.ProgressTracker;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * StateTrace provides helper functions for tracing raft state machine events.
 * Corresponds to the trace* functions in etcd-raft's state_trace.go.
 *
 * <p>All methods are no-ops if the raft instance has no TraceLogger configured.
 */
public final class StateTrace {

    private StateTrace() {}

    @SuppressWarnings("unchecked")
    private static void traceEvent(TracingEvent.EventType evt, Raft r, Eraftpb.Message m, Map<String, Object> props) {
        if (r.traceLogger == null) {
            return;
        }
        TracingEvent.TracingState state = new TracingEvent.TracingState(
                r.hardState().getTerm(),
                Long.toString(r.hardState().getVote()),
                r.hardState().getCommit()
        );
        List<String>[] conf = new List[]{
                formatConf(r.trk.getConfig().getVoters().getConfigs()[0].ids()),
                formatConf(r.trk.getConfig().getVoters().getConfigs()[1].ids())
        };
        TracingEvent.TracingMessage msg = m != null ? TracingEvent.TracingMessage.fromMessage(m) : null;

        r.traceLogger.traceEvent(new TracingEvent(
                evt,
                Long.toString(r.id),
                state,
                r.state.name(),
                r.raftLog.lastIndex(),
                conf,
                msg,
                props
        ));
    }

    private static void traceNodeEvent(TracingEvent.EventType evt, Raft r) {
        traceEvent(evt, r, null, null);
    }

    private static List<String> formatConf(Set<Long> ids) {
        if (ids == null || ids.isEmpty()) {
            return List.of();
        }
        List<String> result = new ArrayList<>(ids.size());
        for (long id : ids) {
            result.add(Long.toString(id));
        }
        return result;
    }

    static void traceInitState(Raft r) {
        if (r.traceLogger == null) return;
        traceNodeEvent(TracingEvent.EventType.InitState, r);
    }

    public static void traceReady(Raft r) {
        traceNodeEvent(TracingEvent.EventType.Ready, r);
    }

    static void traceCommit(Raft r) {
        traceNodeEvent(TracingEvent.EventType.Commit, r);
    }

    static void traceReplicate(Raft r, List<Eraftpb.Entry> entries) {
        if (r.traceLogger == null) return;
        for (Eraftpb.Entry e : entries) {
            if (e.getEntryType() == Eraftpb.EntryType.EntryNormal) {
                traceNodeEvent(TracingEvent.EventType.Replicate, r);
            }
        }
    }

    static void traceBecomeFollower(Raft r) {
        traceNodeEvent(TracingEvent.EventType.BecomeFollower, r);
    }

    static void traceBecomeCandidate(Raft r) {
        traceNodeEvent(TracingEvent.EventType.BecomeCandidate, r);
    }

    static void traceBecomeLeader(Raft r) {
        traceNodeEvent(TracingEvent.EventType.BecomeLeader, r);
    }

    static void traceChangeConfEvent(Eraftpb.ConfChangeV2 cc, Raft r) {
        if (r.traceLogger == null) return;
        Map<String, Object> props = new HashMap<>();
        List<Map<String, String>> changes = new ArrayList<>();
        for (Eraftpb.ConfChangeSingle c : cc.getChangesList()) {
            Map<String, String> change = new HashMap<>();
            change.put("nid", Long.toString(c.getNodeId()));
            switch (c.getType()) {
                case ConfChangeAddNode -> change.put("action", "AddNewServer");
                case ConfChangeRemoveNode -> change.put("action", "RemoveServer");
                case ConfChangeAddLearnerNode -> change.put("action", "AddLearner");
                default -> { continue; }
            }
            changes.add(change);
        }
        if (changes.isEmpty()) return;
        props.put("cc", Map.of("changes", changes));
        traceEvent(TracingEvent.EventType.ChangeConf, r, null, props);
    }

    static void traceConfChangeEvent(ProgressTracker.Config cfg, Raft r) {
        if (r.traceLogger == null) return;
        Map<String, Object> props = new HashMap<>();
        props.put("cc", Map.of(
                "changes", List.of(),
                "newconf", formatConf(cfg.getVoters().getConfigs()[0].ids())
        ));
        traceEvent(TracingEvent.EventType.ApplyConfChange, r, null, props);
    }

    static void traceSendMessage(Raft r, Eraftpb.Message m) {
        if (r.traceLogger == null) return;
        Map<String, Object> props = new HashMap<>();
        TracingEvent.EventType evt;
        switch (m.getMsgType()) {
            case MsgAppend -> {
                evt = TracingEvent.EventType.SendAppendEntriesRequest;
                var pr = r.trk.getProgress().get(m.getFrom());
                if (pr != null) {
                    props.put("match", pr.getMatch());
                    props.put("next", pr.getNext());
                }
            }
            case MsgHeartbeat, MsgSnapshot -> evt = TracingEvent.EventType.SendAppendEntriesRequest;
            case MsgAppendResponse, MsgHeartbeatResponse -> evt = TracingEvent.EventType.SendAppendEntriesResponse;
            case MsgRequestVote -> evt = TracingEvent.EventType.SendRequestVoteRequest;
            case MsgRequestVoteResponse -> evt = TracingEvent.EventType.SendRequestVoteResponse;
            default -> { return; }
        }
        traceEvent(evt, r, m, props.isEmpty() ? null : props);
    }

    static void traceReceiveMessage(Raft r, Eraftpb.Message m) {
        if (r.traceLogger == null) return;
        TracingEvent.EventType evt;
        switch (m.getMsgType()) {
            case MsgAppend, MsgHeartbeat, MsgSnapshot -> evt = TracingEvent.EventType.ReceiveAppendEntriesRequest;
            case MsgAppendResponse, MsgHeartbeatResponse -> evt = TracingEvent.EventType.ReceiveAppendEntriesResponse;
            case MsgRequestVote -> evt = TracingEvent.EventType.ReceiveRequestVoteRequest;
            case MsgRequestVoteResponse -> evt = TracingEvent.EventType.ReceiveRequestVoteResponse;
            default -> { return; }
        }
        traceEvent(evt, r, m, null);
    }
}
