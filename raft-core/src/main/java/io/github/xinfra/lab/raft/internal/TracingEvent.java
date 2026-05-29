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

import java.util.List;
import java.util.Map;

/**
 * TracingEvent represents a raft state machine event for tracing.
 * Corresponds to the TracingEvent struct in etcd-raft's state_trace.go.
 */
public class TracingEvent {

    public enum EventType {
        InitState,
        BecomeCandidate,
        BecomeFollower,
        BecomeLeader,
        Commit,
        Replicate,
        ChangeConf,
        ApplyConfChange,
        Ready,
        SendAppendEntriesRequest,
        ReceiveAppendEntriesRequest,
        SendAppendEntriesResponse,
        ReceiveAppendEntriesResponse,
        SendRequestVoteRequest,
        ReceiveRequestVoteRequest,
        SendRequestVoteResponse,
        ReceiveRequestVoteResponse,
        SendSnapshot,
        ReceiveSnapshot
    }

    private final EventType name;
    private final String nodeId;
    private final TracingState state;
    private final String role;
    private final long logSize;
    private final List<String>[] conf;
    private final TracingMessage message;
    private final Map<String, Object> properties;

    @SuppressWarnings("unchecked")
    public TracingEvent(EventType name, String nodeId, TracingState state, String role,
                        long logSize, List<String>[] conf, TracingMessage message,
                        Map<String, Object> properties) {
        this.name = name;
        this.nodeId = nodeId;
        this.state = state;
        this.role = role;
        this.logSize = logSize;
        this.conf = conf;
        this.message = message;
        this.properties = properties;
    }

    public EventType getName() { return name; }
    public String getNodeId() { return nodeId; }
    public TracingState getState() { return state; }
    public String getRole() { return role; }
    public long getLogSize() { return logSize; }
    public List<String>[] getConf() { return conf; }
    public TracingMessage getMessage() { return message; }
    public Map<String, Object> getProperties() { return properties; }

    /**
     * Tracing state containing term, vote, and commit.
     */
    public record TracingState(long term, String vote, long commit) {}

    /**
     * Tracing message containing message details.
     */
    public record TracingMessage(String type, long term, String from, String to,
                                 int entryLength, long logTerm, long index,
                                 long commit, String vote, boolean reject, long rejectHint) {

        public static TracingMessage fromMessage(Eraftpb.Message m) {
            long logTerm = m.getLogTerm();
            int entries = m.getEntriesCount();
            long index = m.getIndex();
            if (m.getMsgType() == Eraftpb.MessageType.MsgSnapshot) {
                index = 0;
                logTerm = 0;
                entries = (int) m.getSnapshot().getMetadata().getIndex();
            }
            return new TracingMessage(
                    m.getMsgType().name(), m.getTerm(),
                    Long.toString(m.getFrom()), Long.toString(m.getTo()),
                    entries, logTerm, index, m.getCommit(),
                    Long.toString(m.getVote()), m.getReject(), m.getRejectHint()
            );
        }
    }
}
