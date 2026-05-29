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
package io.github.xinfra.lab.raft;
import io.github.xinfra.lab.raft.internal.*;

import org.slf4j.MDC;

/**
 * SLF4J {@link MDC} keys that {@code raft-core} populates on the event-loop
 * thread so log aggregators can filter and correlate by raft identity without
 * parsing message text.
 *
 * <p>{@link DefaultNode}'s event loop sets {@link #NODE_ID} once for the
 * lifetime of the loop and refreshes {@link #TERM}, {@link #STATE}, and
 * {@link #LEADER} on every iteration as raft state advances. Because the loop
 * owns a dedicated thread, every log line emitted while processing an event
 * carries the current context.
 *
 * <p>Hosts that drive {@link RawNode} directly (no {@code DefaultNode}) can
 * call {@link #set} / {@link #clear} around their own ready/advance loop to get
 * the same correlation.
 *
 * <p>To surface these in logback, reference them in the encoder pattern, e.g.
 * {@code %X{raftNodeId} term=%X{raftTerm} %X{raftState}}.
 */
public final class RaftMdc {

    /** Local raft node id (decimal). */
    public static final String NODE_ID = "raftNodeId";

    /** Current raft term (decimal). */
    public static final String TERM = "raftTerm";

    /** Current role: follower / candidate / leader / pre-candidate. */
    public static final String STATE = "raftState";

    /** Known leader id, or {@code none} when there is no leader. */
    public static final String LEADER = "raftLeader";

    private RaftMdc() {}

    /** Sets the per-thread raft context. {@code leader == Util.NONE} renders as {@code none}. */
    public static void set(long nodeId, long term, RaftStateType state, long leader) {
        MDC.put(NODE_ID, Long.toString(nodeId));
        MDC.put(TERM, Long.toString(term));
        MDC.put(STATE, state == null ? "" : state.name());
        MDC.put(LEADER, leader == Util.NONE ? "none" : Long.toString(leader));
    }

    /** Removes only the raft keys, leaving any host-set MDC entries intact. */
    public static void clear() {
        MDC.remove(NODE_ID);
        MDC.remove(TERM);
        MDC.remove(STATE);
        MDC.remove(LEADER);
    }
}
