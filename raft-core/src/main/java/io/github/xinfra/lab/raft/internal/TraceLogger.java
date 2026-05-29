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

/**
 * TraceLogger is an interface for tracing raft state machine events.
 * Corresponds to the TraceLogger interface in etcd-raft's state_trace.go.
 *
 * <p>When configured on a raft Config, the raft state machine will call
 * {@link #traceEvent(TracingEvent)} at important state transitions and
 * message exchanges, enabling external observability and debugging.
 */
public interface TraceLogger {
    void traceEvent(TracingEvent event);
}
