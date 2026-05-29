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

/**
 * The replication state the leader tracks for a follower. Public view of the
 * internal tracker state type, exposed through {@link PeerProgress}.
 *
 * <ul>
 *   <li>{@link #PROBE} — the leader sends at most one entry per heartbeat and
 *       probes the follower's actual log position.</li>
 *   <li>{@link #REPLICATE} — the leader streams entries optimistically, using
 *       inflight flow control.</li>
 *   <li>{@link #SNAPSHOT} — the follower is too far behind; the leader has
 *       stopped sending entries and is installing a snapshot.</li>
 * </ul>
 */
public enum ProgressState {
    PROBE,
    REPLICATE,
    SNAPSHOT;

    @Override
    public String toString() {
        return switch (this) {
            case PROBE -> "StateProbe";
            case REPLICATE -> "StateReplicate";
            case SNAPSHOT -> "StateSnapshot";
        };
    }
}
