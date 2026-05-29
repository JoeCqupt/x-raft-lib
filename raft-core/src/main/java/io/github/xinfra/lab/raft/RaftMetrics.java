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
 * Pluggable metrics sink for raft state-machine events. Implementations bridge
 * to a real metrics backend (Micrometer, Prometheus simpleclient, OpenTelemetry,
 * Dropwizard, ...). Default is {@link #NOOP}.
 *
 * <p>All callbacks fire on the event-loop thread; do not block.
 *
 * <p>This interface intentionally takes simple primitives instead of Tag/Label
 * objects so it has zero external dependencies. Bridge code attaches per-node
 * tags (node id, raft group) once and fans events out to the backend.
 *
 * <h2>Recommended bindings</h2>
 *
 * <ul>
 *   <li>Counters: {@code proposalsAccepted/Dropped/Stopped}, {@code leaderChanges},
 *   {@code readyEmitted}, {@code readIndexDropped}, {@code readStateDropped},
 *   {@code tickSkipped}, {@code peerUnreachable}, {@code snapshotInstall}.</li>
 *   <li>Gauges (call from a periodic poller, not from callbacks):
 *   {@code commitLag = lastIndex - committed}, {@code applyLag = committed - applied},
 *   {@code eventQueueDepth}, {@code readyQueueDepth}, {@code term}, {@code state}.</li>
 *   <li>Histograms: {@code appendDurationNanos}, {@code applyBatchSize},
 *   {@code snapshotInstallDurationNanos}, {@code snapshotSendBytes}.</li>
 * </ul>
 */
public interface RaftMetrics {

    /** No-op metrics sink. Returned when {@code Config.metrics} is unset. */
    RaftMetrics NOOP = new RaftMetrics() {};

    /**
     * Result of a propose attempt before reaching the state machine.
     * Drives the {@code proposalsTotal{result=...}} counter.
     */
    enum ProposalResult { ACCEPTED, DROPPED, STOPPED }

    /** Reason a {@code MsgReadIndex} was dropped (queue cap, no leader, ...). */
    enum ReadIndexDropReason { QUEUE_FULL, NO_LEADER, FORWARDING_DISABLED }

    /** Phase reported for snapshot install. */
    enum SnapshotPhase { STARTED, RESTORED, IGNORED }

    // ---- counters ----

    default void onProposal(ProposalResult result) {}

    /** Local node observed a leader change (incl. transition to NONE). */
    default void onLeaderChange(long newLeader, long term) {}

    /** A {@code MsgReadIndex} was dropped without enqueuing a read state. */
    default void onReadIndexDropped(ReadIndexDropReason reason) {}

    /** A confirmed read state was evicted because the local queue was full. */
    default void onReadStateEvicted() {}

    /** The host application's tick was dropped because the burst limit fired. */
    default void onTickSkipped() {}

    /** A peer was reported unreachable by the host transport. */
    default void onPeerUnreachable(long peerId) {}

    /** Snapshot install transitioned phases on the local follower. */
    default void onSnapshotInstall(SnapshotPhase phase) {}

    // ---- gauges (host should poll these via Status / direct reads) ----

    /** Hint that the host should sample gauges now (called once per Ready cycle). */
    default void onReadyEmitted() {}
}
