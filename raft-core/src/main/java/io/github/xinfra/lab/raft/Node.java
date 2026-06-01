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

import io.github.xinfra.lab.raft.proto.Eraftpb;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Node represents a node in a raft cluster.
 * This corresponds to the Node interface in etcd-raft's node.go.
 */
public interface Node {

    /**
     * Tick increments the internal logical clock for the Node by a single tick.
     * Election timeouts and heartbeat timeouts are in units of ticks.
     */
    void tick();

    /**
     * Campaign causes the Node to transition to candidate state and start campaigning to become leader.
     *
     * @throws RaftException {@link RaftException#ErrStopped} if the node has been stopped.
     */
    void campaign() throws InterruptedException, RaftException;

    /**
     * Propose proposes that data be appended to the log. Note that proposals can be lost without
     * notice, therefore it is user's job to ensure proposal retries.
     *
     * @throws RaftException {@link RaftException#ErrStopped} if the node has been stopped;
     *                       {@link RaftException#ErrProposalDropped} if raft dropped the proposal
     *                       (e.g. no leader, leadership transfer in progress, follower removed).
     */
    void propose(byte[] data) throws InterruptedException, RaftException;

    /**
     * ProposeConfChange proposes a configuration change.
     *
     * @throws RaftException see {@link #propose(byte[])} for codes.
     */
    void proposeConfChange(Eraftpb.ConfChangeV2 cc) throws InterruptedException, RaftException;

    /**
     * Step advances the state machine using the given message.
     *
     * @throws RaftException {@link RaftException#ErrStopped} if the node has been stopped, or
     *                       {@link RaftException#ErrProposalDropped} for a forwarded MsgPropose
     *                       that the local node refuses (e.g. proposals disabled).
     */
    void step(Eraftpb.Message msg) throws InterruptedException, RaftException;

    /**
     * Ready returns the current point-in-time state.
     * Users of the Node must call Advance after retrieving the state returned by Ready
     * (unless async storage writes is enabled).
     */
    Ready ready() throws InterruptedException;

    /**
     * Advance notifies the Node that the application has saved progress up to the last Ready.
     * NOTE: Advance must not be called when using AsyncStorageWrites.
     */
    void advance() throws InterruptedException;

    /**
     * ApplyConfChange applies a config change to the node.
     * Returns the new ConfState.
     */
    Eraftpb.ConfState applyConfChange(Eraftpb.ConfChangeV2 cc) throws InterruptedException;

    /**
     * TransferLeadership attempts to transfer leadership to the given transferee.
     */
    void transferLeadership(long lead, long transferee) throws InterruptedException;

    /**
     * ForgetLeader forgets a follower's current leader, changing it to None.
     *
     * @throws RaftException {@link RaftException#ErrStopped} if the node has been stopped.
     */
    void forgetLeader() throws InterruptedException, RaftException;

    /**
     * ReadIndex requests a read state.
     *
     * @throws RaftException {@link RaftException#ErrStopped} if the node has been stopped.
     */
    void readIndex(byte[] rctx) throws InterruptedException, RaftException;

    /**
     * Status returns the current status of the raft state machine.
     */
    Status status() throws InterruptedException;

    /**
     * ReportUnreachable reports the given node is not reachable for the last send.
     */
    void reportUnreachable(long id);

    /**
     * ReportSnapshot reports the status of the sent snapshot.
     */
    void reportSnapshot(long id, SnapshotStatus status);

    /**
     * Stop performs any necessary termination of the Node. Blocks until the
     * event loop has exited and pending result futures are drained.
     *
     * <p>For production use, prefer {@link #stop(long, TimeUnit)} so a wedged
     * event loop cannot hang the caller indefinitely (e.g. during JVM
     * shutdown).
     */
    void stop() throws InterruptedException;

    /**
     * Stop with a deadline. Returns {@code true} if the node terminated
     * cleanly within the timeout; returns {@code false} if the deadline
     * elapsed first (the event loop may still be running and resources are
     * not fully released).
     *
     * @param timeout how long to wait
     * @param unit    time unit of {@code timeout}
     * @return true on clean shutdown, false on timeout
     */
    boolean stop(long timeout, TimeUnit unit) throws InterruptedException;

    /**
     * Best-effort, non-blocking snapshot of leader/term/commit. Unlike
     * {@link #status()}, this does not enqueue an event onto the loop and
     * therefore returns even when the loop is wedged or stopped — useful
     * for liveness probes / health endpoints. The values are read without
     * synchronisation; treat them as a sample, not a snapshot.
     */
    BasicStatus basicStatus();

    /**
     * Register an observer for leader-change events. Notifications fire on
     * the event-loop thread immediately after the new leader is recorded
     * locally; do not perform blocking work in the callback.
     *
     * @return a handle that, when invoked, deregisters the observer.
     */
    Runnable registerLeaderObserver(LeaderObserver observer);

    /** Snapshot of the host-visible local raft state, sampled lock-free. */
    final class BasicStatus {
        public final long id;
        public final long term;
        public final long lead;
        public final long commit;
        public final long applied;
        public final long lastIndex;
        public final RaftStateType state;
        public BasicStatus(long id, long term, long lead, long commit,
                           long applied, long lastIndex, RaftStateType state) {
            this.id = id;
            this.term = term;
            this.lead = lead;
            this.commit = commit;
            this.applied = applied;
            this.lastIndex = lastIndex;
            this.state = state;
        }
    }

    /** Callback fired when the local node observes a leader change. */
    @FunctionalInterface
    interface LeaderObserver {
        /**
         * @param newLeader id of the new leader, or {@link Util#NONE} if the
         *                  cluster has no leader (e.g. during election).
         * @param term      term in which the new leader was observed.
         */
        void onLeaderChange(long newLeader, long term);
    }

    // ============= Factory methods =============

    /**
     * StartNode returns a new Node given configuration and a list of raft peers.
     */
    static Node startNode(Config c, List<Peer> peers) {
        return DefaultNode.startNode(c, peers);
    }

    /**
     * RestartNode is similar to StartNode but does not take a list of peers.
     */
    static Node restartNode(Config c) {
        return DefaultNode.restartNode(c);
    }
}
