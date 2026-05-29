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

/**
 * Config contains the parameters to start a raft.
 */
public class Config {
    /** ID is the identity of the local raft. ID cannot be 0. */
    public long id;

    /** ElectionTick is the number of Node.Tick invocations that must pass between elections. */
    public int electionTick;

    /** HeartbeatTick is the number of Node.Tick invocations that must pass between heartbeats. */
    public int heartbeatTick;

    /** Storage is the storage for raft. */
    public Storage storage;

    /** Applied is the last applied index. It should only be set when restarting raft. */
    public long applied;

    /** AsyncStorageWrites configures the raft node to write to its local storage using async messages. */
    public boolean asyncStorageWrites;

    /** MaxSizePerMsg limits the max byte size of each append message. */
    public long maxSizePerMsg;

    /** MaxCommittedSizePerReady limits the size of the committed entries which can be applying. */
    public long maxCommittedSizePerReady;

    /** MaxUncommittedEntriesSize limits the aggregate byte size of the uncommitted entries. */
    public long maxUncommittedEntriesSize;

    /** MaxInflightMsgs limits the max number of in-flight append messages. */
    public int maxInflightMsgs;

    /** MaxInflightBytes limits the number of in-flight bytes in append messages. */
    public long maxInflightBytes;

    /**
     * Cap on the leader's queue of pending read-index requests that arrived
     * before the current term has any committed entry. Reads beyond this cap
     * are dropped (oldest first) and the affected client must retry. Default
     * 1024; set to 0 to disable the cap (unbounded — not recommended for
     * production with sustained read load).
     */
    public int maxPendingReadIndexMessages = 1024;

    /**
     * Cap on the local {@code Ready.readStates} queue. New entries beyond the
     * cap evict the oldest. Default 1024; set to 0 to disable the cap.
     */
    public int maxReadStates = 1024;

    /** CheckQuorum specifies if the leader should check quorum activity. */
    public boolean checkQuorum;

    /** PreVote enables the Pre-Vote algorithm. */
    public boolean preVote;

    /** ReadOnlyOption specifies how the read only request is processed. */
    public ReadOnlyOption readOnlyOption = ReadOnlyOption.ReadOnlySafe;

    /** DisableProposalForwarding set to true means that followers will drop proposals. */
    public boolean disableProposalForwarding;

    /** DisableConfChangeValidation turns off propose-time verification of configuration changes. */
    public boolean disableConfChangeValidation;

    /** StepDownOnRemoval makes the leader step down when it is removed from the group. */
    public boolean stepDownOnRemoval;

    /** Logger is the logger used for raft log. Each raft group can have its own logger. */
    public RaftLogger logger;

    /** TraceLogger is the optional trace logger for raft state machine event tracing. */
    public TraceLogger traceLogger;

    /**
     * Optional pluggable metrics sink. Defaults to {@link RaftMetrics#NOOP}
     * when null. See {@link RaftMetrics} for the recommended binding to a
     * real backend (Micrometer / Prometheus / OpenTelemetry).
     */
    public RaftMetrics metrics;

    /**
     * Whether the {@code DefaultNode} event-loop thread is a daemon. Default
     * is {@code true} so JUnit / Surefire / generic test harnesses don't
     * hang when a test forgets to call {@link Node#stop()}.
     *
     * <p><b>Production deployments should set this to {@code false}</b>: a
     * daemon thread is killed without warning when the JVM exits, dropping
     * any in-flight propose that hasn't reached storage. With non-daemon,
     * the host's shutdown logic must explicitly call {@code stop()}.
     */
    public boolean daemonEventLoop = true;

    public void validate() {
        if (id == Util.NONE) {
            throw new IllegalArgumentException("cannot use none as id");
        }
        if (Util.isLocalMsgTarget(id)) {
            throw new IllegalArgumentException("cannot use local target as id");
        }
        if (heartbeatTick <= 0) {
            throw new IllegalArgumentException("heartbeat tick must be greater than 0");
        }
        if (electionTick <= heartbeatTick) {
            throw new IllegalArgumentException("election tick must be greater than heartbeat tick");
        }
        // Recommended ratio per the Raft thesis: electionTick should be an
        // order of magnitude above heartbeatTick so that a missed heartbeat
        // doesn't trigger an election. Logged as warn (not error) so existing
        // tightly-tuned tests keep working.
        if (electionTick < 10 * heartbeatTick) {
            (logger != null ? logger : DefaultRaftLogger.getDefault()).warn(
                    "electionTick={} is less than 10x heartbeatTick={} — single missed heartbeat may trigger an election",
                    electionTick, heartbeatTick);
        }
        if (storage == null) {
            throw new IllegalArgumentException("storage cannot be nil");
        }
        if (maxSizePerMsg < 0) {
            throw new IllegalArgumentException("maxSizePerMsg must be >= 0");
        }
        if (maxSizePerMsg == 0) {
            // 0 used to be silently accepted, which made the leader pack the
            // ENTIRE log into a single MsgAppend on first contact. Refuse it.
            throw new IllegalArgumentException(
                    "maxSizePerMsg must be > 0 (recommend 1MiB - 64MiB depending on workload). " +
                            "Set Config.maxSizePerMsg explicitly.");
        }
        if (maxUncommittedEntriesSize == 0) {
            // NOTE: NO_LIMIT here means *no propose backpressure*. A flapping
            // follower can wedge the leader's heap until OOM. This default is
            // kept for compatibility with etcd-raft's behaviour, but is logged
            // so production deployments are nudged to set it.
            (logger != null ? logger : DefaultRaftLogger.getDefault()).warn(
                    "maxUncommittedEntriesSize is unset (NO_LIMIT) — leader has no propose backpressure. " +
                            "Set Config.maxUncommittedEntriesSize for production.");
            maxUncommittedEntriesSize = RaftLog.NO_LIMIT;
        }
        if (maxCommittedSizePerReady == 0) {
            maxCommittedSizePerReady = maxSizePerMsg;
        }
        if (maxInflightMsgs <= 0) {
            throw new IllegalArgumentException("max inflight messages must be greater than 0");
        }
        if (maxInflightBytes == 0) {
            maxInflightBytes = RaftLog.NO_LIMIT;
        } else if (maxInflightBytes < maxSizePerMsg) {
            throw new IllegalArgumentException("max inflight bytes must be >= max message size");
        }
        if (maxPendingReadIndexMessages < 0) {
            throw new IllegalArgumentException("maxPendingReadIndexMessages must be >= 0");
        }
        if (maxReadStates < 0) {
            throw new IllegalArgumentException("maxReadStates must be >= 0");
        }
        if (readOnlyOption == ReadOnlyOption.ReadOnlyLeaseBased && !checkQuorum) {
            throw new IllegalArgumentException("CheckQuorum must be enabled when ReadOnlyOption is ReadOnlyLeaseBased");
        }
        if (logger == null) {
            logger = DefaultRaftLogger.getDefault();
        }
        if (metrics == null) {
            metrics = RaftMetrics.NOOP;
        }
    }
}
