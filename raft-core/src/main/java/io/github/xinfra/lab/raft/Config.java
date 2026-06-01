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

import io.github.xinfra.lab.raft.internal.RaftLog;
import io.github.xinfra.lab.raft.internal.TraceLogger;
import io.github.xinfra.lab.raft.internal.Util;
import org.jspecify.annotations.Nullable;

/**
 * Config contains the parameters to start a raft. Built via {@link #builder()}:
 *
 * <pre>{@code
 *   Config cfg = Config.builder()
 *           .id(1)
 *           .electionTick(10)
 *           .heartbeatTick(1)
 *           .storage(memStorage)
 *           .maxSizePerMsg(1L << 20)
 *           .maxInflightMsgs(256)
 *           .build();
 * }</pre>
 *
 * <p>{@link Builder#build()} validates the parameters and applies sensible
 * defaults (e.g. {@code maxCommittedSizePerReady = maxSizePerMsg} when unset).
 * The resulting {@code Config} is immutable; call {@link #toBuilder()} to
 * derive a tweaked copy.
 */
public final class Config {

    /** ID is the identity of the local raft. ID cannot be 0. */
    public final long id;

    /** ElectionTick is the number of Node.Tick invocations that must pass between elections. */
    public final int electionTick;

    /** HeartbeatTick is the number of Node.Tick invocations that must pass between heartbeats. */
    public final int heartbeatTick;

    /** Storage is the storage for raft. */
    public final Storage storage;

    /** Applied is the last applied index. It should only be set when restarting raft. */
    public final long applied;

    /** AsyncStorageWrites configures the raft node to write to its local storage using async messages. */
    public final boolean asyncStorageWrites;

    /** MaxSizePerMsg limits the max byte size of each append message. */
    public final long maxSizePerMsg;

    /** MaxCommittedSizePerReady limits the size of the committed entries which can be applying. */
    public final long maxCommittedSizePerReady;

    /** MaxUncommittedEntriesSize limits the aggregate byte size of the uncommitted entries. */
    public final long maxUncommittedEntriesSize;

    /** MaxInflightMsgs limits the max number of in-flight append messages. */
    public final int maxInflightMsgs;

    /** MaxInflightBytes limits the number of in-flight bytes in append messages. */
    public final long maxInflightBytes;

    /**
     * Cap on the leader's queue of pending read-index requests that arrived
     * before the current term has any committed entry. Reads beyond this cap
     * are dropped (oldest first) and the affected client must retry.
     */
    public final int maxPendingReadIndexMessages;

    /** Cap on the local {@code Ready.readStates()} queue. New entries beyond the cap evict the oldest. */
    public final int maxReadStates;

    /** CheckQuorum specifies if the leader should check quorum activity. */
    public final boolean checkQuorum;

    /** PreVote enables the Pre-Vote algorithm. */
    public final boolean preVote;

    /** ReadOnlyOption specifies how the read only request is processed. */
    public final ReadOnlyOption readOnlyOption;

    /** DisableProposalForwarding set to true means that followers will drop proposals. */
    public final boolean disableProposalForwarding;

    /** DisableConfChangeValidation turns off propose-time verification of configuration changes. */
    public final boolean disableConfChangeValidation;

    /** StepDownOnRemoval makes the leader step down when it is removed from the group. */
    public final boolean stepDownOnRemoval;

    /** Logger is the logger used for raft log. Each raft group can have its own logger. */
    public final RaftLogger logger;

    /** TraceLogger is the optional trace logger for raft state machine event tracing. */
    public final @Nullable TraceLogger traceLogger;

    /**
     * Optional pluggable metrics sink. Defaults to {@link RaftMetrics#NOOP}
     * when not set. See {@link RaftMetrics} for the recommended binding to a
     * real backend (Micrometer / Prometheus / OpenTelemetry).
     */
    public final RaftMetrics metrics;

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
    public final boolean daemonEventLoop;

    private Config(Builder b) {
        this.id = b.id;
        this.electionTick = b.electionTick;
        this.heartbeatTick = b.heartbeatTick;
        this.storage = b.storage;
        this.applied = b.applied;
        this.asyncStorageWrites = b.asyncStorageWrites;
        this.maxSizePerMsg = b.maxSizePerMsg;
        this.maxCommittedSizePerReady = b.maxCommittedSizePerReady;
        this.maxUncommittedEntriesSize = b.maxUncommittedEntriesSize;
        this.maxInflightMsgs = b.maxInflightMsgs;
        this.maxInflightBytes = b.maxInflightBytes;
        this.maxPendingReadIndexMessages = b.maxPendingReadIndexMessages;
        this.maxReadStates = b.maxReadStates;
        this.checkQuorum = b.checkQuorum;
        this.preVote = b.preVote;
        this.readOnlyOption = b.readOnlyOption;
        this.disableProposalForwarding = b.disableProposalForwarding;
        this.disableConfChangeValidation = b.disableConfChangeValidation;
        this.stepDownOnRemoval = b.stepDownOnRemoval;
        this.logger = b.logger;
        this.traceLogger = b.traceLogger;
        this.metrics = b.metrics;
        this.daemonEventLoop = b.daemonEventLoop;
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Returns a {@link Builder} pre-populated with this config's values, so
     * callers can derive a tweaked copy without restating every field.
     */
    public Builder toBuilder() {
        Builder b = new Builder();
        b.id = id;
        b.electionTick = electionTick;
        b.heartbeatTick = heartbeatTick;
        b.storage = storage;
        b.applied = applied;
        b.asyncStorageWrites = asyncStorageWrites;
        b.maxSizePerMsg = maxSizePerMsg;
        b.maxCommittedSizePerReady = maxCommittedSizePerReady;
        b.maxUncommittedEntriesSize = maxUncommittedEntriesSize;
        b.maxInflightMsgs = maxInflightMsgs;
        b.maxInflightBytes = maxInflightBytes;
        b.maxPendingReadIndexMessages = maxPendingReadIndexMessages;
        b.maxReadStates = maxReadStates;
        b.checkQuorum = checkQuorum;
        b.preVote = preVote;
        b.readOnlyOption = readOnlyOption;
        b.disableProposalForwarding = disableProposalForwarding;
        b.disableConfChangeValidation = disableConfChangeValidation;
        b.stepDownOnRemoval = stepDownOnRemoval;
        b.logger = logger;
        b.traceLogger = traceLogger;
        b.metrics = metrics;
        b.daemonEventLoop = daemonEventLoop;
        return b;
    }

    public static final class Builder {
        long id;
        int electionTick;
        int heartbeatTick;
        @Nullable Storage storage;
        long applied;
        boolean asyncStorageWrites;
        long maxSizePerMsg;
        long maxCommittedSizePerReady;
        long maxUncommittedEntriesSize;
        int maxInflightMsgs;
        long maxInflightBytes;
        int maxPendingReadIndexMessages = 1024;
        int maxReadStates = 1024;
        boolean checkQuorum;
        boolean preVote;
        ReadOnlyOption readOnlyOption = ReadOnlyOption.ReadOnlySafe;
        boolean disableProposalForwarding;
        boolean disableConfChangeValidation;
        boolean stepDownOnRemoval;
        @Nullable RaftLogger logger;
        @Nullable TraceLogger traceLogger;
        @Nullable RaftMetrics metrics;
        boolean daemonEventLoop = true;

        public Builder id(long v) { this.id = v; return this; }
        public Builder electionTick(int v) { this.electionTick = v; return this; }
        public Builder heartbeatTick(int v) { this.heartbeatTick = v; return this; }
        public Builder storage(Storage v) { this.storage = v; return this; }
        public Builder applied(long v) { this.applied = v; return this; }
        public Builder asyncStorageWrites(boolean v) { this.asyncStorageWrites = v; return this; }
        public Builder maxSizePerMsg(long v) { this.maxSizePerMsg = v; return this; }
        public Builder maxCommittedSizePerReady(long v) { this.maxCommittedSizePerReady = v; return this; }
        public Builder maxUncommittedEntriesSize(long v) { this.maxUncommittedEntriesSize = v; return this; }
        public Builder maxInflightMsgs(int v) { this.maxInflightMsgs = v; return this; }
        public Builder maxInflightBytes(long v) { this.maxInflightBytes = v; return this; }
        public Builder maxPendingReadIndexMessages(int v) { this.maxPendingReadIndexMessages = v; return this; }
        public Builder maxReadStates(int v) { this.maxReadStates = v; return this; }
        public Builder checkQuorum(boolean v) { this.checkQuorum = v; return this; }
        public Builder preVote(boolean v) { this.preVote = v; return this; }
        public Builder readOnlyOption(ReadOnlyOption v) { this.readOnlyOption = v; return this; }
        public Builder disableProposalForwarding(boolean v) { this.disableProposalForwarding = v; return this; }
        public Builder disableConfChangeValidation(boolean v) { this.disableConfChangeValidation = v; return this; }
        public Builder stepDownOnRemoval(boolean v) { this.stepDownOnRemoval = v; return this; }
        public Builder logger(RaftLogger v) { this.logger = v; return this; }
        public Builder traceLogger(@Nullable TraceLogger v) { this.traceLogger = v; return this; }
        public Builder metrics(RaftMetrics v) { this.metrics = v; return this; }
        public Builder daemonEventLoop(boolean v) { this.daemonEventLoop = v; return this; }

        /**
         * Validate inputs, apply defaults, and produce an immutable {@link Config}.
         * Throws {@link IllegalArgumentException} on misconfiguration.
         */
        public Config build() {
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
                                "Set Config.Builder.maxSizePerMsg explicitly.");
            }
            if (maxUncommittedEntriesSize == 0) {
                // NOTE: NO_LIMIT here means *no propose backpressure*. A flapping
                // follower can wedge the leader's heap until OOM. This default is
                // kept for compatibility with etcd-raft's behaviour, but is logged
                // so production deployments are nudged to set it.
                (logger != null ? logger : DefaultRaftLogger.getDefault()).warn(
                        "maxUncommittedEntriesSize is unset (NO_LIMIT) — leader has no propose backpressure. " +
                                "Set Config.Builder.maxUncommittedEntriesSize for production.");
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
            return new Config(this);
        }
    }
}
