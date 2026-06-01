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

import com.google.protobuf.ByteString;
import io.github.xinfra.lab.raft.proto.Eraftpb;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * DefaultNode is the canonical implementation of the Node interface,
 * corresponding to the `node` struct in etcd-raft's node.go.
 *
 * <p>Threading model: a single dedicated event-loop thread owns the {@link Raft}
 * state. All external API calls (propose, step, advance, tick, ...) enqueue
 * an {@link Event} into a single bounded queue; the loop blocks on
 * {@code events.take()} (true event-driven, no idle polling).
 *
 * <p>The input queue is bounded so a slow event-loop applies natural
 * backpressure on producers, mirroring etcd-raft's unbuffered channel
 * semantics. Tick events are an exception: they use a separate counter and
 * drop on burst (matching etcd-raft's buffered tickc), so a backed-up node
 * warns rather than blocking ticks.
 *
 * <p>The output Ready channel ({@link #readyc}) is unbounded so the loop never
 * blocks when emitting a Ready, even if the consumer is slow — which would
 * otherwise stall ticks and break heartbeats under async-storage-writes mode.
 * In sync mode, {@code waitingAdvance} gates emission to at most one in flight.
 */
public class DefaultNode implements Node {

    /** Default capacity for the events queue. Bounds memory under producer floods. */
    static final int DEFAULT_EVENTS_CAPACITY = 1024;

    // ---- Single unified input queue (replaces tickc/propc/recvc/confc/advancec/statusc) ----
    // Bounded: producers block when full (backpressure). Tick burst protection
    // is enforced separately via pendingTicks (offer + counter, never blocks).
    final BlockingQueue<Event> events;

    // Drop ticks once burst exceeds this — matches etcd-raft's buffered
    // chan(128) behavior. Tick is the only event type that drops on overflow.
    private static final int TICK_BURST_LIMIT = 128;
    private final AtomicInteger pendingTicks = new AtomicInteger(0);

    // ---- Output channel: Ready emissions from loop to consumer ----
    // Unbounded so loop's put never blocks. In sync mode waitingAdvance gates
    // emission to at most one in flight; in async mode the consumer is
    // expected to drain promptly.
    final BlockingQueue<Ready> readyc = new LinkedBlockingQueue<>();

    private final AtomicBoolean stopped = new AtomicBoolean(false);
    private final Object stopSignal = new Object();
    private volatile boolean done = false;

    // ---- Self-removal gate (mirrors etcd-raft node.go's `propc = nil` reset) ----
    // Set once a ConfChange removes the local node from voters and the new
    // ConfState confirms it isn't in voters_outgoing either. After that,
    // proposals are dropped at the producer boundary instead of wasting the
    // event-loop on doomed steps.
    private volatile boolean proposalsDisabled = false;

    final RawNode rn;
    private final boolean daemonEventLoop;
    private Thread eventLoop;

    // ---- Lock-free best-effort liveness mirror ----
    // Updated by the event-loop thread on every transition; read by callers of
    // basicStatus() without going through the events queue. Volatile is enough
    // because we only need monotonic visibility per field, not a consistent
    // snapshot across all of them — callers are warned in the javadoc.
    private final AtomicLong liveTerm = new AtomicLong();
    private final AtomicLong liveLead = new AtomicLong(Util.NONE);
    private final AtomicLong liveCommit = new AtomicLong();
    // Mirrors raftLog.applied / lastIndex so basicStatus() never reads a long
    // field directly off the event-loop's mutable Raft state. A plain read of
    // `raftLog.applied` from a non-loop thread can tear on a 32-bit JVM and
    // sees an inconsistent value mid-update on 64-bit. Mirrored every loop
    // iteration after `applied` and `lastIndex` are recomputed.
    private final AtomicLong liveApplied = new AtomicLong();
    private final AtomicLong liveLastIndex = new AtomicLong();
    private volatile RaftStateType liveState = RaftStateType.StateFollower;

    // ---- Leader-change observers ----
    // ConcurrentHashMap so register/deregister can race the event-loop's
    // notification path. Keyed by handle identity so deregister is O(1).
    private final ConcurrentHashMap<Object, LeaderObserver> leaderObservers = new ConcurrentHashMap<>();

    DefaultNode(RawNode rn) {
        this(rn, DEFAULT_EVENTS_CAPACITY, true);
    }

    DefaultNode(RawNode rn, int eventsCapacity) {
        this(rn, eventsCapacity, true);
    }

    DefaultNode(RawNode rn, int eventsCapacity, boolean daemonEventLoop) {
        this.rn = rn;
        this.events = new LinkedBlockingQueue<>(eventsCapacity);
        this.daemonEventLoop = daemonEventLoop;
    }

    public static Node startNode(Config c, List<Peer> peers) {
        if (peers == null || peers.isEmpty()) {
            throw new IllegalArgumentException("no peers given; use restartNode instead");
        }
        RawNode rawNode = RawNode.newRawNode(c);
        rawNode.bootstrap(peers);
        DefaultNode n = new DefaultNode(rawNode, DEFAULT_EVENTS_CAPACITY, c.daemonEventLoop);
        n.startEventLoop();
        return n;
    }

    public static Node restartNode(Config c) {
        RawNode rawNode = RawNode.newRawNode(c);
        DefaultNode n = new DefaultNode(rawNode, DEFAULT_EVENTS_CAPACITY, c.daemonEventLoop);
        n.startEventLoop();
        return n;
    }

    private void startEventLoop() {
        eventLoop = new Thread(this::run, "raft-node-" + rn.raft.id);
        // Daemon by default for test-harness friendliness. Production hosts
        // should set Config.daemonEventLoop = false so a JVM shutdown won't
        // yank the loop mid-fsync; hosts must then call stop() explicitly.
        eventLoop.setDaemon(daemonEventLoop);
        eventLoop.start();
    }

    /** Test/host hook for adjusting the event-loop thread post-construction. */
    public Thread getEventLoop() {
        return eventLoop;
    }

    /**
     * The main event loop, corresponding to node.run() in Go.
     */
    private void run() {
        Raft r = rn.raft;
        long lead = Util.NONE;
        boolean waitingAdvance = false;

        // Seed structured logging context for this dedicated thread; refreshed
        // each iteration (see below) and removed in the finally.
        RaftMdc.set(r.id, r.term, r.state, r.lead);
        try {
            while (!stopped.get()) {
                try {
                    // Emit Ready if available and not waiting for Advance.
                    // readyc is unbounded so put never blocks the loop.
                    if (!waitingAdvance && rn.hasReady()) {
                        Ready rd = rn.readyWithoutAccept();
                        readyc.put(rd);
                        rn.acceptReady(rd);
                        r.metrics.onReadyEmitted();
                        if (!rn.asyncStorageWrites) {
                            waitingAdvance = true;
                        }
                    }

                    // Log leader changes + notify observers + refresh liveness mirror.
                    if (lead != r.lead) {
                        if (r.hasLeader()) {
                            if (lead == Util.NONE) {
                                r.logger.info("raft.node: {:x} elected leader {:x} at term {}", r.id, r.lead, r.term);
                            } else {
                                r.logger.info("raft.node: {:x} changed leader from {:x} to {:x} at term {}", r.id, lead, r.lead, r.term);
                            }
                        } else {
                            r.logger.info("raft.node: {:x} lost leader {:x} at term {}", r.id, lead, r.term);
                        }
                        lead = r.lead;
                        notifyLeaderChange(r.lead, r.term);
                        r.metrics.onLeaderChange(r.lead, r.term);
                    }
                    // Update the lock-free liveness mirror used by basicStatus().
                    // Done every loop iteration so a stuck consumer of Ready
                    // can still observe term/commit progress.
                    liveTerm.lazySet(r.term);
                    liveLead.lazySet(r.lead);
                    liveCommit.lazySet(r.raftLog.committed);
                    liveApplied.lazySet(r.raftLog.applied);
                    liveLastIndex.lazySet(r.raftLog.lastIndex());
                    liveState = r.state;
                    // Refresh structured logging context so logs emitted while
                    // handling the next event carry the current id/term/role/
                    // leader. Cheap (4 MDC puts), confined to this thread.
                    RaftMdc.set(r.id, r.term, r.state, r.lead);

                    // Block until next event (true event-driven; no idle polling).
                    Event ev = events.take();
                    // AdvanceEvent gates Ready emission, so it owns waitingAdvance.
                    // All other events are stateless w.r.t. the loop and handed off
                    // to dispatch().
                    if (ev instanceof AdvanceEvent) {
                        if (waitingAdvance) {
                            rn.advance(null);
                            waitingAdvance = false;
                        }
                        // Spurious advance (no Ready in flight) is ignored.
                    } else {
                        dispatch(r, ev);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Throwable t) {
                    // dispatch() or rn.hasReady()/readyc.put() raised an unchecked
                    // exception (raft state invariant violation, application
                    // callback bug, etc.). Log and exit the loop cleanly. Cleanup
                    // is in the outer finally so even a logger failure here can't
                    // strand producers blocked on events.put().
                    try {
                        rn.raft.logger.error("raft.node: event loop terminated by uncaught exception: {}", t);
                    } catch (Throwable ignored) {
                        // swallow — finally below still runs.
                    }
                    break;
                }
            }
        } finally {
            // MUST run even if the loop exits via an unchecked exception that
            // escaped the inner catches. Without this, producers blocked on
            // events.put() (queue full, consumer dead) would hang forever.
            // Order matters: set done first so any future producer sees ErrStopped
            // at the top check, then drain pending waiters and unblock anyone
            // currently parked on a full-queue put().
            done = true;
            drainPendingOnStop();
            synchronized (stopSignal) {
                stopSignal.notifyAll();
            }
            RaftMdc.clear();
        }
    }

    private void dispatch(Raft r, Event ev) throws InterruptedException {
        if (ev instanceof TickEvent) {
            pendingTicks.decrementAndGet();
            rn.tick();
        } else if (ev instanceof ProposeEvent pe) {
            Eraftpb.Message m = pe.msg.toBuilder().setFrom(r.id).build();
            try {
                r.step(m);
                if (pe.result != null) pe.result.complete(null);
            } catch (RaftException re) {
                // Raft-layer rejection (ErrProposalDropped, ...) — propagate
                // back to the caller via the future without terminating the
                // event loop.
                if (pe.result != null) pe.result.completeExceptionally(re);
            } catch (RuntimeException e) {
                // Invariant violation or programmer error. Complete the
                // future so the caller doesn't block forever, then rethrow
                // so the outer catch in run() shuts the loop down.
                r.logger.error("propose dispatch raised: {}", e);
                if (pe.result != null) pe.result.completeExceptionally(e);
                throw e;
            }
        } else if (ev instanceof RecvEvent re) {
            Eraftpb.Message msg = re.msg;
            if (Util.isResponseMsg(msg.getMsgType()) && !Util.isLocalMsgTarget(msg.getFrom())
                    && r.trk.getProgress().get(msg.getFrom()) == null) {
                // Drop response from unknown peer
            } else {
                try {
                    r.step(msg);
                } catch (RaftException re2) {
                    // No caller is waiting on recv; the common case is a
                    // forwarded MsgPropose hitting ErrProposalDropped at a
                    // follower-that-isn't-leader. Log at debug.
                    r.logger.debug("dropped {} from {:x}: {}",
                            msg.getMsgType(), msg.getFrom(), re2.getMessage());
                }
            }
        } else if (ev instanceof ConfChangeEvent cce) {
            boolean okBefore = r.trk.getProgress().get(r.id) != null;
            Eraftpb.ConfState cs;
            try {
                cs = r.applyConfChange(cce.cc);
            } catch (RuntimeException e) {
                // Always complete the reply, even on raft-side error, so the
                // caller doesn't block forever on reply.get(). Log + rethrow
                // so the loop terminates and the application is alerted.
                r.logger.error("applyConfChange dispatch raised: {}", e);
                cce.reply.complete(Eraftpb.ConfState.getDefaultInstance());
                throw e;
            }
            // If this node was just removed and isn't a member of either half of
            // the new (possibly joint) configuration, block further proposals —
            // mirrors etcd-raft node.go setting propc = nil after self-removal.
            boolean okAfter = r.trk.getProgress().get(r.id) != null;
            if (okBefore && !okAfter) {
                boolean stillMember = false;
                for (long id : cs.getVotersList()) {
                    if (id == r.id) { stillMember = true; break; }
                }
                if (!stillMember) {
                    for (long id : cs.getVotersOutgoingList()) {
                        if (id == r.id) { stillMember = true; break; }
                    }
                }
                if (!stillMember) {
                    proposalsDisabled = true;
                }
            }
            cce.reply.complete(cs);
        } else if (ev instanceof StatusEvent se) {
            se.result.complete(Status.getStatus(r));
        }
    }

    /**
     * Submit an event whose caller is waiting on {@code result}, and resolve
     * the stop race: the event loop may have shut down (and drained the
     * queue) between {@code done}-check and {@code put}, leaving the future
     * never completed. Idempotent — if the loop or drain completed first,
     * this is a no-op.
     */
    private <T> void submitWithResult(Event ev, CompletableFuture<T> result, T stoppedValue) throws InterruptedException {
        events.put(ev);
        if (done && !result.isDone()) {
            result.complete(stoppedValue);
        }
    }

    /**
     * Variant of {@link #submitWithResult} for futures that signal failure
     * via exceptional completion instead of a sentinel return value. Used
     * by propose() where ErrStopped now propagates as a thrown
     * {@link RaftException}.
     */
    private void submitWithStoppedExceptional(Event ev, CompletableFuture<?> result) throws InterruptedException {
        events.put(ev);
        if (done && !result.isDone()) {
            result.completeExceptionally(RaftException.ErrStopped);
        }
    }

    /**
     * Cancel any pending callers blocked on results from the event loop.
     * Without this, stop() would leave them blocked indefinitely.
     *
     * <p>Must be called after {@code done = true}; a producer racing with this
     * drain will observe done in its post-put check and complete its own future.
     */
    private void drainPendingOnStop() {
        Event ev;
        while ((ev = events.poll()) != null) {
            if (ev instanceof ProposeEvent pe) {
                if (pe.result != null) {
                    pe.result.completeExceptionally(RaftException.ErrStopped);
                }
            } else if (ev instanceof StatusEvent se) {
                se.result.complete(null);
            } else if (ev instanceof ConfChangeEvent cce) {
                cce.reply.complete(Eraftpb.ConfState.getDefaultInstance());
            }
            // tick / advance / recv: nothing waiting on a result, discard.
        }
    }

    @Override
    public void tick() {
        if (done) return;
        // Atomic guard so concurrent ticks can't exceed the burst limit.
        if (pendingTicks.incrementAndGet() > TICK_BURST_LIMIT) {
            pendingTicks.decrementAndGet();
            rn.raft.logger.warn("{:x} A tick missed to fire. Node blocks too long!", rn.raft.id);
            rn.raft.metrics.onTickSkipped();
            return;
        }
        // Tick is non-blocking; if the events queue is saturated by other
        // producers (recv flood, slow loop), drop the tick rather than block.
        // Matches etcd-raft's tickc default-case drop.
        if (!events.offer(TickEvent.INSTANCE)) {
            pendingTicks.decrementAndGet();
            rn.raft.logger.warn("{:x} A tick missed to fire. Events queue full!", rn.raft.id);
        }
    }

    @Override
    public void campaign() throws InterruptedException, RaftException {
        // Fire-and-forget: MsgHup is a local message, bypass the network-source filter
        // in step() by enqueuing directly. Matches etcd-raft's Campaign() semantics.
        if (done) throw RaftException.ErrStopped;
        events.put(new RecvEvent(Eraftpb.Message.newBuilder()
                .setMsgType(Eraftpb.MessageType.MsgHup)
                .build()));
    }

    @Override
    public void propose(byte[] data) throws InterruptedException, RaftException {
        if (done) {
            rn.raft.metrics.onProposal(RaftMetrics.ProposalResult.STOPPED);
            throw RaftException.ErrStopped;
        }
        if (proposalsDisabled) {
            rn.raft.metrics.onProposal(RaftMetrics.ProposalResult.DROPPED);
            throw RaftException.ErrProposalDropped;
        }
        Eraftpb.Message msg = Eraftpb.Message.newBuilder()
                .setMsgType(Eraftpb.MessageType.MsgPropose)
                .addEntries(Eraftpb.Entry.newBuilder()
                        .setData(data != null ? ByteString.copyFrom(data) : ByteString.EMPTY))
                .build();
        CompletableFuture<Void> result = new CompletableFuture<>();
        submitWithStoppedExceptional(new ProposeEvent(msg, result), result);
        try {
            result.get();
        } catch (InterruptedException e) {
            // Preserve the caller's interrupt status so loops/cancellation
            // upstream can react. Method already declares throws InterruptedException.
            Thread.currentThread().interrupt();
            throw e;
        } catch (ExecutionException e) {
            // Unwrap the cause: dispatch() completes exceptionally with the
            // RaftException it caught from r.step(), or with a RuntimeException
            // that took the loop down. Preserve the cause so callers don't
            // lose stack context.
            Throwable cause = e.getCause();
            if (cause instanceof RaftException re) throw re;
            if (cause instanceof RuntimeException rte) throw rte;
            throw new RaftException(RaftException.Code.STOPPED,
                    "propose failed: " + (cause != null ? cause.getMessage() : "unknown"), cause);
        }
    }

    @Override
    public void proposeConfChange(Eraftpb.ConfChangeV2 cc) throws InterruptedException, RaftException {
        Eraftpb.Message m = io.github.xinfra.lab.raft.internal.confchange.Changer.toMessage(cc);
        step(m);
    }

    @Override
    public void step(Eraftpb.Message msg) throws InterruptedException, RaftException {
        if (done) throw RaftException.ErrStopped;
        // Ignore unexpected local messages received over network.
        if (Util.isLocalMsg(msg.getMsgType()) && !Util.isLocalMsgTarget(msg.getFrom())) {
            return;
        }
        // Forwarded MsgPropose goes through the ProposeEvent path (no result future
        // since step() is fire-and-forget); others as RecvEvent.
        if (msg.getMsgType() == Eraftpb.MessageType.MsgPropose) {
            if (proposalsDisabled) throw RaftException.ErrProposalDropped;
            events.put(new ProposeEvent(msg, null));
        } else {
            events.put(new RecvEvent(msg));
        }
    }

    @Override
    public Ready ready() throws InterruptedException {
        return readyc.take();
    }

    @Override
    public void advance() throws InterruptedException {
        if (done) return;
        events.put(AdvanceEvent.INSTANCE);
    }

    @Override
    public Eraftpb.ConfState applyConfChange(Eraftpb.ConfChangeV2 cc) throws InterruptedException {
        if (done) return Eraftpb.ConfState.getDefaultInstance();
        CompletableFuture<Eraftpb.ConfState> reply = new CompletableFuture<>();
        submitWithResult(new ConfChangeEvent(cc, reply), reply, Eraftpb.ConfState.getDefaultInstance());
        try {
            return reply.get();
        } catch (ExecutionException e) {
            // Preserve the cause so the failure is debuggable. Returning
            // default-instance keeps the API forgiving (callers' apply path
            // doesn't crash), but the cause is no longer silently swallowed.
            rn.raft.logger.error("applyConfChange failed: {}", e.getCause() != null ? e.getCause() : e);
            return Eraftpb.ConfState.getDefaultInstance();
        }
    }

    @Override
    public void transferLeadership(long lead, long transferee) throws InterruptedException {
        if (done) return;
        events.put(new RecvEvent(Eraftpb.Message.newBuilder()
                .setMsgType(Eraftpb.MessageType.MsgTransferLeader)
                .setFrom(transferee)
                .setTo(lead)
                .build()));
    }

    @Override
    public void forgetLeader() throws InterruptedException, RaftException {
        if (done) throw RaftException.ErrStopped;
        events.put(new RecvEvent(Eraftpb.Message.newBuilder()
                .setMsgType(Eraftpb.MessageType.MsgForgetLeader)
                .build()));
    }

    @Override
    public void readIndex(byte[] rctx) throws InterruptedException, RaftException {
        if (done) throw RaftException.ErrStopped;
        events.put(new RecvEvent(Eraftpb.Message.newBuilder()
                .setMsgType(Eraftpb.MessageType.MsgReadIndex)
                .addEntries(Eraftpb.Entry.newBuilder()
                        .setData(rctx != null ? ByteString.copyFrom(rctx) : ByteString.EMPTY))
                .build()));
    }

    @Override
    public Status status() throws InterruptedException {
        if (done) return new Status();
        CompletableFuture<Status> future = new CompletableFuture<>();
        submitWithResult(new StatusEvent(future), future, new Status());
        try {
            return future.get();
        } catch (ExecutionException e) {
            rn.raft.logger.error("status() failed: {}", e.getCause() != null ? e.getCause() : e);
            return new Status();
        }
    }

    @Override
    public void reportUnreachable(long id) {
        if (done) return;
        rn.raft.metrics.onPeerUnreachable(id);
        // Block until accepted (matches etcd-raft's blocking recvc send).
        // Reliable delivery: dropping these would let the leader keep streaming
        // to a known-unreachable follower until heartbeat timeout.
        try {
            events.put(new RecvEvent(Eraftpb.Message.newBuilder()
                    .setMsgType(Eraftpb.MessageType.MsgUnreachable)
                    .setFrom(id)
                    .build()));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void reportSnapshot(long id, SnapshotStatus status) {
        if (done) return;
        boolean rej = status == SnapshotStatus.SnapshotFailure;
        // Block until accepted; dropping a SnapshotFailure would leave the
        // leader stuck in StateSnapshot for that follower.
        try {
            events.put(new RecvEvent(Eraftpb.Message.newBuilder()
                    .setMsgType(Eraftpb.MessageType.MsgSnapStatus)
                    .setFrom(id)
                    .setReject(rej)
                    .build()));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void stop() throws InterruptedException {
        signalStop();
        synchronized (stopSignal) {
            while (!done) {
                stopSignal.wait();
            }
        }
    }

    @Override
    public boolean stop(long timeout, TimeUnit unit) throws InterruptedException {
        signalStop();
        long deadlineNanos = System.nanoTime() + unit.toNanos(timeout);
        synchronized (stopSignal) {
            while (!done) {
                long remaining = deadlineNanos - System.nanoTime();
                if (remaining <= 0) {
                    rn.raft.logger.warn(
                            "{:x} stop() timed out after {} {} — event loop may still be running",
                            rn.raft.id, timeout, unit.name().toLowerCase());
                    return false;
                }
                long ms = remaining / 1_000_000L;
                int ns = (int) (remaining % 1_000_000L);
                stopSignal.wait(ms, ns);
            }
        }
        return true;
    }

    private void signalStop() {
        if (stopped.compareAndSet(false, true)) {
            if (eventLoop != null) {
                eventLoop.interrupt();
            }
        }
    }

    @Override
    public BasicStatus basicStatus() {
        return new BasicStatus(
                rn.raft.id,
                liveTerm.get(),
                liveLead.get(),
                liveCommit.get(),
                liveApplied.get(),
                liveLastIndex.get(),
                liveState);
    }

    @Override
    public Runnable registerLeaderObserver(LeaderObserver observer) {
        if (observer == null) {
            throw new IllegalArgumentException("observer must not be null");
        }
        Object key = new Object();
        leaderObservers.put(key, observer);
        return () -> leaderObservers.remove(key);
    }

    private void notifyLeaderChange(long newLead, long term) {
        if (leaderObservers.isEmpty()) return;
        for (LeaderObserver obs : leaderObservers.values()) {
            try {
                obs.onLeaderChange(newLead, term);
            } catch (Throwable t) {
                // Never let an observer take down the event loop.
                rn.raft.logger.error("{:x} leader observer threw: {}", rn.raft.id, t);
            }
        }
    }

    // ---- Event types ----

    sealed interface Event permits TickEvent, AdvanceEvent, ProposeEvent, RecvEvent, ConfChangeEvent, StatusEvent {}

    static final class TickEvent implements Event {
        static final TickEvent INSTANCE = new TickEvent();
        private TickEvent() {}
    }

    static final class AdvanceEvent implements Event {
        static final AdvanceEvent INSTANCE = new AdvanceEvent();
        private AdvanceEvent() {}
    }

    static final class ProposeEvent implements Event {
        final Eraftpb.Message msg;
        /** null when caller doesn't wait; otherwise completed (or completed-exceptionally) by dispatch. */
        final CompletableFuture<Void> result;
        ProposeEvent(Eraftpb.Message msg, CompletableFuture<Void> result) {
            this.msg = msg;
            this.result = result;
        }
    }

    static final class RecvEvent implements Event {
        final Eraftpb.Message msg;
        RecvEvent(Eraftpb.Message msg) { this.msg = msg; }
    }

    static final class ConfChangeEvent implements Event {
        final Eraftpb.ConfChangeV2 cc;
        final CompletableFuture<Eraftpb.ConfState> reply;
        ConfChangeEvent(Eraftpb.ConfChangeV2 cc, CompletableFuture<Eraftpb.ConfState> reply) {
            this.cc = cc;
            this.reply = reply;
        }
    }

    static final class StatusEvent implements Event {
        final CompletableFuture<Status> result;
        StatusEvent(CompletableFuture<Status> result) { this.result = result; }
    }
}
