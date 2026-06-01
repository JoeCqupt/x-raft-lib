/*
 * Copyright 2024-2026 The x-raft-lib Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * SPDX-License-Identifier: Apache-2.0
 */
package io.github.xinfra.lab.raft.tests.linearizability;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Append-only event log used to feed {@link LinearizabilityChecker}.
 *
 * <p>Each client thread (a "process") emits two events per operation:
 * an {@link Event.Type#INVOKE} when it dispatches and an
 * {@link Event.Type#COMPLETE} (or {@link Event.Type#INFO}) when it
 * observes the response. A history is consistent if there is some
 * total order of completed operations such that each operation
 * appears between its invoke and complete events AND each operation's
 * response is what the sequential spec would have produced.
 *
 * <p>{@code INFO} (timeout / cancellation) leaves the operation
 * "pending" — the checker may place it anywhere after its invoke,
 * including never having executed, because the system is allowed to
 * have applied it without the client observing the response.
 *
 * <p>Thread-safe: multiple client threads append concurrently via
 * {@link #record}. Reads ({@link #events}) must only happen once
 * recording is finished.
 */
public final class History {

    public enum OpType { PUT, GET, DELETE }

    public static final class Event {
        public enum Type { INVOKE, COMPLETE, INFO }

        public final long process;
        public final long sequence;
        public final long timestampNanos;
        public final Type type;
        public final OpType op;
        public final String key;
        /** Value for PUT (input) or GET (observed output); null for DELETE. */
        public final String value;

        Event(long process, long sequence, long ts, Type type,
              OpType op, String key, String value) {
            this.process = process;
            this.sequence = sequence;
            this.timestampNanos = ts;
            this.type = type;
            this.op = op;
            this.key = key;
            this.value = value;
        }

        @Override
        public String toString() {
            return String.format("[p=%d seq=%d %s %s key=%s val=%s]",
                    process, sequence, type, op, key, value);
        }
    }

    private final List<Event> events =
            Collections.synchronizedList(new ArrayList<>());
    private final AtomicLong invokeSequence = new AtomicLong();

    /**
     * Record an INVOKE event and return the operation's sequence id. Pass
     * the same id back to {@link #complete}/{@link #info} so the checker
     * can pair the response with this invocation.
     */
    public long invoke(long process, OpType op, String key, String value) {
        long seq = invokeSequence.incrementAndGet();
        events.add(new Event(process, seq, System.nanoTime(),
                Event.Type.INVOKE, op, key, value));
        return seq;
    }

    /** Record a COMPLETE event for the invocation with id {@code invokeSeq}. */
    public void complete(long invokeSeq, long process, OpType op, String key, String value) {
        events.add(new Event(process, invokeSeq, System.nanoTime(),
                Event.Type.COMPLETE, op, key, value));
    }

    /**
     * Record an INFO completion: the client lost track of whether the
     * operation succeeded (timeout / cancellation). The checker treats
     * INFO ops as "may have happened anywhere after invoke or not at all".
     */
    public void info(long invokeSeq, long process, OpType op, String key) {
        events.add(new Event(process, invokeSeq, System.nanoTime(),
                Event.Type.INFO, op, key, null));
    }

    /** Snapshot of all events recorded so far, in the order they were appended. */
    public List<Event> events() {
        synchronized (events) {
            return new ArrayList<>(events);
        }
    }

    public int size() {
        return events.size();
    }
}
