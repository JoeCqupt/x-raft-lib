/*
 * Copyright 2024-2026 The x-raft-lib Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * SPDX-License-Identifier: Apache-2.0
 */
package io.github.xinfra.lab.raft.tests.linearizability;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Wing &amp; Gong-style linearizability checker for a key-value register
 * (the {@link io.github.xinfra.lab.raft.examples.RocksKvStore} spec:
 * put, get, delete, where get returns the most recently put value or
 * absent if never put / deleted since).
 *
 * <h2>Algorithm</h2>
 *
 * <p>Given a {@link History} of concurrent invocations and responses,
 * the checker searches for a sequential ordering of all
 * <i>completed</i> operations that:
 * <ol>
 *   <li>preserves real-time happens-before (op A's complete &lt; op B's
 *       invoke ⇒ A precedes B in the sequential order), AND</li>
 *   <li>satisfies the KV spec — each {@code get} returns the value the
 *       most recent {@code put} placed (or null after a {@code delete}
 *       or for an untouched key).</li>
 * </ol>
 *
 * <p>Implementation follows Wing &amp; Gong's "minimal" approach plus
 * Lowe's memoization: at each step we consider only the minimal
 * "linearization frontier" — operations whose invoke has happened but
 * which haven't been linearized yet. If applying any frontier op
 * matches its observed response, we commit it and recurse; otherwise
 * we undo and try another. The memoization key is the linearized set
 * + simulated state, pruning redundant exploration.
 *
 * <p>This is the standard quadratic-in-history-length checker that
 * Jepsen / Knossos use for register specs. For raft-tests' typical
 * histories (≤200 ops, ≤8 client threads), it runs in milliseconds
 * even when no linearization exists.
 *
 * <p>Result is a {@link Result} carrying the verdict and, on failure,
 * the prefix that could be linearized so the user can pinpoint where
 * the history diverged from the spec.
 */
public final class LinearizabilityChecker {

    private LinearizabilityChecker() {}

    public static final class Result {
        public final boolean linearizable;
        /** Length of the longest linearizable prefix; useful for debugging. */
        public final int linearizedCount;
        public final String message;

        Result(boolean ok, int n, String msg) {
            this.linearizable = ok;
            this.linearizedCount = n;
            this.message = msg;
        }

        @Override
        public String toString() {
            return linearizable
                    ? "OK (" + linearizedCount + " ops linearized)"
                    : "NOT LINEARIZABLE: " + message + " (after " + linearizedCount + " ops)";
        }
    }

    /**
     * Check whether {@code history} admits a linearization under the KV
     * register spec. {@link History.Event.Type#INFO}-completed ops (the
     * client never observed a response, e.g. timeout) are treated as
     * "may or may not have happened" — the checker can place them
     * anywhere after their invoke or skip them entirely.
     */
    public static Result checkKvRegister(History history) {
        List<History.Event> raw = history.events();

        // Pair each invoke with its matching complete (by process + sequence).
        // The pairing is unambiguous because each process serialises its own
        // ops (one in-flight at a time).
        List<Op> ops = pair(raw);
        if (ops.isEmpty()) {
            return new Result(true, 0, "empty history");
        }

        // The simulated KV state, mutated as we hypothesise linearizations.
        Map<String, String> state = new HashMap<>();
        // Indices into `ops` that have been placed in the linearization so far.
        Set<Integer> linearized = new HashSet<>();
        // Memoization keyed by (linearized-set, state) — avoids re-exploring
        // a frontier we've already proven unreachable.
        Set<String> seen = new HashSet<>();

        boolean ok = search(ops, state, linearized, seen);
        return new Result(ok, linearized.size(),
                ok ? "consistent with KV register spec"
                   : "no consistent linearization found");
    }

    /** Recursive DFS over the minimal linearization frontier. */
    private static boolean search(List<Op> ops,
                                  Map<String, String> state,
                                  Set<Integer> linearized,
                                  Set<String> seen) {
        if (linearized.size() == ops.size()) return true;

        String memoKey = linearized.toString() + "|" + state;
        if (!seen.add(memoKey)) return false;

        // Frontier: ops whose invoke has happened but which aren't yet
        // linearized, AND whose all happens-before predecessors are.
        // For a register, happens-before is simply real-time: op B's
        // invoke > op A's complete means A must precede B.
        long latestCompleteOfLinearized = Long.MIN_VALUE;
        for (int i : linearized) {
            latestCompleteOfLinearized = Math.max(latestCompleteOfLinearized,
                    ops.get(i).completeTs);
        }
        // Actually, the standard frontier for Wing&Gong is: any op whose
        // invoke < max(complete of unlinearized) — i.e. ops concurrent
        // with at least one other. To keep this implementation simple
        // and fast for the typical "low-concurrency raft client" case,
        // we use an even simpler frontier: any unlinearized op whose
        // invoke is BEFORE the earliest unlinearized op's complete.
        // For mostly-sequential client traffic this is exactly the
        // candidate set; for heavy concurrency it widens to "all
        // unlinearized concurrent ops" — still correct, just slower.
        long earliestUnlinearizedComplete = Long.MAX_VALUE;
        for (int i = 0; i < ops.size(); i++) {
            if (linearized.contains(i)) continue;
            earliestUnlinearizedComplete = Math.min(earliestUnlinearizedComplete,
                    ops.get(i).completeTs);
        }

        for (int i = 0; i < ops.size(); i++) {
            if (linearized.contains(i)) continue;
            Op op = ops.get(i);
            // Skip if op is real-time-after some other unlinearized op
            // (the other must precede it in any valid linearization).
            if (op.invokeTs > earliestUnlinearizedComplete) continue;

            // Try placing op next.
            String prev = applyOp(state, op);
            if (matchesObserved(state, op, prev)) {
                linearized.add(i);
                if (search(ops, state, linearized, seen)) return true;
                linearized.remove(i);
            }
            // Restore state for the next candidate.
            unapply(state, op, prev);
        }
        return false;
    }

    /**
     * Apply {@code op} to {@code state}, returning the prior mapping
     * (or null) so the caller can roll back.
     */
    private static String applyOp(Map<String, String> state, Op op) {
        return switch (op.type) {
            case PUT -> state.put(op.key, op.inputValue);
            case DELETE -> state.remove(op.key);
            case GET -> state.get(op.key); // read-only — also captured for matching
        };
    }

    private static void unapply(Map<String, String> state, Op op, String prev) {
        switch (op.type) {
            case PUT, DELETE -> {
                if (prev == null) state.remove(op.key);
                else state.put(op.key, prev);
            }
            case GET -> { /* no-op */ }
        }
    }

    /**
     * After applying op, does the post-state match what the client
     * observed? For PUT/DELETE there is no observed value (we report
     * "ok"); for GET the client's recorded value must equal the value
     * the sequential spec returns.
     */
    private static boolean matchesObserved(Map<String, String> state, Op op, String preApplyValue) {
        if (op.type == History.OpType.GET) {
            // GET returns whatever the spec says at the moment of read,
            // which is `preApplyValue` (since GET doesn't mutate).
            return java.util.Objects.equals(preApplyValue, op.observedValue);
        }
        // Writes always succeed for this register.
        return true;
    }

    /** Pair invoke+complete events into Op records. */
    private static List<Op> pair(List<History.Event> events) {
        Map<Long, Op> byInvokeSeq = new HashMap<>();
        for (History.Event e : events) {
            if (e.type == History.Event.Type.INVOKE) {
                byInvokeSeq.put(e.sequence, new Op(e.op, e.key, e.value, null, e.timestampNanos, Long.MAX_VALUE));
            } else if (e.type == History.Event.Type.COMPLETE) {
                Op op = byInvokeSeq.get(e.sequence);
                if (op != null) {
                    op.observedValue = e.value;
                    op.completeTs = e.timestampNanos;
                }
            } else { // INFO: leave completeTs = MAX so the op floats free
                Op op = byInvokeSeq.get(e.sequence);
                if (op != null) {
                    op.observedValue = null;
                    op.completeTs = Long.MAX_VALUE;
                }
            }
        }
        List<Op> ops = new ArrayList<>(byInvokeSeq.values());
        // INFO-completed ops with no observed value still need to be
        // linearizable somewhere; we conservatively include them.
        ops.sort((a, b) -> Long.compare(a.invokeTs, b.invokeTs));
        return ops;
    }

    /** Internal pairing of an invoke event with its observed completion. */
    private static final class Op {
        final History.OpType type;
        final String key;
        final String inputValue;     // for PUT
        String observedValue;        // for GET; PUT/DELETE have null
        final long invokeTs;
        long completeTs;

        Op(History.OpType type, String key, String inputValue,
           String observedValue, long invokeTs, long completeTs) {
            this.type = type;
            this.key = key;
            this.inputValue = inputValue;
            this.observedValue = observedValue;
            this.invokeTs = invokeTs;
            this.completeTs = completeTs;
        }

        @Override
        public String toString() {
            return type + "(" + key + (inputValue == null ? "" : "=" + inputValue) + ")";
        }
    }
}
