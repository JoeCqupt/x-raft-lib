/*
 * Copyright 2024-2026 The x-raft-lib Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * SPDX-License-Identifier: Apache-2.0
 */
package io.github.xinfra.lab.raft.tests.linearizability;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Self-tests for {@link LinearizabilityChecker}: hand-crafted histories
 * we know are linearizable and others we know are not, so the checker's
 * verdict can be trusted when fed real cluster histories.
 */
class LinearizabilityCheckerTest {

    @Test
    void emptyHistoryIsTriviallyLinearizable() {
        History h = new History();
        assertThat(LinearizabilityChecker.checkKvRegister(h).linearizable).isTrue();
    }

    /** Single put followed by a get — must see the value. */
    @Test
    void singleProcessSequentialPutGetLinearizable() {
        History h = new History();
        long s1 = h.invoke(1, History.OpType.PUT, "k", "v1");
        h.complete(s1, 1, History.OpType.PUT, "k", "v1");
        long s2 = h.invoke(1, History.OpType.GET, "k", null);
        h.complete(s2, 1, History.OpType.GET, "k", "v1");

        LinearizabilityChecker.Result r = LinearizabilityChecker.checkKvRegister(h);
        assertThat(r.linearizable).as("%s", r).isTrue();
        assertThat(r.linearizedCount).isEqualTo(2);
        // Sanity: pairing matched both invokes to their completes.
        assertThat(s1).isNotEqualTo(s2);
    }

    /** Concurrent PUTs from two processes; GET observes one of them. */
    @Test
    void concurrentPutsLinearizableEitherWay() {
        History h = new History();
        // P1 puts v1 concurrently with P2 puts v2; P3 reads "v2"
        // afterwards. Linearizable: v1, v2, get="v2".
        long s1 = h.invoke(1, History.OpType.PUT, "k", "v1");
        long s2 = h.invoke(2, History.OpType.PUT, "k", "v2");
        h.complete(s1, 1, History.OpType.PUT, "k", "v1");
        h.complete(s2, 2, History.OpType.PUT, "k", "v2");
        long s3 = h.invoke(3, History.OpType.GET, "k", null);
        h.complete(s3, 3, History.OpType.GET, "k", "v2");

        assertThat(LinearizabilityChecker.checkKvRegister(h).linearizable).isTrue();
    }

    /**
     * Stale read: P1 puts v1 and waits for the ack, then P2 reads and
     * sees null. Not linearizable for a register — the read happened
     * strictly after the put completed, so it must see v1.
     */
    @Test
    void staleReadAfterWriteCompleteIsNotLinearizable() {
        History h = new History();
        long s1 = h.invoke(1, History.OpType.PUT, "k", "v1");
        h.complete(s1, 1, History.OpType.PUT, "k", "v1");
        long s2 = h.invoke(2, History.OpType.GET, "k", null);
        h.complete(s2, 2, History.OpType.GET, "k", null); // saw nothing — VIOLATION

        LinearizabilityChecker.Result r = LinearizabilityChecker.checkKvRegister(h);
        assertThat(r.linearizable).as("%s", r).isFalse();
    }

    /** GET returns a value never written — clear violation. */
    @Test
    void getReturningFabricatedValueIsNotLinearizable() {
        History h = new History();
        long s1 = h.invoke(1, History.OpType.PUT, "k", "v1");
        h.complete(s1, 1, History.OpType.PUT, "k", "v1");
        long s2 = h.invoke(1, History.OpType.GET, "k", null);
        h.complete(s2, 1, History.OpType.GET, "k", "ZZZ");

        LinearizabilityChecker.Result r = LinearizabilityChecker.checkKvRegister(h);
        assertThat(r.linearizable).as("%s", r).isFalse();
    }

    /** DELETE wipes the key — subsequent reads return null. */
    @Test
    void putThenDeleteThenGetIsLinearizable() {
        History h = new History();
        long s1 = h.invoke(1, History.OpType.PUT, "k", "v1");
        h.complete(s1, 1, History.OpType.PUT, "k", "v1");
        long s2 = h.invoke(1, History.OpType.DELETE, "k", null);
        h.complete(s2, 1, History.OpType.DELETE, "k", null);
        long s3 = h.invoke(1, History.OpType.GET, "k", null);
        h.complete(s3, 1, History.OpType.GET, "k", null);

        LinearizabilityChecker.Result r = LinearizabilityChecker.checkKvRegister(h);
        assertThat(r.linearizable).as("%s", r).isTrue();
    }
}
