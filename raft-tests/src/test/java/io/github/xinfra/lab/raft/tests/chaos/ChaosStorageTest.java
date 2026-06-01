/*
 * Copyright 2024-2026 The x-raft-lib Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * SPDX-License-Identifier: Apache-2.0
 */
package io.github.xinfra.lab.raft.tests.chaos;

import io.github.xinfra.lab.raft.MemoryStorage;
import io.github.xinfra.lab.raft.RaftInvariantException;
import io.github.xinfra.lab.raft.proto.Eraftpb;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Direct unit tests for {@link ChaosStorage}'s three fault modes against a
 * {@link MemoryStorage} delegate. Verifies each mode produces the exact
 * exception/state Storage's contract demands so an integration test using
 * this wrapper can rely on the failure shape.
 */
class ChaosStorageTest {

    @Test
    void diskFullThrowsStorageIoOnEveryWrite() {
        MemoryStorage inner = new MemoryStorage();
        ChaosStorage chaos = new ChaosStorage(inner);
        chaos.setMode(ChaosStorage.Mode.DISK_FULL);

        List<Eraftpb.Entry> entries = List.of(Eraftpb.Entry.newBuilder()
                .setIndex(1).setTerm(1).build());

        assertThatThrownBy(() -> chaos.append(entries))
                .isInstanceOf(RaftInvariantException.class)
                .satisfies(e -> assertThat(((RaftInvariantException) e).category())
                        .isEqualTo(RaftInvariantException.Category.STORAGE_IO));

        assertThatThrownBy(() -> chaos.setHardState(
                Eraftpb.HardState.newBuilder().setTerm(1).build()))
                .isInstanceOf(RaftInvariantException.class);

        // Read paths still work — the contract is "writes fail, reads OK".
        assertThat(chaos.lastIndex()).isZero();
        assertThat(chaos.initialState()).isNotNull();
        assertThat(chaos.injectedFailureCount()).isEqualTo(2);
    }

    @Test
    void fsyncFailSilentlyDropsButReturnsNormally() throws Exception {
        MemoryStorage inner = new MemoryStorage();
        ChaosStorage chaos = new ChaosStorage(inner);

        // Healthy: write goes through.
        chaos.append(List.of(Eraftpb.Entry.newBuilder().setIndex(1).setTerm(1).build()));
        assertThat(chaos.lastIndex()).isEqualTo(1);

        // Switch to fsync-fail: the call returns normally but the entry
        // never lands in the delegate — exactly the silent-loss scenario
        // raft's host must detect via its own ack-after-fsync contract.
        chaos.setMode(ChaosStorage.Mode.FSYNC_FAIL);
        chaos.append(List.of(Eraftpb.Entry.newBuilder().setIndex(2).setTerm(1).build()));
        assertThat(chaos.lastIndex()).as("entry 2 must have been silently dropped").isEqualTo(1);
        assertThat(chaos.injectedFailureCount()).isEqualTo(1);
    }

    @Test
    void partialWriteKeepsOnlyTheFirstN() throws Exception {
        MemoryStorage inner = new MemoryStorage();
        ChaosStorage chaos = new ChaosStorage(inner);
        chaos.setMode(ChaosStorage.Mode.PARTIAL_WRITE);
        chaos.setPartialWriteLimit(2);

        // Submit 5; only 2 persist.
        List<Eraftpb.Entry> batch = List.of(
                Eraftpb.Entry.newBuilder().setIndex(1).setTerm(1).build(),
                Eraftpb.Entry.newBuilder().setIndex(2).setTerm(1).build(),
                Eraftpb.Entry.newBuilder().setIndex(3).setTerm(1).build(),
                Eraftpb.Entry.newBuilder().setIndex(4).setTerm(1).build(),
                Eraftpb.Entry.newBuilder().setIndex(5).setTerm(1).build());
        chaos.append(batch);
        assertThat(chaos.lastIndex()).isEqualTo(2);
        assertThat(chaos.injectedFailureCount()).isEqualTo(1);

        // Below the limit: no injection.
        chaos.setPartialWriteLimit(10);
        chaos.append(List.of(Eraftpb.Entry.newBuilder().setIndex(3).setTerm(1).build()));
        assertThat(chaos.lastIndex()).isEqualTo(3);
        assertThat(chaos.injectedFailureCount()).isEqualTo(1);
    }

    @Test
    void healthyModeIsPureDelegation() throws Exception {
        MemoryStorage inner = new MemoryStorage();
        ChaosStorage chaos = new ChaosStorage(inner);
        chaos.append(List.of(Eraftpb.Entry.newBuilder().setIndex(1).setTerm(1).build()));
        chaos.setHardState(Eraftpb.HardState.newBuilder().setTerm(3).setVote(1).setCommit(1).build());

        assertThat(chaos.lastIndex()).isEqualTo(1);
        assertThat(chaos.initialState().hardState().getTerm()).isEqualTo(3);
        assertThat(chaos.injectedFailureCount()).isZero();
    }
}
