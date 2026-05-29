/*
 * Copyright 2024-2026 The x-raft-lib Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * SPDX-License-Identifier: Apache-2.0
 */
package io.github.xinfra.lab.raft;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class RaftInvariantExceptionTest {

    @Test
    void legacyConstructorsDefaultToBug() {
        RaftInvariantException e1 = new RaftInvariantException("boom");
        RaftInvariantException e2 = new RaftInvariantException("boom", new RuntimeException());
        assertThat(e1.category()).isEqualTo(RaftInvariantException.Category.BUG);
        assertThat(e2.category()).isEqualTo(RaftInvariantException.Category.BUG);
    }

    @Test
    void categoryConstructorsCarryCategory() {
        RaftInvariantException io = new RaftInvariantException(
                RaftInvariantException.Category.STORAGE_IO, "disk gone");
        RaftInvariantException corrupt = new RaftInvariantException(
                RaftInvariantException.Category.DATA_CORRUPTION, "bad bytes", new RuntimeException());
        assertThat(io.category()).isEqualTo(RaftInvariantException.Category.STORAGE_IO);
        assertThat(corrupt.category()).isEqualTo(RaftInvariantException.Category.DATA_CORRUPTION);
    }

    @Test
    void nullCategoryFallsBackToBug() {
        RaftInvariantException e = new RaftInvariantException(null, "x");
        assertThat(e.category()).isEqualTo(RaftInvariantException.Category.BUG);
    }

    @Test
    void messageIsPrefixedWithCategoryForTriage() {
        RaftInvariantException e = new RaftInvariantException(
                RaftInvariantException.Category.STORAGE_IO, "fsync failed");
        assertThat(e.getMessage()).isEqualTo("[STORAGE_IO] fsync failed");
    }
}
