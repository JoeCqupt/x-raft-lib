/*
 * Copyright 2024-2026 The x-raft-lib Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * SPDX-License-Identifier: Apache-2.0
 */
package io.github.xinfra.lab.raft.fuzz;

import com.code_intelligence.jazzer.junit.FuzzTest;
import com.google.protobuf.InvalidProtocolBufferException;
import io.github.xinfra.lab.raft.proto.Eraftpb;

/**
 * Coverage-guided fuzzing of the protobuf parse boundary for every
 * wire-format message a peer can hand the local node. The intent is to
 * prove that no malicious / corrupted peer payload can take the parse
 * path down via an unchecked exception or a {@code RuntimeException} —
 * which would crash the gRPC handler thread in production and leave
 * the node in an undefined state.
 *
 * <p><b>Default run.</b> Under {@code mvn test}, {@code @FuzzTest}
 * executes once per harness with the seed corpus only — a fast smoke
 * test that catches regressions on inputs we already know about.
 *
 * <p><b>Fuzz session.</b> Under
 * {@code mvn -P fuzz test -Dtest=EraftpbParseFuzzTest} (CI nightly),
 * Jazzer takes over and explores the input space with coverage
 * feedback; any uncaught exception or infinite loop fails the build.
 *
 * <p>Seed corpus lives under
 * {@code src/test/resources/io/github/xinfra/lab/raft/fuzz/EraftpbParseFuzzTest/}
 * — one byte file per starting input.
 */
class EraftpbParseFuzzTest {

    /**
     * The full {@link Eraftpb.Message} surface: {@code parseFrom} is the
     * entry point every transport hits before raft sees the bytes. The
     * only acceptable failure is the documented
     * {@link InvalidProtocolBufferException}; anything else (NPE,
     * IllegalArgumentException, OOM-via-allocation-bomb, infinite loop)
     * is a finding.
     */
    @FuzzTest
    void messageParse(byte[] data) {
        if (data == null) return;
        try {
            Eraftpb.Message msg = Eraftpb.Message.parseFrom(data);
            // Force lazy fields (entries, snapshot data) to materialise so
            // the fuzzer also covers ByteString → concrete-type conversions.
            msg.getSerializedSize();
            msg.toString();
        } catch (InvalidProtocolBufferException expected) {
            // Documented contract for malformed bytes.
        }
    }

    @FuzzTest
    void hardStateParse(byte[] data) {
        if (data == null) return;
        try {
            Eraftpb.HardState.parseFrom(data).getSerializedSize();
        } catch (InvalidProtocolBufferException expected) {
            // ok
        }
    }

    @FuzzTest
    void snapshotParse(byte[] data) {
        if (data == null) return;
        try {
            Eraftpb.Snapshot.parseFrom(data).getSerializedSize();
        } catch (InvalidProtocolBufferException expected) {
            // ok
        }
    }

    @FuzzTest
    void confChangeV2Parse(byte[] data) {
        if (data == null) return;
        try {
            Eraftpb.ConfChangeV2.parseFrom(data).getSerializedSize();
        } catch (InvalidProtocolBufferException expected) {
            // ok
        }
    }
}
