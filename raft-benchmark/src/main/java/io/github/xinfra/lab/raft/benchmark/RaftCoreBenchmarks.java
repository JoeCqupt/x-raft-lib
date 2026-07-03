/*
 * Copyright 2024-2026 The x-raft-lib Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * SPDX-License-Identifier: Apache-2.0
 */
package io.github.xinfra.lab.raft.benchmark;

import io.github.xinfra.lab.raft.Config;
import io.github.xinfra.lab.raft.MemoryStorage;
import io.github.xinfra.lab.raft.RaftException;
import io.github.xinfra.lab.raft.RawNode;
import io.github.xinfra.lab.raft.Ready;
import io.github.xinfra.lab.raft.internal.quorum.MajorityConfig;
import io.github.xinfra.lab.raft.proto.Eraftpb;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.Arrays;
import java.util.HashSet;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 3)
@Fork(value = 1)
@State(Scope.Benchmark)
public class RaftCoreBenchmarks {

    @Param({"3", "5"})
    public int voters;

    @Param({"0", "128", "1024", "4096", "65536"})
    public int payloadBytes;

    private RawNode rn;
    private MemoryStorage storage;
    private byte[] payload;

    @Setup(Level.Iteration)
    public void setUp() throws RaftException {
        storage = new MemoryStorage();
        Eraftpb.SnapshotMetadata.Builder mb = storage.getSnapshot().getMetadata().toBuilder();
        Eraftpb.ConfState.Builder cb = mb.getConfState().toBuilder();
        for (long i = 1; i <= voters; i++) cb.addVoters(i);
        mb.setConfState(cb);
        storage.setSnapshot(storage.getSnapshot().toBuilder().setMetadata(mb).build());

        Config cfg = Config.builder()
                .id(1)
                .electionTick(10)
                .heartbeatTick(1)
                .storage(storage)
                .maxSizePerMsg(Long.MAX_VALUE)
                .maxInflightMsgs(1024)
                .build();

        rn = RawNode.newRawNode(cfg);
        rn.campaign();
        while (rn.hasReady()) {
            Ready rd = rn.ready();
            if (!rd.entries().isEmpty()) storage.append(rd.entries());
            rn.advance(rd);
        }

        payload = new byte[payloadBytes];
        Arrays.fill(payload, (byte) 'a');
    }

    @TearDown(Level.Iteration)
    public void tearDown() {
        rn = null;
        storage = null;
    }

    @Benchmark
    public Ready proposeAndDrain() throws RaftException {
        rn.propose(payload);
        Ready rd = rn.ready();
        if (!rd.entries().isEmpty()) storage.append(rd.entries());
        rn.advance(rd);
        return rd;
    }

    @Benchmark
    public void leaderHandleAppendResponse() throws RaftException {
        long lastIndex = rn.raft.raftLog().lastIndex();
        for (long peerId = 2; peerId <= voters; peerId++) {
            rn.step(Eraftpb.Message.newBuilder()
                    .setMsgType(Eraftpb.MessageType.MsgAppendResponse)
                    .setFrom(peerId)
                    .setTo(1)
                    .setTerm(rn.raft.term())
                    .setIndex(lastIndex)
                    .build());
        }
    }

    // ============= Ready assembly under load =============

    @State(Scope.Benchmark)
    public static class ReadyLoadState {
        @Param({"3", "5"})
        public int voters;

        private RawNode rn;
        private MemoryStorage storage;
        private byte[] payload;

        @Setup(Level.Invocation)
        public void setUp() throws RaftException {
            storage = new MemoryStorage();
            Eraftpb.SnapshotMetadata.Builder mb = storage.getSnapshot().getMetadata().toBuilder();
            Eraftpb.ConfState.Builder cb = mb.getConfState().toBuilder();
            for (long i = 1; i <= voters; i++) cb.addVoters(i);
            mb.setConfState(cb);
            storage.setSnapshot(storage.getSnapshot().toBuilder().setMetadata(mb).build());

            Config cfg = Config.builder()
                    .id(1)
                    .electionTick(10)
                    .heartbeatTick(1)
                    .storage(storage)
                    .maxSizePerMsg(Long.MAX_VALUE)
                    .maxInflightMsgs(4096)
                    .build();

            rn = RawNode.newRawNode(cfg);
            rn.campaign();
            while (rn.hasReady()) {
                Ready rd = rn.ready();
                if (!rd.entries().isEmpty()) storage.append(rd.entries());
                rn.advance(rd);
            }

            payload = new byte[32 * 1024];
            Arrays.fill(payload, (byte) 'x');
            for (int i = 0; i < 100; i++) {
                rn.propose(payload);
            }
        }
    }

    @Benchmark
    public Ready readyUnderLoad(ReadyLoadState st) {
        return st.rn.readyWithoutAccept();
    }

    // ============= Quorum committedIndex =============

    @State(Scope.Benchmark)
    public static class CommittedIndexState {
        @Param({"1", "3", "5", "7", "9", "11"})
        public int voterCount;

        private MajorityConfig config;
        private long[] indexes;

        @Setup(Level.Iteration)
        public void setUp() {
            Set<Long> ids = new HashSet<>();
            for (long i = 1; i <= voterCount; i++) ids.add(i);
            config = new MajorityConfig(ids);
            indexes = new long[voterCount];
            ThreadLocalRandom rng = ThreadLocalRandom.current();
            for (int i = 0; i < voterCount; i++) {
                indexes[i] = rng.nextLong(1, Long.MAX_VALUE);
            }
        }
    }

    @Benchmark
    public long committedIndex(CommittedIndexState st) {
        return st.config.committedIndex(voterID -> {
            int idx = (int) (voterID - 1);
            if (idx >= 0 && idx < st.indexes.length) {
                return OptionalLong.of(st.indexes[idx]);
            }
            return OptionalLong.empty();
        });
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(args.length > 0 ? args[0] : RaftCoreBenchmarks.class.getSimpleName())
                .build();
        new Runner(opt).run();
    }
}
