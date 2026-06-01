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

import io.github.xinfra.lab.raft.proto.Eraftpb;
import java.util.List;
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
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

/**
 * JMH benchmarks for hot-path raft operations. Useful for verifying performance
 * fixes (e.g. the Unstable.stableTo and Raft.reset reuse changes) and for
 * regression-checking future tweaks. The set is intentionally small; expand it
 * to cover the operations relevant to a given change.
 *
 * <p>Run all benchmarks:
 * <pre>
 *   mvn test-compile
 *   java -cp target/test-classes:target/classes:$(mvn -q dependency:build-classpath -Dmdep.outputFile=/dev/stdout) \
 *        io.github.xinfra.lab.raft.bench.RaftBenchmarks
 * </pre>
 *
 * <p>Or run a specific benchmark:
 * <pre>
 *   java ... io.github.xinfra.lab.raft.bench.RaftBenchmarks proposeAndDrain
 * </pre>
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 3)
@Fork(value = 1)
@State(Scope.Benchmark)
public class RaftBenchmarks {

    @Param({"3", "5"})
    public int voters;

    @Param({"64", "1024"})
    public int payloadBytes;

    private RawNode rn;
    private MemoryStorage storage;
    private byte[] payload;

    @Setup(Level.Iteration)
    public void setUp() throws RaftException {
        storage = new MemoryStorage();
        // Pre-set ConfState with N voters so the leader bootstraps without
        // full conf-change traffic.
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
        // Drain Readys until we are leader.
        while (rn.hasReady()) {
            Ready rd = rn.ready();
            if (!rd.entries().isEmpty()) storage.append(rd.entries());
            rn.advance(rd);
        }

        payload = new byte[payloadBytes];
        java.util.Arrays.fill(payload, (byte) 'a');
    }

    @TearDown(Level.Iteration)
    public void tearDown() throws RaftException {
        rn = null;
        storage = null;
    }

    /**
     * propose + drain Ready cycle for a single entry. Exercises the hot
     * append → Ready → advance loop on a leader without peers responding,
     * so it isolates the local state-machine cost.
     */
    @Benchmark
    public Ready proposeAndDrain() throws RaftException {
        rn.propose(payload);
        Ready rd = rn.ready();
        if (!rd.entries().isEmpty()) storage.append(rd.entries());
        rn.advance(rd);
        return rd;
    }

    /**
     * Exercises a single inbound MsgAppendResponse on the leader, which is
     * the dominant message type in steady-state replication. Tests the
     * Progress.maybeUpdate / maybeCommit / Inflights.freeLE chain.
     */
    @Benchmark
    public void leaderHandleAppendResponse() throws RaftException {
        long lastIndex = rn.raft.raftLog.lastIndex();
        // Simulate every other voter ack-ing the leader's last entry.
        for (long peerId = 2; peerId <= voters; peerId++) {
            rn.step(Eraftpb.Message.newBuilder()
                    .setMsgType(Eraftpb.MessageType.MsgAppendResponse)
                    .setFrom(peerId)
                    .setTo(1)
                    .setTerm(rn.raft.term)
                    .setIndex(lastIndex)
                    .build());
        }
    }

    // ============= Targeted micro-benchmarks for the Unstable hot path =============
    // proposeAndDrain mostly exercises top-level Ready cycling and doesn't show
    // the per-allocation cost of Unstable.stableTo. The benchmarks below
    // construct a fresh Unstable each run and drive a known sequence of
    // append + stableTo operations, isolating the change from #6 (in-place
    // shrink vs. ArrayList rebuild on every stableTo).

    @State(Scope.Benchmark)
    public static class UnstableState {
        @Param({"100", "1000"})
        public int batchSize;

        public Unstable unstable;
        public List<Eraftpb.Entry> batch;

        @Setup(Level.Invocation)
        public void setUp() throws RaftException {
            unstable = new Unstable(1);
            batch = new java.util.ArrayList<>(batchSize);
            for (int i = 0; i < batchSize; i++) {
                batch.add(Eraftpb.Entry.newBuilder()
                        .setIndex(i + 1).setTerm(1).build());
            }
            unstable.truncateAndAppend(batch);
        }
    }

    /**
     * Stabilize a freshly-populated Unstable one batch at a time, in chunks of
     * 10. Each stableTo previously rebuilt the entries ArrayList; #6 made it
     * an in-place removeRange.
     */
    @Benchmark
    public void unstableStableToInChunks(UnstableState st) throws RaftException {
        int chunk = 10;
        long upTo = 0;
        while (upTo + chunk <= st.batchSize) {
            upTo += chunk;
            st.unstable.stableTo(new EntryID(1, upTo));
        }
    }

    // ============= RaftLog.slice (#7) benchmark =============
    // The fix removed an unnecessary `new ArrayList<>(...)` wrapping of the
    // unstable subList view inside RaftLog.slice. Benchmark exercises a
    // raftLog whose entries are all in unstable.

    @State(Scope.Benchmark)
    public static class RaftLogSliceState {
        @Param({"100", "1000"})
        public int logSize;

        public RaftLog raftLog;

        @Setup(Level.Iteration)
        public void setUp() throws RaftException {
            MemoryStorage storage = new MemoryStorage();
            raftLog = RaftLog.newLog(storage);
            List<Eraftpb.Entry> batch = new java.util.ArrayList<>(logSize);
            for (int i = 0; i < logSize; i++) {
                batch.add(Eraftpb.Entry.newBuilder()
                        .setIndex(i + 1).setTerm(1).build());
            }
            raftLog.append(batch);
        }
    }

    /**
     * Read the full unstable range via {@link RaftLog#entries}, exercising
     * the slice → unstable.slice → Util.limitSize path that #7 simplified.
     */
    @Benchmark
    public List<Eraftpb.Entry> raftLogSliceUnstable(RaftLogSliceState st) throws RaftException {
        return st.raftLog.entries(1, Long.MAX_VALUE);
    }

    // ============= ReadOnly.maybeAdvance (#18) benchmark =============
    // The fix replaced "two new ArrayLists per advance" with "one copy + one
    // in-place subList.clear()". Benchmark adds N requests then advances
    // them all at once, measuring throughput of the advance path.

    @State(Scope.Benchmark)
    public static class ReadOnlyState {
        @Param({"100", "1000"})
        public int requestCount;

        public ReadOnly ro;
        public io.github.xinfra.lab.raft.internal.quorum.JointConfig voters;

        @Setup(Level.Invocation)
        public void setUp() throws RaftException {
            ro = new ReadOnly(ReadOnlyOption.ReadOnlySafe);
            io.github.xinfra.lab.raft.internal.quorum.MajorityConfig in =
                    new io.github.xinfra.lab.raft.internal.quorum.MajorityConfig(java.util.Set.of(1L, 2L, 3L));
            voters = new io.github.xinfra.lab.raft.internal.quorum.JointConfig(in, null);
            for (int i = 0; i < requestCount; i++) {
                Eraftpb.Message req = Eraftpb.Message.newBuilder()
                        .setMsgType(Eraftpb.MessageType.MsgReadIndex)
                        .build();
                ro.addRequest(i, req);
            }
            // Synthesize acks from a quorum so maybeAdvance has work to do.
            // recvAck encodes the index in little-endian bytes.
            byte[] enc = new byte[8];
            java.nio.ByteBuffer.wrap(enc).order(java.nio.ByteOrder.LITTLE_ENDIAN)
                    .putLong(requestCount);
            ro.recvAck(1L, enc);
            ro.recvAck(2L, enc);
        }
    }

    /**
     * Advance all unconfirmed reads at once. Exercises the slice/copy path
     * inside maybeAdvance that #18 simplified.
     */
    @Benchmark
    public List<ReadOnly.ReadIndexRequest> readOnlyMaybeAdvance(ReadOnlyState st) {
        return st.ro.maybeAdvance(st.voters);
    }

    public static void main(String[] args) throws Exception {
        Options opt = new OptionsBuilder()
                .include(args.length > 0 ? args[0] : RaftBenchmarks.class.getSimpleName())
                .build();
        new Runner(opt).run();
    }
}
