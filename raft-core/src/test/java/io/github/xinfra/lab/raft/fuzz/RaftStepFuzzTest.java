/*
 * Copyright 2024-2026 The x-raft-lib Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * SPDX-License-Identifier: Apache-2.0
 */
package io.github.xinfra.lab.raft.fuzz;

import com.code_intelligence.jazzer.api.FuzzedDataProvider;
import com.code_intelligence.jazzer.junit.FuzzTest;
import com.google.protobuf.ByteString;
import io.github.xinfra.lab.raft.Config;
import io.github.xinfra.lab.raft.MemoryStorage;
import io.github.xinfra.lab.raft.Peer;
import io.github.xinfra.lab.raft.RaftException;
import io.github.xinfra.lab.raft.RawNode;
import io.github.xinfra.lab.raft.internal.Raft;
import io.github.xinfra.lab.raft.proto.Eraftpb;

import java.util.List;

import static io.github.xinfra.lab.raft.internal.TestUtil.NO_LIMIT;
import static io.github.xinfra.lab.raft.internal.TestUtil.newTestConfig;
import static io.github.xinfra.lab.raft.internal.TestUtil.newTestMemoryStorage;
import static io.github.xinfra.lab.raft.internal.TestUtil.withPeers;

/**
 * Coverage-guided fuzzing of {@link Raft#step(Eraftpb.Message)} — the
 * single hot entry point every peer-supplied message hits before raft
 * mutates state. A real production cluster has zero trust in the bytes
 * a peer sends; a crash or invariant violation on a hostile message is
 * an availability bug.
 *
 * <p>The fuzzer hands us a {@link FuzzedDataProvider} which we use to
 * construct a structured-but-arbitrary {@link Eraftpb.Message}, then
 * feed it through a fresh {@link RawNode} that has just won an
 * election. Documented {@link RaftException} returns are accepted;
 * anything else (NPE, ISE, IndexOutOfBounds, ClassCastException,
 * RaftInvariantException for paths a peer can reach) is a finding.
 *
 * <p>See {@link EraftpbParseFuzzTest} for the parse-layer harness;
 * this one fuzzes <em>after</em> the parse boundary.
 */
class RaftStepFuzzTest {

    /**
     * Wrap a leader RawNode and feed it a synthetic peer message
     * constructed from arbitrary fuzzer bytes. The leader is a
     * single-voter cluster so it stays leader regardless of what we
     * step — the fuzz target is step's internal dispatch, not the
     * election state machine.
     */
    @FuzzTest
    void leaderStepArbitraryMessage(FuzzedDataProvider data) throws RaftException {
        RawNode rn = newLeader();
        Eraftpb.Message msg = synthesize(data, rn.raft.id);
        try {
            rn.step(msg);
        } catch (RaftException expected) {
            // Documented raft-layer rejection (e.g. ErrProposalDropped).
        }
    }

    /**
     * Same as above, but the node is a freshly-restarted follower
     * (no leader elected) — covers the dropped-proposal / no-leader
     * branches that a leader can't reach.
     */
    @FuzzTest
    void followerStepArbitraryMessage(FuzzedDataProvider data) throws RaftException {
        MemoryStorage s = newTestMemoryStorage(withPeers(1, 2, 3));
        Config cfg = newTestConfig(1, 10, 1, s);
        cfg.maxSizePerMsg = NO_LIMIT;
        cfg.maxInflightMsgs = 256;
        RawNode rn = RawNode.newRawNode(cfg);
        Eraftpb.Message msg = synthesize(data, rn.raft.id);
        try {
            rn.step(msg);
        } catch (RaftException expected) {
            // ok
        }
    }

    // ---- helpers ----

    private static RawNode newLeader() throws RaftException {
        MemoryStorage s = newTestMemoryStorage(withPeers(1));
        Config cfg = newTestConfig(1, 10, 1, s);
        cfg.maxSizePerMsg = NO_LIMIT;
        cfg.maxInflightMsgs = 256;
        RawNode rn = RawNode.newRawNode(cfg);
        rn.bootstrap(List.of(new Peer(1)));
        rn.campaign();
        // Drain the election Ready so r.state == Leader.
        if (rn.hasReady()) {
            var rd = rn.ready();
            s.append(rd.entries);
            rn.advance(rd);
        }
        return rn;
    }

    /**
     * Build a structured Eraftpb.Message from the fuzzer's stream. We
     * keep the constructed message in raft's valid wire-shape (message
     * type, from/to, term, payload) but let every field's value drift
     * arbitrarily — that's where the interesting paths live.
     */
    private static Eraftpb.Message synthesize(FuzzedDataProvider data, long localId) {
        Eraftpb.MessageType[] types = Eraftpb.MessageType.values();
        // UNRECOGNIZED is the last value and isn't a valid wire type;
        // exclude it so the dispatch switch can be exercised.
        int typeIdx = data.consumeInt(0, types.length - 2);
        Eraftpb.MessageType type = types[typeIdx];

        Eraftpb.Message.Builder mb = Eraftpb.Message.newBuilder()
                .setMsgType(type)
                .setFrom(data.consumeLong())
                .setTo(localId)
                .setTerm(data.consumeLong(0, Long.MAX_VALUE))
                .setLogTerm(data.consumeLong(0, Long.MAX_VALUE))
                .setIndex(data.consumeLong(0, Long.MAX_VALUE))
                .setCommit(data.consumeLong(0, Long.MAX_VALUE))
                .setVote(data.consumeLong())
                .setReject(data.consumeBoolean())
                .setRejectHint(data.consumeLong(0, Long.MAX_VALUE));

        // Optionally include a small payload entry — propose paths and
        // append paths both branch on entries.
        if (data.consumeBoolean()) {
            int n = data.consumeInt(0, 8);
            for (int i = 0; i < n; i++) {
                int payloadLen = data.consumeInt(0, 64);
                mb.addEntries(Eraftpb.Entry.newBuilder()
                        .setTerm(data.consumeLong(0, Long.MAX_VALUE))
                        .setIndex(data.consumeLong(0, Long.MAX_VALUE))
                        .setData(ByteString.copyFrom(data.consumeBytes(payloadLen))));
            }
        }

        if (data.consumeBoolean()) {
            mb.setContext(ByteString.copyFrom(data.consumeBytes(32)));
        }

        return mb.build();
    }
}
