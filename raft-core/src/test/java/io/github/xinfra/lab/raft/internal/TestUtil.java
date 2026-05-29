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

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * Test helper utilities, mirroring Go test helpers like index(n).terms(...).
 */
public class TestUtil {

    /** noLimit mirrors Go's noLimit = math.MaxUint64 */
    public static final long NO_LIMIT = Long.MAX_VALUE;

    /**
     * Creates an EntryBuilder starting at the given index.
     * Usage: index(5).terms(1, 2, 3) produces entries at indices 5,6,7 with terms 1,2,3.
     */
    public static EntryBuilder index(long startIndex) {
        return new EntryBuilder(startIndex);
    }

    public static class EntryBuilder {
        private final long startIndex;

        EntryBuilder(long startIndex) {
            this.startIndex = startIndex;
        }

        public List<Eraftpb.Entry> terms(long... terms) {
            List<Eraftpb.Entry> entries = new ArrayList<>(terms.length);
            long idx = startIndex;
            for (long term : terms) {
                entries.add(Eraftpb.Entry.newBuilder()
                        .setIndex(idx)
                        .setTerm(term)
                        .build());
                idx++;
            }
            return entries;
        }

        public List<Eraftpb.Entry> termRange(long from, long to) {
            List<Eraftpb.Entry> entries = new ArrayList<>((int) (to - from));
            long idx = startIndex;
            for (long term = from; term < to; term++) {
                entries.add(Eraftpb.Entry.newBuilder()
                        .setIndex(idx)
                        .setTerm(term)
                        .build());
                idx++;
            }
            return entries;
        }
    }

    public static Eraftpb.Entry entry(long index, long term) {
        return Eraftpb.Entry.newBuilder().setIndex(index).setTerm(term).build();
    }

    public static Eraftpb.Snapshot snapshot(long index, long term) {
        return Eraftpb.Snapshot.newBuilder()
                .setMetadata(Eraftpb.SnapshotMetadata.newBuilder()
                        .setIndex(index)
                        .setTerm(term))
                .build();
    }

    // ============= Raft test helpers (mirroring Go newTestRaft etc.) =============

    @SafeVarargs
    public static MemoryStorage newTestMemoryStorage(Consumer<MemoryStorage>... opts) {
        MemoryStorage ms = new MemoryStorage();
        for (Consumer<MemoryStorage> o : opts) {
            o.accept(ms);
        }
        return ms;
    }

    public static Consumer<MemoryStorage> withPeers(long... peers) {
        return ms -> {
            Eraftpb.Snapshot.Builder sb = ms.getSnapshot().toBuilder();
            Eraftpb.SnapshotMetadata.Builder mb = sb.getMetadata().toBuilder();
            Eraftpb.ConfState.Builder cb = mb.getConfState().toBuilder();
            cb.clearVoters();
            for (long p : peers) {
                cb.addVoters(p);
            }
            mb.setConfState(cb);
            sb.setMetadata(mb);
            ms.setSnapshot(sb.build());
        };
    }

    public static Consumer<MemoryStorage> withLearners(long... learners) {
        return ms -> {
            Eraftpb.Snapshot.Builder sb = ms.getSnapshot().toBuilder();
            Eraftpb.SnapshotMetadata.Builder mb = sb.getMetadata().toBuilder();
            Eraftpb.ConfState.Builder cb = mb.getConfState().toBuilder();
            cb.clearLearners();
            for (long l : learners) {
                cb.addLearners(l);
            }
            mb.setConfState(cb);
            sb.setMetadata(mb);
            ms.setSnapshot(sb.build());
        };
    }

    public static Config newTestConfig(long id, int election, int heartbeat, Storage storage) {
        Config c = new Config();
        c.id = id;
        c.electionTick = election;
        c.heartbeatTick = heartbeat;
        c.storage = storage;
        c.maxSizePerMsg = NO_LIMIT;
        c.maxInflightMsgs = 256;
        return c;
    }

    public static Raft newTestRaft(long id, int election, int heartbeat, Storage storage) {
        return Raft.newRaft(newTestConfig(id, election, heartbeat, storage));
    }
}
