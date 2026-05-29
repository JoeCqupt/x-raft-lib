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
package io.github.xinfra.lab.raft;

import io.github.xinfra.lab.raft.proto.Eraftpb;

import java.util.ArrayList;
import java.util.List;

/**
 * MemoryStorage implements the Storage interface backed by an in-memory array.
 */
public class MemoryStorage implements Storage {
    private Eraftpb.HardState hardState;
    private Eraftpb.Snapshot snapshot;
    private List<Eraftpb.Entry> ents;

    public MemoryStorage() {
        this.hardState = Eraftpb.HardState.getDefaultInstance();
        this.snapshot = Eraftpb.Snapshot.getDefaultInstance();
        this.ents = new ArrayList<>();
        // dummy entry at term zero
        this.ents.add(Eraftpb.Entry.getDefaultInstance());
    }

    /** Package-private constructor for testing with pre-populated entries. */
    MemoryStorage(List<Eraftpb.Entry> ents) {
        this.hardState = Eraftpb.HardState.getDefaultInstance();
        this.snapshot = Eraftpb.Snapshot.getDefaultInstance();
        this.ents = new ArrayList<>(ents);
    }

    /**
     * Package-private accessor for testing. Returns an immutable snapshot of the
     * internal list — callers must not assume the result reflects later mutations
     * and must not attempt to modify it. Going through a copy preserves the
     * monitor's invariants for any subsequent storage call.
     */
    synchronized List<Eraftpb.Entry> getEntries() {
        return List.copyOf(ents);
    }

    /** Package-private accessor for testing. */
    public synchronized Eraftpb.Snapshot getSnapshot() {
        return snapshot;
    }

    /** Package-private setter for testing. */
    public synchronized void setSnapshot(Eraftpb.Snapshot snap) {
        this.snapshot = snap;
    }

    /**
     * Package-private bulk setter for tests that need to seed a specific log
     * state. Atomic under the monitor so concurrent storage calls see either
     * the old or the new list, never an in-progress mutation. Preserves the
     * dummy entry at index 0.
     */
    public synchronized void setEntries(List<Eraftpb.Entry> entries) {
        List<Eraftpb.Entry> next = new ArrayList<>(entries.size() + 1);
        next.add(Eraftpb.Entry.getDefaultInstance()); // dummy
        next.addAll(entries);
        this.ents = next;
    }

    @Override
    public synchronized InitialStateResult initialState() {
        Eraftpb.ConfState cs = snapshot.hasMetadata() ?
                snapshot.getMetadata().getConfState() : Eraftpb.ConfState.getDefaultInstance();
        return new InitialStateResult(hardState, cs);
    }

    public synchronized void setHardState(Eraftpb.HardState st) {
        this.hardState = st;
    }

    @Override
    public synchronized List<Eraftpb.Entry> entries(long lo, long hi, long maxSize) throws RaftException {
        long offset = ents.get(0).getIndex();
        if (lo <= offset) {
            throw RaftException.ErrCompacted;
        }
        if (hi > lastIndexInternal() + 1) {
            throw new RaftInvariantException("entries' hi(" + hi + ") is out of bound lastindex(" + lastIndexInternal() + ")");
        }
        if (ents.size() == 1) {
            throw RaftException.ErrUnavailable;
        }

        List<Eraftpb.Entry> result = new ArrayList<>();
        long size = 0;
        for (int i = (int)(lo - offset); i < (int)(hi - offset); i++) {
            Eraftpb.Entry e = ents.get(i);
            size += e.getSerializedSize();
            if (result.size() > 0 && size > maxSize) {
                break;
            }
            result.add(e);
        }
        return result;
    }

    @Override
    public synchronized long term(long i) throws RaftException {
        long offset = ents.get(0).getIndex();
        if (i < offset) {
            throw RaftException.ErrCompacted;
        }
        if ((int)(i - offset) >= ents.size()) {
            throw RaftException.ErrUnavailable;
        }
        return ents.get((int)(i - offset)).getTerm();
    }

    @Override
    public synchronized long lastIndex() {
        return lastIndexInternal();
    }

    private long lastIndexInternal() {
        return ents.get(0).getIndex() + ents.size() - 1;
    }

    @Override
    public synchronized long firstIndex() {
        return firstIndexInternal();
    }

    private long firstIndexInternal() {
        return ents.get(0).getIndex() + 1;
    }

    @Override
    public synchronized Eraftpb.Snapshot snapshot() {
        return snapshot;
    }

    public synchronized void applySnapshot(Eraftpb.Snapshot snap) throws RaftException {
        long msIndex = snapshot.getMetadata().getIndex();
        long snapIndex = snap.getMetadata().getIndex();
        if (msIndex != 0 && msIndex >= snapIndex) {
            throw RaftException.ErrSnapOutOfDate;
        }
        snapshot = snap;
        ents = new ArrayList<>();
        ents.add(Eraftpb.Entry.newBuilder()
                .setTerm(snap.getMetadata().getTerm())
                .setIndex(snap.getMetadata().getIndex())
                .build());
    }

    public synchronized Eraftpb.Snapshot createSnapshot(long i, Eraftpb.ConfState cs, byte[] data) throws RaftException {
        if (i <= snapshot.getMetadata().getIndex()) {
            throw RaftException.ErrSnapOutOfDate;
        }
        long offset = ents.get(0).getIndex();
        if (i > lastIndexInternal()) {
            throw new RaftInvariantException("snapshot " + i + " is out of bound lastindex(" + lastIndexInternal() + ")");
        }

        Eraftpb.SnapshotMetadata.Builder metaBuilder = Eraftpb.SnapshotMetadata.newBuilder()
                .setIndex(i)
                .setTerm(ents.get((int)(i - offset)).getTerm());
        if (cs != null) {
            metaBuilder.setConfState(cs);
        }

        snapshot = Eraftpb.Snapshot.newBuilder()
                .setMetadata(metaBuilder)
                .setData(data != null ? com.google.protobuf.ByteString.copyFrom(data) : com.google.protobuf.ByteString.EMPTY)
                .build();
        return snapshot;
    }

    public synchronized void compact(long compactIndex) throws RaftException {
        long offset = ents.get(0).getIndex();
        if (compactIndex <= offset) {
            throw RaftException.ErrCompacted;
        }
        if (compactIndex > lastIndexInternal()) {
            throw new RaftInvariantException("compact " + compactIndex + " is out of bound lastindex(" + lastIndexInternal() + ")");
        }

        int i = (int)(compactIndex - offset);
        List<Eraftpb.Entry> newEnts = new ArrayList<>();
        newEnts.add(Eraftpb.Entry.newBuilder()
                .setIndex(ents.get(i).getIndex())
                .setTerm(ents.get(i).getTerm())
                .build());
        for (int j = i + 1; j < ents.size(); j++) {
            newEnts.add(ents.get(j));
        }
        ents = newEnts;
    }

    public synchronized void append(List<Eraftpb.Entry> entries) {
        if (entries.isEmpty()) {
            return;
        }

        long first = firstIndexInternal();
        long last = entries.get(0).getIndex() + entries.size() - 1;

        if (last < first) {
            return;
        }

        // truncate compacted entries
        int startIdx = 0;
        if (first > entries.get(0).getIndex()) {
            startIdx = (int)(first - entries.get(0).getIndex());
        }

        long offset = entries.get(startIdx).getIndex() - ents.get(0).getIndex();
        if (ents.size() > offset) {
            List<Eraftpb.Entry> newEnts = new ArrayList<>(ents.subList(0, (int)offset));
            for (int i = startIdx; i < entries.size(); i++) {
                newEnts.add(entries.get(i));
            }
            ents = newEnts;
        } else if (ents.size() == offset) {
            for (int i = startIdx; i < entries.size(); i++) {
                ents.add(entries.get(i));
            }
        } else {
            throw new RaftInvariantException("missing log entry [last: " + lastIndexInternal() + ", append at: " + entries.get(startIdx).getIndex() + "]");
        }
    }
}
