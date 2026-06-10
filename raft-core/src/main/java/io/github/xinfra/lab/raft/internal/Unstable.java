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

/**
 * Unstable contains "unstable" log entries and snapshot state that has
 * not yet been written to Storage.
 */
public class Unstable {
    private Eraftpb.Snapshot snapshot;
    private List<Eraftpb.Entry> entries;
    private long offset;
    private boolean snapshotInProgress;
    private long offsetInProgress;

    public Unstable(long offset) {
        this.offset = offset;
        this.offsetInProgress = offset;
        this.entries = new ArrayList<>();
    }

    public Eraftpb.Snapshot getSnapshot() { return snapshot; }
    public List<Eraftpb.Entry> getEntries() { return entries; }
    public long getOffset() { return offset; }
    public long getOffsetInProgress() { return offsetInProgress; }
    public boolean isSnapshotInProgress() { return snapshotInProgress; }

    /** Package-private setters for testing. */
    void setOffsetInProgress(long v) { this.offsetInProgress = v; }
    void setEntriesForTest(List<Eraftpb.Entry> e) { this.entries = new ArrayList<>(e); }
    void setSnapshotForTest(Eraftpb.Snapshot s) { this.snapshot = s; }
    void setSnapshotInProgressForTest(boolean v) { this.snapshotInProgress = v; }

    public java.util.OptionalLong maybeFirstIndex() {
        if (snapshot != null) {
            return java.util.OptionalLong.of(snapshot.getMetadata().getIndex() + 1);
        }
        return java.util.OptionalLong.empty();
    }

    public java.util.OptionalLong maybeLastIndex() {
        if (!entries.isEmpty()) {
            return java.util.OptionalLong.of(offset + entries.size() - 1);
        }
        if (snapshot != null) {
            return java.util.OptionalLong.of(snapshot.getMetadata().getIndex());
        }
        return java.util.OptionalLong.empty();
    }

    public java.util.OptionalLong maybeTerm(long i) {
        if (i < offset) {
            if (snapshot != null && snapshot.getMetadata().getIndex() == i) {
                return java.util.OptionalLong.of(snapshot.getMetadata().getTerm());
            }
            return java.util.OptionalLong.empty();
        }

        java.util.OptionalLong last = maybeLastIndex();
        if (last.isEmpty()) {
            return java.util.OptionalLong.empty();
        }
        if (i > last.getAsLong()) {
            return java.util.OptionalLong.empty();
        }
        return java.util.OptionalLong.of(entries.get((int)(i - offset)).getTerm());
    }

    public List<Eraftpb.Entry> nextEntries() {
        int inProgress = (int)(offsetInProgress - offset);
        if (entries.size() == inProgress) {
            return null;
        }
        return entries.subList(inProgress, entries.size());
    }

    public Eraftpb.Snapshot nextSnapshot() {
        if (snapshot == null || snapshotInProgress) {
            return null;
        }
        return snapshot;
    }

    public void acceptInProgress() {
        if (!entries.isEmpty()) {
            offsetInProgress = entries.get(entries.size() - 1).getIndex() + 1;
        }
        if (snapshot != null) {
            snapshotInProgress = true;
        }
    }

    public void stableTo(EntryID id) {
        java.util.OptionalLong gt = maybeTerm(id.index());
        if (gt.isEmpty()) {
            return;
        }
        if (id.index() < offset) {
            return;
        }
        if (gt.getAsLong() != id.term()) {
            return;
        }
        int num = (int)(id.index() + 1 - offset);
        // Remove the stabilized prefix in-place (System.arraycopy shift, no
        // per-call allocation). Mirrors etcd-raft's u.entries = u.entries[num:].
        entries.subList(0, num).clear();
        offset = id.index() + 1;
        offsetInProgress = Math.max(offsetInProgress, offset);
        shrinkEntriesArray();
    }

    /**
     * Drops the backing array when entries is empty so the next bulk append
     * starts from a fresh capacity. Mirrors etcd-raft's shrinkEntriesArray
     * heuristic. Java's ArrayList hides capacity, so we only shrink in the
     * cleanest case (empty); steady-state growth is bounded by ArrayList's
     * 1.5x growth policy and the application's peak unstable size.
     */
    private void shrinkEntriesArray() {
        if (entries.isEmpty()) {
            entries = new ArrayList<>();
        }
    }

    public void stableSnapTo(long i) {
        if (snapshot != null && snapshot.getMetadata().getIndex() == i) {
            snapshot = null;
            snapshotInProgress = false;
        }
    }

    public void restore(Eraftpb.Snapshot s) {
        offset = s.getMetadata().getIndex() + 1;
        offsetInProgress = offset;
        entries = new ArrayList<>();
        snapshot = s;
        snapshotInProgress = false;
    }

    public void truncateAndAppend(List<Eraftpb.Entry> ents) {
        long fromIndex = ents.get(0).getIndex();
        if (fromIndex == offset + entries.size()) {
            entries.addAll(ents);
        } else if (fromIndex <= offset) {
            entries = new ArrayList<>(ents);
            offset = fromIndex;
            offsetInProgress = offset;
        } else {
            List<Eraftpb.Entry> keep = new ArrayList<>(slice(offset, fromIndex));
            keep.addAll(ents);
            entries = keep;
            offsetInProgress = Math.min(offsetInProgress, fromIndex);
        }
    }

    public List<Eraftpb.Entry> slice(long lo, long hi) {
        mustCheckOutOfBounds(lo, hi);
        return entries.subList((int)(lo - offset), (int)(hi - offset));
    }

    private void mustCheckOutOfBounds(long lo, long hi) {
        if (lo > hi) {
            throw new RaftInvariantException("invalid unstable.slice " + lo + " > " + hi);
        }
        long upper = offset + entries.size();
        if (lo < offset || hi > upper) {
            throw new RaftInvariantException("unstable.slice[" + lo + "," + hi + ") out of bound [" + offset + "," + upper + "]");
        }
    }
}
