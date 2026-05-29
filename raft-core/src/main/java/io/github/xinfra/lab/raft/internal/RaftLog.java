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
import java.util.Collections;
import java.util.List;

/**
 * RaftLog manages the raft log, combining storage and unstable log.
 */
public class RaftLog {
    private static final RaftLogger logger = DefaultRaftLogger.getDefault();

    public static final long NO_LIMIT = Long.MAX_VALUE;

    // storage contains all stable entries since the last snapshot.
    public Storage storage;
    // unstable contains all unstable entries and snapshot.
    Unstable unstable;

    // committed is the highest log position that is known to be in
    // stable storage on a quorum of nodes.
    public long committed;
    // applying is the highest log position that the application has
    // been instructed to apply to its state machine.
    long applying;
    // applied is the highest log position that the application has
    // successfully applied to its state machine.
    public long applied;

    // maxApplyingEntsSize limits the outstanding byte size of the messages
    // returned from calls to nextCommittedEnts.
    long maxApplyingEntsSize;
    // applyingEntsSize is the current outstanding byte size of the messages.
    long applyingEntsSize;
    // applyingEntsPaused is true when entry application has been paused.
    boolean applyingEntsPaused;

    public static RaftLog newLog(Storage storage) {
        return newLogWithSize(storage, NO_LIMIT);
    }

    public static RaftLog newLogWithSize(Storage storage, long maxApplyingEntsSize) {
        long firstIndex = storage.firstIndex();
        long lastIndex = storage.lastIndex();

        RaftLog log = new RaftLog();
        log.storage = storage;
        log.unstable = new Unstable(lastIndex + 1);
        log.maxApplyingEntsSize = maxApplyingEntsSize;
        log.committed = firstIndex - 1;
        log.applying = firstIndex - 1;
        log.applied = firstIndex - 1;
        return log;
    }

    @Override
    public String toString() {
        return String.format("committed=%d, applied=%d, applying=%d, unstable.offset=%d, unstable.offsetInProgress=%d, len(unstable.Entries)=%d",
                committed, applied, applying, unstable.getOffset(), unstable.getOffsetInProgress(), unstable.getEntries().size());
    }

    /**
     * maybeAppend returns (0, false) if the entries cannot be appended. Otherwise,
     * it returns (last index of new entries, true).
     */
    public MaybeAppendResult maybeAppend(LogSlice a, long committedIndex) {
        if (!matchTerm(a.prev())) {
            return new MaybeAppendResult(0, false);
        }

        long lastnewi = a.prev().index() + a.entries().size();
        long ci = findConflict(a.entries());
        if (ci == 0) {
            // no conflict
        } else if (ci <= committed) {
            throw new RaftInvariantException(String.format("entry %d conflict with committed entry [committed(%d)]", ci, committed));
        } else {
            long offset = a.prev().index() + 1;
            if (ci - offset > a.entries().size()) {
                throw new RaftInvariantException(String.format("index, %d, is out of range [%d]", ci - offset, a.entries().size()));
            }
            append(a.entries().subList((int)(ci - offset), a.entries().size()));
        }
        commitTo(Math.min(committedIndex, lastnewi));
        return new MaybeAppendResult(lastnewi, true);
    }

    public record MaybeAppendResult(long lastNewIndex, boolean ok) {}

    public long append(List<Eraftpb.Entry> ents) {
        if (ents.isEmpty()) {
            return lastIndex();
        }
        long after = ents.get(0).getIndex() - 1;
        if (after < committed) {
            throw new RaftInvariantException(String.format("after(%d) is out of range [committed(%d)]", after, committed));
        }
        unstable.truncateAndAppend(ents);
        return lastIndex();
    }

    /**
     * findConflict finds the index of the conflict.
     * Returns 0 if no conflicting entries or all entries already exist.
     */
    public long findConflict(List<Eraftpb.Entry> ents) {
        for (Eraftpb.Entry ent : ents) {
            EntryID id = EntryID.of(ent);
            if (!matchTerm(id)) {
                if (id.index() <= lastIndex()) {
                    logger.info("found conflict at index {} [existing term: {}, conflicting term: {}]",
                            id.index(), zeroTermOnOutOfBounds(termResult(id.index())), id.term());
                }
                return id.index();
            }
        }
        return 0;
    }

    /**
     * findConflictByTerm returns a best guess on where this log ends matching
     * another log.
     */
    public FindConflictResult findConflictByTerm(long index, long term) {
        for (; index > 0; index--) {
            TermResult tr = termResult(index);
            if (tr.err != null) {
                return new FindConflictResult(index, 0);
            } else if (tr.term <= term) {
                return new FindConflictResult(index, tr.term);
            }
        }
        return new FindConflictResult(0, 0);
    }

    public record FindConflictResult(long index, long term) {}

    /**
     * nextUnstableEnts returns all entries that are available to be written to
     * the local stable log and are not already in-progress.
     */
    public List<Eraftpb.Entry> nextUnstableEnts() {
        return unstable.nextEntries();
    }

    public boolean hasNextUnstableEnts() {
        List<Eraftpb.Entry> ents = nextUnstableEnts();
        return ents != null && !ents.isEmpty();
    }

    public boolean hasNextOrInProgressUnstableEnts() {
        return !unstable.getEntries().isEmpty();
    }

    /**
     * nextCommittedEnts returns all the available entries for execution.
     */
    public List<Eraftpb.Entry> nextCommittedEnts(boolean allowUnstable) {
        if (applyingEntsPaused) {
            return null;
        }
        if (hasNextOrInProgressSnapshot()) {
            return null;
        }
        long lo = applying + 1;
        long hi = maxAppliableIndex(allowUnstable) + 1;
        if (lo >= hi) {
            return null;
        }
        long maxSize = maxApplyingEntsSize - applyingEntsSize;
        if (maxSize <= 0) {
            throw new RaftInvariantException(String.format("applying entry size (%d-%d)=%d not positive",
                    maxApplyingEntsSize, applyingEntsSize, maxSize));
        }
        try {
            return slice(lo, hi, maxSize);
        } catch (RaftException e) {
            throw new RaftInvariantException("unexpected error when getting unapplied entries", e);
        }
    }

    public boolean hasNextCommittedEnts(boolean allowUnstable) {
        if (applyingEntsPaused) {
            return false;
        }
        if (hasNextOrInProgressSnapshot()) {
            return false;
        }
        long lo = applying + 1;
        long hi = maxAppliableIndex(allowUnstable) + 1;
        return lo < hi;
    }

    public long maxAppliableIndex(boolean allowUnstable) {
        long hi = committed;
        if (!allowUnstable) {
            hi = Math.min(hi, unstable.getOffset() - 1);
        }
        return hi;
    }

    public Eraftpb.Snapshot nextUnstableSnapshot() {
        return unstable.nextSnapshot();
    }

    public boolean hasNextUnstableSnapshot() {
        return unstable.nextSnapshot() != null;
    }

    public boolean hasNextOrInProgressSnapshot() {
        return unstable.getSnapshot() != null;
    }

    public Eraftpb.Snapshot snapshot() throws RaftException {
        if (unstable.getSnapshot() != null) {
            return unstable.getSnapshot();
        }
        return storage.snapshot();
    }

    public long firstIndex() {
        java.util.OptionalLong idx = unstable.maybeFirstIndex();
        if (idx.isPresent()) {
            return idx.getAsLong();
        }
        return storage.firstIndex();
    }

    public long lastIndex() {
        java.util.OptionalLong idx = unstable.maybeLastIndex();
        if (idx.isPresent()) {
            return idx.getAsLong();
        }
        return storage.lastIndex();
    }

    public void commitTo(long tocommit) {
        if (committed < tocommit) {
            if (lastIndex() < tocommit) {
                throw new RaftInvariantException(String.format(
                        "tocommit(%d) is out of range [lastIndex(%d)]. Was the raft log corrupted, truncated, or lost?",
                        tocommit, lastIndex()));
            }
            committed = tocommit;
        }
    }

    public void appliedTo(long i, long size) {
        if (committed < i || i < applied) {
            throw new RaftInvariantException(String.format(
                    "applied(%d) is out of range [prevApplied(%d), committed(%d)]", i, applied, committed));
        }
        applied = i;
        applying = Math.max(applying, i);
        if (applyingEntsSize > size) {
            applyingEntsSize -= size;
        } else {
            applyingEntsSize = 0;
        }
        applyingEntsPaused = applyingEntsSize >= maxApplyingEntsSize;
    }

    public void acceptApplying(long i, long size, boolean allowUnstable) {
        if (committed < i) {
            throw new RaftInvariantException(String.format(
                    "applying(%d) is out of range [prevApplying(%d), committed(%d)]", i, applying, committed));
        }
        applying = i;
        applyingEntsSize += size;
        applyingEntsPaused = applyingEntsSize >= maxApplyingEntsSize ||
                i < maxAppliableIndex(allowUnstable);
    }

    public void stableTo(EntryID id) { unstable.stableTo(id); }
    public void stableSnapTo(long i) { unstable.stableSnapTo(i); }
    public void acceptUnstable() { unstable.acceptInProgress(); }

    public EntryID lastEntryID() {
        long index = lastIndex();
        TermResult tr = termResult(index);
        if (tr.err != null) {
            throw new RaftInvariantException("unexpected error when getting the last term at " + index + ": " + tr.err);
        }
        return new EntryID(tr.term, index);
    }

    /**
     * term returns the term of entry i, or throws RaftException.
     * Use termResult for non-throwing version.
     */
    public long term(long i) throws RaftException {
        TermResult tr = termResult(i);
        if (tr.err != null) {
            throw tr.err;
        }
        return tr.term;
    }

    /**
     * Internal term result that carries both value and error.
     */
    public record TermResult(long term, RaftException err) {}

    public TermResult termResult(long i) {
        // Check unstable first
        java.util.OptionalLong t = unstable.maybeTerm(i);
        if (t.isPresent()) {
            return new TermResult(t.getAsLong(), null);
        }

        if (i + 1 < firstIndex()) {
            return new TermResult(0, RaftException.ErrCompacted);
        }
        if (i > lastIndex()) {
            return new TermResult(0, RaftException.ErrUnavailable);
        }

        try {
            long st = storage.term(i);
            return new TermResult(st, null);
        } catch (RaftException e) {
            if (e.is(RaftException.Code.COMPACTED) || e.is(RaftException.Code.UNAVAILABLE)) {
                return new TermResult(0, e);
            }
            throw new RaftInvariantException("unexpected term error", e);
        }
    }

    public List<Eraftpb.Entry> entries(long i, long maxSize) throws RaftException {
        if (i > lastIndex()) {
            return Collections.emptyList();
        }
        return slice(i, lastIndex() + 1, maxSize);
    }

    public List<Eraftpb.Entry> allEntries() {
        while (true) {
            try {
                return entries(firstIndex(), NO_LIMIT);
            } catch (RaftException e) {
                if (e.is(RaftException.Code.COMPACTED)) {
                    continue; // retry on racing compaction
                }
                throw new RaftInvariantException("unexpected error from entries()", e);
            }
        }
    }

    public boolean isUpToDate(EntryID their) {
        EntryID our = lastEntryID();
        return their.term() > our.term() || (their.term() == our.term() && their.index() >= our.index());
    }

    public boolean matchTerm(EntryID id) {
        TermResult tr = termResult(id.index());
        if (tr.err != null) {
            return false;
        }
        return tr.term == id.term();
    }

    public boolean maybeCommit(EntryID at) {
        if (at.term() != 0 && at.index() > committed && matchTerm(at)) {
            commitTo(at.index());
            return true;
        }
        return false;
    }

    public void restore(Eraftpb.Snapshot s) {
        logger.info("log [{}] starts to restore snapshot [index: {}, term: {}]",
                this, s.getMetadata().getIndex(), s.getMetadata().getTerm());
        committed = s.getMetadata().getIndex();
        unstable.restore(s);
    }

    /**
     * Functional interface for scan visitors. Return {@code true} to break out
     * of the scan early, or {@code false} to continue scanning.
     */
    @FunctionalInterface
    public interface ScanVisitor {
        boolean visit(List<Eraftpb.Entry> entries);
    }

    /**
     * scan visits all log entries in the [lo, hi) range, returning them via the
     * given callback. Returns up to pageSize bytes worth of entries at a time.
     * The visitor returns {@code true} to break the scan early.
     */
    public void scan(long lo, long hi, long pageSize, ScanVisitor visitor) throws RaftException {
        while (lo < hi) {
            List<Eraftpb.Entry> ents = slice(lo, hi, pageSize);
            if (ents.isEmpty()) {
                throw new RaftInvariantException("got 0 entries in [" + lo + ", " + hi + ")");
            }
            if (visitor.visit(ents)) {
                return; // break requested
            }
            lo += ents.size();
        }
    }

    /**
     * slice returns a slice of log entries from lo through hi-1, inclusive.
     */
    public List<Eraftpb.Entry> slice(long lo, long hi, long maxSize) throws RaftException {
        RaftException err = mustCheckOutOfBounds(lo, hi);
        if (err != null) {
            throw err;
        }
        if (lo == hi) {
            return Collections.emptyList();
        }
        if (lo >= unstable.getOffset()) {
            // limitSize already returns a subList view of the unstable entries;
            // pb.Entry is immutable so sharing the view is safe. Skip the extra
            // ArrayList copy that previously matched Go's `ents[:len:len]`
            // capacity-cap (Java has no slice-append aliasing to defend against).
            return Util.limitSize(unstable.slice(lo, hi), maxSize);
        }

        long cut = Math.min(hi, unstable.getOffset());
        List<Eraftpb.Entry> ents;
        try {
            ents = storage.entries(lo, cut, maxSize);
        } catch (RaftException e) {
            if (e.is(RaftException.Code.COMPACTED)) {
                throw e;
            } else if (e.is(RaftException.Code.UNAVAILABLE)) {
                throw new RaftInvariantException(String.format("entries[%d:%d) is unavailable from storage", lo, cut));
            } else {
                throw new RaftInvariantException("unexpected storage error", e);
            }
        }
        if (hi <= unstable.getOffset()) {
            return ents;
        }

        // Fast path check
        if (ents.size() < cut - lo) {
            return ents;
        }
        long size = Util.entsSize(ents);
        if (size >= maxSize) {
            return ents;
        }

        List<Eraftpb.Entry> unstableEnts = Util.limitSize(unstable.slice(unstable.getOffset(), hi), maxSize - size);
        if (unstableEnts.size() == 1 && size + Util.entsSize(unstableEnts) > maxSize) {
            return ents;
        }

        List<Eraftpb.Entry> result = new ArrayList<>(ents.size() + unstableEnts.size());
        result.addAll(ents);
        result.addAll(unstableEnts);
        return result;
    }

    private RaftException mustCheckOutOfBounds(long lo, long hi) {
        if (lo > hi) {
            throw new RaftInvariantException(String.format("invalid slice %d > %d", lo, hi));
        }
        long fi = firstIndex();
        if (lo < fi) {
            return RaftException.ErrCompacted;
        }
        long length = lastIndex() + 1 - fi;
        if (hi > fi + length) {
            throw new RaftInvariantException(String.format("slice[%d,%d) out of bound [%d,%d]", lo, hi, fi, lastIndex()));
        }
        return null;
    }

    public long zeroTermOnOutOfBounds(TermResult tr) {
        if (tr.err == null) {
            return tr.term;
        }
        if (tr.err.is(RaftException.Code.COMPACTED) || tr.err.is(RaftException.Code.UNAVAILABLE)) {
            return 0;
        }
        throw new RaftInvariantException("unexpected error: " + tr.err);
    }
}
