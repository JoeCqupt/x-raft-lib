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
package io.github.xinfra.lab.raft.storage.rocksdb;

import com.google.protobuf.InvalidProtocolBufferException;
import io.github.xinfra.lab.raft.RaftException;
import io.github.xinfra.lab.raft.RaftInvariantException;
import io.github.xinfra.lab.raft.Storage;
import io.github.xinfra.lab.raft.proto.Eraftpb;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * RocksDB-backed {@link Storage}. Three column families:
 * <ul>
 *   <li>{@code log} — log entries, key = uint64 BE index, value = serialized
 *   {@link Eraftpb.Entry}.</li>
 *   <li>{@code state} — singleton keys: {@code "hard_state"} →
 *   {@link Eraftpb.HardState}, {@code "conf_state"} →
 *   {@link Eraftpb.ConfState}, {@code "first_index"} → 8-byte BE long.</li>
 *   <li>{@code snapshot} — singleton {@code "snapshot"} →
 *   {@link Eraftpb.Snapshot}.</li>
 * </ul>
 *
 * <p><b>Atomicity per Ready cycle.</b> Wrap any sequence of
 * {@link #append}/{@link #setHardState}/{@link #applySnapshot} that should
 * land together in a {@code WriteBatch} via the public batch helpers.
 * Single-call methods are atomic on their own (one fsync per call) but
 * cross-method atomicity requires the host to use {@link #writeBatched}.
 *
 * <p><b>fsync.</b> {@code WriteOptions.setSync(true)} is applied to every
 * write so this storage is durable on return. Hosts can opt-out
 * (asyncStorageWrites / non-critical paths) by passing
 * {@code sync=false} via the alternative ctor, but Raft safety then
 * requires the host to fsync separately before sending responses.
 *
 * <p><b>Snapshots.</b> Two persistence paths are supported. The
 * {@code byte[]}-based {@link #createSnapshot(long, Eraftpb.ConfState, byte[])}
 * and {@link #applySnapshot(Eraftpb.Snapshot)} store the payload <em>inline</em>
 * in RocksDB — simple, but the whole payload must fit in heap. For large or
 * petabyte-scale state machines use the <em>streaming</em> overloads
 * ({@link #createSnapshotStreaming(long, Eraftpb.ConfState, Storage.SnapshotDataWriter)},
 * {@link #applySnapshot(Eraftpb.Snapshot, java.io.InputStream)},
 * {@link #openSnapshotData(Eraftpb.Snapshot)}): the payload streams to/from a
 * side-car file under {@code <dbDir>/snapshots/} and only the small
 * {@link Eraftpb.SnapshotMetadata} is kept in RocksDB. {@code KEY_SNAPSHOT_FILE}
 * is the single source of truth for which path backs the current snapshot, so
 * the two paths can be mixed safely. Side-car writes are crash-safe
 * (temp file → fsync → atomic rename → directory fsync) and stale {@code .tmp}
 * files are reaped on open.
 */
public class RocksDbStorage implements Storage, AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(RocksDbStorage.class);

    private static final byte[] CF_DEFAULT = RocksDB.DEFAULT_COLUMN_FAMILY;
    private static final byte[] CF_LOG = bytes("log");
    private static final byte[] CF_STATE = bytes("state");
    private static final byte[] CF_SNAPSHOT = bytes("snapshot");

    private static final byte[] KEY_HARD_STATE = bytes("hard_state");
    private static final byte[] KEY_FIRST_INDEX = bytes("first_index");
    private static final byte[] KEY_SNAPSHOT = bytes("snapshot");
    private static final byte[] KEY_APPLIED = bytes("applied");
    private static final byte[] KEY_CONF_STATE = bytes("conf_state");
    /**
     * Present in CF_STATE iff the current snapshot's payload lives in a
     * side-car file (streaming path); value = the file name under
     * {@link #snapDir}. Absent means the payload is inline in the snapshot
     * proto stored under {@link #KEY_SNAPSHOT}. This is the single source of
     * truth for "is the current snapshot streamed or inline".
     */
    private static final byte[] KEY_SNAPSHOT_FILE = bytes("snapshot_file");

    /** Subdirectory under the DB dir holding streamed snapshot payload files. */
    private static final String SNAP_SUBDIR = "snapshots";

    static { RocksDB.loadLibrary(); }

    private final RocksDB db;
    private final List<ColumnFamilyHandle> cfHandles;
    private final ColumnFamilyHandle cfDefault;
    private final ColumnFamilyHandle cfLog;
    private final ColumnFamilyHandle cfState;
    private final ColumnFamilyHandle cfSnap;
    private final WriteOptions writeOpts;
    private final Path snapDir;

    private volatile boolean closed = false;

    public RocksDbStorage(Path dir) throws RocksDBException, IOException {
        this(dir, true);
    }

    public RocksDbStorage(Path dir, boolean fsync) throws RocksDBException, IOException {
        Files.createDirectories(dir);
        this.snapDir = dir.resolve(SNAP_SUBDIR);
        Files.createDirectories(snapDir);
        // Drop any *.tmp side-car files left behind by a crash mid-write.
        cleanupTempSnapshotFiles(snapDir);

        try (Options opts = new Options().setCreateIfMissing(true).setCreateMissingColumnFamilies(true);
             ColumnFamilyOptions cfOpts = new ColumnFamilyOptions()) {
            // Probe existing CFs for tolerant open.
            List<byte[]> existing;
            try {
                existing = RocksDB.listColumnFamilies(opts, dir.toString());
            } catch (RocksDBException e) {
                // Fresh DB; will be created with our descriptor list.
                existing = new ArrayList<>();
            }

            List<ColumnFamilyDescriptor> descriptors = new ArrayList<>();
            for (byte[] name : Arrays.asList(CF_DEFAULT, CF_LOG, CF_STATE, CF_SNAPSHOT)) {
                descriptors.add(new ColumnFamilyDescriptor(name, cfOpts));
            }
            // If RocksDB's CF list contains anything else (legacy state),
            // include it so open() doesn't fail.
            for (byte[] name : existing) {
                boolean known = false;
                for (ColumnFamilyDescriptor d : descriptors) {
                    if (Arrays.equals(d.getName(), name)) { known = true; break; }
                }
                if (!known) descriptors.add(new ColumnFamilyDescriptor(name, cfOpts));
            }

            this.cfHandles = new ArrayList<>();
            DBOptions dbOpts = new DBOptions()
                    .setCreateIfMissing(true)
                    .setCreateMissingColumnFamilies(true);
            this.db = RocksDB.open(dbOpts, dir.toString(), descriptors, this.cfHandles);
            this.cfDefault = cfHandles.get(0);
            this.cfLog = cfHandles.get(1);
            this.cfState = cfHandles.get(2);
            this.cfSnap = cfHandles.get(3);
            this.writeOpts = new WriteOptions().setSync(fsync);
        }
    }

    // ====================== Storage read API ======================

    @Override
    public synchronized InitialStateResult initialState() {
        try {
            byte[] hsBytes = db.get(cfState, KEY_HARD_STATE);
            Eraftpb.HardState hs = hsBytes == null
                    ? Eraftpb.HardState.getDefaultInstance()
                    : Eraftpb.HardState.parseFrom(hsBytes);
            // Prefer the explicitly-persisted ConfState (host updates this
            // after each applyConfChange). Fall back to the latest snapshot's
            // metadata, then to default. This matches etcd-raft's recovery
            // order: snapshot meta is the bootstrap baseline, but a node
            // that's done conf changes since then has more recent state.
            byte[] csBytes = db.get(cfState, KEY_CONF_STATE);
            Eraftpb.ConfState cs;
            if (csBytes != null) {
                cs = Eraftpb.ConfState.parseFrom(csBytes);
            } else {
                byte[] snapBytes = db.get(cfSnap, KEY_SNAPSHOT);
                cs = (snapBytes != null)
                        ? Eraftpb.Snapshot.parseFrom(snapBytes).getMetadata().getConfState()
                        : Eraftpb.ConfState.getDefaultInstance();
            }
            return new InitialStateResult(hs, cs);
        } catch (InvalidProtocolBufferException e) {
            throw new RaftInvariantException(RaftInvariantException.Category.DATA_CORRUPTION,
                    "rocksdb initialState parse failed (corrupt hard/conf state)", e);
        } catch (RocksDBException e) {
            throw new RaftInvariantException(RaftInvariantException.Category.STORAGE_IO,
                    "rocksdb initialState read failed", e);
        }
    }

    @Override
    public synchronized List<Eraftpb.Entry> entries(long lo, long hi, long maxSize) throws RaftException {
        long first = firstIndex();
        long last = lastIndex();
        if (lo < first) throw RaftException.ErrCompacted;
        if (hi > last + 1) throw new RaftInvariantException(
                "entries' hi(" + hi + ") is out of bound lastindex(" + last + ")");

        List<Eraftpb.Entry> result = new ArrayList<>();
        long size = 0;
        try (RocksIterator it = db.newIterator(cfLog)) {
            it.seek(indexKey(lo));
            while (it.isValid()) {
                long k = decodeIndex(it.key());
                if (k >= hi) break;
                Eraftpb.Entry e;
                try {
                    e = Eraftpb.Entry.parseFrom(it.value());
                } catch (InvalidProtocolBufferException ipe) {
                    throw new RaftInvariantException(RaftInvariantException.Category.DATA_CORRUPTION,
                            "rocksdb log entry corrupt at " + k, ipe);
                }
                size += e.getSerializedSize();
                if (!result.isEmpty() && size > maxSize) break;
                result.add(e);
                it.next();
            }
        }
        return result;
    }

    @Override
    public synchronized long term(long i) throws RaftException {
        // Treat the snapshot point (or virtual index 0 on fresh storage)
        // as a dummy with stored term, matching MemoryStorage's
        // dummy-entry-at-snap-index semantics. Callers (RaftLog.lastEntryID
        // etc.) rely on term(0)==0 on bootstrap.
        Eraftpb.Snapshot snap = readSnapshotInternal();
        long snapIdx = snap == null ? 0 : snap.getMetadata().getIndex();
        long snapTerm = snap == null ? 0 : snap.getMetadata().getTerm();
        if (i == snapIdx) return snapTerm;
        if (i < snapIdx) throw RaftException.ErrCompacted;
        if (i > lastIndex()) throw RaftException.ErrUnavailable;
        try {
            byte[] v = db.get(cfLog, indexKey(i));
            if (v == null) throw RaftException.ErrUnavailable;
            return Eraftpb.Entry.parseFrom(v).getTerm();
        } catch (InvalidProtocolBufferException e) {
            throw new RaftInvariantException(RaftInvariantException.Category.DATA_CORRUPTION,
                    "rocksdb log entry corrupt at " + i, e);
        } catch (RocksDBException e) {
            throw new RaftInvariantException(RaftInvariantException.Category.STORAGE_IO,
                    "rocksdb term read failed at " + i, e);
        }
    }

    @Override
    public synchronized long lastIndex() {
        try (RocksIterator it = db.newIterator(cfLog)) {
            it.seekToLast();
            if (it.isValid()) return decodeIndex(it.key());
        }
        // No log entries: lastIndex = snapshot.index (or 0 if no snapshot).
        Eraftpb.Snapshot snap = readSnapshotInternal();
        return snap == null ? 0 : snap.getMetadata().getIndex();
    }

    @Override
    public synchronized long firstIndex() {
        // With snapshot: firstIndex = snap.index + 1.
        // Without snapshot: firstIndex = lowest stored log key, or 1 if log is empty.
        // Note: unlike MemoryStorage we do NOT keep a dummy entry at the
        // compaction point — log entries here are keyed by their actual
        // raft index. compact() / applySnapshot() leave the log starting
        // at compactIndex+1, which is correctly the firstIndex.
        Eraftpb.Snapshot snap = readSnapshotInternal();
        if (snap != null && snap.getMetadata().getIndex() > 0) {
            return snap.getMetadata().getIndex() + 1;
        }
        try (RocksIterator it = db.newIterator(cfLog)) {
            it.seekToFirst();
            if (it.isValid()) {
                return decodeIndex(it.key());
            }
        }
        return 1;
    }

    @Override
    public synchronized Eraftpb.Snapshot snapshot() throws RaftException {
        Eraftpb.Snapshot snap = readSnapshotInternal();
        return snap == null ? Eraftpb.Snapshot.getDefaultInstance() : snap;
    }

    // ====================== Storage write API ======================

    @Override
    public synchronized void setHardState(Eraftpb.HardState hs) {
        try (WriteBatch wb = new WriteBatch()) {
            wb.put(cfState, KEY_HARD_STATE, hs.toByteArray());
            db.write(writeOpts, wb);
        } catch (RocksDBException e) {
            throw new RaftInvariantException(RaftInvariantException.Category.STORAGE_IO,
                    "rocksdb setHardState failed", e);
        }
    }

    @Override
    public synchronized void append(List<Eraftpb.Entry> entries) {
        if (entries == null || entries.isEmpty()) return;
        try (WriteBatch wb = new WriteBatch()) {
            appendIntoBatch(wb, entries);
            db.write(writeOpts, wb);
        } catch (RocksDBException e) {
            throw new RaftInvariantException(RaftInvariantException.Category.STORAGE_IO,
                    "rocksdb append failed", e);
        }
    }

    @Override
    public synchronized void applySnapshot(Eraftpb.Snapshot snap) throws RaftException {
        if (alreadyInstalledOutOfBand(snap)) {
            // Metadata-only re-apply of a snapshot already streamed into the
            // side-car; persisting it inline would orphan the side-car. No-op.
            return;
        }
        Eraftpb.Snapshot existing = readSnapshotInternal();
        if (existing != null && existing.getMetadata().getIndex() >= snap.getMetadata().getIndex()) {
            throw RaftException.ErrSnapOutOfDate;
        }
        String prevFile = readSnapshotFileName();
        try (WriteBatch wb = new WriteBatch()) {
            wb.put(cfSnap, KEY_SNAPSHOT, snap.toByteArray());
            // Inline payload: this snapshot is no longer side-car backed.
            wb.delete(cfState, KEY_SNAPSHOT_FILE);
            // Discard log entries up through snap.index inclusive.
            long snapIdx = snap.getMetadata().getIndex();
            wb.deleteRange(cfLog, indexKey(0), indexKey(snapIdx + 1));
            db.write(writeOpts, wb);
        } catch (RocksDBException e) {
            throw new RaftInvariantException(RaftInvariantException.Category.STORAGE_IO,
                    "rocksdb applySnapshot failed", e);
        }
        deleteOldSidecar(prevFile, null);
    }

    @Override
    public synchronized Eraftpb.Snapshot createSnapshot(long i, Eraftpb.ConfState cs, byte[] data) throws RaftException {
        Eraftpb.Snapshot existing = readSnapshotInternal();
        if (existing != null && i <= existing.getMetadata().getIndex()) {
            throw RaftException.ErrSnapOutOfDate;
        }
        if (i > lastIndex()) {
            throw new RaftInvariantException("createSnapshot " + i + " > lastIndex " + lastIndex());
        }
        long term;
        try {
            term = term(i);
        } catch (RaftException e) {
            throw new RaftInvariantException("createSnapshot: term(" + i + ") failed: " + e.code(), e);
        }
        Eraftpb.SnapshotMetadata.Builder mb = Eraftpb.SnapshotMetadata.newBuilder()
                .setIndex(i).setTerm(term);
        if (cs != null) mb.setConfState(cs);
        Eraftpb.Snapshot snap = Eraftpb.Snapshot.newBuilder()
                .setMetadata(mb)
                .setData(data == null ? com.google.protobuf.ByteString.EMPTY : com.google.protobuf.ByteString.copyFrom(data))
                .build();
        String prevFile = readSnapshotFileName();
        try (WriteBatch wb = new WriteBatch()) {
            wb.put(cfSnap, KEY_SNAPSHOT, snap.toByteArray());
            wb.delete(cfState, KEY_SNAPSHOT_FILE);
            db.write(writeOpts, wb);
        } catch (RocksDBException e) {
            throw new RaftInvariantException(RaftInvariantException.Category.STORAGE_IO,
                    "rocksdb createSnapshot failed", e);
        }
        deleteOldSidecar(prevFile, null);
        return snap;
    }

    @Override
    public synchronized void compact(long compactIndex) throws RaftException {
        long first = firstIndex();
        if (compactIndex < first) throw RaftException.ErrCompacted;
        long last = lastIndex();
        if (compactIndex > last) {
            throw new RaftInvariantException("compact " + compactIndex + " > lastIndex " + last);
        }
        try {
            db.deleteRange(cfLog, indexKey(0), indexKey(compactIndex + 1));
        } catch (RocksDBException e) {
            throw new RaftInvariantException(RaftInvariantException.Category.STORAGE_IO,
                    "rocksdb compact failed", e);
        }
    }

    // ====================== Streaming snapshots (side-car file) ======================

    @Override
    public boolean supportsStreamingSnapshot() {
        return true;
    }

    @Override
    public synchronized Eraftpb.Snapshot createSnapshotStreaming(long i, Eraftpb.ConfState cs, SnapshotDataWriter writer)
            throws RaftException, IOException {
        Eraftpb.Snapshot existing = readSnapshotInternal();
        if (existing != null && i <= existing.getMetadata().getIndex()) {
            throw RaftException.ErrSnapOutOfDate;
        }
        if (i > lastIndex()) {
            throw new RaftInvariantException("createSnapshot " + i + " > lastIndex " + lastIndex());
        }
        long term;
        try {
            term = term(i);
        } catch (RaftException e) {
            throw new RaftInvariantException("createSnapshot: term(" + i + ") failed: " + e.code(), e);
        }

        String fileName = sidecarName(i, term);
        // Stream the application payload straight to a side-car file; never
        // hold it all in heap. temp -> fsync -> atomic rename -> dir fsync.
        Path finalPath = snapDir.resolve(fileName);
        Path tmpPath = snapDir.resolve(fileName + ".tmp");
        try (OutputStream out = new BufferedOutputStream(Files.newOutputStream(
                tmpPath, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE))) {
            writer.writeTo(out);
            out.flush();
        }
        fsyncFile(tmpPath);
        Files.move(tmpPath, finalPath, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        fsyncDir(snapDir);

        Eraftpb.SnapshotMetadata.Builder mb = Eraftpb.SnapshotMetadata.newBuilder()
                .setIndex(i).setTerm(term);
        if (cs != null) mb.setConfState(cs);
        // Metadata only — payload lives in the side-car file, not inline.
        Eraftpb.Snapshot meta = Eraftpb.Snapshot.newBuilder().setMetadata(mb).build();

        String prevFile = readSnapshotFileName();
        try (WriteBatch wb = new WriteBatch()) {
            wb.put(cfSnap, KEY_SNAPSHOT, meta.toByteArray());
            wb.put(cfState, KEY_SNAPSHOT_FILE, bytes(fileName));
            db.write(writeOpts, wb);
        } catch (RocksDBException e) {
            throw new RaftInvariantException(RaftInvariantException.Category.STORAGE_IO,
                    "rocksdb streaming createSnapshot failed", e);
        }
        deleteOldSidecar(prevFile, fileName);
        return meta;
    }

    @Override
    public synchronized void applySnapshot(Eraftpb.Snapshot meta, InputStream data)
            throws RaftException, IOException {
        long idx = meta.getMetadata().getIndex();
        long term = meta.getMetadata().getTerm();
        Eraftpb.Snapshot existing = readSnapshotInternal();
        if (existing != null && existing.getMetadata().getIndex() >= idx) {
            throw RaftException.ErrSnapOutOfDate;
        }

        String fileName = sidecarName(idx, term);
        Path finalPath = snapDir.resolve(fileName);
        Path tmpPath = snapDir.resolve(fileName + ".tmp");
        try (OutputStream out = new BufferedOutputStream(Files.newOutputStream(
                tmpPath, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE))) {
            data.transferTo(out);
            out.flush();
        }
        fsyncFile(tmpPath);
        Files.move(tmpPath, finalPath, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        fsyncDir(snapDir);

        // Persist metadata only (strip any inline data the caller may have set).
        Eraftpb.Snapshot metaOnly = meta.toBuilder().clearData().build();
        String prevFile = readSnapshotFileName();
        try (WriteBatch wb = new WriteBatch()) {
            wb.put(cfSnap, KEY_SNAPSHOT, metaOnly.toByteArray());
            wb.put(cfState, KEY_SNAPSHOT_FILE, bytes(fileName));
            wb.deleteRange(cfLog, indexKey(0), indexKey(idx + 1));
            db.write(writeOpts, wb);
        } catch (RocksDBException e) {
            throw new RaftInvariantException(RaftInvariantException.Category.STORAGE_IO,
                    "rocksdb streaming applySnapshot failed", e);
        }
        deleteOldSidecar(prevFile, fileName);
    }

    /**
     * Stage an inbound out-of-band snapshot payload durably WITHOUT yet
     * committing the snapshot metadata or truncating the log. This is the
     * follower-side counterpart used by the zero-copy install path: the payload
     * is written to its side-car (temp → fsync → atomic rename → dir fsync), but
     * {@code KEY_SNAPSHOT} / {@code KEY_SNAPSHOT_FILE} stay untouched so the raft
     * core's {@code restore} still sees the OLD storage state and performs a real
     * restore (a premature metadata commit would make the core believe it already
     * has the entry — {@code matchTerm} reads through to storage — and "ignore"
     * the snapshot, stranding {@code applied} behind a compacted log).
     *
     * <p>The Ready cycle that follows the core's restore calls
     * {@link #writeBatched} with the metadata-only snapshot; it detects the
     * staged side-car and finalizes the link + log truncation atomically.
     *
     * <p>If the local snapshot is already at/ahead of {@code meta}, the payload
     * is drained and discarded (no-op): the core will ignore it too.
     */
    public synchronized void stageSnapshotData(Eraftpb.Snapshot meta, InputStream data)
            throws RaftException, IOException {
        long idx = meta.getMetadata().getIndex();
        long term = meta.getMetadata().getTerm();
        Eraftpb.Snapshot existing = readSnapshotInternal();
        if (existing != null && existing.getMetadata().getIndex() >= idx) {
            data.transferTo(OutputStream.nullOutputStream());
            return;
        }
        String fileName = sidecarName(idx, term);
        Path finalPath = snapDir.resolve(fileName);
        Path tmpPath = snapDir.resolve(fileName + ".tmp");
        try (OutputStream out = new BufferedOutputStream(Files.newOutputStream(
                tmpPath, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE))) {
            data.transferTo(out);
            out.flush();
        }
        fsyncFile(tmpPath);
        Files.move(tmpPath, finalPath, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        fsyncDir(snapDir);
        // Intentionally NOT linked yet: writeBatched(rd.snapshot) finalizes after
        // the core restores.
    }

    @Override
    public synchronized InputStream openSnapshotData(Eraftpb.Snapshot snap) throws RaftException, IOException {
        // Use the side-car file only when it backs the *current* snapshot;
        // otherwise fall back to whatever inline payload the caller passed.
        String fileName = readSnapshotFileName();
        if (fileName != null) {
            Eraftpb.Snapshot cur = readSnapshotInternal();
            long curIdx = cur == null ? 0 : cur.getMetadata().getIndex();
            long wantIdx = snap == null ? curIdx : snap.getMetadata().getIndex();
            Path p = snapDir.resolve(fileName);
            if (wantIdx == curIdx && Files.exists(p)) {
                return new BufferedInputStream(Files.newInputStream(p, StandardOpenOption.READ));
            }
        }
        com.google.protobuf.ByteString inline =
                snap != null ? snap.getData() : com.google.protobuf.ByteString.EMPTY;
        return inline.newInput();
    }

    /**
     * True when {@code snap} is a metadata-only snapshot whose index already
     * matches the currently-stored side-car-backed snapshot — i.e. its payload
     * was already streamed in durably via
     * {@link #applySnapshot(Eraftpb.Snapshot, InputStream)}. Re-persisting it
     * through an inline path would drop the side-car pointer, so callers skip it.
     */
    private boolean alreadyInstalledOutOfBand(Eraftpb.Snapshot snap) {
        if (snap == null || !snap.getData().isEmpty()) {
            return false;
        }
        String curFile = readSnapshotFileName();
        if (curFile == null) {
            return false;
        }
        Eraftpb.Snapshot cur = readSnapshotInternal();
        return cur != null && cur.getMetadata().getIndex() == snap.getMetadata().getIndex();
    }

    private String readSnapshotFileName() {
        try {
            byte[] v = db.get(cfState, KEY_SNAPSHOT_FILE);
            return v == null ? null : new String(v, StandardCharsets.UTF_8);
        } catch (RocksDBException e) {
            throw new RaftInvariantException(RaftInvariantException.Category.STORAGE_IO,
                    "rocksdb snapshot-file read failed", e);
        }
    }

    private void deleteOldSidecar(String prevFile, String keep) {
        if (prevFile == null || prevFile.equals(keep)) return;
        try {
            Files.deleteIfExists(snapDir.resolve(prevFile));
        } catch (IOException e) {
            LOG.warn("failed to delete stale snapshot file {}: {}", prevFile, e.toString());
        }
    }

    private static String sidecarName(long index, long term) {
        return "snap-" + index + "-" + term + ".data";
    }

    private static void fsyncFile(Path p) throws IOException {
        try (FileChannel ch = FileChannel.open(p, StandardOpenOption.WRITE)) {
            ch.force(true);
        }
    }

    private static void fsyncDir(Path dir) {
        // Directory fsync makes the rename durable on POSIX. Best-effort:
        // some platforms (e.g. Windows) reject opening a directory channel.
        try (FileChannel ch = FileChannel.open(dir, StandardOpenOption.READ)) {
            ch.force(true);
        } catch (IOException ignored) {
            // directory fsync unsupported on this platform — tolerate it
        }
    }

    private static void cleanupTempSnapshotFiles(Path dir) {
        try (DirectoryStream<Path> ds = Files.newDirectoryStream(dir, "*.tmp")) {
            for (Path p : ds) {
                try { Files.deleteIfExists(p); } catch (IOException ignored) { /* best-effort */ }
            }
        } catch (IOException ignored) {
            // directory not readable yet — nothing to clean
        }
    }

    @Override
    public synchronized void close() {
        if (closed) return;
        closed = true;
        for (ColumnFamilyHandle h : cfHandles) {
            try { h.close(); } catch (Throwable ignored) { /* leaking native handle on shutdown is fine */ }
        }
        try { writeOpts.close(); } catch (Throwable ignored) {}
        try { db.close(); } catch (Throwable ignored) {}
    }

    // ====================== Applied-index watermark ======================

    /**
     * Read the persisted applied-index watermark, or 0 if never written.
     * Hosts pass this back as {@link io.github.xinfra.lab.raft.Config#applied}
     * on restart so raft does NOT re-deliver previously-applied entries.
     */
    public synchronized long getApplied() {
        try {
            byte[] v = db.get(cfState, KEY_APPLIED);
            if (v == null || v.length != 8) return 0;
            return ByteBuffer.wrap(v).getLong();
        } catch (RocksDBException e) {
            throw new RaftInvariantException(RaftInvariantException.Category.STORAGE_IO,
                    "rocksdb getApplied failed", e);
        }
    }

    /**
     * Persist the applied-index watermark. Hosts should call this after
     * the application's apply for index {@code applied} is durable on
     * the application's own state machine. Calling more often than
     * necessary is fine; calling less often risks duplicate apply on
     * restart.
     */
    public synchronized void setApplied(long applied) {
        try {
            byte[] v = ByteBuffer.allocate(8).putLong(applied).array();
            db.put(cfState, writeOpts, KEY_APPLIED, v);
        } catch (RocksDBException e) {
            throw new RaftInvariantException(RaftInvariantException.Category.STORAGE_IO,
                    "rocksdb setApplied failed", e);
        }
    }

    /**
     * Persist the current cluster {@link Eraftpb.ConfState}. The host
     * should call this each time it applies a ConfChange entry so a
     * subsequent restart recovers the membership without replaying the
     * log into raft (raft itself doesn't track conf-state durability —
     * it asks {@link #initialState()} once at boot).
     */
    public synchronized void setConfState(Eraftpb.ConfState cs) {
        try {
            db.put(cfState, writeOpts, KEY_CONF_STATE, cs.toByteArray());
        } catch (RocksDBException e) {
            throw new RaftInvariantException(RaftInvariantException.Category.STORAGE_IO,
                    "rocksdb setConfState failed", e);
        }
    }

    // ====================== Atomic Ready-cycle helper ======================

    /**
     * Atomically apply the entries / hardState / snapshot from a single
     * Ready cycle. This is the recommended host integration:
     *
     * <pre>{@code
     *   storage.writeBatched(rd.entries, rd.hardState, rd.snapshot);
     * }</pre>
     */
    public synchronized void writeBatched(List<Eraftpb.Entry> entries,
                                          Eraftpb.HardState hs,
                                          Eraftpb.Snapshot snap) {
        boolean snapApplied = snap != null && !isEmptySnap(snap);
        // Side-car file name to LINK when finalizing an out-of-band snapshot
        // whose payload was already staged durably via stageSnapshotData; null
        // means the inline path (drop any side-car pointer).
        String linkFile = null;
        if (snapApplied && alreadyInstalledOutOfBand(snap)) {
            // This exact metadata-only snapshot is already finalized (KEY_SNAPSHOT
            // + KEY_SNAPSHOT_FILE both point at it). Re-persisting it would be a
            // no-op at best and could orphan the side-car at worst, so skip the
            // snapshot sub-batch and keep entries/hardState only.
            snapApplied = false;
        } else if (snapApplied && snap.getData().isEmpty()) {
            // Metadata-only snapshot: the payload travelled out-of-band. If its
            // side-car was staged durably (stageSnapshotData), finalize by LINKING
            // it here instead of dropping the pointer — this is the Ready cycle
            // that runs after the core restored the snapshot. (A genuinely empty
            // payload with no staged file falls through to the inline path.)
            String staged = sidecarName(snap.getMetadata().getIndex(), snap.getMetadata().getTerm());
            if (Files.exists(snapDir.resolve(staged))) {
                linkFile = staged;
            }
        }
        String prevFile = snapApplied ? readSnapshotFileName() : null;
        try (WriteBatch wb = new WriteBatch()) {
            if (entries != null && !entries.isEmpty()) appendIntoBatch(wb, entries);
            if (hs != null && !isEmptyHardState(hs)) {
                wb.put(cfState, KEY_HARD_STATE, hs.toByteArray());
            }
            if (snapApplied) {
                wb.put(cfSnap, KEY_SNAPSHOT, snap.toByteArray());
                if (linkFile != null) {
                    // Out-of-band: point at the already-staged side-car payload.
                    wb.put(cfState, KEY_SNAPSHOT_FILE, bytes(linkFile));
                } else {
                    // Inline payload via writeBatched: drop any side-car pointer.
                    wb.delete(cfState, KEY_SNAPSHOT_FILE);
                }
                long snapIdx = snap.getMetadata().getIndex();
                wb.deleteRange(cfLog, indexKey(0), indexKey(snapIdx + 1));
            }
            db.write(writeOpts, wb);
        } catch (RocksDBException e) {
            throw new RaftInvariantException(RaftInvariantException.Category.STORAGE_IO,
                    "rocksdb writeBatched failed", e);
        }
        // Reclaim the previous side-car, but never the one we just linked.
        if (snapApplied) deleteOldSidecar(prevFile, linkFile);
    }

    private void appendIntoBatch(WriteBatch wb, List<Eraftpb.Entry> entries) throws RocksDBException {
        // Truncate any uncommitted suffix that conflicts with the new
        // entries' starting index.
        long fromIdx = entries.get(0).getIndex();
        long currentLast = lastIndex();
        if (fromIdx <= currentLast) {
            wb.deleteRange(cfLog, indexKey(fromIdx), indexKey(currentLast + 1));
        }
        for (Eraftpb.Entry e : entries) {
            wb.put(cfLog, indexKey(e.getIndex()), e.toByteArray());
        }
    }

    private Eraftpb.Snapshot readSnapshotInternal() {
        try {
            byte[] v = db.get(cfSnap, KEY_SNAPSHOT);
            if (v == null) return null;
            return Eraftpb.Snapshot.parseFrom(v);
        } catch (InvalidProtocolBufferException e) {
            throw new RaftInvariantException(RaftInvariantException.Category.DATA_CORRUPTION,
                    "rocksdb snapshot parse failed (corrupt snapshot metadata)", e);
        } catch (RocksDBException e) {
            throw new RaftInvariantException(RaftInvariantException.Category.STORAGE_IO,
                    "rocksdb snapshot read failed", e);
        }
    }

    private static boolean isEmptyHardState(Eraftpb.HardState hs) {
        return hs.getTerm() == 0 && hs.getVote() == 0 && hs.getCommit() == 0;
    }

    private static boolean isEmptySnap(Eraftpb.Snapshot s) {
        return !s.hasMetadata() || s.getMetadata().getIndex() == 0;
    }

    private static byte[] indexKey(long i) {
        return ByteBuffer.allocate(8).putLong(i).array();
    }

    private static long decodeIndex(byte[] key) {
        return ByteBuffer.wrap(key).getLong();
    }

    private static byte[] bytes(String s) {
        return s.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }
}
