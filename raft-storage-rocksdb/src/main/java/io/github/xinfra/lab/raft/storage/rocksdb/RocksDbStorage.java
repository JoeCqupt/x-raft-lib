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
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.LRUCache;
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
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

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
    private final ColumnFamilyHandle cfLog;
    private final ColumnFamilyHandle cfState;
    private final ColumnFamilyHandle cfSnap;
    private final WriteOptions writeOpts;
    private final Path snapDir;

    private final LRUCache blockCache;
    private final BloomFilter bloomFilter;

    /**
     * ReadWriteLock replaces the former single mutex. Read-only storage queries
     * (term, lastIndex, entries, …) acquire the shared read-lock and therefore
     * proceed concurrently. Mutating operations (writeBatched, compact,
     * createSnapshot, …) acquire the exclusive write-lock. This eliminates the
     * scenario where a slow RocksDB iterator read (e.g. lastIndex seekToLast
     * during background compaction) blocks the hot-path writeBatched call for
     * hundreds of milliseconds.
     */
    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final Lock readLock = rwLock.readLock();
    private final Lock writeLock = rwLock.writeLock();

    /**
     * Cached index values maintained incrementally by write operations.
     * Reads are completely lock-free (volatile guarantees visibility).
     * Writes happen only under writeLock — no concurrent updates possible.
     */
    private volatile long cachedLastIndex;
    private volatile long cachedFirstIndex;

    private volatile boolean closed = false;
    private static final ThreadLocal<String> lockHolder = ThreadLocal.withInitial(() -> "none");
    private static final ThreadLocal<Long> lockHeldSince = ThreadLocal.withInitial(() -> 0L);

    public RocksDbStorage(Path dir) throws RocksDBException, IOException {
        this(dir, RocksDbStorageOptions.DEFAULT);
    }

    public RocksDbStorage(Path dir, boolean fsync) throws RocksDBException, IOException {
        this(dir, RocksDbStorageOptions.builder().fsync(fsync).build());
    }

    public RocksDbStorage(Path dir, RocksDbStorageOptions options) throws RocksDBException, IOException {
        Files.createDirectories(dir);
        this.snapDir = dir.resolve(SNAP_SUBDIR);
        Files.createDirectories(snapDir);
        cleanupTempSnapshotFiles(snapDir);

        this.blockCache = new LRUCache(options.blockCacheSize);
        this.bloomFilter = options.bloomFilterBitsPerKey > 0
                ? new BloomFilter(options.bloomFilterBitsPerKey)
                : null;

        try (Options opts = new Options().setCreateIfMissing(true).setCreateMissingColumnFamilies(true)) {
            List<byte[]> existing;
            try {
                existing = RocksDB.listColumnFamilies(opts, dir.toString());
            } catch (RocksDBException e) {
                existing = new ArrayList<>();
            }

            BlockBasedTableConfig tableConfig = new BlockBasedTableConfig()
                    .setBlockCache(blockCache)
                    .setBlockSize(options.blockSize);
            if (bloomFilter != null) {
                tableConfig.setFilterPolicy(bloomFilter)
                        .setCacheIndexAndFilterBlocks(true);
            }

            ColumnFamilyOptions cfOpts = new ColumnFamilyOptions()
                    .setTableFormatConfig(tableConfig)
                    .setWriteBufferSize(options.writeBufferSize)
                    .setMaxWriteBufferNumber(options.maxWriteBufferNumber)
                    .setCompressionType(options.compressionType)
                    .setCompactionStyle(options.compactionStyle);

            List<ColumnFamilyDescriptor> descriptors = new ArrayList<>();
            for (byte[] name : Arrays.asList(CF_DEFAULT, CF_LOG, CF_STATE, CF_SNAPSHOT)) {
                descriptors.add(new ColumnFamilyDescriptor(name, cfOpts));
            }
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
                    .setCreateMissingColumnFamilies(true)
                    .setMaxBackgroundJobs(options.maxBackgroundJobs);
            this.db = RocksDB.open(dbOpts, dir.toString(), descriptors, this.cfHandles);
            // cfHandles.get(0) is the RocksDB default CF (unused by raft)
            this.cfLog = cfHandles.get(1);
            this.cfState = cfHandles.get(2);
            this.cfSnap = cfHandles.get(3);
            this.writeOpts = new WriteOptions().setSync(options.fsync);
        }

        // Cold-start: compute index caches from the DB state (one-time seekToLast/seekToFirst).
        this.cachedLastIndex = computeRawLastIndex();
        this.cachedFirstIndex = computeRawFirstIndex();
    }

    private void enterLock(String methodName) {
        lockHolder.set(methodName);
        lockHeldSince.set(System.nanoTime());
    }

    private void exitLock() {
        long startNs = lockHeldSince.get();
        long heldMs = startNs == 0 ? 0 : (System.nanoTime() - startNs) / 1_000_000;
        String holder = lockHolder.get();
        lockHolder.set("none");
        lockHeldSince.set(0L);
        if (heldMs > 100) {
            LOG.warn("lock held too long by {}: {}ms", holder, heldMs);
        }
    }

    // ====================== Storage read API ======================

    @Override
    public InitialStateResult initialState() {
        readLock.lock();
        enterLock("initialState");
        try {
            byte[] hsBytes = db.get(cfState, KEY_HARD_STATE);
            Eraftpb.HardState hs = hsBytes == null
                    ? Eraftpb.HardState.getDefaultInstance()
                    : Eraftpb.HardState.parseFrom(hsBytes);
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
        } finally {
            exitLock();
            readLock.unlock();
        }
    }

    @Override
    public  List<Eraftpb.Entry> entries(long lo, long hi, long maxSize) throws RaftException {
        readLock.lock();
        enterLock("entries");
        try {
            long first = firstIndexNoLock();
            long last = lastIndexNoLock();
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
        } finally {
            exitLock();
            readLock.unlock();
        }
    }

    @Override
    public  long term(long i) throws RaftException {
        readLock.lock();
        enterLock("term");
        try {
            return termNoLock(i);
        } finally {
            exitLock();
            readLock.unlock();
        }
    }

    private long termNoLock(long i) throws RaftException {
        Eraftpb.Snapshot snap = readSnapshotInternal();
        long snapIdx = snap == null ? 0 : snap.getMetadata().getIndex();
        long snapTerm = snap == null ? 0 : snap.getMetadata().getTerm();
        if (i == snapIdx) return snapTerm;
        if (i < snapIdx) throw RaftException.ErrCompacted;
        if (i > lastIndexNoLock()) throw RaftException.ErrUnavailable;
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
    public  long lastIndex() {
        return cachedLastIndex;
    }

    /** Returns cached lastIndex — O(1), no I/O. Used within locked contexts. */
    private long lastIndexNoLock() {
        return cachedLastIndex;
    }

    @Override
    public  long firstIndex() {
        return cachedFirstIndex;
    }

    /** Returns cached firstIndex — O(1), no I/O. Used within locked contexts. */
    private long firstIndexNoLock() {
        return cachedFirstIndex;
    }

    /**
     * Computes lastIndex from the DB. Only called at construction time.
     */
    private long computeRawLastIndex() {
        try (RocksIterator it = db.newIterator(cfLog)) {
            it.seekToLast();
            if (it.isValid()) return decodeIndex(it.key());
        }
        Eraftpb.Snapshot snap = readSnapshotInternal();
        return snap == null ? 0 : snap.getMetadata().getIndex();
    }

    /**
     * Computes firstIndex from the DB. Only called at construction time.
     */
    private long computeRawFirstIndex() {
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
    public  Eraftpb.Snapshot snapshot() throws RaftException {
        readLock.lock();
        enterLock("snapshot");
        try {
            Eraftpb.Snapshot snap = readSnapshotInternal();
            return snap == null ? Eraftpb.Snapshot.getDefaultInstance() : snap;
        } finally {
            exitLock();
            readLock.unlock();
        }
    }

    // ====================== Storage write API ======================

    @Override
    public  void setHardState(Eraftpb.HardState hs) {
        writeLock.lock();
        enterLock("setHardState");
        try {
            try (WriteBatch wb = new WriteBatch()) {
                wb.put(cfState, KEY_HARD_STATE, hs.toByteArray());
                db.write(writeOpts, wb);
            } catch (RocksDBException e) {
                throw new RaftInvariantException(RaftInvariantException.Category.STORAGE_IO,
                        "rocksdb setHardState failed", e);
            }
        } finally {
            exitLock();
            writeLock.unlock();
        }
    }

    @Override
    public  void append(List<Eraftpb.Entry> entries) {
        long tBefore = System.nanoTime();
        writeLock.lock();
        long waitMs = (System.nanoTime() - tBefore) / 1_000_000;
        if (waitMs > 50) {
            LOG.warn("lock contention entering append: waited {}ms", waitMs);
        }
        enterLock("append");
        try {
            if (entries == null || entries.isEmpty()) return;
            try (WriteBatch wb = new WriteBatch()) {
                appendIntoBatch(wb, entries);
                db.write(writeOpts, wb);
            } catch (RocksDBException e) {
                throw new RaftInvariantException(RaftInvariantException.Category.STORAGE_IO,
                        "rocksdb append failed", e);
            }
            cachedLastIndex = entries.get(entries.size() - 1).getIndex();
        } finally {
            exitLock();
            writeLock.unlock();
        }
    }

    @Override
    public  void applySnapshot(Eraftpb.Snapshot snap) throws RaftException {
        long tBefore = System.nanoTime();
        writeLock.lock();
        long waitMs = (System.nanoTime() - tBefore) / 1_000_000;
        if (waitMs > 50) {
            LOG.warn("lock contention entering applySnapshot: waited {}ms", waitMs);
        }
        enterLock("applySnapshot");
        try {
            if (alreadyInstalledOutOfBand(snap)) {
                return;
            }
            Eraftpb.Snapshot existing = readSnapshotInternal();
            if (existing != null && existing.getMetadata().getIndex() >= snap.getMetadata().getIndex()) {
                throw RaftException.ErrSnapOutOfDate;
            }
            String prevFile = readSnapshotFileName();
            long snapIdx = snap.getMetadata().getIndex();
            try (WriteBatch wb = new WriteBatch()) {
                wb.put(cfSnap, KEY_SNAPSHOT, snap.toByteArray());
                wb.delete(cfState, KEY_SNAPSHOT_FILE);
                wb.deleteRange(cfLog, indexKey(0), indexKey(snapIdx + 1));
                db.write(writeOpts, wb);
            } catch (RocksDBException e) {
                throw new RaftInvariantException(RaftInvariantException.Category.STORAGE_IO,
                        "rocksdb applySnapshot failed", e);
            }
            cachedFirstIndex = snapIdx + 1;
            if (cachedLastIndex < snapIdx) cachedLastIndex = snapIdx;
            deleteOldSidecar(prevFile, null);
        } finally {
            exitLock();
            writeLock.unlock();
        }
    }

    @Override
    public  Eraftpb.Snapshot createSnapshot(long i, Eraftpb.ConfState cs, byte[] data) throws RaftException {
        long tBefore = System.nanoTime();
        writeLock.lock();
        long waitMs = (System.nanoTime() - tBefore) / 1_000_000;
        if (waitMs > 50) {
            LOG.warn("lock contention entering createSnapshot: waited {}ms", waitMs);
        }
        enterLock("createSnapshot");
        try {
            Eraftpb.Snapshot existing = readSnapshotInternal();
            if (existing != null && i <= existing.getMetadata().getIndex()) {
                throw RaftException.ErrSnapOutOfDate;
            }
            if (i > lastIndexNoLock()) {
                throw new RaftInvariantException("createSnapshot " + i + " > lastIndex " + lastIndexNoLock());
            }
            long term;
            try {
                term = termNoLock(i);
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
            cachedFirstIndex = i + 1;
            deleteOldSidecar(prevFile, null);
            return snap;
        } finally {
            exitLock();
            writeLock.unlock();
        }
    }

    @Override
    public  void compact(long compactIndex) throws RaftException {
        long tBefore = System.nanoTime();
        writeLock.lock();
        long waitMs = (System.nanoTime() - tBefore) / 1_000_000;
        if (waitMs > 50) {
            LOG.warn("lock contention entering compact: waited {}ms", waitMs);
        }
        enterLock("compact");
        try {
            long first = firstIndexNoLock();
            if (compactIndex < first) throw RaftException.ErrCompacted;
            long last = lastIndexNoLock();
            if (compactIndex > last) {
                throw new RaftInvariantException("compact " + compactIndex + " > lastIndex " + last);
            }
            try {
                db.deleteRange(cfLog, indexKey(0), indexKey(compactIndex + 1));
            } catch (RocksDBException e) {
                throw new RaftInvariantException(RaftInvariantException.Category.STORAGE_IO,
                        "rocksdb compact failed", e);
            }
            if (cachedFirstIndex < compactIndex + 1) {
                cachedFirstIndex = compactIndex + 1;
            }
        } finally {
            exitLock();
            writeLock.unlock();
        }
    }

    // ====================== Streaming snapshots (side-car file) ======================

    @Override
    public boolean supportsStreamingSnapshot() {
        return true;
    }

    @Override
    public  Eraftpb.Snapshot createSnapshotStreaming(long i, Eraftpb.ConfState cs, SnapshotDataWriter writer)
            throws RaftException, IOException {
        // Phase 1: validate under write lock (short critical section).
        long term;
        long tBefore = System.nanoTime();
        writeLock.lock();
        long waitMs = (System.nanoTime() - tBefore) / 1_000_000;
        if (waitMs > 50) {
            LOG.warn("lock contention entering createSnapshotStreaming: waited {}ms", waitMs);
        }
        enterLock("createSnapshotStreaming:validate");
        try {
            Eraftpb.Snapshot existing = readSnapshotInternal();
            if (existing != null && i <= existing.getMetadata().getIndex()) {
                throw RaftException.ErrSnapOutOfDate;
            }
            if (i > lastIndexNoLock()) {
                throw new RaftInvariantException("createSnapshot " + i + " > lastIndex " + lastIndexNoLock());
            }
            try {
                term = termNoLock(i);
            } catch (RaftException e) {
                throw new RaftInvariantException("createSnapshot: term(" + i + ") failed: " + e.code(), e);
            }
        } finally {
            exitLock();
            writeLock.unlock();
        }

        // Phase 2: file I/O outside the lock (unique file name per index/term).
        String fileName = sidecarName(i, term);
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

        // Phase 3: persist metadata under write lock (short critical section).
        Eraftpb.SnapshotMetadata.Builder mb = Eraftpb.SnapshotMetadata.newBuilder()
                .setIndex(i).setTerm(term);
        if (cs != null) mb.setConfState(cs);
        Eraftpb.Snapshot meta = Eraftpb.Snapshot.newBuilder().setMetadata(mb).build();

        writeLock.lock();
        enterLock("createSnapshotStreaming:commit");
        try {
            String prevFile = readSnapshotFileName();
            try (WriteBatch wb = new WriteBatch()) {
                wb.put(cfSnap, KEY_SNAPSHOT, meta.toByteArray());
                wb.put(cfState, KEY_SNAPSHOT_FILE, bytes(fileName));
                db.write(writeOpts, wb);
            } catch (RocksDBException e) {
                throw new RaftInvariantException(RaftInvariantException.Category.STORAGE_IO,
                        "rocksdb streaming createSnapshot failed", e);
            }
            cachedFirstIndex = i + 1;
            deleteOldSidecar(prevFile, fileName);
            return meta;
        } finally {
            exitLock();
            writeLock.unlock();
        }
    }

    @Override
    public  void applySnapshot(Eraftpb.Snapshot meta, InputStream data)
            throws RaftException, IOException {
        long idx = meta.getMetadata().getIndex();
        long term = meta.getMetadata().getTerm();

        // Phase 1: validate under write lock (short critical section).
        long tBefore = System.nanoTime();
        writeLock.lock();
        long waitMs = (System.nanoTime() - tBefore) / 1_000_000;
        if (waitMs > 50) {
            LOG.warn("lock contention entering applySnapshot(stream): waited {}ms", waitMs);
        }
        enterLock("applySnapshot(stream):validate");
        try {
            Eraftpb.Snapshot existing = readSnapshotInternal();
            if (existing != null && existing.getMetadata().getIndex() >= idx) {
                throw RaftException.ErrSnapOutOfDate;
            }
        } finally {
            exitLock();
            writeLock.unlock();
        }

        // Phase 2: file I/O outside the lock.
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

        // Phase 3: persist metadata + truncate log under write lock.
        Eraftpb.Snapshot metaOnly = meta.toBuilder().clearData().build();
        writeLock.lock();
        enterLock("applySnapshot(stream):commit");
        try {
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
            cachedFirstIndex = idx + 1;
            if (cachedLastIndex < idx) cachedLastIndex = idx;
            deleteOldSidecar(prevFile, fileName);
        } finally {
            exitLock();
            writeLock.unlock();
        }
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
    public  void stageSnapshotData(Eraftpb.Snapshot meta, InputStream data)
            throws RaftException, IOException {
        long idx = meta.getMetadata().getIndex();
        long term = meta.getMetadata().getTerm();

        // Short critical section: check whether the incoming snapshot is stale.
        long tBefore = System.nanoTime();
        readLock.lock();
        long waitMs = (System.nanoTime() - tBefore) / 1_000_000;
        if (waitMs > 50) {
            LOG.warn("lock contention entering stageSnapshotData: waited {}ms", waitMs);
        }
        enterLock("stageSnapshotData");
        try {
            Eraftpb.Snapshot existing = readSnapshotInternal();
            if (existing != null && existing.getMetadata().getIndex() >= idx) {
                data.transferTo(OutputStream.nullOutputStream());
                return;
            }
        } finally {
            exitLock();
            readLock.unlock();
        }

        // File I/O outside the lock.
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
    }

    @Override
    public  InputStream openSnapshotData(Eraftpb.Snapshot snap) throws RaftException, IOException {
        readLock.lock();
        enterLock("openSnapshotData");
        try {
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
        } finally {
            exitLock();
            readLock.unlock();
        }
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
    public  void close() {
        writeLock.lock();
        enterLock("close");
        try {
            if (closed) return;
            closed = true;
            for (ColumnFamilyHandle h : cfHandles) {
                try {
                    h.close();
                } catch (Throwable t) {
                    LOG.warn("close column-family handle failed: {}", t.toString());
                }
            }
            try {
                writeOpts.close();
            } catch (Throwable t) {
                LOG.warn("close writeOpts failed: {}", t.toString());
            }
            try {
                db.close();
            } catch (Throwable t) {
                LOG.warn("close db failed: {}", t.toString());
            }
            try {
                if (bloomFilter != null) bloomFilter.close();
            } catch (Throwable t) {
                LOG.warn("close bloomFilter failed: {}", t.toString());
            }
            try {
                blockCache.close();
            } catch (Throwable t) {
                LOG.warn("close blockCache failed: {}", t.toString());
            }
        } finally {
            exitLock();
            writeLock.unlock();
        }
    }

    // ====================== Applied-index watermark ======================

    /**
     * Read the persisted applied-index watermark, or 0 if never written.
     * Hosts pass this back as {@link io.github.xinfra.lab.raft.Config#applied}
     * on restart so raft does NOT re-deliver previously-applied entries.
     */
    public  long getApplied() {
        readLock.lock();
        enterLock("getApplied");
        try {
            byte[] v = db.get(cfState, KEY_APPLIED);
            if (v == null || v.length != 8) return 0;
            return ByteBuffer.wrap(v).getLong();
        } catch (RocksDBException e) {
            throw new RaftInvariantException(RaftInvariantException.Category.STORAGE_IO,
                    "rocksdb getApplied failed", e);
        } finally {
            exitLock();
            readLock.unlock();
        }
    }

    /**
     * Persist the applied-index watermark. Hosts should call this after
     * the application's apply for index {@code applied} is durable on
     * the application's own state machine. Calling more often than
     * necessary is fine; calling less often risks duplicate apply on
     * restart.
     */
    public  void setApplied(long applied) {
        long tBefore = System.nanoTime();
        writeLock.lock();
        long waitMs = (System.nanoTime() - tBefore) / 1_000_000;
        if (waitMs > 50) {
            LOG.warn("lock contention entering setApplied: waited {}ms", waitMs);
        }
        enterLock("setApplied");
        try {
            byte[] v = ByteBuffer.allocate(8).putLong(applied).array();
            db.put(cfState, writeOpts, KEY_APPLIED, v);
        } catch (RocksDBException e) {
            throw new RaftInvariantException(RaftInvariantException.Category.STORAGE_IO,
                    "rocksdb setApplied failed", e);
        } finally {
            exitLock();
            writeLock.unlock();
        }
    }

    /**
     * Persist the current cluster {@link Eraftpb.ConfState}. The host
     * should call this each time it applies a ConfChange entry so a
     * subsequent restart recovers the membership without replaying the
     * log into raft (raft itself doesn't track conf-state durability —
     * it asks {@link #initialState()} once at boot).
     */
    public  void setConfState(Eraftpb.ConfState cs) {
        writeLock.lock();
        enterLock("setConfState");
        try {
            db.put(cfState, writeOpts, KEY_CONF_STATE, cs.toByteArray());
        } catch (RocksDBException e) {
            throw new RaftInvariantException(RaftInvariantException.Category.STORAGE_IO,
                    "rocksdb setConfState failed", e);
        } finally {
            exitLock();
            writeLock.unlock();
        }
    }

    // ====================== Atomic Ready-cycle helper ======================

    /**
     * Atomically apply the entries / hardState / snapshot from a single
     * Ready cycle. This is the recommended host integration:
     *
     * <pre>{@code
     *   storage.writeBatched(rd.entries(), rd.hardState(), rd.snapshot());
     * }</pre>
     */
    public void writeBatched(List<Eraftpb.Entry> entries,
                             Eraftpb.HardState hs,
                             Eraftpb.Snapshot snap) {
        long tBeforeLock = System.nanoTime();
        writeLock.lock();
        long tAfterLock = System.nanoTime();
        long lockWaitMs = (tAfterLock - tBeforeLock) / 1_000_000;
        if (lockWaitMs > 50) {
            LOG.warn("lock contention entering writeBatched: waited {}ms", lockWaitMs);
        }
        enterLock("writeBatched");
        try {
            long tStart = System.nanoTime();
            boolean snapApplied = snap != null && !isEmptySnap(snap);
            String linkFile = null;
            if (snapApplied && alreadyInstalledOutOfBand(snap)) {
                snapApplied = false;
            } else if (snapApplied && snap.getData().isEmpty()) {
                String staged = sidecarName(snap.getMetadata().getIndex(), snap.getMetadata().getTerm());
                if (Files.exists(snapDir.resolve(staged))) {
                    linkFile = staged;
                }
            }
            String prevFile = snapApplied ? readSnapshotFileName() : null;
            long tPrepSnapDone = System.nanoTime();

            int batchCount = 0;
            long tBuildBatchDone;
            long tDbWriteDone;
            try (WriteBatch wb = new WriteBatch()) {
                if (entries != null && !entries.isEmpty()) appendIntoBatch(wb, entries);
                if (hs != null && !isEmptyHardState(hs)) {
                    wb.put(cfState, KEY_HARD_STATE, hs.toByteArray());
                }
                if (snapApplied) {
                    wb.put(cfSnap, KEY_SNAPSHOT, snap.toByteArray());
                    if (linkFile != null) {
                        wb.put(cfState, KEY_SNAPSHOT_FILE, bytes(linkFile));
                    } else {
                        wb.delete(cfState, KEY_SNAPSHOT_FILE);
                    }
                    long snapIdx = snap.getMetadata().getIndex();
                    long tDeleteRange = System.nanoTime();
                    wb.deleteRange(cfLog, indexKey(0), indexKey(snapIdx + 1));
                    long deleteRangeMs = (System.nanoTime() - tDeleteRange) / 1_000_000;
                    if (deleteRangeMs > 50) {
                        LOG.debug("snapshot log truncation deleteRange(0, {}) took {}ms",
                                snapIdx + 1, deleteRangeMs);
                    }
                }
                tBuildBatchDone = System.nanoTime();
                batchCount = wb.count();
                if (batchCount > 0) {
                    db.write(writeOpts, wb);
                }
                tDbWriteDone = System.nanoTime();
            } catch (RocksDBException e) {
                throw new RaftInvariantException(RaftInvariantException.Category.STORAGE_IO,
                        "rocksdb writeBatched failed", e);
            }

            // Update index caches after successful write.
            if (entries != null && !entries.isEmpty()) {
                cachedLastIndex = entries.get(entries.size() - 1).getIndex();
            }
            if (snapApplied) {
                long si = snap.getMetadata().getIndex();
                cachedFirstIndex = si + 1;
                if (cachedLastIndex < si) cachedLastIndex = si;
            }

            if (snapApplied) deleteOldSidecar(prevFile, linkFile);
            long tEnd = System.nanoTime();

            long totalMs = (tEnd - tStart) / 1_000_000;
            long prepSnapMs = (tPrepSnapDone - tStart) / 1_000_000;
            long buildBatchMs = (tBuildBatchDone - tPrepSnapDone) / 1_000_000;
            long dbWriteMs = (tDbWriteDone - tBuildBatchDone) / 1_000_000;
            long cleanupMs = (tEnd - tDbWriteDone) / 1_000_000;

            if (dbWriteMs > 500) {
                LOG.warn("SLOW db.write(): {}ms (batch ops={}, entries={}, snapApplied={})",
                        dbWriteMs, batchCount,
                        entries == null ? 0 : entries.size(), snapApplied);
            }
            if (totalMs > 100 || lockWaitMs > 50) {
                LOG.debug("writeBatched: total={}ms lockWait={}ms [prepSnap={}ms buildBatch={}ms dbWrite={}ms cleanup={}ms] entries={} snap={}",
                        totalMs, lockWaitMs, prepSnapMs, buildBatchMs, dbWriteMs, cleanupMs,
                        entries == null ? 0 : entries.size(),
                        snapApplied ? snap.getMetadata().getIndex() : 0);
            }
        } finally {
            exitLock();
            writeLock.unlock();
        }
    }

    private void appendIntoBatch(WriteBatch wb, List<Eraftpb.Entry> entries) throws RocksDBException {
        long fromIdx = entries.get(0).getIndex();
        long currentLast = lastIndexNoLock();

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
