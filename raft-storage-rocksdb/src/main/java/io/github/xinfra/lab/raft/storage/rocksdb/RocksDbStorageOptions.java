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

import org.rocksdb.CompactionStyle;
import org.rocksdb.CompressionType;

/**
 * Tuning knobs for {@link RocksDbStorage}. Built via {@link #builder()}:
 *
 * <pre>{@code
 *   RocksDbStorageOptions opts = RocksDbStorageOptions.builder()
 *           .blockCacheSize(128L << 20)    // 128 MiB
 *           .writeBufferSize(64L << 20)    // 64 MiB
 *           .compressionType(CompressionType.ZSTD_COMPRESSION)
 *           .build();
 *   RocksDbStorage storage = new RocksDbStorage(dir, opts);
 * }</pre>
 *
 * <p>All parameters have sensible defaults; calling {@code builder().build()}
 * with no overrides is equivalent to the no-arg {@link RocksDbStorage}
 * constructor.
 */
public final class RocksDbStorageOptions {

    /**
     * Whether every write is fsynced. {@code true} guarantees durability on
     * return (required for raft safety); {@code false} lets the host handle
     * fsync externally — useful for tests and non-critical paths.
     */
    public final boolean fsync;

    /**
     * Shared LRU block cache size in bytes. Governs how much uncompressed
     * block data RocksDB keeps in memory across all column families.
     * Default 64 MiB; production deployments should size this to 10-30%
     * of available heap.
     */
    public final long blockCacheSize;

    /**
     * Per-column-family memtable size in bytes. Larger values reduce flush
     * frequency at the cost of memory. Default 64 MiB.
     */
    public final long writeBufferSize;

    /**
     * Maximum number of memtables (active + immutable) per column family
     * before writes stall. Default 3 (one active, up to two flushing).
     */
    public final int maxWriteBufferNumber;

    /**
     * Number of background threads for compaction and flush. Default 2.
     * Production deployments with fast storage should increase this to
     * match available cores.
     */
    public final int maxBackgroundJobs;

    /**
     * Compression algorithm for SST files. Default
     * {@link CompressionType#LZ4_COMPRESSION} (fast, moderate ratio).
     * Use {@link CompressionType#ZSTD_COMPRESSION} for better ratio at
     * slightly higher CPU, or {@link CompressionType#NO_COMPRESSION} for
     * latency-critical paths.
     */
    public final CompressionType compressionType;

    /**
     * Compaction strategy. Default {@link CompactionStyle#LEVEL} (best for
     * write-heavy sequential-key workloads like raft log). Use
     * {@link CompactionStyle#UNIVERSAL} for space-amplification-sensitive
     * deployments.
     */
    public final CompactionStyle compactionStyle;

    /**
     * Bits per key for the bloom filter on each SST file. Default 10
     * (~1% false positive rate). Set to 0 to disable bloom filters.
     */
    public final int bloomFilterBitsPerKey;

    /**
     * SST block size in bytes. Default 4096 (4 KiB). Larger blocks
     * improve compression ratio and sequential scan throughput at the
     * cost of point-lookup amplification.
     */
    public final int blockSize;

    /** Default options — equivalent to the no-arg constructor. */
    public static final RocksDbStorageOptions DEFAULT = builder().build();

    private RocksDbStorageOptions(Builder b) {
        this.fsync = b.fsync;
        this.blockCacheSize = b.blockCacheSize;
        this.writeBufferSize = b.writeBufferSize;
        this.maxWriteBufferNumber = b.maxWriteBufferNumber;
        this.maxBackgroundJobs = b.maxBackgroundJobs;
        this.compressionType = b.compressionType;
        this.compactionStyle = b.compactionStyle;
        this.bloomFilterBitsPerKey = b.bloomFilterBitsPerKey;
        this.blockSize = b.blockSize;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        boolean fsync = true;
        long blockCacheSize = 64L << 20;        // 64 MiB
        long writeBufferSize = 64L << 20;        // 64 MiB
        int maxWriteBufferNumber = 3;
        int maxBackgroundJobs = 2;
        CompressionType compressionType = CompressionType.LZ4_COMPRESSION;
        CompactionStyle compactionStyle = CompactionStyle.LEVEL;
        int bloomFilterBitsPerKey = 10;
        int blockSize = 4096;

        public Builder fsync(boolean v) { this.fsync = v; return this; }
        public Builder blockCacheSize(long v) { this.blockCacheSize = v; return this; }
        public Builder writeBufferSize(long v) { this.writeBufferSize = v; return this; }
        public Builder maxWriteBufferNumber(int v) { this.maxWriteBufferNumber = v; return this; }
        public Builder maxBackgroundJobs(int v) { this.maxBackgroundJobs = v; return this; }
        public Builder compressionType(CompressionType v) { this.compressionType = v; return this; }
        public Builder compactionStyle(CompactionStyle v) { this.compactionStyle = v; return this; }
        public Builder bloomFilterBitsPerKey(int v) { this.bloomFilterBitsPerKey = v; return this; }
        public Builder blockSize(int v) { this.blockSize = v; return this; }

        public RocksDbStorageOptions build() {
            if (blockCacheSize < 0) {
                throw new IllegalArgumentException("blockCacheSize must be >= 0");
            }
            if (writeBufferSize <= 0) {
                throw new IllegalArgumentException("writeBufferSize must be > 0");
            }
            if (maxWriteBufferNumber <= 0) {
                throw new IllegalArgumentException("maxWriteBufferNumber must be > 0");
            }
            if (maxBackgroundJobs <= 0) {
                throw new IllegalArgumentException("maxBackgroundJobs must be > 0");
            }
            if (compressionType == null) {
                throw new IllegalArgumentException("compressionType must not be null");
            }
            if (compactionStyle == null) {
                throw new IllegalArgumentException("compactionStyle must not be null");
            }
            if (bloomFilterBitsPerKey < 0) {
                throw new IllegalArgumentException("bloomFilterBitsPerKey must be >= 0");
            }
            if (blockSize <= 0) {
                throw new IllegalArgumentException("blockSize must be > 0");
            }
            return new RocksDbStorageOptions(this);
        }
    }
}
