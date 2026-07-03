# raft-benchmark

JMH benchmark suite for x-raft-lib. Measures Raft core micro-operations and
end-to-end KV cluster performance across varying payload sizes, concurrency
levels, and read/write mixes.

## Benchmark Matrix

### Core Micro-Benchmarks (`RaftCoreBenchmarks`)

Single-node, in-process benchmarks that isolate individual hot-path operations
with no I/O. Useful for A/B testing optimizations to internal data structures.

| Benchmark | What it measures |
|-----------|-----------------|
| `proposeAndDrain` | propose -> Ready -> advance cycle (payload x voters) |
| `leaderHandleAppendResponse` | MsgAppendResponse processing (Progress/Inflights) |
| `readyUnderLoad` | `ready()` assembly cost with 100 pending 32KB entries |
| `committedIndex` | Quorum committed index calculation (1-11 voters) |

### Propose Benchmarks (`RaftProposeBenchmark`)

End-to-end write throughput and latency on a real 3-node cluster (gRPC + RocksDB).

| Method | Mode | What it measures |
|--------|------|-----------------|
| `syncPropose` | Throughput + SampleTime | Single-request latency and throughput. Each JMH invocation blocks until the proposal is committed. Latency percentiles (p50/p90/p99/p999) come from JMH SampleTime. Scale concurrency via `-t` flag. |
| `asyncPropose` | Throughput | Pipeline throughput under backpressure. Proposals are fire-and-forget with a semaphore (1024 max in-flight). JMH throughput = sustained commit rate. Callback tracks avg latency, error count, timeout count. |

**Dimensions:**
- Payload size: 128B / 1KB / 4KB / 16KB / 64KB
- Concurrency (sync): controlled via JMH `-t` flag (e.g. `-t 1`, `-t 4`, `-t 8`)
- Pipeline depth (async): fixed at 1024 max in-flight

**Metrics:** ops/s, latency percentiles (sync), avg latency (async), error rate, timeout rate.

### Read Benchmarks (`RaftReadBenchmark`)

Linearizable read performance under controlled write load on a real 3-node cluster.

**Rationale:** Linearizable read (ReadIndex protocol) requires:
1. Leader confirms it still holds leadership (heartbeat round-trip)
2. State machine must apply up to the read index before serving the read

Under write load, the apply queue grows — reads must wait for pending proposals
to commit and apply before they can be served. This benchmark quantifies that
relationship by measuring read performance at different background write rates.

| Method | Mode | What it measures |
|--------|------|-----------------|
| `syncRead` | Throughput + SampleTime | Per-read latency and throughput under controlled write load. JMH SampleTime gives latency percentiles. |
| `asyncRead` | Throughput | Read pipeline throughput under controlled write load. Semaphore-limited (1024 max in-flight reads). Callback tracks avg latency, error count, timeout count. |

**Dimensions:**
- Background propose QPS: 0 / 100 / 500 / 1000 / 2000
  - `0` = baseline read latency (no write contention)
  - `2000` = heavy write load (measures read degradation)

**Metrics:** read ops/s, read latency percentiles (sync), avg read latency (async),
error rate, timeout rate.

## Running Benchmarks

### Build the uber-jar

```bash
mvn -B -ntp package -pl raft-benchmark -am -DskipTests -Djacoco.skip=true
```

### Quick start

```bash
# All benchmarks (long — runs full parameter matrix)
java -jar raft-benchmark/target/raft-benchmark.jar

# Core micro-benchmarks only (~3 min)
java -jar raft-benchmark/target/raft-benchmark.jar RaftCoreBenchmarks

# Sync propose, single thread (~8 min)
java -jar raft-benchmark/target/raft-benchmark.jar syncPropose -t 1

# Async propose, pipeline (~5 min)
java -jar raft-benchmark/target/raft-benchmark.jar asyncPropose -t 1

# Read under write load (~8 min)
java -jar raft-benchmark/target/raft-benchmark.jar syncRead -t 1
```

### Concurrency scaling

```bash
# Sync propose at 1, 4, 8 threads
for t in 1 4 8; do
  java -jar raft-benchmark/target/raft-benchmark.jar syncPropose \
    -t $t -rf json -rff "propose-t${t}.json" \
    -wi 3 -w 5s -i 5 -r 10s -f 1
done
```

### Specific payload size

```bash
java -jar raft-benchmark/target/raft-benchmark.jar syncPropose \
  -t 1 -p payloadSize=4096 -wi 3 -i 5 -r 10s -f 1
```

### Specific read QPS

```bash
java -jar raft-benchmark/target/raft-benchmark.jar syncRead \
  -t 1 -p proposeQps=1000 -wi 3 -i 5 -r 10s -f 1
```

## Interpreting Results

### Sync benchmarks (SampleTime mode)

JMH reports latency percentiles directly:

```
Benchmark                          (payloadSize)  Mode    Cnt   Score   Error  Units
syncPropose                               1024   sample  50    1.234 ± 0.056  ms/op
syncPropose:syncPropose·p0.50             1024   sample        0.987          ms/op
syncPropose:syncPropose·p0.90             1024   sample        1.456          ms/op
syncPropose:syncPropose·p0.99             1024   sample        2.345          ms/op
syncPropose:syncPropose·p0.999            1024   sample        5.678          ms/op
```

### Async benchmarks (Throughput mode)

JMH reports throughput; callback metrics are printed per iteration:

```
Benchmark                          (payloadSize)  Mode   Cnt    Score    Error  Units
asyncPropose                              1024    thrpt   5   8234.5 ± 123.4  ops/ms
[async] ops=82345 errors=0 timeouts=0 avgLatency=121.3us
```

### Read under write load

Compare latency across proposeQps values to see degradation:

```
Benchmark            (proposeQps)  Mode    Score    Units
syncRead                       0  sample   0.456   ms/op   ← baseline
syncRead                     500  sample   0.678   ms/op   ← moderate load
syncRead                    2000  sample   1.234   ms/op   ← heavy load
```

## Design Decisions

### Why sync + async for propose?

- **Sync** measures true end-to-end latency (propose → commit → return). JMH
  SampleTime gives accurate p50/p90/p99 percentiles. Scale concurrency via
  JMH's `-t` flag.
- **Async** measures peak pipeline throughput under backpressure. A single thread
  keeps 1024 proposals in flight via semaphore; throughput reflects the node's
  ability to batch and group-commit. Latency is tracked in callbacks (avg only —
  percentiles would require HdrHistogram).

### Why rate-controlled propose for read benchmarks?

Fixed-rate background propose creates a reproducible, steady-state write load.
This is superior to "write as fast as possible" because:
1. Results are deterministic across runs (same QPS = same load)
2. The QPS sweep (0 → 2000) traces the latency curve, showing exactly where
   read performance starts degrading
3. Saturation-based tests conflate "how fast can we write" with "how does read
   suffer" — rate control isolates the second question

### Why no separate concurrency benchmark?

`RaftProposeBenchmark.syncPropose` with JMH's `-t` flag covers all concurrency
levels. A dedicated concurrency benchmark with hardcoded thread count is
redundant. Run `-t 1`, `-t 4`, `-t 8`, `-t 16` to find the saturation point.
