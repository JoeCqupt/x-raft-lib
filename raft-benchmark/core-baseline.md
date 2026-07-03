# JMH Baseline — Raft Core

Performance baseline for the perf-relevant fixes from the initial code review.
Numbers are reported as `ops/μs` (throughput); higher is better.

All measurements:
- JVM: OpenJDK / JRE 17
- Mode: Throughput
- Warmup: 2 iterations × 1 second
- Measurement: 3 iterations × 2 seconds
- Forks: 1
- Run via `java -jar raft-benchmark/target/raft-benchmark.jar RaftCoreBenchmarks`

These numbers should be regenerated and compared whenever:
1. Anything in the `Unstable`, `RaftLog`, `Raft`, or `ReadOnly` hot path is
   modified.
2. A perf-related code review claim is made — measure first, claim second.

## Method

For each perf-relevant fix, run JMH with HEAD's version and with the proposed
change. The A/B procedure used in the review:

```bash
# 1) snapshot current
cp src/main/java/io/github/xinfra/lab/raft/X.java /tmp/after/

# 2) revert to baseline + bench
git checkout HEAD -- src/main/java/io/github/xinfra/lab/raft/X.java
mvn test-compile
java -cp "target/test-classes:target/classes:$CP" io.github.xinfra.lab.raft.RaftBenchmarks <benchmark> -wi 2 -i 3 -r 2s -w 1s -f 1

# 3) restore + bench
cp /tmp/after/X.java src/main/java/io/github/xinfra/lab/raft/X.java
mvn test-compile
java -cp "target/test-classes:target/classes:$CP" io.github.xinfra.lab.raft.RaftBenchmarks <benchmark> -wi 2 -i 3 -r 2s -w 1s -f 1
```

## Results

### #6 — `Unstable.stableTo`: in-place removeRange vs. ArrayList rebuild

**Benchmark**: `unstableStableToInChunks`
Populate `Unstable` with N entries, then call `stableTo` in chunks of 10
(simulating the steady-state pattern where each Ready persists ~10 entries).

| batchSize | BEFORE (HEAD) | AFTER (#6) | Speedup |
|-----------|--------------:|-----------:|--------:|
| 100       |   5.18–5.69   |  6.69–6.90 | **+22–30%** |
| 1000      | 0.072–0.099   | 0.233–0.235 | **+140% (≈ 2.4×)** ⭐ |

**Decision**: KEEP. Bigger batches reveal larger wins because the previous
implementation did O(batchSize²/chunk) total copy work. The new
`subList(0, num).clear()` is O(remaining-shift) per call with no allocation,
plus shrinkEntriesArray reclaims the backing array on empty.

### #7 — `RaftLog.slice`: skip redundant ArrayList wrap of unstable subList

**Benchmark**: `raftLogSliceUnstable`
Call `RaftLog.entries(1, NO_LIMIT)` on a raftLog with `logSize` entries, all
sitting in `Unstable`.

| logSize | BEFORE (HEAD) | AFTER (#7) | Speedup |
|---------|--------------:|-----------:|--------:|
| 1000    |    0.595      |    0.640   | **+7.5%** |

**Decision**: KEEP. Modest but consistent (low variance). Removing one
ArrayList wrap on the hot read path frees up ~5–8% throughput.

### #18 — `ReadOnly.maybeAdvance`: in-place clear vs. dual ArrayList copy ❌

**Benchmark**: `readOnlyMaybeAdvance`
Add N read requests, ack from a quorum, then call `maybeAdvance` to flush.

| requestCount | BEFORE (HEAD) | AFTER (proposed "optimization") | Δ |
|--------------|--------------:|--------------------------------:|---:|
| 100          | 18.5–19       | 13.0–13.3                       | **-30%** ⬇️ |
| 1000         | 4.4–4.6       | 2.2–2.3                         | **-50%** ⬇️ |

**Decision**: ❌ **REVERTED**. The "fewer allocation" optimization is
counterproductive in the common path (advancing the entire pending list at
once): `ArrayList.removeRange` includes a `for (i = newSize; i < oldSize;
i++) elementData[i] = null;` loop, and that null-fill cost dominates over
the savings of avoiding one tail `new ArrayList<>(emptySubList)` (which is
essentially free for an empty source).

This is the canonical case for **"measure before you optimize"**: a code
review can identify code smells, but JMH is the only reliable arbiter of
whether a change is faster, slower, or a wash. The current code retains the
HEAD dual-copy form with a comment block citing this evidence so future
reviewers don't re-attempt the same "optimization".

### `proposeAndDrain` (control benchmark)

**Benchmark**: `proposeAndDrain`
End-to-end single-node propose → Ready → advance cycle.

| voters | payload | BEFORE        | AFTER         | Δ |
|--------|---------|--------------:|--------------:|---|
| 3      | 64      | 0.137         | 0.135         | ≈ 0 |
| 3      | 1024    | 0.131         | 0.131         | 0 |
| 5      | 64      | 0.134         | 0.134         | 0 |
| 5      | 1024    | 0.132         | 0.133         | 0 |

**Why no delta**: this benchmark is dominated by Ready scaffolding (build →
emit → accept → advance), not by `Unstable.stableTo` or `RaftLog.slice`. The
per-iteration work in those hot paths happens too few times to surface in
the throughput number. Use the targeted micro-benchmarks above for those
fixes' wins.

## Per-fix summary

| Issue | Description | Status | A/B Evidence |
|------:|-------------|:------:|--------------|
| #6    | Unstable.stableTo: in-place shrink | ✅ KEEP | up to **+140%** |
| #7    | RaftLog.slice: drop redundant copy | ✅ KEEP | **+7.5%** |
| #11   | Raft.reset: reuse Progress/Inflights | ✅ KEEP | Untested in JMH (term-change is a rare path). Code review evidence: avoids 2N allocs per term change. |
| #18   | ReadOnly.maybeAdvance: in-place clear | ❌ REVERTED | -30 to -50%; null-fill cost dominates |
| #9    | SecureRandom → ThreadLocalRandom | ✅ KEEP | Untested in JMH (election is rare). Avoids `/dev/random` blocking on cold start. |

## Reproducing this baseline

```bash
cd x-raft-lib
mvn -B -ntp package -pl raft-benchmark -am -DskipTests

# All core benchmarks (~15 min)
java -jar raft-benchmark/target/raft-benchmark.jar RaftCoreBenchmarks

# A specific benchmark
java -jar raft-benchmark/target/raft-benchmark.jar unstableStableToInChunks
```

Last updated: 2026-05-22 (review round 9).
