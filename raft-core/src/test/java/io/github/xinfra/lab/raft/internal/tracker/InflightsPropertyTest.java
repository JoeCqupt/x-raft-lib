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
package io.github.xinfra.lab.raft.internal.tracker;
import io.github.xinfra.lab.raft.*;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property-based tests for {@link Inflights}, exercising the ring buffer with
 * randomized add/freeLE sequences and comparing against a simple
 * {@link Deque}-backed model.
 *
 * <p>Inflights is fertile ground for property tests: the ring buffer wraps,
 * grows on demand, and tracks both count and bytes. A handwritten test cannot
 * cover all the wrap-around boundaries that random sequences exercise.
 */
class InflightsPropertyTest {

    /** Operation against the inflights buffer. */
    sealed interface Op {
        record Add(long index, long bytes) implements Op {}
        record FreeLE(long to) implements Op {}
    }

    /**
     * Reference model: a deque of (index, bytes) entries plus running totals.
     * Mirrors the contract of {@link Inflights} without using a ring buffer.
     */
    static final class Model {
        final int size;
        final long maxBytes;
        final Deque<long[]> entries = new ArrayDeque<>(); // [index, bytes]
        long bytes = 0;

        Model(int size, long maxBytes) {
            this.size = size;
            this.maxBytes = maxBytes;
        }

        boolean full() {
            return entries.size() == size || (maxBytes != 0 && bytes >= maxBytes);
        }

        int count() { return entries.size(); }

        void add(long index, long b) {
            entries.addLast(new long[]{index, b});
            bytes += b;
        }

        void freeLE(long to) {
            if (entries.isEmpty() || to < entries.peekFirst()[0]) return;
            while (!entries.isEmpty() && entries.peekFirst()[0] <= to) {
                bytes -= entries.pollFirst()[1];
            }
        }
    }

    /**
     * Generate a random buffer size and a sequence of operations whose
     * indices are monotonically non-decreasing (Inflights' contract requires
     * indices added to be in order; freeLE always uses an index from the
     * already-added range or a "free everything" sentinel).
     */
    @Provide
    Arbitrary<Scenario> scenarios() {
        return Arbitraries.integers().between(1, 32).flatMap(size ->
                Arbitraries.longs().between(0L, 1_000_000L).flatMap(maxBytes ->
                        Arbitraries.integers().between(0, 200).flatMap(opCount ->
                                Arbitraries.randomValue(rand -> {
                                    java.util.List<Op> ops = new java.util.ArrayList<>(opCount);
                                    long nextIdx = 1;
                                    long lastFreed = 0;
                                    for (int i = 0; i < opCount; i++) {
                                        // 70% add, 30% freeLE — biased toward add
                                        // to actually fill the buffer.
                                        if (ops.size() < size && rand.nextDouble() < 0.7) {
                                            long byteCnt = rand.nextInt(1024);
                                            ops.add(new Op.Add(nextIdx, byteCnt));
                                            nextIdx++;
                                        } else {
                                            // Free up to a random previously-added index.
                                            long upper = Math.max(lastFreed, nextIdx - 1);
                                            long to = lastFreed + (long) (rand.nextDouble() * (upper - lastFreed + 1));
                                            ops.add(new Op.FreeLE(to));
                                            lastFreed = to;
                                        }
                                    }
                                    return new Scenario(size, maxBytes, ops);
                                }))));
    }

    record Scenario(int size, long maxBytes, List<Op> ops) {}

    /**
     * For any scenario, the Inflights ring buffer agrees with the deque model
     * on count() and full() at every step, and never throws on legal
     * operations (i.e. add when not full, freeLE on any value).
     */
    @Property(tries = 500)
    void inflightsMatchesModel(@ForAll("scenarios") Scenario s) {
        Inflights real = new Inflights(s.size, s.maxBytes);
        Model model = new Model(s.size, s.maxBytes);

        int step = 0;
        for (Op op : s.ops) {
            step++;
            String where = String.format("step %d size=%d maxBytes=%d op=%s",
                    step, s.size, s.maxBytes, op);
            // count() and full() must agree before each op.
            assertThat(real.count()).as(where + " count").isEqualTo(model.count());
            assertThat(real.full()).as(where + " full").isEqualTo(model.full());

            if (op instanceof Op.Add add) {
                if (!model.full()) {
                    real.add(add.index, add.bytes);
                    model.add(add.index, add.bytes);
                }
                // If the model is full, skip — Inflights.add() panics on full.
            } else if (op instanceof Op.FreeLE free) {
                real.freeLE(free.to);
                model.freeLE(free.to);
            }
        }

        // Final state agrees too.
        assertThat(real.count()).isEqualTo(model.count());
        assertThat(real.full()).isEqualTo(model.full());
    }

    /**
     * clone() produces an independent copy: subsequent operations on either
     * the clone or the original do not affect the other. This catches sharing
     * of the underlying buffer arrays.
     */
    @Property(tries = 200)
    void cloneIsIndependent(@ForAll("scenarios") Scenario s) {
        Inflights a = new Inflights(s.size, s.maxBytes);
        // Pre-populate to a non-trivial state.
        long idx = 1;
        for (Op op : s.ops) {
            if (op instanceof Op.Add add) {
                if (!a.full()) {
                    a.add(idx++, add.bytes);
                }
            }
        }
        Inflights b = a.clone();
        int countBefore = a.count();
        // Mutate b only.
        b.freeLE(idx); // free everything in b
        // a must be unaffected.
        assertThat(a.count()).as("clone independence").isEqualTo(countBefore);
        assertThat(b.count()).isZero();
    }

    /**
     * After freeLE that exhausts everything, count() == 0 and full() == false
     * (assuming maxBytes was the only constraint and now bytes == 0).
     */
    @Property(tries = 200)
    void exhaustiveFreeReturnsToEmpty(@ForAll("scenarios") Scenario s) {
        Inflights real = new Inflights(s.size, s.maxBytes);
        long idx = 1;
        for (Op op : s.ops) {
            if (op instanceof Op.Add add) {
                if (!real.full()) real.add(idx++, add.bytes);
            }
        }
        // Free far beyond any added index.
        real.freeLE(Long.MAX_VALUE / 2);
        assertThat(real.count()).isZero();
        assertThat(real.full()).isFalse();
    }
}
