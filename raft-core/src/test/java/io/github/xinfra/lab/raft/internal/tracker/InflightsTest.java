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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;

/**
 * Tests for Inflights, ported from etcd-raft tracker/inflights_test.go.
 */
class InflightsTest {

    @Test
    void testInflightsAdd() throws RaftException {
        // no rotating case
        Inflights in = new Inflights(10, 0);
        for (int i = 0; i < 5; i++) {
            in.add(i, 100 + i);
        }
        assertThat(in.count()).isEqualTo(5);
        assertThat(in.full()).isFalse();

        for (int i = 5; i < 10; i++) {
            in.add(i, 100 + i);
        }
        assertThat(in.count()).isEqualTo(10);
        assertThat(in.full()).isTrue();
    }

    @Test
    void testInflightsFreeTo() throws RaftException {
        // no rotating case
        Inflights in = new Inflights(10, 0);
        for (int i = 0; i < 10; i++) {
            in.add(i, 100 + i);
        }

        in.freeLE(0);
        assertThat(in.count()).isEqualTo(9);

        in.freeLE(4);
        assertThat(in.count()).isEqualTo(5);

        in.freeLE(8);
        assertThat(in.count()).isEqualTo(1);

        // rotating case
        for (int i = 10; i < 15; i++) {
            in.add(i, 100 + i);
        }

        in.freeLE(12);
        assertThat(in.count()).isEqualTo(2);

        in.freeLE(14);
        assertThat(in.count()).isEqualTo(0);
    }

    @Test
    void testInflightsFull_alwaysFull() throws RaftException {
        Inflights in = new Inflights(0, 0);
        assertThat(in.full()).isTrue();
    }

    @Test
    void testInflightsFull_singleEntry() throws RaftException {
        Inflights in = new Inflights(1, 0);
        assertThat(in.full()).isFalse();
        in.add(0, 100);
        assertThat(in.full()).isTrue();
        in.freeLE(0);
        assertThat(in.full()).isFalse();
        in.add(1, 101);
        assertThat(in.full()).isTrue();
    }

    @Test
    void testInflightsFull_multiEntry() throws RaftException {
        Inflights in = new Inflights(15, 0);
        for (int i = 0; i < 15; i++) {
            assertThat(in.full()).isFalse();
            in.add(i, 100 + i);
        }
        assertThat(in.full()).isTrue();
        in.freeLE(6);
        for (int i = 15; i < 22; i++) {
            assertThat(in.full()).isFalse();
            in.add(i, 100 + i);
        }
        assertThat(in.full()).isTrue();
    }

    @Test
    void testInflightsFull_maxBytes() throws RaftException {
        // size=8, maxBytes=400
        Inflights in = new Inflights(8, 400);
        for (int i = 0; i < 4; i++) {
            assertThat(in.full()).isFalse();
            in.add(i, 100 + i);
        }
        assertThat(in.full()).isTrue();

        in.freeLE(2);
        for (int i = 4; i < 7; i++) {
            assertThat(in.full()).isFalse();
            in.add(i, 100 + i);
        }
        assertThat(in.full()).isTrue();
    }

    @Test
    void testInflightsReset() throws RaftException {
        Inflights in = new Inflights(10, 1000);
        long idx = 0;
        for (int epoch = 0; epoch < 100; epoch++) {
            in.reset();
            // Add 5 messages. They should not max out the limit yet.
            for (int i = 0; i < 5; i++) {
                assertThat(in.full()).isFalse();
                idx++;
                in.add(idx, 16);
            }
            // Ack all but last 2 indices.
            in.freeLE(idx - 2);
            assertThat(in.full()).isFalse();
            assertThat(in.count()).isEqualTo(2);
        }
        in.freeLE(idx);
        assertThat(in.count()).isEqualTo(0);
    }

    @Test
    void testInflightsAddPanicsWhenFull() throws RaftException {
        Inflights in = new Inflights(1, 0);
        in.add(0, 100);
        assertThat(in.full()).isTrue();
        assertThatThrownBy(() -> in.add(1, 101))
                .isInstanceOf(IllegalStateException.class);
    }
}
