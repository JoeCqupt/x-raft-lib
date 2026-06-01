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
 * Tests for Progress, ported from etcd-raft tracker/progress_test.go.
 */
class ProgressTest {

    @Test
    void testProgressString() throws RaftException {
        Inflights ins = new Inflights(1, 0);
        ins.add(123, 1);
        Progress pr = new Progress(1, 2, ins, true);
        pr.setState(StateType.StateSnapshot);
        pr.setPendingSnapshot(123);
        pr.setRecentActive(false);
        pr.setMsgAppFlowPaused(true);

        String exp = "StateSnapshot match=1 next=2 learner paused pendingSnap=123 inactive inflight=1[full]";
        assertThat(pr.toString()).isEqualTo(exp);
    }

    @Test
    void testProgressIsPaused() throws RaftException {
        Object[][] tests = {
                {StateType.StateProbe, false, false},
                {StateType.StateProbe, true, true},
                {StateType.StateReplicate, false, false},
                {StateType.StateReplicate, true, true},
                {StateType.StateSnapshot, false, true},
                {StateType.StateSnapshot, true, true},
        };

        for (Object[] tt : tests) {
            Progress p = new Progress(0, 0, new Inflights(256, 0), false);
            p.setState((StateType) tt[0]);
            p.setMsgAppFlowPaused((boolean) tt[1]);
            assertThat(p.isPaused()).isEqualTo(tt[2]);
        }
    }

    @Test
    void testProgressResume() throws RaftException {
        Progress p = new Progress();
        p.setNext(2);
        p.setInflights(new Inflights(256, 0));
        p.setMsgAppFlowPaused(true);

        p.maybeDecrTo(1, 1);
        assertThat(p.isMsgAppFlowPaused()).isFalse();

        p.setMsgAppFlowPaused(true);
        p.maybeUpdate(2);
        assertThat(p.isMsgAppFlowPaused()).isFalse();
    }

    @Test
    void testProgressBecomeProbe() throws RaftException {
        // from StateReplicate
        Progress p1 = new Progress(1, 5, new Inflights(256, 0), false);
        p1.setState(StateType.StateReplicate);
        p1.becomeProbe();
        assertThat(p1.getState()).isEqualTo(StateType.StateProbe);
        assertThat(p1.getMatch()).isEqualTo(1);
        assertThat(p1.getNext()).isEqualTo(2);

        // from StateSnapshot with pending snapshot finish
        Progress p2 = new Progress(1, 5, new Inflights(256, 0), false);
        p2.setState(StateType.StateSnapshot);
        p2.setPendingSnapshot(10);
        p2.becomeProbe();
        assertThat(p2.getState()).isEqualTo(StateType.StateProbe);
        assertThat(p2.getMatch()).isEqualTo(1);
        assertThat(p2.getNext()).isEqualTo(11);

        // from StateSnapshot with no pending snapshot
        Progress p3 = new Progress(1, 5, new Inflights(256, 0), false);
        p3.setState(StateType.StateSnapshot);
        p3.setPendingSnapshot(0);
        p3.becomeProbe();
        assertThat(p3.getState()).isEqualTo(StateType.StateProbe);
        assertThat(p3.getMatch()).isEqualTo(1);
        assertThat(p3.getNext()).isEqualTo(2);
    }

    @Test
    void testProgressBecomeReplicate() throws RaftException {
        Progress p = new Progress(1, 5, new Inflights(256, 0), false);
        p.setState(StateType.StateProbe);
        p.becomeReplicate();
        assertThat(p.getState()).isEqualTo(StateType.StateReplicate);
        assertThat(p.getMatch()).isEqualTo(1);
        assertThat(p.getNext()).isEqualTo(2);  // match + 1
    }

    @Test
    void testProgressBecomeSnapshot() throws RaftException {
        Progress p = new Progress(1, 5, new Inflights(256, 0), false);
        p.setState(StateType.StateProbe);
        p.becomeSnapshot(10);
        assertThat(p.getState()).isEqualTo(StateType.StateSnapshot);
        assertThat(p.getMatch()).isEqualTo(1);
        assertThat(p.getPendingSnapshot()).isEqualTo(10);
    }

    @Test
    void testProgressUpdate() throws RaftException {
        long prevM = 3, prevN = 5;

        Object[][] tests = {
                {prevM - 1, prevM, prevN, false},
                {prevM, prevM, prevN, false},
                {prevM + 1, prevM + 1, prevN, true},
                {prevM + 2, prevM + 2, prevN + 1, true},
        };

        for (Object[] tt : tests) {
            Progress p = new Progress();
            p.setMatch(prevM);
            p.setNext(prevN);
            p.setInflights(new Inflights(256, 0));

            boolean ok = p.maybeUpdate((long) tt[0]);
            assertThat(ok).isEqualTo(tt[3]);
            assertThat(p.getMatch()).isEqualTo(tt[1]);
            assertThat(p.getNext()).isEqualTo(tt[2]);
        }
    }

    @Test
    void testProgressMaybeDecr() throws RaftException {
        Object[][] tests = {
                // state, match, next, rejected, last, result, wNext
                {StateType.StateReplicate, 5L, 10L, 5L, 5L, false, 10L},
                {StateType.StateReplicate, 5L, 10L, 4L, 4L, false, 10L},
                {StateType.StateReplicate, 5L, 10L, 9L, 9L, true, 6L},
                {StateType.StateProbe, 0L, 0L, 0L, 0L, false, 0L},
                {StateType.StateProbe, 0L, 10L, 5L, 5L, false, 10L},
                {StateType.StateProbe, 0L, 10L, 9L, 9L, true, 9L},
                {StateType.StateProbe, 0L, 2L, 1L, 1L, true, 1L},
                {StateType.StateProbe, 0L, 1L, 0L, 0L, true, 1L},
                {StateType.StateProbe, 0L, 10L, 9L, 2L, true, 3L},
                {StateType.StateProbe, 0L, 10L, 9L, 0L, true, 1L},
        };

        for (Object[] tt : tests) {
            Progress p = new Progress();
            p.setState((StateType) tt[0]);
            p.setMatch((long) tt[1]);
            p.setNext((long) tt[2]);
            p.setInflights(new Inflights(256, 0));

            boolean ok = p.maybeDecrTo((long) tt[3], (long) tt[4]);
            assertThat(ok).isEqualTo(tt[5]);
            assertThat(p.getMatch()).isEqualTo(tt[1]);
            assertThat(p.getNext()).isEqualTo(tt[6]);
        }
    }
}
