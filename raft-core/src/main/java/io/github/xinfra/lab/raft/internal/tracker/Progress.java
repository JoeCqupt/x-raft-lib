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
import io.github.xinfra.lab.raft.internal.*;

/**
 * Progress represents a follower's progress in the view of the leader.
 */
public class Progress {
    private long match;
    private long next;
    private long sentCommit;
    private StateType state;
    private long pendingSnapshot;
    private boolean recentActive;
    private boolean msgAppFlowPaused;
    private Inflights inflights;
    private boolean isLearner;

    public Progress() {
        this.state = StateType.StateProbe;
    }

    public Progress(long match, long next, Inflights inflights, boolean isLearner) {
        this.match = match;
        this.next = next;
        this.inflights = inflights;
        this.isLearner = isLearner;
        this.state = StateType.StateProbe;
    }

    public long getMatch() { return match; }
    public void setMatch(long match) { this.match = match; }
    public long getNext() { return next; }
    public void setNext(long next) { this.next = next; }
    public long getSentCommit() { return sentCommit; }
    public void setSentCommit(long sentCommit) { this.sentCommit = sentCommit; }
    public StateType getState() { return state; }
    public void setState(StateType state) { this.state = state; }
    public long getPendingSnapshot() { return pendingSnapshot; }
    public void setPendingSnapshot(long pendingSnapshot) { this.pendingSnapshot = pendingSnapshot; }
    public boolean isRecentActive() { return recentActive; }
    public void setRecentActive(boolean recentActive) { this.recentActive = recentActive; }
    public boolean isMsgAppFlowPaused() { return msgAppFlowPaused; }
    public void setMsgAppFlowPaused(boolean paused) { this.msgAppFlowPaused = paused; }
    public Inflights getInflights() { return inflights; }
    public void setInflights(Inflights inflights) { this.inflights = inflights; }
    public boolean isLearner() { return isLearner; }
    public void setLearner(boolean learner) { isLearner = learner; }

    @Override
    public Progress clone() {
        Progress p = new Progress();
        p.match = match;
        p.next = next;
        p.sentCommit = sentCommit;
        p.state = state;
        p.pendingSnapshot = pendingSnapshot;
        p.recentActive = recentActive;
        p.msgAppFlowPaused = msgAppFlowPaused;
        p.inflights = inflights != null ? inflights.clone() : null;
        p.isLearner = isLearner;
        return p;
    }

    public void resetState(StateType state) {
        this.msgAppFlowPaused = false;
        this.pendingSnapshot = 0;
        this.state = state;
        this.inflights.reset();
    }

    public void becomeProbe() {
        if (state == StateType.StateSnapshot) {
            long pending = pendingSnapshot;
            resetState(StateType.StateProbe);
            next = Math.max(match + 1, pending + 1);
        } else {
            resetState(StateType.StateProbe);
            next = match + 1;
        }
        sentCommit = Math.min(sentCommit, next - 1);
    }

    public void becomeReplicate() {
        resetState(StateType.StateReplicate);
        next = match + 1;
    }

    public void becomeSnapshot(long snapshoti) {
        resetState(StateType.StateSnapshot);
        pendingSnapshot = snapshoti;
        next = snapshoti + 1;
        sentCommit = snapshoti;
    }

    public void sentEntries(int entries, long bytes) {
        switch (state) {
            case StateReplicate -> {
                if (entries > 0) {
                    next += entries;
                    inflights.add(next - 1, bytes);
                }
                msgAppFlowPaused = inflights.full();
            }
            case StateProbe -> {
                if (entries > 0) {
                    msgAppFlowPaused = true;
                }
            }
            default -> throw new IllegalStateException("sending append in unhandled state " + state);
        }
    }

    public boolean canBumpCommit(long index) {
        return index > sentCommit && sentCommit < next - 1;
    }

    public void sentCommit(long commit) {
        this.sentCommit = commit;
    }

    public boolean maybeUpdate(long n) {
        if (n <= match) {
            return false;
        }
        match = n;
        next = Math.max(next, n + 1);
        msgAppFlowPaused = false;
        return true;
    }

    public boolean maybeDecrTo(long rejected, long matchHint) {
        if (state == StateType.StateReplicate) {
            if (rejected <= match) {
                return false;
            }
            next = match + 1;
            sentCommit = Math.min(sentCommit, next - 1);
            return true;
        }

        if (next - 1 != rejected) {
            return false;
        }

        next = Math.max(Math.min(rejected, matchHint + 1), match + 1);
        sentCommit = Math.min(sentCommit, next - 1);
        msgAppFlowPaused = false;
        return true;
    }

    public boolean isPaused() {
        return switch (state) {
            case StateProbe -> msgAppFlowPaused;
            case StateReplicate -> msgAppFlowPaused;
            case StateSnapshot -> true;
        };
    }

    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder();
        buf.append(state).append(" match=").append(match).append(" next=").append(next);
        if (isLearner) {
            buf.append(" learner");
        }
        if (isPaused()) {
            buf.append(" paused");
        }
        if (pendingSnapshot > 0) {
            buf.append(" pendingSnap=").append(pendingSnapshot);
        }
        if (!recentActive) {
            buf.append(" inactive");
        }
        if (inflights.count() > 0) {
            buf.append(" inflight=").append(inflights.count());
            if (inflights.full()) {
                buf.append("[full]");
            }
        }
        return buf.toString();
    }
}
