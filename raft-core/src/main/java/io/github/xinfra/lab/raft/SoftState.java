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
package io.github.xinfra.lab.raft;

import java.util.Objects;

/**
 * SoftState provides state that is useful for logging and debugging.
 * The state is volatile and does not need to be persisted to the WAL.
 */
public class SoftState {
    public long lead;
    public RaftStateType raftState;

    public SoftState() {}

    public SoftState(long lead, RaftStateType raftState) {
        this.lead = lead;
        this.raftState = raftState;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SoftState other)) return false;
        return lead == other.lead && raftState == other.raftState;
    }

    @Override
    public int hashCode() {
        return Objects.hash(lead, raftState);
    }
}
