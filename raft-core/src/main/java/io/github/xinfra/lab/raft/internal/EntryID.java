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
package io.github.xinfra.lab.raft.internal;
import io.github.xinfra.lab.raft.*;

import io.github.xinfra.lab.raft.proto.Eraftpb;

/**
 * EntryID uniquely identifies a raft log entry.
 */
public record EntryID(long term, long index) {
    public static EntryID of(Eraftpb.Entry entry) {
        return new EntryID(entry.getTerm(), entry.getIndex());
    }

    @Override
    public String toString() {
        return "{term=" + term + ", index=" + index + "}";
    }
}
