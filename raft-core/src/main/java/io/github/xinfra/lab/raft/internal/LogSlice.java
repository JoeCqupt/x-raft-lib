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

import java.util.List;

/**
 * LogSlice describes a correct slice of a raft log.
 */
public record LogSlice(long term, EntryID prev, List<Eraftpb.Entry> entries) {

    public long lastIndex() {
        return prev.index() + entries.size();
    }

    public EntryID lastEntryID() {
        if (!entries.isEmpty()) {
            return EntryID.of(entries.get(entries.size() - 1));
        }
        return prev;
    }

    public String validate() {
        EntryID p = prev;
        for (int i = 0; i < entries.size(); i++) {
            EntryID id = EntryID.of(entries.get(i));
            if (id.term() < p.term() || id.index() != p.index() + 1) {
                return String.format("leader term %d: entries %s and %s not consistent", term, p, id);
            }
            p = id;
        }
        if (term < p.term()) {
            return String.format("leader term %d: entry %s has a newer term", term, p);
        }
        return null;
    }
}
