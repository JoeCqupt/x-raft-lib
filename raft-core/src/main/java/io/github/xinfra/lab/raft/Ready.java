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

import io.github.xinfra.lab.raft.proto.Eraftpb;

import java.util.Collections;
import java.util.List;

/**
 * Ready encapsulates the entries and messages that are ready to read,
 * be saved to stable storage, committed or sent to other peers.
 */
public class Ready {
    /**
     * The current volatile state of a Node.
     * SoftState will be null if there is no update.
     */
    public SoftState softState;

    /**
     * The current state of a Node to be saved to stable storage BEFORE
     * Messages are sent.
     */
    public Eraftpb.HardState hardState;

    /**
     * ReadStates can be used for node to serve linearizable read requests locally.
     */
    public List<ReadState> readStates;

    /**
     * Entries specifies entries to be saved to stable storage BEFORE Messages are sent.
     */
    public List<Eraftpb.Entry> entries;

    /**
     * Snapshot specifies the snapshot to be saved to stable storage.
     */
    public Eraftpb.Snapshot snapshot;

    /**
     * CommittedEntries specifies entries to be committed to a store/state-machine.
     */
    public List<Eraftpb.Entry> committedEntries;

    /**
     * Messages specifies outbound messages.
     */
    public List<Eraftpb.Message> messages;

    /**
     * MustSync indicates whether the HardState and Entries must be durably written to disk.
     */
    public boolean mustSync;

    public Ready() {
        this.hardState = Eraftpb.HardState.getDefaultInstance();
        this.snapshot = Eraftpb.Snapshot.getDefaultInstance();
        this.entries = Collections.emptyList();
        this.committedEntries = Collections.emptyList();
        this.messages = Collections.emptyList();
        this.readStates = Collections.emptyList();
    }
}
