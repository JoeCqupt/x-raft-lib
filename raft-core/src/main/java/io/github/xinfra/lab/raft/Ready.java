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
import org.jspecify.annotations.Nullable;

import java.util.Collections;
import java.util.List;

/**
 * Ready encapsulates the entries and messages that are ready to read,
 * be saved to stable storage, committed or sent to other peers.
 *
 * <p><b>Immutable.</b> Constructed exclusively by {@link RawNode#ready()}
 * (or via the package-private {@link Builder} for tests). Hosts must NOT
 * mutate the returned lists; this is enforced by returning unmodifiable
 * views via the canonical accessors.
 */
public record Ready(
        /** The current volatile state of a Node. {@code null} means no update. */
        @Nullable SoftState softState,
        /** State to be saved to stable storage BEFORE messages are sent. */
        Eraftpb.HardState hardState,
        /** ReadStates can be used by the host to serve linearizable reads locally. */
        List<ReadState> readStates,
        /** Entries to be saved to stable storage BEFORE messages are sent. */
        List<Eraftpb.Entry> entries,
        /** Snapshot to be saved to stable storage. */
        Eraftpb.Snapshot snapshot,
        /** Entries to be committed to a store / state-machine. */
        List<Eraftpb.Entry> committedEntries,
        /** Outbound messages. */
        List<Eraftpb.Message> messages,
        /** True iff hardState and entries must be durably synced before send. */
        boolean mustSync) {

    /**
     * Sentinel "no work pending" Ready. All collections are empty,
     * {@code hardState} / {@code snapshot} are protobuf defaults,
     * {@code mustSync} is false. Useful when {@link RawNode#hasReady()} is
     * false but the call site wants a non-null reference.
     */
    public static Ready empty() {
        return new Ready(
                null,
                Eraftpb.HardState.getDefaultInstance(),
                Collections.emptyList(),
                Collections.emptyList(),
                Eraftpb.Snapshot.getDefaultInstance(),
                Collections.emptyList(),
                Collections.emptyList(),
                false);
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Mutable builder used by {@link RawNode#readyWithoutAccept()} to assemble
     * a {@link Ready} field-by-field as it walks the raft state machine. Hosts
     * should not need to construct {@code Ready} directly — read it from
     * {@code rn.ready()}.
     */
    public static final class Builder {
        @Nullable SoftState softState;
        Eraftpb.HardState hardState = Eraftpb.HardState.getDefaultInstance();
        List<ReadState> readStates = Collections.emptyList();
        List<Eraftpb.Entry> entries = Collections.emptyList();
        Eraftpb.Snapshot snapshot = Eraftpb.Snapshot.getDefaultInstance();
        List<Eraftpb.Entry> committedEntries = Collections.emptyList();
        List<Eraftpb.Message> messages = Collections.emptyList();
        boolean mustSync;

        public Builder softState(@Nullable SoftState v) { this.softState = v; return this; }
        public Builder hardState(Eraftpb.HardState v) { this.hardState = v; return this; }
        public Builder readStates(List<ReadState> v) { this.readStates = v; return this; }
        public Builder entries(List<Eraftpb.Entry> v) { this.entries = v; return this; }
        public Builder snapshot(Eraftpb.Snapshot v) { this.snapshot = v; return this; }
        public Builder committedEntries(List<Eraftpb.Entry> v) { this.committedEntries = v; return this; }
        public Builder messages(List<Eraftpb.Message> v) { this.messages = v; return this; }
        public Builder mustSync(boolean v) { this.mustSync = v; return this; }

        public Ready build() {
            return new Ready(softState, hardState, readStates, entries, snapshot,
                    committedEntries, messages, mustSync);
        }
    }
}
