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

/**
 * An immutable snapshot of the leader's view of one peer's replication
 * progress, exposed through {@link Status#progress}. This is the public-API
 * counterpart of the internal tracker {@code Progress} type.
 *
 * @param match            highest log index known to be replicated to the peer
 * @param next             next log index the leader will send to the peer
 * @param state            replication state for the peer
 * @param pendingSnapshot  index of the in-flight snapshot, or {@code 0} if none
 * @param recentActive     whether the peer responded since the last election timeout
 * @param paused           whether the leader is currently withholding entries from the peer
 * @param inflightCount    number of in-flight {@code MsgApp} messages
 * @param learner          whether the peer is a learner (non-voting)
 */
public record PeerProgress(
        long match,
        long next,
        ProgressState state,
        long pendingSnapshot,
        boolean recentActive,
        boolean paused,
        int inflightCount,
        boolean learner) {}
