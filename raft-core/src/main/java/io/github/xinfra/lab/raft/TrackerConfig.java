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

import java.util.Set;

/**
 * An immutable snapshot of the membership configuration tracked by a Raft peer,
 * exposed through {@link Status#config}. This is the public-API counterpart of
 * the internal tracker {@code Config} type.
 *
 * <p>During a joint configuration change both {@link #voters} (the incoming
 * config) and {@link #votersOutgoing} (the outgoing config) are non-empty;
 * otherwise {@link #votersOutgoing} is empty.
 *
 * @param voters         voting members of the incoming configuration
 * @param votersOutgoing voting members of the outgoing configuration (empty when not in a joint change)
 * @param learners       non-voting learner members
 * @param learnersNext   learners that will become voters once the joint change completes
 * @param autoLeave      whether the joint configuration auto-transitions out once committed
 */
public record TrackerConfig(
        Set<Long> voters,
        Set<Long> votersOutgoing,
        Set<Long> learners,
        Set<Long> learnersNext,
        boolean autoLeave) {}
