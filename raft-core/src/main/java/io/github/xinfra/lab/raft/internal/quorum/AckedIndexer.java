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
package io.github.xinfra.lab.raft.internal.quorum;
import io.github.xinfra.lab.raft.*;
import io.github.xinfra.lab.raft.internal.*;

import java.util.OptionalLong;

/**
 * AckedIndexer allows looking up a commit index for a given ID of a voter
 * from a corresponding MajorityConfig.
 */
public interface AckedIndexer {
    /**
     * Returns the acked index for the given voter ID, or empty if the voter
     * is not found.
     */
    OptionalLong ackedIndex(long voterID);
}
