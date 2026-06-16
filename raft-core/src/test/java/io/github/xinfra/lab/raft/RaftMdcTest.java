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

import io.github.xinfra.lab.raft.internal.Util;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;

import static org.assertj.core.api.Assertions.assertThat;

class RaftMdcTest {

    @AfterEach
    void cleanup() {
        RaftMdc.clear();
    }

    @Test
    void setSetsAllFourKeys() {
        RaftMdc.set(42, 7, RaftStateType.StateLeader, 42);

        assertThat(MDC.get(RaftMdc.NODE_ID)).isEqualTo("42");
        assertThat(MDC.get(RaftMdc.TERM)).isEqualTo("7");
        assertThat(MDC.get(RaftMdc.STATE)).isEqualTo("StateLeader");
        assertThat(MDC.get(RaftMdc.LEADER)).isEqualTo("42");
    }

    @Test
    void setRendersNoneLeaderAsNone() {
        RaftMdc.set(1, 3, RaftStateType.StateFollower, Util.NONE);

        assertThat(MDC.get(RaftMdc.LEADER)).isEqualTo("none");
    }

    @Test
    void setHandlesNullState() {
        RaftMdc.set(1, 1, null, 2);

        assertThat(MDC.get(RaftMdc.STATE)).isEmpty();
    }

    @Test
    void clearRemovesOnlyRaftKeys() {
        MDC.put("appKey", "appValue");
        RaftMdc.set(1, 1, RaftStateType.StateCandidate, Util.NONE);

        RaftMdc.clear();

        assertThat(MDC.get(RaftMdc.NODE_ID)).isNull();
        assertThat(MDC.get(RaftMdc.TERM)).isNull();
        assertThat(MDC.get(RaftMdc.STATE)).isNull();
        assertThat(MDC.get(RaftMdc.LEADER)).isNull();
        assertThat(MDC.get("appKey")).isEqualTo("appValue");

        MDC.remove("appKey");
    }

    @Test
    void setOverwritesPreviousValues() {
        RaftMdc.set(1, 1, RaftStateType.StateFollower, Util.NONE);
        RaftMdc.set(2, 5, RaftStateType.StateLeader, 2);

        assertThat(MDC.get(RaftMdc.NODE_ID)).isEqualTo("2");
        assertThat(MDC.get(RaftMdc.TERM)).isEqualTo("5");
        assertThat(MDC.get(RaftMdc.STATE)).isEqualTo("StateLeader");
        assertThat(MDC.get(RaftMdc.LEADER)).isEqualTo("2");
    }
}
