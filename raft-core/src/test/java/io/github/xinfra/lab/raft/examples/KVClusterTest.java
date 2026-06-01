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
package io.github.xinfra.lab.raft.examples;

import io.github.xinfra.lab.raft.RaftException;
import org.junit.jupiter.api.Test;


import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end demo: a 3-node replicated KV store on top of {@link io.github.xinfra.lab.raft.RawNode}.
 *
 * <p>Acts as both a smoke test for the example app AND a working sample any
 * future user of the raft library can copy-paste from.
 */
class KVClusterTest {

    @Test
    void putGetDeleteReplicatesAcrossCluster() throws RaftException {
        KVCluster cluster = new KVCluster(1, 2, 3);
        cluster.electLeader(1);

        assertThat(cluster.leader()).isNotNull();
        assertThat(cluster.leader().id).isEqualTo(1L);

        // Put a few keys via the leader.
        cluster.put("k1", "v1");
        cluster.put("k2", "v2");
        cluster.put("k3", "v3");

        // All three nodes' state machines converge.
        for (long id : new long[]{1, 2, 3}) {
            KVStore s = cluster.node(id).kvStore;
            assertThat(s.get("k1")).contains("v1");
            assertThat(s.get("k2")).contains("v2");
            assertThat(s.get("k3")).contains("v3");
            assertThat(s.size()).isEqualTo(3);
        }

        // Update + delete round-trip.
        cluster.put("k1", "v1-updated");
        cluster.delete("k2");

        for (long id : new long[]{1, 2, 3}) {
            KVStore s = cluster.node(id).kvStore;
            assertThat(s.get("k1")).contains("v1-updated");
            assertThat(s.get("k2")).isEmpty();
            assertThat(s.get("k3")).contains("v3");
            assertThat(s.size()).isEqualTo(2);
        }
    }

    @Test
    void commandSerializationRoundTrip() throws RaftException {
        KVStore.Command put = new KVStore.Command(KVStore.Command.Op.PUT, "foo", "bar baz");
        KVStore.Command parsed = KVStore.Command.deserialize(put.serialize());
        assertThat(parsed.op).isEqualTo(KVStore.Command.Op.PUT);
        assertThat(parsed.key).isEqualTo("foo");
        assertThat(parsed.value).isEqualTo("bar baz");

        KVStore.Command del = new KVStore.Command(KVStore.Command.Op.DELETE, "k", null);
        KVStore.Command parsedDel = KVStore.Command.deserialize(del.serialize());
        assertThat(parsedDel.op).isEqualTo(KVStore.Command.Op.DELETE);
        assertThat(parsedDel.key).isEqualTo("k");
    }
}
