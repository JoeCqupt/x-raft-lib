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

import com.google.protobuf.ByteString;
import io.github.xinfra.lab.raft.proto.Eraftpb;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for Util, mirroring etcd-raft util_test.go.
 */
class UtilTest {

    @Test
    void testDescribeEntry() {
        Eraftpb.Entry entry = Eraftpb.Entry.newBuilder()
                .setTerm(1)
                .setIndex(2)
                .setEntryType(Eraftpb.EntryType.EntryNormal)
                .setData(ByteString.copyFromUtf8("hello\u0000world"))
                .build();

        String desc1 = Util.describeEntry(entry, null);
        assertThat(desc1).isEqualTo("1/2 EntryNormal \"hello\u0000world\"");

        Util.EntryFormatter formatter = data -> new String(data).toUpperCase();
        String desc2 = Util.describeEntry(entry, formatter);
        assertThat(desc2).isEqualTo("1/2 EntryNormal HELLO\u0000WORLD");
    }

    @Test
    void testLimitSize() {
        List<Eraftpb.Entry> ents = List.of(
                Eraftpb.Entry.newBuilder().setIndex(4).setTerm(4).build(),
                Eraftpb.Entry.newBuilder().setIndex(5).setTerm(5).build(),
                Eraftpb.Entry.newBuilder().setIndex(6).setTerm(6).build()
        );

        // All entries returned when maxSize is huge.
        assertThat(Util.limitSize(ents, Long.MAX_VALUE)).hasSize(3);

        // Even if maxSize is 0, the first entry should be returned.
        assertThat(Util.limitSize(ents, 0)).hasSize(1);

        long s0 = ents.get(0).getSerializedSize();
        long s1 = ents.get(1).getSerializedSize();
        long s2 = ents.get(2).getSerializedSize();

        // Limit to 2.
        assertThat(Util.limitSize(ents, s0 + s1)).hasSize(2);
        assertThat(Util.limitSize(ents, s0 + s1 + s2 / 2)).hasSize(2);
        assertThat(Util.limitSize(ents, s0 + s1 + s2 - 1)).hasSize(2);

        // All.
        assertThat(Util.limitSize(ents, s0 + s1 + s2)).hasSize(3);
    }

    @Test
    void testIsLocalMsg() {
        assertThat(Util.isLocalMsg(Eraftpb.MessageType.MsgHup)).isTrue();
        assertThat(Util.isLocalMsg(Eraftpb.MessageType.MsgBeat)).isTrue();
        assertThat(Util.isLocalMsg(Eraftpb.MessageType.MsgUnreachable)).isTrue();
        assertThat(Util.isLocalMsg(Eraftpb.MessageType.MsgSnapStatus)).isTrue();
        assertThat(Util.isLocalMsg(Eraftpb.MessageType.MsgCheckQuorum)).isTrue();
        assertThat(Util.isLocalMsg(Eraftpb.MessageType.MsgStorageAppend)).isTrue();
        assertThat(Util.isLocalMsg(Eraftpb.MessageType.MsgStorageAppendResp)).isTrue();
        assertThat(Util.isLocalMsg(Eraftpb.MessageType.MsgStorageApply)).isTrue();
        assertThat(Util.isLocalMsg(Eraftpb.MessageType.MsgStorageApplyResp)).isTrue();

        assertThat(Util.isLocalMsg(Eraftpb.MessageType.MsgTransferLeader)).isFalse();
        assertThat(Util.isLocalMsg(Eraftpb.MessageType.MsgPropose)).isFalse();
        assertThat(Util.isLocalMsg(Eraftpb.MessageType.MsgAppend)).isFalse();
        assertThat(Util.isLocalMsg(Eraftpb.MessageType.MsgAppendResponse)).isFalse();
        assertThat(Util.isLocalMsg(Eraftpb.MessageType.MsgRequestVote)).isFalse();
        assertThat(Util.isLocalMsg(Eraftpb.MessageType.MsgRequestVoteResponse)).isFalse();
        assertThat(Util.isLocalMsg(Eraftpb.MessageType.MsgSnapshot)).isFalse();
        assertThat(Util.isLocalMsg(Eraftpb.MessageType.MsgHeartbeat)).isFalse();
        assertThat(Util.isLocalMsg(Eraftpb.MessageType.MsgHeartbeatResponse)).isFalse();
        assertThat(Util.isLocalMsg(Eraftpb.MessageType.MsgTimeoutNow)).isFalse();
        assertThat(Util.isLocalMsg(Eraftpb.MessageType.MsgReadIndex)).isFalse();
        assertThat(Util.isLocalMsg(Eraftpb.MessageType.MsgReadIndexResp)).isFalse();
        assertThat(Util.isLocalMsg(Eraftpb.MessageType.MsgRequestPreVote)).isFalse();
        assertThat(Util.isLocalMsg(Eraftpb.MessageType.MsgRequestPreVoteResponse)).isFalse();
    }

    @Test
    void testIsResponseMsg() {
        assertThat(Util.isResponseMsg(Eraftpb.MessageType.MsgAppendResponse)).isTrue();
        assertThat(Util.isResponseMsg(Eraftpb.MessageType.MsgRequestVoteResponse)).isTrue();
        assertThat(Util.isResponseMsg(Eraftpb.MessageType.MsgHeartbeatResponse)).isTrue();
        assertThat(Util.isResponseMsg(Eraftpb.MessageType.MsgUnreachable)).isTrue();
        assertThat(Util.isResponseMsg(Eraftpb.MessageType.MsgReadIndexResp)).isTrue();
        assertThat(Util.isResponseMsg(Eraftpb.MessageType.MsgRequestPreVoteResponse)).isTrue();
        assertThat(Util.isResponseMsg(Eraftpb.MessageType.MsgStorageAppendResp)).isTrue();
        assertThat(Util.isResponseMsg(Eraftpb.MessageType.MsgStorageApplyResp)).isTrue();

        assertThat(Util.isResponseMsg(Eraftpb.MessageType.MsgHup)).isFalse();
        assertThat(Util.isResponseMsg(Eraftpb.MessageType.MsgBeat)).isFalse();
        assertThat(Util.isResponseMsg(Eraftpb.MessageType.MsgSnapStatus)).isFalse();
        assertThat(Util.isResponseMsg(Eraftpb.MessageType.MsgCheckQuorum)).isFalse();
        assertThat(Util.isResponseMsg(Eraftpb.MessageType.MsgTransferLeader)).isFalse();
        assertThat(Util.isResponseMsg(Eraftpb.MessageType.MsgPropose)).isFalse();
        assertThat(Util.isResponseMsg(Eraftpb.MessageType.MsgAppend)).isFalse();
        assertThat(Util.isResponseMsg(Eraftpb.MessageType.MsgRequestVote)).isFalse();
        assertThat(Util.isResponseMsg(Eraftpb.MessageType.MsgSnapshot)).isFalse();
        assertThat(Util.isResponseMsg(Eraftpb.MessageType.MsgHeartbeat)).isFalse();
        assertThat(Util.isResponseMsg(Eraftpb.MessageType.MsgTimeoutNow)).isFalse();
        assertThat(Util.isResponseMsg(Eraftpb.MessageType.MsgReadIndex)).isFalse();
        assertThat(Util.isResponseMsg(Eraftpb.MessageType.MsgRequestPreVote)).isFalse();
        assertThat(Util.isResponseMsg(Eraftpb.MessageType.MsgStorageAppend)).isFalse();
        assertThat(Util.isResponseMsg(Eraftpb.MessageType.MsgStorageApply)).isFalse();
    }

    @Test
    void testPayloadSizeOfEmptyEntry() {
        Eraftpb.Entry e = Eraftpb.Entry.getDefaultInstance();
        assertThat(Util.payloadSize(e)).isZero();
    }
}
