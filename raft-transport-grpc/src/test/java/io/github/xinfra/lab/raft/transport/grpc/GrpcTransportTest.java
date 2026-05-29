/*
 * Copyright 2024-2026 The x-raft-lib Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * SPDX-License-Identifier: Apache-2.0
 */
package io.github.xinfra.lab.raft.transport.grpc;

import com.google.protobuf.ByteString;
import io.github.xinfra.lab.raft.proto.Eraftpb;
import org.junit.jupiter.api.Test;

import java.net.ServerSocket;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class GrpcTransportTest {

    private static int freePort() throws Exception {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    /**
     * Two-node loopback: node 1 on port A, node 2 on port B. Each is a
     * server and a client of the other. A heartbeat MsgHeartbeat sent
     * from 1 → 2 is observed by node 2's receiver.
     */
    @Test
    void unaryRoundTrip() throws Exception {
        int port1 = freePort();
        int port2 = freePort();

        BlockingQueue<Eraftpb.Message> inbox2 = new ArrayBlockingQueue<>(8);

        GrpcTransport t1 = new GrpcTransport(1L, port1);
        GrpcTransport t2 = new GrpcTransport(2L, port2);
        try {
            t1.setReceiver(m -> { /* not asserted in this test */ });
            t2.setReceiver(inbox2::offer);
            t1.start();
            t2.start();

            t1.addPeer(2L, "localhost:" + port2);

            Eraftpb.Message hb = Eraftpb.Message.newBuilder()
                    .setMsgType(Eraftpb.MessageType.MsgHeartbeat)
                    .setFrom(1L).setTo(2L).setTerm(7L).setCommit(42L)
                    .build();
            t1.send(2L, hb);

            Eraftpb.Message received = inbox2.poll(5, TimeUnit.SECONDS);
            assertThat(received).as("heartbeat delivered").isNotNull();
            assertThat(received.getMsgType()).isEqualTo(Eraftpb.MessageType.MsgHeartbeat);
            assertThat(received.getFrom()).isEqualTo(1L);
            assertThat(received.getTo()).isEqualTo(2L);
            assertThat(received.getTerm()).isEqualTo(7L);
            assertThat(received.getCommit()).isEqualTo(42L);
        } finally {
            t1.close();
            t2.close();
        }
    }

    /**
     * Snapshot chunking: a 5 MB snapshot data blob is sent via streaming
     * RPC and the receiver re-assembles the original {@code Eraftpb.Message}
     * with the full snapshot.data byte-for-byte. Uses a 1 MB chunk size so
     * the test exercises the multi-chunk path.
     */
    @Test
    void snapshotChunkRoundTrip() throws Exception {
        int port1 = freePort();
        int port2 = freePort();

        BlockingQueue<Eraftpb.Message> inbox2 = new ArrayBlockingQueue<>(2);

        GrpcTransport t1 = new GrpcTransport(1L, port1, 1 << 20);
        GrpcTransport t2 = new GrpcTransport(2L, port2, 1 << 20);
        try {
            t1.setReceiver(m -> { });
            t2.setReceiver(inbox2::offer);
            t1.start();
            t2.start();
            t1.addPeer(2L, "localhost:" + port2);

            byte[] data = new byte[5 * (1 << 20) + 17]; // 5 MiB + 17 bytes
            for (int i = 0; i < data.length; i++) data[i] = (byte) (i * 31 + 7);

            Eraftpb.Snapshot snap = Eraftpb.Snapshot.newBuilder()
                    .setMetadata(Eraftpb.SnapshotMetadata.newBuilder()
                            .setIndex(123L).setTerm(2L))
                    .setData(ByteString.copyFrom(data))
                    .build();
            Eraftpb.Message msg = Eraftpb.Message.newBuilder()
                    .setMsgType(Eraftpb.MessageType.MsgSnapshot)
                    .setFrom(1L).setTo(2L).setTerm(2L)
                    .setSnapshot(snap)
                    .build();
            t1.send(2L, msg);

            Eraftpb.Message received = inbox2.poll(15, TimeUnit.SECONDS);
            assertThat(received).as("snapshot delivered").isNotNull();
            assertThat(received.getMsgType()).isEqualTo(Eraftpb.MessageType.MsgSnapshot);
            assertThat(received.getSnapshot().getMetadata().getIndex()).isEqualTo(123L);
            assertThat(received.getSnapshot().getMetadata().getTerm()).isEqualTo(2L);
            byte[] reassembled = received.getSnapshot().getData().toByteArray();
            assertThat(reassembled.length).isEqualTo(data.length);
            assertThat(reassembled).isEqualTo(data);
        } finally {
            t1.close();
            t2.close();
        }
    }
}
