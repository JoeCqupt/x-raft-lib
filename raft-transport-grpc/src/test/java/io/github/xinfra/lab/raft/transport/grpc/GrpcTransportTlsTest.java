/*
 * Copyright 2024-2026 The x-raft-lib Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * SPDX-License-Identifier: Apache-2.0
 */
package io.github.xinfra.lab.raft.transport.grpc;

import io.github.xinfra.lab.raft.proto.Eraftpb;
import io.grpc.netty.shaded.io.netty.handler.ssl.util.SelfSignedCertificate;
import org.junit.jupiter.api.Test;

import java.net.ServerSocket;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies the transport carries raft traffic over TLS and mTLS. Both
 * directions use a single self-signed cert (CN=localhost) as the node
 * identity and, simultaneously, as the trust anchor — the same pattern a
 * self-signed mesh deployment uses.
 */
class GrpcTransportTlsTest {

    private static int freePort() throws Exception {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    /** One-way TLS: client verifies the server cert; server does not demand a client cert. */
    @Test
    void oneWayTlsRoundTrip() throws Exception {
        SelfSignedCertificate ssc = new SelfSignedCertificate("localhost");
        TlsConfig tls = TlsConfig.builder()
                .certChain(ssc.certificate())
                .privateKey(ssc.privateKey())
                .trustCertCollection(ssc.certificate())
                .build();

        roundTrip(tls, tls);
    }

    /** Mutual TLS: each side presents and verifies a certificate. */
    @Test
    void mutualTlsRoundTrip() throws Exception {
        SelfSignedCertificate ssc = new SelfSignedCertificate("localhost");
        TlsConfig tls = TlsConfig.builder()
                .certChain(ssc.certificate())
                .privateKey(ssc.privateKey())
                .trustCertCollection(ssc.certificate())
                .requireClientAuth(true)
                .build();

        roundTrip(tls, tls);
    }

    private void roundTrip(TlsConfig tls1, TlsConfig tls2) throws Exception {
        int port1 = freePort();
        int port2 = freePort();

        BlockingQueue<Eraftpb.Message> inbox2 = new ArrayBlockingQueue<>(8);

        GrpcTransport t1 = new GrpcTransport(1L, port1, tls1);
        GrpcTransport t2 = new GrpcTransport(2L, port2, tls2);
        try {
            t1.setReceiver(m -> { /* not asserted */ });
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
            assertThat(received).as("heartbeat delivered over TLS").isNotNull();
            assertThat(received.getMsgType()).isEqualTo(Eraftpb.MessageType.MsgHeartbeat);
            assertThat(received.getTerm()).isEqualTo(7L);
            assertThat(received.getCommit()).isEqualTo(42L);
        } finally {
            t1.close();
            t2.close();
        }
    }

    /**
     * A plaintext client cannot talk to a TLS server: the heartbeat never
     * arrives. Guards against silently dropping TLS (which would re-open the
     * plaintext hole this feature closes).
     */
    @Test
    void plaintextClientCannotReachTlsServer() throws Exception {
        SelfSignedCertificate ssc = new SelfSignedCertificate("localhost");
        TlsConfig tls = TlsConfig.builder()
                .certChain(ssc.certificate())
                .privateKey(ssc.privateKey())
                .trustCertCollection(ssc.certificate())
                .build();

        int port1 = freePort();
        int port2 = freePort();

        BlockingQueue<Eraftpb.Message> inbox2 = new ArrayBlockingQueue<>(8);

        GrpcTransport t1 = new GrpcTransport(1L, port1);       // plaintext
        GrpcTransport t2 = new GrpcTransport(2L, port2, tls);  // TLS
        try {
            t1.setReceiver(m -> { });
            t2.setReceiver(inbox2::offer);
            t1.start();
            t2.start();

            t1.addPeer(2L, "localhost:" + port2);

            Eraftpb.Message hb = Eraftpb.Message.newBuilder()
                    .setMsgType(Eraftpb.MessageType.MsgHeartbeat)
                    .setFrom(1L).setTo(2L).setTerm(7L)
                    .build();
            t1.send(2L, hb);

            Eraftpb.Message received = inbox2.poll(2, TimeUnit.SECONDS);
            assertThat(received).as("plaintext must not reach a TLS server").isNull();
        } finally {
            t1.close();
            t2.close();
        }
    }
}
