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
package io.github.xinfra.lab.raft.transport.grpc;

import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.netty.handler.ssl.ClientAuth;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContextBuilder;

import javax.net.ssl.SSLException;
import java.io.File;
import java.util.Objects;

/**
 * TLS / mTLS configuration for {@link GrpcTransport}. Inter-node raft
 * traffic (votes, log entries, snapshots) is consensus-critical and must
 * not cross an untrusted network in plaintext, so production deployments
 * should always configure this.
 *
 * <p>Each raft node is simultaneously a gRPC <em>server</em> (it accepts
 * connections from peers) and a gRPC <em>client</em> (it dials peers). One
 * {@code TlsConfig} therefore describes both roles:
 *
 * <ul>
 *   <li><b>Server identity</b> — {@code certChain} + {@code privateKey}. The
 *       certificate this node presents to peers that dial it. Required.</li>
 *   <li><b>Trust anchors</b> — {@code trustCertCollection}. The CA (or set of
 *       peer certs) used to verify the <em>other</em> end. Used by this
 *       node's client side to verify peer servers, and (under mTLS) by its
 *       server side to verify peer clients. {@code null} falls back to the
 *       JDK default trust store.</li>
 *   <li><b>Client identity</b> — only needed for mutual TLS. Defaults to the
 *       server identity ({@code certChain}/{@code privateKey}) so a single
 *       per-node certificate covers both directions; override with
 *       {@code clientCertChain}/{@code clientPrivateKey} if you issue
 *       separate client certs.</li>
 *   <li><b>{@code requireClientAuth}</b> — when {@code true}, this node's
 *       server demands a valid client certificate from every dialing peer
 *       (mutual TLS). When {@code false}, only the server is authenticated
 *       (one-way TLS).</li>
 * </ul>
 *
 * <p>The certificate's subject (CN/SAN) must match the address peers use in
 * {@link GrpcTransport#addPeer(long, String)} (e.g. {@code host:port} → the
 * cert must be valid for {@code host}). For test or split-horizon setups
 * where they differ, set {@link Builder#authorityOverride(String)}.
 *
 * <p>Build instances with {@link #builder()}.
 */
public final class TlsConfig {

    private final File certChain;
    private final File privateKey;
    private final String privateKeyPassword;
    private final File trustCertCollection;
    private final File clientCertChain;
    private final File clientPrivateKey;
    private final String clientPrivateKeyPassword;
    private final boolean requireClientAuth;
    private final String authorityOverride;

    private TlsConfig(Builder b) {
        this.certChain = Objects.requireNonNull(b.certChain, "certChain (server certificate) is required");
        this.privateKey = Objects.requireNonNull(b.privateKey, "privateKey (server key) is required");
        this.privateKeyPassword = b.privateKeyPassword;
        this.trustCertCollection = b.trustCertCollection;
        this.clientCertChain = b.clientCertChain;
        this.clientPrivateKey = b.clientPrivateKey;
        this.clientPrivateKeyPassword = b.clientPrivateKeyPassword;
        this.requireClientAuth = b.requireClientAuth;
        this.authorityOverride = b.authorityOverride;
        if (requireClientAuth) {
            // mTLS needs a client identity (own or inherited) plus a trust
            // anchor to verify the other side. We allow trust=null only if
            // the JDK default store can validate the peers (public CA);
            // self-signed mesh certs must supply trustCertCollection.
            File cc = clientCertChain != null ? clientCertChain : certChain;
            File ck = clientPrivateKey != null ? clientPrivateKey : privateKey;
            if (cc == null || ck == null) {
                throw new IllegalArgumentException(
                        "requireClientAuth=true needs a client cert/key (or a server cert/key to inherit)");
            }
        }
    }

    String authorityOverride() {
        return authorityOverride;
    }

    /**
     * Builds the server-side {@link SslContext}: presents this node's
     * certificate and, under mTLS, demands and verifies a peer client cert.
     * Package-private so the shaded netty type does not leak into the
     * module's public API.
     */
    SslContext buildServerContext() throws SSLException {
        SslContextBuilder b = (privateKeyPassword == null)
                ? SslContextBuilder.forServer(certChain, privateKey)
                : SslContextBuilder.forServer(certChain, privateKey, privateKeyPassword);
        if (requireClientAuth) {
            if (trustCertCollection != null) {
                b.trustManager(trustCertCollection);
            }
            b.clientAuth(ClientAuth.REQUIRE);
        }
        return GrpcSslContexts.configure(b).build();
    }

    /**
     * Builds the client-side {@link SslContext}: verifies peer servers
     * against the trust anchors and, under mTLS, presents this node's
     * client certificate. Package-private to keep shaded netty internal.
     */
    SslContext buildClientContext() throws SSLException {
        SslContextBuilder b = SslContextBuilder.forClient();
        if (trustCertCollection != null) {
            b.trustManager(trustCertCollection);
        }
        if (requireClientAuth) {
            File cc = clientCertChain != null ? clientCertChain : certChain;
            File ck = clientPrivateKey != null ? clientPrivateKey : privateKey;
            String pw = clientPrivateKey != null ? clientPrivateKeyPassword : privateKeyPassword;
            if (pw == null) {
                b.keyManager(cc, ck);
            } else {
                b.keyManager(cc, ck, pw);
            }
        }
        return GrpcSslContexts.configure(b).build();
    }

    public static Builder builder() {
        return new Builder();
    }

    /** Fluent builder for {@link TlsConfig}. */
    public static final class Builder {
        private File certChain;
        private File privateKey;
        private String privateKeyPassword;
        private File trustCertCollection;
        private File clientCertChain;
        private File clientPrivateKey;
        private String clientPrivateKeyPassword;
        private boolean requireClientAuth;
        private String authorityOverride;

        private Builder() {}

        /** Server certificate chain (PEM). Required. */
        public Builder certChain(File certChain) {
            this.certChain = certChain;
            return this;
        }

        /** Server private key (PEM, PKCS#8). Required. */
        public Builder privateKey(File privateKey) {
            this.privateKey = privateKey;
            return this;
        }

        /** Password for an encrypted server private key. Optional. */
        public Builder privateKeyPassword(String privateKeyPassword) {
            this.privateKeyPassword = privateKeyPassword;
            return this;
        }

        /**
         * Trust anchors (CA bundle, PEM) used to verify the other end.
         * Omit to use the JDK default trust store.
         */
        public Builder trustCertCollection(File trustCertCollection) {
            this.trustCertCollection = trustCertCollection;
            return this;
        }

        /** Separate client certificate chain for mTLS. Defaults to {@link #certChain}. */
        public Builder clientCertChain(File clientCertChain) {
            this.clientCertChain = clientCertChain;
            return this;
        }

        /** Separate client private key for mTLS. Defaults to {@link #privateKey}. */
        public Builder clientPrivateKey(File clientPrivateKey) {
            this.clientPrivateKey = clientPrivateKey;
            return this;
        }

        /** Password for an encrypted client private key. Optional. */
        public Builder clientPrivateKeyPassword(String clientPrivateKeyPassword) {
            this.clientPrivateKeyPassword = clientPrivateKeyPassword;
            return this;
        }

        /** Enable mutual TLS: this node's server demands a valid peer client cert. */
        public Builder requireClientAuth(boolean requireClientAuth) {
            this.requireClientAuth = requireClientAuth;
            return this;
        }

        /**
         * Override the TLS authority (SNI / cert-name check) used when dialing
         * peers. Useful when the dialed {@code host:port} does not match the
         * certificate subject (common in tests). Use with care in production.
         */
        public Builder authorityOverride(String authorityOverride) {
            this.authorityOverride = authorityOverride;
            return this;
        }

        public TlsConfig build() {
            return new TlsConfig(this);
        }
    }
}
