/*
 * Copyright 2024-2026 The x-raft-lib Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * SPDX-License-Identifier: Apache-2.0
 */
package io.github.xinfra.lab.raft.examples;

import io.github.xinfra.lab.raft.RaftStateType;
import io.github.xinfra.lab.raft.examples.proto.DeleteRequest;
import io.github.xinfra.lab.raft.examples.proto.GetRequest;
import io.github.xinfra.lab.raft.examples.proto.GetResponse;
import io.github.xinfra.lab.raft.examples.proto.KvCommand;
import io.github.xinfra.lab.raft.examples.proto.KvServiceGrpc;
import io.github.xinfra.lab.raft.examples.proto.PutRequest;
import io.github.xinfra.lab.raft.examples.proto.KvAdminServiceGrpc;
import io.github.xinfra.lab.raft.examples.proto.GetClusterInfoRequest;
import io.github.xinfra.lab.raft.examples.proto.GetClusterInfoResponse;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.ServerSocket;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;

class KvServerIntegrationTest {

    @TempDir Path tmp;

    @Test
    void fullFeatureShowcase() throws Exception {
        int nodeCount = 3;
        int[] raftPorts = freePorts(nodeCount + 1);
        int[] kvPorts = freePorts(nodeCount + 1);

        Map<Long, String> peers = new LinkedHashMap<>();
        for (int i = 0; i < nodeCount; i++) {
            peers.put((long) (i + 1), "localhost:" + raftPorts[i]);
        }

        List<KvServer> servers = new ArrayList<>();
        List<ManagedChannel> channels = new ArrayList<>();
        try {
            // === Phase 1: Bootstrap 3-node cluster ===
            for (int i = 0; i < nodeCount; i++) {
                long id = i + 1;
                servers.add(new KvServer(id, raftPorts[i], kvPorts[i],
                        tmp.resolve("node-" + id), peers, true));
            }

            KvServer leader = awaitLeaderServer(servers, 15_000);
            assertThat(leader).as("must elect a leader").isNotNull();

            // === Phase 2: KV CRUD via direct API (verify propose+apply works) ===
            leader.proposeCommand(KvCommand.newBuilder().setOp(KvCommand.Op.PUT)
                    .setKey("user:1").setValue("alice").build()).get(10, TimeUnit.SECONDS);
            leader.proposeCommand(KvCommand.newBuilder().setOp(KvCommand.Op.PUT)
                    .setKey("user:2").setValue("bob").build()).get(10, TimeUnit.SECONDS);
            leader.proposeCommand(KvCommand.newBuilder().setOp(KvCommand.Op.PUT)
                    .setKey("config:mode").setValue("production").build()).get(10, TimeUnit.SECONDS);
            leader.proposeCommand(KvCommand.newBuilder().setOp(KvCommand.Op.PUT)
                    .setKey("user:1").setValue("alice-updated").build()).get(10, TimeUnit.SECONDS);
            leader.proposeCommand(KvCommand.newBuilder().setOp(KvCommand.Op.DELETE)
                    .setKey("user:2").build()).get(10, TimeUnit.SECONDS);

            assertThat(awaitTrue(() -> servers.stream().allMatch(s ->
                    s.stateMachine().get("user:1").orElse("").equals("alice-updated")
                            && s.stateMachine().get("user:2").isEmpty()
                            && s.stateMachine().get("config:mode").orElse("").equals("production")),
                    15_000)).as("all nodes converged").isTrue();

            // === Phase 3: ReadIndex (linearizable read via direct API) ===
            Optional<String> readResult = leader.linearizableGet("user:1").get(10, TimeUnit.SECONDS);
            assertThat(readResult).isPresent().hasValue("alice-updated");

            Optional<String> readMissing = leader.linearizableGet("user:2").get(10, TimeUnit.SECONDS);
            assertThat(readMissing).isEmpty();

            // === Phase 4: gRPC CRUD (verify gRPC layer works) ===
            ManagedChannel leaderChannel = ManagedChannelBuilder
                    .forAddress("localhost", kvPorts[(int) (leader.status().id - 1)])
                    .usePlaintext().build();
            channels.add(leaderChannel);
            KvServiceGrpc.KvServiceBlockingStub kvStub = KvServiceGrpc.newBlockingStub(leaderChannel);
            KvAdminServiceGrpc.KvAdminServiceBlockingStub adminStub = KvAdminServiceGrpc.newBlockingStub(leaderChannel);

            kvStub.withDeadlineAfter(10, TimeUnit.SECONDS)
                    .put(PutRequest.newBuilder().setKey("grpc:test").setValue("works").build());
            GetResponse grpcGet = kvStub.withDeadlineAfter(10, TimeUnit.SECONDS)
                    .get(GetRequest.newBuilder().setKey("grpc:test").build());
            assertThat(grpcGet.getFound()).isTrue();
            assertThat(grpcGet.getValue()).isEqualTo("works");

            kvStub.withDeadlineAfter(10, TimeUnit.SECONDS)
                    .delete(DeleteRequest.newBuilder().setKey("grpc:test").build());
            GetResponse afterDelete = kvStub.withDeadlineAfter(10, TimeUnit.SECONDS)
                    .get(GetRequest.newBuilder().setKey("grpc:test").build());
            assertThat(afterDelete.getFound()).isFalse();

            // === Phase 5: Cluster info ===
            GetClusterInfoResponse info = adminStub.withDeadlineAfter(10, TimeUnit.SECONDS)
                    .getClusterInfo(GetClusterInfoRequest.getDefaultInstance());
            assertThat(info.getLeaderId()).isPositive();
            assertThat(info.getVotersCount()).isEqualTo(3);

            // === Phase 6: Leader Transfer ===
            long oldLeaderId = leader.status().id;
            long transferee = servers.stream()
                    .filter(s -> s.status().id != oldLeaderId)
                    .findFirst().orElseThrow().status().id;

            leader.transferLeader(transferee);

            assertThat(awaitTrue(() -> {
                for (KvServer s : servers) {
                    if (s.status().id == transferee && s.status().state == RaftStateType.StateLeader) {
                        return true;
                    }
                }
                return false;
            }, 15_000)).as("leadership transferred to node %d", transferee).isTrue();

            KvServer newLeader = findLeaderServer(servers);
            assertThat(newLeader).isNotNull();

            newLeader.proposeCommand(KvCommand.newBuilder().setOp(KvCommand.Op.PUT)
                    .setKey("phase").setValue("6-done").build()).get(10, TimeUnit.SECONDS);
            assertThat(awaitTrue(() -> servers.stream().allMatch(s ->
                    s.stateMachine().get("phase").orElse("").equals("6-done")), 10_000))
                    .as("post-transfer propose converged").isTrue();

            // === Phase 7: Dynamic Membership (ConfChange) ===
            long joinerId = nodeCount + 1;
            Map<Long, String> allPeers = new LinkedHashMap<>(peers);
            allPeers.put(joinerId, "localhost:" + raftPorts[nodeCount]);

            KvServer joiner = new KvServer(joinerId, raftPorts[nodeCount], kvPorts[nodeCount],
                    tmp.resolve("node-" + joinerId), allPeers, false);
            servers.add(joiner);

            newLeader.addNode(joinerId, "localhost:" + raftPorts[nodeCount], true)
                    .get(30, TimeUnit.SECONDS);

            assertThat(awaitTrue(() ->
                    joiner.status().commit >= newLeader.status().commit - 1, 25_000))
                    .as("learner caught up").isTrue();

            newLeader.addNode(joinerId, "localhost:" + raftPorts[nodeCount], false)
                    .get(30, TimeUnit.SECONDS);

            assertThat(awaitTrue(() -> {
                var cs = newLeader.raftKvNode().storage.initialState().confState();
                return cs.getVotersList().contains(joinerId) && cs.getVotersCount() == 4;
            }, 15_000)).as("node promoted to voter").isTrue();

            newLeader.proposeCommand(KvCommand.newBuilder().setOp(KvCommand.Op.PUT)
                    .setKey("phase").setValue("7-done").build()).get(10, TimeUnit.SECONDS);
            assertThat(awaitTrue(() -> servers.stream().allMatch(s ->
                    s.stateMachine().get("phase").orElse("").equals("7-done")), 10_000))
                    .as("4-node cluster converged").isTrue();

        } finally {
            for (ManagedChannel ch : channels) {
                ch.shutdown();
                try { ch.awaitTermination(2, TimeUnit.SECONDS); } catch (InterruptedException ignored) {}
            }
            for (int i = servers.size() - 1; i >= 0; i--) {
                try { servers.get(i).close(); } catch (Throwable ignored) {}
            }
        }
    }

    // ---- helpers ----

    private static int[] freePorts(int n) throws Exception {
        ServerSocket[] sockets = new ServerSocket[n];
        int[] ports = new int[n];
        try {
            for (int i = 0; i < n; i++) {
                sockets[i] = new ServerSocket(0);
                ports[i] = sockets[i].getLocalPort();
            }
        } finally {
            for (ServerSocket s : sockets) {
                if (s != null) s.close();
            }
        }
        return ports;
    }

    private static KvServer findLeaderServer(List<KvServer> servers) {
        for (KvServer s : servers) {
            if (s.status().state == RaftStateType.StateLeader) return s;
        }
        return null;
    }

    private static KvServer awaitLeaderServer(List<KvServer> servers, long timeoutMillis) throws InterruptedException {
        KvServer[] leader = {null};
        awaitTrue(() -> {
            KvServer l = findLeaderServer(servers);
            if (l != null) { leader[0] = l; return true; }
            return false;
        }, timeoutMillis);
        return leader[0];
    }

    private static boolean awaitTrue(BooleanSupplier cond, long timeoutMillis) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            if (cond.getAsBoolean()) return true;
            Thread.sleep(25);
        }
        return cond.getAsBoolean();
    }
}
