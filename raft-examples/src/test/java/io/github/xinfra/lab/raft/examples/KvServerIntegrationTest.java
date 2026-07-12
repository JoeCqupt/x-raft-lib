/*
 * Copyright 2024-2026 The x-raft-lib Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * SPDX-License-Identifier: Apache-2.0
 */
package io.github.xinfra.lab.raft.examples;

import io.github.xinfra.lab.raft.RaftStateType;
import io.github.xinfra.lab.raft.proto.Eraftpb;
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
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

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

    private static final long SNAPSHOT_THRESHOLD = 50;
    private static final int MAX_NODES = 6;

    static Stream<Arguments> modeMatrix() {
        return Stream.of(
                Arguments.of(false, false),
                Arguments.of(true, false),
                Arguments.of(false, true)
        );
    }

    @ParameterizedTest(name = "snapshotStreaming={0}, asyncStorageWrites={1}")
    @MethodSource("modeMatrix")
    void fullFeatureShowcase(boolean snapshotStreaming, boolean asyncStorageWrites) throws Exception {
        int[] raftPorts = freePorts(MAX_NODES);
        int[] kvPorts = freePorts(MAX_NODES);

        List<KvServer> servers = new ArrayList<>();
        List<ManagedChannel> channels = new ArrayList<>();
        try {
            // === Phase 1: Single-node bootstrap ===
            Map<Long, String> node1Peers = Map.of(1L, "localhost:" + raftPorts[0]);
            servers.add(createServer(1, raftPorts[0], kvPorts[0], node1Peers, true,
                    snapshotStreaming, asyncStorageWrites));

            KvServer leader = awaitLeaderServer(servers, 15_000);
            assertThat(leader).as("single-node must become leader").isNotNull();
            assertThat(leader.status().id).isEqualTo(1);

            // === Phase 2: Add node 2 (learner → voter) ===
            Map<Long, String> node2Peers = new LinkedHashMap<>();
            node2Peers.put(1L, "localhost:" + raftPorts[0]);
            node2Peers.put(2L, "localhost:" + raftPorts[1]);
            servers.add(createServer(2, raftPorts[1], kvPorts[1], node2Peers, false,
                    snapshotStreaming, asyncStorageWrites));

            KvServer p2Leader = leader;
            p2Leader.addNode(2, "localhost:" + raftPorts[1], true)
                    .get(30, TimeUnit.SECONDS);
            assertThat(awaitTrue(() ->
                    serverById(servers, 2).status().commit >= p2Leader.status().commit - 1, 25_000))
                    .as("node 2 learner caught up").isTrue();

            p2Leader.addNode(2, "localhost:" + raftPorts[1], false)
                    .get(30, TimeUnit.SECONDS);
            assertThat(awaitTrue(() -> {
                var cs = p2Leader.raftKvNode().storage.initialState().confState();
                return cs.getVotersList().contains(2L) && cs.getVotersCount() == 2;
            }, 15_000)).as("node 2 promoted to voter, 2-voter cluster").isTrue();

            // === Phase 3: Add node 3 (learner → voter) ===
            KvServer p3Leader = findLeaderServer(servers);
            assertThat(p3Leader).isNotNull();

            Map<Long, String> node3Peers = new LinkedHashMap<>();
            node3Peers.put(1L, "localhost:" + raftPorts[0]);
            node3Peers.put(2L, "localhost:" + raftPorts[1]);
            node3Peers.put(3L, "localhost:" + raftPorts[2]);
            servers.add(createServer(3, raftPorts[2], kvPorts[2], node3Peers, false,
                    snapshotStreaming, asyncStorageWrites));

            p3Leader.addNode(3, "localhost:" + raftPorts[2], true)
                    .get(30, TimeUnit.SECONDS);
            assertThat(awaitTrue(() ->
                    serverById(servers, 3).status().commit >= p3Leader.status().commit - 1, 25_000))
                    .as("node 3 learner caught up").isTrue();

            p3Leader.addNode(3, "localhost:" + raftPorts[2], false)
                    .get(30, TimeUnit.SECONDS);
            assertThat(awaitTrue(() -> {
                var cs = p3Leader.raftKvNode().storage.initialState().confState();
                return cs.getVotersList().contains(3L) && cs.getVotersCount() == 3;
            }, 15_000)).as("node 3 promoted to voter, 3-voter cluster").isTrue();

            // === Phase 4: Propose + convergence ===
            KvServer p4Leader = findLeaderServer(servers);
            assertThat(p4Leader).isNotNull();

            p4Leader.proposeCommand(KvCommand.newBuilder().setOp(KvCommand.Op.PUT)
                    .setKey("user:1").setValue("alice").build()).get(10, TimeUnit.SECONDS);
            p4Leader.proposeCommand(KvCommand.newBuilder().setOp(KvCommand.Op.PUT)
                    .setKey("user:2").setValue("bob").build()).get(10, TimeUnit.SECONDS);
            p4Leader.proposeCommand(KvCommand.newBuilder().setOp(KvCommand.Op.PUT)
                    .setKey("config:mode").setValue("production").build()).get(10, TimeUnit.SECONDS);
            p4Leader.proposeCommand(KvCommand.newBuilder().setOp(KvCommand.Op.PUT)
                    .setKey("user:1").setValue("alice-updated").build()).get(10, TimeUnit.SECONDS);
            p4Leader.proposeCommand(KvCommand.newBuilder().setOp(KvCommand.Op.DELETE)
                    .setKey("user:2").build()).get(10, TimeUnit.SECONDS);

            assertThat(awaitTrue(() -> servers.stream().allMatch(s ->
                    s.stateMachine().get("user:1").orElse("").equals("alice-updated")
                            && s.stateMachine().get("user:2").isEmpty()
                            && s.stateMachine().get("config:mode").orElse("").equals("production")),
                    15_000)).as("all 3 nodes converged on KV state").isTrue();

            // === Phase 5: Linearizable read ===
            Optional<String> readResult = p4Leader.linearizableGet("user:1").get(10, TimeUnit.SECONDS);
            assertThat(readResult).isPresent().hasValue("alice-updated");

            Optional<String> readMissing = p4Leader.linearizableGet("user:2").get(10, TimeUnit.SECONDS);
            assertThat(readMissing).isEmpty();

            // === Phase 6: Leader transfer ===
            long oldLeaderId = p4Leader.status().id;
            long transferee = servers.stream()
                    .filter(s -> s.status().id != oldLeaderId)
                    .findFirst().orElseThrow().status().id;

            p4Leader.transferLeader(transferee);

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

            // === Phase 7: Add node 4 as learner, then trigger snapshot ===
            // Add node 4 BEFORE writing bulk entries so that the snapshot's
            // ConfState includes node 4 (otherwise raft rejects the snapshot).
            KvServer p7Leader = findLeaderServer(servers);
            assertThat(p7Leader).isNotNull();

            long node4Id = 4;
            Map<Long, String> node4Peers = new LinkedHashMap<>();
            for (KvServer s : servers) {
                long sid = s.status().id;
                node4Peers.put(sid, "localhost:" + raftPorts[(int)(sid - 1)]);
            }
            node4Peers.put(node4Id, "localhost:" + raftPorts[3]);
            servers.add(createServer(node4Id, raftPorts[3], kvPorts[3], node4Peers, false,
                    snapshotStreaming, asyncStorageWrites));

            p7Leader.addNode(node4Id, "localhost:" + raftPorts[3], true)
                    .get(30, TimeUnit.SECONDS);

            // Wait for node 4 learner to catch up before bulk writes.
            assertThat(awaitTrue(() ->
                    serverById(servers, node4Id).status().commit >= p7Leader.status().commit - 1, 25_000))
                    .as("node 4 learner caught up").isTrue();

            // Write bulk entries to exceed SNAPSHOT_THRESHOLD (50) and trigger snapshot.
            for (int i = 0; i < SNAPSHOT_THRESHOLD + 10; i++) {
                p7Leader.proposeCommand(KvCommand.newBuilder().setOp(KvCommand.Op.PUT)
                        .setKey("bulk:" + i).setValue("v" + i).build()).get(10, TimeUnit.SECONDS);
            }

            // Verify snapshot was created on the leader.
            assertThat(awaitTrue(() -> {
                try {
                    var snap = p7Leader.raftKvNode().storage.snapshot();
                    return snap.getMetadata().getIndex() > 0;
                } catch (Exception e) {
                    return false;
                }
            }, 15_000)).as("snapshot created on leader").isTrue();

            // Verify all nodes (including node 4 learner) converged on bulk data.
            assertThat(awaitTrue(() -> servers.stream().allMatch(s ->
                    s.stateMachine().get("bulk:0").orElse("").equals("v0")
                            && s.stateMachine().get("bulk:" + (SNAPSHOT_THRESHOLD - 1))
                                    .orElse("").equals("v" + (SNAPSHOT_THRESHOLD - 1))),
                    15_000)).as("all nodes have bulk data").isTrue();

            // === Phase 8: Promote node 4 to voter ===
            p7Leader.addNode(node4Id, "localhost:" + raftPorts[3], false)
                    .get(30, TimeUnit.SECONDS);
            assertThat(awaitTrue(() -> {
                var cs = p7Leader.raftKvNode().storage.initialState().confState();
                return cs.getVotersList().contains(node4Id) && cs.getVotersCount() == 4;
            }, 15_000)).as("node 4 promoted to voter").isTrue();

            // === Phase 9: gRPC layer ===
            KvServer grpcLeader = findLeaderServer(servers);
            assertThat(grpcLeader).isNotNull();
            int grpcKvPort = kvPorts[(int)(grpcLeader.status().id - 1)];
            ManagedChannel leaderChannel = ManagedChannelBuilder
                    .forAddress("localhost", grpcKvPort)
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

            GetClusterInfoResponse info = adminStub.withDeadlineAfter(10, TimeUnit.SECONDS)
                    .getClusterInfo(GetClusterInfoRequest.getDefaultInstance());
            assertThat(info.getLeaderId()).isPositive();
            assertThat(info.getVotersCount()).isEqualTo(4);

            // === Phase 10: Node removal ===
            KvServer phase10Leader = findLeaderServer(servers);
            assertThat(phase10Leader).isNotNull();

            phase10Leader.removeNode(node4Id).get(30, TimeUnit.SECONDS);

            assertThat(awaitTrue(() -> {
                var cs = phase10Leader.raftKvNode().storage.initialState().confState();
                return !cs.getVotersList().contains(node4Id) && cs.getVotersCount() == 3;
            }, 15_000)).as("node 4 removed from cluster").isTrue();

            servers.remove(serverById(servers, node4Id));

            phase10Leader.proposeCommand(KvCommand.newBuilder().setOp(KvCommand.Op.PUT)
                    .setKey("phase").setValue("10-done").build()).get(10, TimeUnit.SECONDS);
            assertThat(awaitTrue(() -> servers.stream().allMatch(s ->
                    s.stateMachine().get("phase").orElse("").equals("10-done")), 10_000))
                    .as("3-node cluster works after removal").isTrue();

            // === Phase 11: Joint consensus (atomic voter replacement) ===
            KvServer phase11Leader = findLeaderServer(servers);
            assertThat(phase11Leader).isNotNull();
            long victimId = servers.stream()
                    .filter(s -> s.status().id != phase11Leader.status().id)
                    .findFirst().orElseThrow().status().id;

            long replacementId = 5;
            Map<Long, String> replacementPeers = new LinkedHashMap<>();
            for (KvServer s : servers) {
                long sid = s.status().id;
                replacementPeers.put(sid, "localhost:" + raftPorts[(int)(sid - 1)]);
            }
            replacementPeers.put(replacementId, "localhost:" + raftPorts[4]);

            servers.add(createServer(replacementId, raftPorts[4], kvPorts[4],
                    replacementPeers, false, snapshotStreaming, asyncStorageWrites));

            Eraftpb.ConfState jointCs = phase11Leader
                    .replaceNode(victimId, replacementId, "localhost:" + raftPorts[4])
                    .get(30, TimeUnit.SECONDS);

            assertThat(jointCs.getVotersOutgoingCount())
                    .as("enter joint: votersOutgoing must be non-empty").isPositive();
            assertThat(jointCs.getVotersList()).contains(replacementId);
            assertThat(jointCs.getVotersOutgoingList()).contains(victimId);

            assertThat(awaitTrue(() -> {
                var cs = phase11Leader.raftKvNode().storage.initialState().confState();
                return cs.getVotersOutgoingCount() == 0
                        && cs.getVotersList().contains(replacementId)
                        && !cs.getVotersList().contains(victimId)
                        && cs.getVotersCount() == 3;
            }, 15_000)).as("auto-leave joint: final config has replacement, not victim").isTrue();

            KvServer victim = servers.stream()
                    .filter(s -> s.status().id == victimId).findFirst().orElse(null);
            if (victim != null) servers.remove(victim);

            KvServer postJointLeader = awaitLeaderServer(servers, 15_000);
            assertThat(postJointLeader).isNotNull();

            // Write enough entries to trigger a new snapshot whose ConfState
            // includes node 5.  Without this, node 5 can never catch up because
            // the old snapshot (pre-joint) doesn't contain it in the ConfState
            // and raft rejects such snapshots.
            for (int i = 0; i < SNAPSHOT_THRESHOLD + 10; i++) {
                postJointLeader.proposeCommand(KvCommand.newBuilder().setOp(KvCommand.Op.PUT)
                        .setKey("joint:" + i).setValue("j" + i).build()).get(10, TimeUnit.SECONDS);
            }

            postJointLeader.proposeCommand(KvCommand.newBuilder().setOp(KvCommand.Op.PUT)
                    .setKey("phase").setValue("11-done").build()).get(10, TimeUnit.SECONDS);
            assertThat(awaitTrue(() -> servers.stream().allMatch(s ->
                    s.stateMachine().get("phase").orElse("").equals("11-done")), 30_000))
                    .as("cluster works after joint consensus replacement").isTrue();

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

    private KvServer createServer(long id, int raftPort, int kvPort,
                                  Map<Long, String> peers, boolean bootstrap,
                                  boolean snapshotStreaming, boolean asyncStorageWrites) throws Exception {
        return new KvServer(id, raftPort, kvPort, tmp.resolve("node-" + id),
                peers, bootstrap, snapshotStreaming, asyncStorageWrites, SNAPSHOT_THRESHOLD);
    }

    private static KvServer serverById(List<KvServer> servers, long id) {
        return servers.stream().filter(s -> s.status().id == id).findFirst().orElseThrow();
    }

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
