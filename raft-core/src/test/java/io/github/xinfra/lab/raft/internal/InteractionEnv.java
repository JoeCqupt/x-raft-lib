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

import io.github.xinfra.lab.raft.internal.datadriven.Datadriven;
import io.github.xinfra.lab.raft.proto.Eraftpb;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Multi-node simulation environment for datadriven raft tests, modelled
 * loosely on etcd-raft's rafttest.InteractionEnv but tailored to the Java
 * {@link RawNode} API. Each node is driven by an in-memory {@link MemoryStorage};
 * messages are routed manually between nodes (no real network).
 *
 * <p>Supported commands:
 * <ul>
 *   <li>{@code add-nodes id=<id1>,<id2>,... voters=<id1>,<id2>
 *       [prevote=true] [checkquorum=true] [election-tick=<n>] [heartbeat-tick=<n>]}
 *       — initialize one or more nodes with the given voter set and optional
 *       PreVote/CheckQuorum configuration.</li>
 *   <li>{@code campaign id=<id>} — trigger a Hup on the given node.</li>
 *   <li>{@code propose id=<id> data=<bytes>} — submit a proposal.</li>
 *   <li>{@code tick id=<id> n=<count>} — advance the logical clock {@code n}
 *       times (default 1). Use to drive election timeouts deterministically.</li>
 *   <li>{@code tick-election id=<id>} — convenience for ticking the configured
 *       electionTick + heartbeatTick times, enough to fire one election timeout
 *       on the named node.</li>
 *   <li>{@code deliver-msgs id=<id>} — process all queued inbox messages on
 *       the given node, without touching others.</li>
 *   <li>{@code drop-msgs id=<id> from=<id?>} — drop pending inbox messages
 *       on {@code id}; if {@code from} is supplied, only drop messages from
 *       that sender. Models a network partition.</li>
 *   <li>{@code process-ready id=<id>} — drain a single Ready on the given
 *       node and route its outgoing messages.</li>
 *   <li>{@code stabilize} — drain Ready/messages across all nodes until quiescent.</li>
 *   <li>{@code status id=<id>} — print compact node status.</li>
 *   <li>{@code log-state id=<id>} — print the node's current log entries.</li>
 *   <li>{@code propose-conf-change id=<id> changes=<spec>} — propose a V2
 *       ConfChange whose changes are encoded as comma-separated tokens
 *       {@code v<id>}/{@code l<id>}/{@code r<id>} (voter, learner, remove).</li>
 *   <li>{@code apply-conf-change id=<id> changes=<spec>} — apply a V2
 *       ConfChange directly (use after the entry has been committed).</li>
 *   <li>{@code transfer-leader id=<id> to=<id>} — send MsgTransferLeader on
 *       node {@code id} so leadership transfers to {@code to}.</li>
 *   <li>{@code forget-leader id=<id>} — make {@code id} forget its leader.</li>
 *   <li>{@code report-unreachable id=<id> target=<id>} — tell {@code id}'s
 *       raft that {@code target} is unreachable.</li>
 *   <li>{@code progress id=<id>} — print per-peer Progress (state/match/next),
 *       useful for verifying probe/replicate transitions.</li>
 *   <li>{@code create-snapshot id=<id> index=<idx>} — call storage.createSnapshot
 *       at {@code idx} using the node's current ConfState.</li>
 *   <li>{@code compact id=<id> index=<idx>} — call storage.compact, discarding
 *       all entries up to and including {@code idx}.</li>
 *   <li>{@code log-level <level>} — accepted but ignored (logger config is global).</li>
 * </ul>
 */
public class InteractionEnv {
    private final Map<Long, NodeContext> nodes = new LinkedHashMap<>();

    /** Per-node simulation state. */
    public static class NodeContext {
        public final long id;
        public final MemoryStorage storage;
        public final RawNode rn;
        // Inbox of messages addressed to this node, populated by stabilize().
        final List<Eraftpb.Message> inbox = new ArrayList<>();

        NodeContext(long id, MemoryStorage storage, RawNode rn) {
            this.id = id;
            this.storage = storage;
            this.rn = rn;
        }
    }

    /** Dispatches a directive to its handler. */
    public String handle(Datadriven.Directive d) throws RaftException {
        switch (d.command) {
            case "log-level":            return "ok";
            case "add-nodes":            return cmdAddNodes(d);
            case "campaign":             return cmdCampaign(d);
            case "propose":              return cmdPropose(d);
            case "tick":                 return cmdTick(d);
            case "tick-election":        return cmdTickElection(d);
            case "deliver-msgs":         return cmdDeliverMsgs(d);
            case "drop-msgs":            return cmdDropMsgs(d);
            case "process-ready":        return cmdProcessReady(d);
            case "stabilize":            return cmdStabilize(d);
            case "status":               return cmdStatus(d);
            case "log-state":            return cmdLogState(d);
            case "propose-conf-change":  return cmdProposeConfChange(d);
            case "apply-conf-change":    return cmdApplyConfChange(d);
            case "transfer-leader":      return cmdTransferLeader(d);
            case "forget-leader":        return cmdForgetLeader(d);
            case "report-unreachable":   return cmdReportUnreachable(d);
            case "progress":             return cmdProgress(d);
            case "create-snapshot":      return cmdCreateSnapshot(d);
            case "compact":              return cmdCompact(d);
            default:
                throw new IllegalArgumentException("unknown command: " + d.command);
        }
    }

    // ---- Commands ----

    private String cmdAddNodes(Datadriven.Directive d) {
        List<Long> idList = parseIds(d.getKv("id", null));
        List<Long> voters = parseIds(d.getKv("voters", ""));
        boolean preVote = Boolean.parseBoolean(d.getKv("prevote", "false"));
        boolean checkQuorum = Boolean.parseBoolean(d.getKv("checkquorum", "false"));
        int electionTick = Integer.parseInt(d.getKv("election-tick", "10"));
        int heartbeatTick = Integer.parseInt(d.getKv("heartbeat-tick", "1"));
        int inflight = Integer.parseInt(d.getKv("inflight", "256"));

        StringBuilder out = new StringBuilder();
        for (long id : idList) {
            MemoryStorage s = new MemoryStorage();
            // Pre-set the conf state so RawNode initializes with the right voter set.
            Eraftpb.SnapshotMetadata.Builder mb = s.getSnapshot().getMetadata().toBuilder();
            Eraftpb.ConfState.Builder cb = mb.getConfState().toBuilder();
            for (long v : voters) cb.addVoters(v);
            mb.setConfState(cb);
            s.setSnapshot(s.getSnapshot().toBuilder().setMetadata(mb).build());

            Config cfg = Config.builder()
                    .id(id)
                    .electionTick(electionTick)
                    .heartbeatTick(heartbeatTick)
                    .storage(s)
                    .maxSizePerMsg(Long.MAX_VALUE)
                    .maxInflightMsgs(inflight)
                    .preVote(preVote)
                    .checkQuorum(checkQuorum)
                    .build();

            RawNode rn = RawNode.newRawNode(cfg);
            nodes.put(id, new NodeContext(id, s, rn));
            out.append(String.format("INFO %d switched to configuration voters=(%s)%n",
                    id, joinIds(voters)));
            out.append(String.format("INFO %d became follower at term 0%n", id));
        }
        return out.toString();
    }

    private String cmdTickElection(Datadriven.Directive d) {
        long id = Long.parseLong(d.getKv("id", null));
        NodeContext ctx = requireNode(id);
        // Tick electionTick + heartbeatTick times — enough to guarantee one
        // election timeout fires (electionTimeout is randomized to
        // [electionTick, 2*electionTick), so this isn't strictly enough; we
        // tick 2 * electionTick to be safe).
        int n = 2 * ctx.rn.raft.electionTimeout;
        for (int i = 0; i < n; i++) {
            ctx.rn.tick();
        }
        return "ok";
    }

    private String cmdCampaign(Datadriven.Directive d) throws RaftException {
        long id = Long.parseLong(d.getKv("id", d.args.isEmpty() ? null : d.args.get(0)));
        NodeContext ctx = requireNode(id);
        ctx.rn.campaign();
        // Report the post-campaign state so PreCandidate vs Candidate is visible.
        return String.format("INFO %d is starting a new election%nINFO %d became %s at term %d",
                id, id, ctx.rn.raft.state, ctx.rn.raft.term);
    }

    private String cmdPropose(Datadriven.Directive d) throws RaftException {
        long id = Long.parseLong(d.getKv("id", null));
        String data = d.getKv("data", "");
        NodeContext ctx = requireNode(id);
        ctx.rn.propose(data.getBytes());
        return "ok";
    }

    private String cmdTick(Datadriven.Directive d) {
        long id = Long.parseLong(d.getKv("id", null));
        int n = Integer.parseInt(d.getKv("n", "1"));
        NodeContext ctx = requireNode(id);
        for (int i = 0; i < n; i++) {
            ctx.rn.tick();
        }
        return "ok";
    }

    private String cmdDeliverMsgs(Datadriven.Directive d) {
        long id = Long.parseLong(d.getKv("id", null));
        Long fromFilter = d.kvArgs.containsKey("from")
                ? Long.parseLong(d.getKv("from", null)) : null;
        NodeContext ctx = requireNode(id);
        StringBuilder out = new StringBuilder();
        boolean processed = drainInbox(ctx, out, fromFilter, /*drop*/ false);
        if (!processed) out.append(String.format("> %d: inbox empty%n", ctx.id));
        return out.toString();
    }

    private String cmdDropMsgs(Datadriven.Directive d) {
        long id = Long.parseLong(d.getKv("id", null));
        Long fromFilter = d.kvArgs.containsKey("from")
                ? Long.parseLong(d.getKv("from", null)) : null;
        NodeContext ctx = requireNode(id);
        StringBuilder out = new StringBuilder();
        drainInbox(ctx, out, fromFilter, /*drop*/ true);
        return out.toString();
    }

    private String cmdProcessReady(Datadriven.Directive d) throws RaftException {
        long id = Long.parseLong(d.getKv("id", null));
        NodeContext ctx = requireNode(id);
        StringBuilder out = new StringBuilder();
        if (!ctx.rn.hasReady()) {
            out.append(String.format("> %d: no Ready%n", ctx.id));
        } else {
            handleReady(ctx, out);
        }
        return out.toString();
    }

    private String cmdStabilize(Datadriven.Directive d) throws RaftException {
        StringBuilder out = new StringBuilder();
        // Iterate Ready/dispatch until no node has any pending work, capped to
        // avoid runaway loops in misconfigured tests.
        for (int iter = 0; iter < 200; iter++) {
            boolean progress = false;
            for (NodeContext ctx : nodes.values()) {
                if (drainInbox(ctx, out, null, false)) progress = true;
                if (ctx.rn.hasReady()) {
                    handleReady(ctx, out);
                    progress = true;
                }
            }
            if (!progress) break;
        }
        return out.toString();
    }

    private String cmdStatus(Datadriven.Directive d) {
        long id = Long.parseLong(d.getKv("id", null));
        NodeContext ctx = requireNode(id);
        RaftStateType state = ctx.rn.raft.state;
        return String.format("id=%d state=%s term=%d lead=%d commit=%d lastIndex=%d",
                id, state, ctx.rn.raft.term, ctx.rn.raft.lead,
                ctx.rn.raft.raftLog.committed,
                ctx.rn.raft.raftLog.lastIndex());
    }

    private String cmdProposeConfChange(Datadriven.Directive d) throws RaftException {
        long id = Long.parseLong(d.getKv("id", null));
        String spec = d.getKv("changes", "");
        Eraftpb.ConfChangeV2 cc = parseConfChangeSpec(spec);
        NodeContext ctx = requireNode(id);
        ctx.rn.proposeConfChange(cc);
        return "ok";
    }

    private String cmdApplyConfChange(Datadriven.Directive d) {
        long id = Long.parseLong(d.getKv("id", null));
        String spec = d.getKv("changes", "");
        Eraftpb.ConfChangeV2 cc = parseConfChangeSpec(spec);
        NodeContext ctx = requireNode(id);
        Eraftpb.ConfState cs = ctx.rn.applyConfChange(cc);
        return String.format("voters=(%s) learners=(%s) auto-leave=%s",
                joinIds(toLongList(cs.getVotersList())),
                joinIds(toLongList(cs.getLearnersList())),
                cs.getAutoLeave());
    }

    private String cmdTransferLeader(Datadriven.Directive d) {
        long id = Long.parseLong(d.getKv("id", null));
        long to = Long.parseLong(d.getKv("to", null));
        NodeContext ctx = requireNode(id);
        ctx.rn.transferLeader(to);
        return "ok";
    }

    private String cmdForgetLeader(Datadriven.Directive d) throws RaftException {
        long id = Long.parseLong(d.getKv("id", null));
        NodeContext ctx = requireNode(id);
        ctx.rn.forgetLeader();
        return "ok";
    }

    /**
     * Parse a comma-separated change spec like {@code v2,l3,r4} into a
     * {@code ConfChangeV2}. Token prefixes: v=AddNode, l=AddLearnerNode,
     * r=RemoveNode. Empty spec produces an empty (LeaveJoint) change.
     */
    private static Eraftpb.ConfChangeV2 parseConfChangeSpec(String spec) {
        Eraftpb.ConfChangeV2.Builder b = Eraftpb.ConfChangeV2.newBuilder();
        if (spec == null || spec.isEmpty()) return b.build();
        for (String tok : spec.split(",")) {
            tok = tok.trim();
            if (tok.isEmpty()) continue;
            char prefix = tok.charAt(0);
            long nodeId = Long.parseLong(tok.substring(1));
            Eraftpb.ConfChangeType t;
            switch (prefix) {
                case 'v': t = Eraftpb.ConfChangeType.ConfChangeAddNode; break;
                case 'l': t = Eraftpb.ConfChangeType.ConfChangeAddLearnerNode; break;
                case 'r': t = Eraftpb.ConfChangeType.ConfChangeRemoveNode; break;
                default: throw new IllegalArgumentException("bad change token: " + tok);
            }
            b.addChanges(Eraftpb.ConfChangeSingle.newBuilder()
                    .setType(t).setNodeId(nodeId));
        }
        return b.build();
    }

    private static List<Long> toLongList(List<Long> in) {
        // Already List<Long>; this helper exists for symmetry / safety.
        return in;
    }

    private String cmdReportUnreachable(Datadriven.Directive d) {
        long id = Long.parseLong(d.getKv("id", null));
        long target = Long.parseLong(d.getKv("target", null));
        NodeContext ctx = requireNode(id);
        ctx.rn.reportUnreachable(target);
        return "ok";
    }

    private String cmdProgress(Datadriven.Directive d) {
        long id = Long.parseLong(d.getKv("id", null));
        NodeContext ctx = requireNode(id);
        StringBuilder out = new StringBuilder();
        // Sort by peer id for deterministic output.
        List<Long> peerIds = new ArrayList<>(ctx.rn.raft.trk.getProgress().keySet());
        java.util.Collections.sort(peerIds);
        for (long peer : peerIds) {
            var pr = ctx.rn.raft.trk.getProgress().get(peer);
            StringBuilder line = new StringBuilder(String.format(
                    "%d: %s match=%d next=%d", peer, pr.getState(),
                    pr.getMatch(), pr.getNext()));
            if (pr.isPaused()) line.append(" paused");
            if (pr.getInflights() != null && pr.getInflights().count() > 0) {
                line.append(String.format(" inflight=%d", pr.getInflights().count()));
                if (pr.getInflights().full()) line.append("[full]");
            }
            out.append(line).append('\n');
        }
        return out.toString();
    }

    private String cmdCreateSnapshot(Datadriven.Directive d) {
        long id = Long.parseLong(d.getKv("id", null));
        long index = Long.parseLong(d.getKv("index", null));
        NodeContext ctx = requireNode(id);
        try {
            Eraftpb.Snapshot snap = ctx.storage.createSnapshot(index,
                    ctx.rn.raft.trk.confState(), new byte[0]);
            return String.format("snapshot index=%d term=%d", index, snap.getMetadata().getTerm());
        } catch (RaftException e) {
            return "error: " + e;
        }
    }

    private String cmdCompact(Datadriven.Directive d) {
        long id = Long.parseLong(d.getKv("id", null));
        long index = Long.parseLong(d.getKv("index", null));
        NodeContext ctx = requireNode(id);
        try {
            ctx.storage.compact(index);
            return String.format("compacted to index=%d", index);
        } catch (RaftException e) {
            return "error: " + e;
        }
    }

    private String cmdLogState(Datadriven.Directive d) {
        long id = Long.parseLong(d.getKv("id", null));
        NodeContext ctx = requireNode(id);
        StringBuilder out = new StringBuilder();
        long lastIdx = ctx.rn.raft.raftLog.lastIndex();
        long firstIdx = ctx.rn.raft.raftLog.firstIndex();
        out.append(String.format("id=%d firstIndex=%d lastIndex=%d committed=%d applied=%d%n",
                id, firstIdx, lastIdx,
                ctx.rn.raft.raftLog.committed,
                ctx.rn.raft.raftLog.applied));
        try {
            for (long i = firstIdx; i <= lastIdx; i++) {
                long term = ctx.rn.raft.raftLog.term(i);
                out.append(String.format("  %d/%d%n", term, i));
            }
        } catch (RaftException e) {
            out.append(String.format("  <error: %s>%n", e));
        }
        return out.toString();
    }

    /**
     * Drains messages from {@code ctx}'s inbox, either stepping them into
     * raft (drop=false) or discarding (drop=true). When {@code fromFilter} is
     * non-null, only messages from that sender are touched; others stay in
     * the inbox.
     */
    private boolean drainInbox(NodeContext ctx, StringBuilder out, Long fromFilter, boolean drop) {
        boolean processed = false;
        java.util.Iterator<Eraftpb.Message> it = ctx.inbox.iterator();
        while (it.hasNext()) {
            Eraftpb.Message m = it.next();
            if (fromFilter != null && m.getFrom() != fromFilter) continue;
            if (drop) {
                out.append(String.format("> %d dropping %s from %d%n",
                        ctx.id, m.getMsgType(), m.getFrom()));
            } else {
                out.append(String.format("> %d receiving %s from %d%n",
                        ctx.id, m.getMsgType(), m.getFrom()));
                try {
                    ctx.rn.step(m);
                } catch (RaftException re) {
                    out.append(String.format("> %d rejected: %s%n", ctx.id, re.getMessage()));
                }
            }
            it.remove();
            processed = true;
        }
        return processed;
    }

    /** Process exactly one Ready on {@code ctx}, route its messages. */
    private void handleReady(NodeContext ctx, StringBuilder out) throws RaftException {
        Ready rd = ctx.rn.ready();
        out.append(String.format("> %d handling Ready: state=%s term=%d commit=%d%n",
                ctx.id, ctx.rn.raft.state, ctx.rn.raft.term,
                ctx.rn.raft.raftLog.committed));
        // Apply snapshot to storage FIRST (mirrors how a real app handles
        // Ready: snapshot install → entries append → message send → advance).
        if (rd.snapshot() != null && !Util.isEmptySnap(rd.snapshot())) {
            try {
                ctx.storage.applySnapshot(rd.snapshot());
            } catch (RaftException e) {
                out.append(String.format("> %d applySnapshot error: %s%n", ctx.id, e));
            }
        }
        if (!rd.entries().isEmpty()) ctx.storage.append(rd.entries());
        for (Eraftpb.Message m : rd.messages()) {
            NodeContext target = nodes.get(m.getTo());
            if (target != null && target != ctx) {
                target.inbox.add(m);
            }
        }
        ctx.rn.advance(rd);
    }

    // ---- Helpers ----

    private NodeContext requireNode(long id) {
        NodeContext ctx = nodes.get(id);
        if (ctx == null) throw new IllegalArgumentException("unknown node id=" + id);
        return ctx;
    }

    private static List<Long> parseIds(String spec) {
        if (spec == null || spec.isEmpty()) return Collections.emptyList();
        // Strip surrounding parens and braces if present.
        spec = spec.replaceAll("[()\\[\\]{}]", "");
        String[] parts = spec.split(",");
        List<Long> out = new ArrayList<>(parts.length);
        for (String p : parts) {
            String t = p.trim();
            if (!t.isEmpty()) out.add(Long.parseLong(t));
        }
        return out;
    }

    private static String joinIds(List<Long> ids) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < ids.size(); i++) {
            if (i > 0) sb.append(' ');
            sb.append(ids.get(i));
        }
        return sb.toString();
    }
}
