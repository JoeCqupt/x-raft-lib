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

import io.github.xinfra.lab.raft.proto.Eraftpb;

import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

/**
 * Utility functions for raft.
 */
public final class Util {
    public static final long NONE = 0;
    public static final long LOCAL_APPEND_THREAD = Long.MAX_VALUE;
    public static final long LOCAL_APPLY_THREAD = Long.MAX_VALUE - 1;

    private static final Set<Eraftpb.MessageType> LOCAL_MSGS = EnumSet.of(
            Eraftpb.MessageType.MsgHup,
            Eraftpb.MessageType.MsgBeat,
            Eraftpb.MessageType.MsgUnreachable,
            Eraftpb.MessageType.MsgSnapStatus,
            Eraftpb.MessageType.MsgCheckQuorum,
            Eraftpb.MessageType.MsgStorageAppend,
            Eraftpb.MessageType.MsgStorageAppendResp,
            Eraftpb.MessageType.MsgStorageApply,
            Eraftpb.MessageType.MsgStorageApplyResp
    );

    private static final Set<Eraftpb.MessageType> RESPONSE_MSGS = EnumSet.of(
            Eraftpb.MessageType.MsgAppendResponse,
            Eraftpb.MessageType.MsgRequestVoteResponse,
            Eraftpb.MessageType.MsgHeartbeatResponse,
            Eraftpb.MessageType.MsgUnreachable,
            Eraftpb.MessageType.MsgReadIndexResp,
            Eraftpb.MessageType.MsgRequestPreVoteResponse,
            Eraftpb.MessageType.MsgStorageAppendResp,
            Eraftpb.MessageType.MsgStorageApplyResp
    );

    private Util() {}

    public static boolean isLocalMsg(Eraftpb.MessageType msgt) {
        return LOCAL_MSGS.contains(msgt);
    }

    public static boolean isResponseMsg(Eraftpb.MessageType msgt) {
        return RESPONSE_MSGS.contains(msgt);
    }

    public static boolean isLocalMsgTarget(long id) {
        return id == LOCAL_APPEND_THREAD || id == LOCAL_APPLY_THREAD;
    }

    public static Eraftpb.MessageType voteRespMsgType(Eraftpb.MessageType msgt) {
        return switch (msgt) {
            case MsgRequestVote -> Eraftpb.MessageType.MsgRequestVoteResponse;
            case MsgRequestPreVote -> Eraftpb.MessageType.MsgRequestPreVoteResponse;
            default -> throw new IllegalArgumentException("not a vote message: " + msgt);
        };
    }

    public static long entsSize(List<Eraftpb.Entry> ents) {
        long size = 0;
        for (Eraftpb.Entry ent : ents) {
            size += ent.getSerializedSize();
        }
        return size;
    }

    /**
     * limitSize returns the longest prefix of the given entries slice, such that
     * its total byte size does not exceed maxSize. Always returns at least one entry
     * if the input is non-empty.
     */
    public static List<Eraftpb.Entry> limitSize(List<Eraftpb.Entry> ents, long maxSize) {
        if (ents.isEmpty()) {
            return ents;
        }
        long size = ents.get(0).getSerializedSize();
        int limit = 1;
        while (limit < ents.size()) {
            size += ents.get(limit).getSerializedSize();
            if (size > maxSize) {
                break;
            }
            limit++;
        }
        return ents.subList(0, limit);
    }

    public static long payloadSize(Eraftpb.Entry e) {
        return e.getData().size();
    }

    public static long payloadsSize(List<Eraftpb.Entry> ents) {
        long s = 0;
        for (Eraftpb.Entry e : ents) {
            s += payloadSize(e);
        }
        return s;
    }

    public static boolean isEmptyHardState(Eraftpb.HardState st) {
        return st.getTerm() == 0 && st.getVote() == 0 && st.getCommit() == 0;
    }

    public static boolean isHardStateEqual(Eraftpb.HardState a, Eraftpb.HardState b) {
        return a.getTerm() == b.getTerm() && a.getVote() == b.getVote() && a.getCommit() == b.getCommit();
    }

    public static boolean isEmptySnap(Eraftpb.Snapshot sp) {
        return !sp.hasMetadata() || sp.getMetadata().getIndex() == 0;
    }

    public static boolean mustSync(Eraftpb.HardState st, Eraftpb.HardState prevst, int entsnum) {
        return entsnum != 0 || st.getVote() != prevst.getVote() || st.getTerm() != prevst.getTerm();
    }

    public static void assertConfStatesEquivalent(Eraftpb.ConfState cs1, Eraftpb.ConfState cs2) {
        if (!confStatesEquivalent(cs1, cs2)) {
            throw new RaftInvariantException("conf states not equivalent: " + cs1 + " vs " + cs2);
        }
    }

    private static boolean confStatesEquivalent(Eraftpb.ConfState a, Eraftpb.ConfState b) {
        return a.getVotersList().equals(b.getVotersList()) &&
               a.getLearnersList().equals(b.getLearnersList()) &&
               a.getVotersOutgoingList().equals(b.getVotersOutgoingList()) &&
               a.getLearnersNextList().equals(b.getLearnersNextList()) &&
               a.getAutoLeave() == b.getAutoLeave();
    }

    // ============= Describe methods =============

    /**
     * EntryFormatter can be used to provide human-readable formatting of entry data.
     */
    @FunctionalInterface
    public interface EntryFormatter extends Function<byte[], String> {}

    public static String describeHardState(Eraftpb.HardState hs) {
        StringBuilder buf = new StringBuilder();
        buf.append(String.format("Term:%d", hs.getTerm()));
        if (hs.getVote() != 0) {
            buf.append(String.format(" Vote:%d", hs.getVote()));
        }
        buf.append(String.format(" Commit:%d", hs.getCommit()));
        return buf.toString();
    }

    public static String describeSoftState(SoftState ss) {
        return String.format("Lead:%d State:%s", ss.lead, ss.raftState);
    }

    public static String describeConfState(Eraftpb.ConfState state) {
        return String.format("Voters:%s VotersOutgoing:%s Learners:%s LearnersNext:%s AutoLeave:%s",
                state.getVotersList(), state.getVotersOutgoingList(),
                state.getLearnersList(), state.getLearnersNextList(), state.getAutoLeave());
    }

    public static String describeSnapshot(Eraftpb.Snapshot snap) {
        Eraftpb.SnapshotMetadata m = snap.getMetadata();
        return String.format("Index:%d Term:%d ConfState:%s", m.getIndex(), m.getTerm(), describeConfState(m.getConfState()));
    }

    public static String describeReady(Ready rd, EntryFormatter f) {
        StringBuilder buf = new StringBuilder();
        if (rd.softState != null) {
            buf.append(describeSoftState(rd.softState));
            buf.append('\n');
        }
        if (!isEmptyHardState(rd.hardState)) {
            buf.append(String.format("HardState %s", describeHardState(rd.hardState)));
            buf.append('\n');
        }
        if (rd.readStates != null && !rd.readStates.isEmpty()) {
            buf.append(String.format("ReadStates %s\n", rd.readStates));
        }
        if (rd.entries != null && !rd.entries.isEmpty()) {
            buf.append("Entries:\n");
            buf.append(describeEntries(rd.entries, f));
        }
        if (rd.snapshot != null && !isEmptySnap(rd.snapshot)) {
            buf.append(String.format("Snapshot %s\n", describeSnapshot(rd.snapshot)));
        }
        if (rd.committedEntries != null && !rd.committedEntries.isEmpty()) {
            buf.append("CommittedEntries:\n");
            buf.append(describeEntries(rd.committedEntries, f));
        }
        if (rd.messages != null && !rd.messages.isEmpty()) {
            buf.append("Messages:\n");
            for (Eraftpb.Message msg : rd.messages) {
                buf.append(describeMessage(msg, f));
                buf.append('\n');
            }
        }
        if (buf.length() > 0) {
            return String.format("Ready MustSync=%b:\n%s", rd.mustSync, buf);
        }
        return "<empty Ready>";
    }

    public static String describeMessage(Eraftpb.Message m, EntryFormatter f) {
        return describeMessageWithIndent("", m, f);
    }

    private static String describeMessageWithIndent(String indent, Eraftpb.Message m, EntryFormatter f) {
        StringBuilder buf = new StringBuilder();
        buf.append(String.format("%s%s->%s %s Term:%d Log:%d/%d", indent,
                describeTarget(m.getFrom()), describeTarget(m.getTo()), m.getMsgType(),
                m.getTerm(), m.getLogTerm(), m.getIndex()));
        if (m.getReject()) {
            buf.append(String.format(" Rejected (Hint: %d)", m.getRejectHint()));
        }
        if (m.getCommit() != 0) {
            buf.append(String.format(" Commit:%d", m.getCommit()));
        }
        if (m.getVote() != 0) {
            buf.append(String.format(" Vote:%d", m.getVote()));
        }
        int ln = m.getEntriesCount();
        if (ln == 1) {
            buf.append(String.format(" Entries:[%s]", describeEntry(m.getEntries(0), f)));
        } else if (ln > 1) {
            buf.append(" Entries:[");
            for (Eraftpb.Entry e : m.getEntriesList()) {
                buf.append(String.format("\n%s  ", indent));
                buf.append(describeEntry(e, f));
            }
            buf.append(String.format("\n%s]", indent));
        }
        if (m.hasSnapshot() && !isEmptySnap(m.getSnapshot())) {
            buf.append(String.format("\n%s  Snapshot: %s", indent, describeSnapshot(m.getSnapshot())));
        }
        return buf.toString();
    }

    public static String describeTarget(long id) {
        if (id == NONE) {
            return "None";
        } else if (id == LOCAL_APPEND_THREAD) {
            return "AppendThread";
        } else if (id == LOCAL_APPLY_THREAD) {
            return "ApplyThread";
        } else {
            return String.format("%x", id);
        }
    }

    public static String describeEntry(Eraftpb.Entry e, EntryFormatter f) {
        if (f == null) {
            f = data -> String.format("\"%s\"", new String(data));
        }
        String formatted;
        switch (e.getEntryType()) {
            case EntryNormal:
                formatted = f.apply(e.getData().toByteArray());
                break;
            case EntryConfChange:
                try {
                    Eraftpb.ConfChange cc = Eraftpb.ConfChange.parseFrom(e.getData());
                    formatted = describeConfChange(cc);
                } catch (com.google.protobuf.InvalidProtocolBufferException ex) {
                    // Render the failure as a parse-error tag so a corrupted
                    // entry doesn't masquerade as legitimate content in logs.
                    formatted = "<parse-error: " + ex.getMessage() + ">";
                }
                break;
            case EntryConfChangeV2:
                try {
                    Eraftpb.ConfChangeV2 cc = Eraftpb.ConfChangeV2.parseFrom(e.getData());
                    formatted = describeConfChanges(cc.getChangesList());
                } catch (com.google.protobuf.InvalidProtocolBufferException ex) {
                    formatted = "<parse-error: " + ex.getMessage() + ">";
                }
                break;
            default:
                formatted = "unknown";
                break;
        }
        if (formatted != null && !formatted.isEmpty()) {
            formatted = " " + formatted;
        }
        return String.format("%d/%d %s%s", e.getTerm(), e.getIndex(), e.getEntryType(), formatted);
    }

    public static String describeEntries(List<Eraftpb.Entry> ents, EntryFormatter f) {
        StringBuilder buf = new StringBuilder();
        for (Eraftpb.Entry e : ents) {
            buf.append(describeEntry(e, f));
            buf.append('\n');
        }
        return buf.toString();
    }

    private static String describeConfChange(Eraftpb.ConfChange cc) {
        return String.format("%s %s%d", cc.getChangeType(), "", cc.getNodeId());
    }

    private static String describeConfChanges(List<Eraftpb.ConfChangeSingle> changes) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < changes.size(); i++) {
            if (i > 0) sb.append(" ");
            Eraftpb.ConfChangeSingle ccs = changes.get(i);
            sb.append(String.format("%s %d", ccs.getType(), ccs.getNodeId()));
        }
        return sb.toString();
    }
}
