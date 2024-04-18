package io.journalkeeper.core.raft;

import io.journalkeeper.core.api.JournalEntry;
import io.journalkeeper.core.api.RaftJournal;
import io.journalkeeper.core.api.VoterState;
import io.journalkeeper.core.entry.internal.InternalEntriesSerializeSupport;
import io.journalkeeper.core.entry.internal.InternalEntryType;
import io.journalkeeper.core.entry.internal.UpdateVotersS2Entry;
import io.journalkeeper.rpc.server.AsyncAppendEntriesRequest;
import io.journalkeeper.rpc.server.RequestVoteRequest;
import io.journalkeeper.rpc.server.RequestVoteResponse;
import io.journalkeeper.utils.actor.Actor;
import io.journalkeeper.utils.actor.ActorListener;
import io.journalkeeper.utils.actor.ActorMsg;

import static io.journalkeeper.core.api.RaftJournal.INTERNAL_PARTITION;
import static io.journalkeeper.core.entry.internal.InternalEntryType.TYPE_UPDATE_VOTERS_S1;
import static io.journalkeeper.core.entry.internal.InternalEntryType.TYPE_UPDATE_VOTERS_S2;

public class VoterActor implements RaftVoter{

    private final Actor actor = new Actor("Voter");
    private final VoterStateMachine voterStateMachine = new VoterStateMachine();
    private final RaftJournal journal;
    VoterActor(RaftJournal journal) {
        this.journal = journal;
        actor.setHandlerInstance(this);

    }

    @ActorListener(response = true, payload = true)
    private RequestVoteResponse requestVote(RequestVoteRequest request) {
        // TODO
        return null;
    }

    @Override
    public VoterState getVoterState() {
        return voterStateMachine.getState();
    }

    public RaftVoter getRaftVoter() {
        return this;
    }

    private static class VoterStateMachine {
        private VoterState state = VoterState.FOLLOWER;

        private void convertToLeader() {
            if (state == VoterState.CANDIDATE) {
                state = VoterState.LEADER;
            } else {
                throw new IllegalStateException(String.format("Change voter state from %s to %s is not allowed!", state, VoterState.LEADER));
            }
        }

        private void convertToFollower() {
            state = VoterState.FOLLOWER;
        }

        private void convertToCandidate() {
            if (state == VoterState.PRE_VOTING) {
                state = VoterState.CANDIDATE;
            } else {
                throw new IllegalStateException(String.format("Change voter state from %s to %s is not allowed!", state, VoterState.FOLLOWER));
            }
        }

        private void convertToPreVoting() {
            if (state == VoterState.PRE_VOTING || state == VoterState.FOLLOWER) {
                state = VoterState.PRE_VOTING;
            } else {
                throw new IllegalStateException(String.format("Change voter state from %s to %s is not allowed!", state, VoterState.FOLLOWER));
            }
        }

        public VoterState getState() {
            return state;
        }

    }

    public Actor getActor() {
        return actor;
    }


}
