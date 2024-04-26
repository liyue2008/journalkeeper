package io.journalkeeper.core.raft;

import io.journalkeeper.core.api.JournalEntry;
import io.journalkeeper.core.api.RaftJournal;
import io.journalkeeper.core.api.RaftServer;
import io.journalkeeper.core.api.VoterState;
import io.journalkeeper.rpc.server.AsyncAppendEntriesRequest;
import io.journalkeeper.rpc.server.AsyncAppendEntriesResponse;
import io.journalkeeper.rpc.server.RequestVoteRequest;
import io.journalkeeper.rpc.server.RequestVoteResponse;
import io.journalkeeper.utils.actor.Actor;
import io.journalkeeper.utils.actor.ActorMsg;
import io.journalkeeper.utils.actor.annotation.ActorListener;
import io.journalkeeper.utils.actor.annotation.ActorMessage;
import io.journalkeeper.utils.actor.annotation.ResponseManually;
import io.journalkeeper.utils.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;

public class VoterActor implements RaftVoter{
    public final static float RAND_INTERVAL_RANGE = 0.5F;
    private static final Logger logger = LoggerFactory.getLogger( VoterActor.class);
    private final Actor actor = Actor.builder("Voter").setHandlerInstance(this).build();
    private final VoterStateMachine voterState = new VoterStateMachine();
    private final RaftJournal journal;
    private final RaftState state;
    private int term = 0;
    private long lastHeartbeat;
    private long electionTimeoutMs;
    private final Config config;
    private URI votedFor = null;
    VoterActor(RaftJournal journal, RaftState state, Config config) {
        this.journal = journal;
        this.state = state;

        this.config = config;
    }

    @ActorListener
    private RequestVoteResponse requestVote(RequestVoteRequest request) {
        // TODO
        return null;
    }

    @Override
    public VoterState getVoterState() {
        return voterState.getState();
    }

    public RaftVoter getRaftVoter() {
        return this;
    }


    private void convertToFollower() {
            VoterState oldState = voterState.getState();
            voterState.convertToFollower();
            this.electionTimeoutMs = config.<Long>get("election_timeout_ms") + randomInterval(config.get("election_timeout_ms"));
            logger.info("Convert voter state from {} to FOLLOWER, electionTimeout: {}.", oldState, electionTimeoutMs);
    }

    private long randomInterval(long interval) {
        return interval + Math.round(ThreadLocalRandom.current().nextDouble(-1 * RAND_INTERVAL_RANGE, RAND_INTERVAL_RANGE) * interval);
    }

    private boolean checkTerm(int term) {
        boolean isTermChanged;
            if (term > this.term) {
                logger.info("Set current term from {} to {}.", this.term, term);
                this.term = term;
                this.votedFor = null;
                isTermChanged = true;
            } else {
                isTermChanged = false;
            }

        if (isTermChanged) {
            convertToFollower();
        }
        return isTermChanged;
    }

    @ActorListener
    @ResponseManually
    public void asyncAppendEntries(@ActorMessage ActorMsg msg) {
        AsyncAppendEntriesRequest request = msg.getPayload();

        if (state.getRole() != RaftServer.Roll.VOTER) {
            actor.reply(msg, new AsyncAppendEntriesResponse(false, request.getPrevLogIndex() + 1,
                    term, request.getEntries().size()));
            return;
        }
        boolean isTermChanged = checkTerm(request.getTerm());

        if (request.getTerm() < term) {
            // 如果收到的请求term小于当前term，拒绝请求
            actor.reply(msg, new AsyncAppendEntriesResponse(false, request.getPrevLogIndex() + 1,
                    term, request.getEntries().size()));
            return;
        }

        if (voterState.getState() != VoterState.FOLLOWER) {
            convertToFollower();
        }
        if (logger.isDebugEnabled() && request.getEntries() != null && !request.getEntries().isEmpty()) {
            logger.debug("Received appendEntriesRequest, term: {}, leader: {}, prevLogIndex: {}, prevLogTerm: {}, " +
                            "entries: {}, leaderCommit: {}.",
                    request.getTerm(), request.getLeader(), request.getPrevLogIndex(), request.getPrevLogTerm(),
                    request.getEntries().size(), request.getLeaderCommit());
        }

        // reset heartbeat
        lastHeartbeat = System.currentTimeMillis();
        if (logger.isDebugEnabled()) {
            logger.debug("Update lastHeartbeat.");
        }


        if (isTermChanged) {
            actor.send("State", "setLeader", request.getLeader());
        }
        actor.send("Follower", "asyncAppendEntries", request);
    }
    @ActorListener
    private void maybeUpdateTermOnRecovery() {
        if (journal.minIndex() < journal.maxIndex()) {
            JournalEntry lastEntry = journal.read(journal.maxIndex() - 1);
            if (lastEntry.getTerm() > term) {
                term = lastEntry.getTerm();
                logger.info("Set current term to {}, this is the term of the last entry in the journal.",
                        term);
            }
        }
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
