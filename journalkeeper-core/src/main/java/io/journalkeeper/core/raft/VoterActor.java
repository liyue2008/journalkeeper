package io.journalkeeper.core.raft;

import io.journalkeeper.core.api.JournalEntry;
import io.journalkeeper.core.api.RaftJournal;
import io.journalkeeper.core.api.RaftServer;
import io.journalkeeper.core.api.VoterState;
import io.journalkeeper.core.state.ConfigState;
import io.journalkeeper.exceptions.NotLeaderException;
import io.journalkeeper.rpc.client.QueryStateRequest;
import io.journalkeeper.rpc.client.QueryStateResponse;
import io.journalkeeper.rpc.server.*;
import io.journalkeeper.utils.actor.Actor;
import io.journalkeeper.utils.actor.ActorMsg;
import io.journalkeeper.utils.actor.annotation.*;
import io.journalkeeper.utils.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class VoterActor implements RaftVoter{
    public final static float RAND_INTERVAL_RANGE = 0.5F;
    private static final Logger logger = LoggerFactory.getLogger( VoterActor.class);
    private final Actor actor = Actor.builder("Voter").setHandlerInstance(this).build();
    private final VoterStateMachine voterState = new VoterStateMachine();
    private final RaftJournal journal;
    private final RaftState state;
    private long lastHeartbeat;
    private long electionTimeoutMs;
    private long nextElectionTime;
    private final Config config;
    private URI votedFor = null;
    private int term = 0;
    private URI leaderUri = null;
    VoterActor(RaftJournal journal, RaftState state, Config config) {
        this.journal = journal;
        this.state = state;
        this.config = config;
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
    /**
     * 接收者收到requestVote方法后的实现流程如下：
     * <p>
     * 如果请求中的任期号 < 节点当前任期号，返回false；
     * 如果votedFor为空或者与candidateId相同，并且候选人的日志至少和自己的日志一样新，则给该候选人投票；
     */
    @ActorListener
    private RequestVoteResponse requestVote(RequestVoteRequest request) {
        logger.debug("RequestVoteRpc received: term: {}, candidate: {}, " +
                        "lastLogIndex: {}, lastLogTerm: {}, fromPreferredLeader: {}, isPreVote: {}, {}.",
                request.getTerm(), request.getCandidate(),
                request.getLastLogIndex(), request.getLastLogTerm(), request.isFromPreferredLeader(), request.isPreVote(),
                voterInfo());
        String rejectMsg;
        int currentTerm = this.term;
        // 来自推荐Leader的投票请求例外
        if (!request.isFromPreferredLeader()) {
            // 如果当前是LEADER那直接拒绝投票

            if (voterState.getState() == VoterState.LEADER) {
                rejectMsg = "I'm the leader";
                return rejectAndResponse(currentTerm, request.getCandidate(), rejectMsg);
            }
            // 如何上次收到心跳的至今小于最小选举超时，拒绝投票
            if (System.currentTimeMillis() - lastHeartbeat < config.<Long>get("election_timeout_ms")) {
                rejectMsg = "An election timeout not passed since last heartbeat received";
                return rejectAndResponse(currentTerm, request.getCandidate(), rejectMsg);
            }
        }
        // 如果候选人的term 比我的term小，拒绝投票
        if (request.getTerm() < currentTerm) {
            rejectMsg = String.format("The candidate's term %d less than my term %d.",
                    request.getTerm(), currentTerm);
            return rejectAndResponse(currentTerm, request.getCandidate(), rejectMsg);
        }

        // 如果不是预投票，检查term
        if (!request.isPreVote()) {
            checkTerm(request.getTerm());
            currentTerm = this.term;
        }
        // 如果已经投票给其它候选人，拒绝投票
        if (votedFor != null && currentTerm == request.getTerm() && !votedFor.equals(request.getCandidate())) {
            rejectMsg = "Already vote to " + votedFor.toString();
            return rejectAndResponse(currentTerm, request.getCandidate(), rejectMsg);
        }
        // 如果term相同，候选人的日志比我的短，拒绝投票
        final long finalMaxJournalIndex = journal.maxIndex();
        final int lastLogTerm = journal.getTerm(finalMaxJournalIndex - 1);
        if ((request.getLastLogTerm() <= lastLogTerm
                && (request.getLastLogTerm() != lastLogTerm
                || request.getLastLogIndex() < finalMaxJournalIndex - 1))) {
            rejectMsg = "Candidate’s log is at least as up-to-date as my log";
            return rejectAndResponse(currentTerm, request.getCandidate(), rejectMsg);
        }

        if(request.isPreVote()) {
            logger.debug("Grant pre vote to candidate {}, {}.", request.getCandidate(), voterInfo());
        } else {
            logger.debug("Grant vote to candidate {}, {}.", request.getCandidate(), voterInfo());
            this.votedFor = request.getCandidate();
        }
        return new RequestVoteResponse(currentTerm, true);
    }

    private boolean checkTerm(int term) {
        boolean isTermChanged;
            if (term > this.term) {
                logger.info("Set current term from {} to {}, {}.", this.term, term, voterInfo());
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

    private RequestVoteResponse rejectAndResponse(int term, URI candidate, String rejectMessage) {
        logger.debug("Reject vote request from candidate {}, cause: [{}], {}.", candidate, rejectMessage, voterInfo());
        return new RequestVoteResponse(term, false);
    }

    @Override
    public VoterState getVoterState() {
        return voterState.getState();
    }

    public RaftVoter getRaftVoter() {
        return this;
    }


    @ActorListener
    private void convertToFollower() {
            VoterState oldState = voterState.getState();

            if (oldState == VoterState.LEADER) {
                leaderUri = null;
                actor.sendThen("Leader", "setActive", false, term)
                        .thenRun(voterState::convertToFollower)
                        .thenCompose(ignored -> actor.sendThen("Follower", "setActive", true))
                        .thenRun(() -> {
                            this.electionTimeoutMs = config.<Long>get("election_timeout_ms") + randomInterval(config.get("election_timeout_ms"));
                            logger.info("Convert voter state from {} to FOLLOWER, electionTimeout: {}.", oldState, electionTimeoutMs);
                        });
            } else {
                voterState.convertToFollower();
                actor.sendThen("Follower", "setActive", true, term).thenRun(() -> {
                    this.electionTimeoutMs = config.<Long>get("election_timeout_ms") + randomInterval(config.get("election_timeout_ms"));
                    logger.info("Convert voter state from {} to FOLLOWER, electionTimeout: {}.", oldState, electionTimeoutMs);
                });
            }
    }

    private void convertToCandidate() {
        VoterState oldState = voterState.getState();
        voterState.convertToCandidate();
        logger.info("Convert voter state from {} to CANDIDATE, electionTimeout: {}, {}.", oldState, electionTimeoutMs, voterInfo());
        if(isSingleNodeCluster()) {
            convertToLeader();
        }
    }


    private void convertToPreVoting() {
            VoterState oldState = voterState.getState();
            this.leaderUri = null;
            if (oldState == VoterState.FOLLOWER) {
                actor.sendThen("Follower", "setActive", false, term)
                        .thenRun(() ->  {
                            voterState.convertToPreVoting();
                            logger.info("Convert voter state from {} to PRE_VOTING, electionTimeout: {}, {}.", oldState, electionTimeoutMs, voterInfo());
                            if(isSingleNodeCluster()){
                                convertToCandidate();
                            }
                        });
            } else {
                voterState.convertToPreVoting();
                logger.info("Convert voter state from {} to PRE_VOTING, electionTimeout: {}, {}.", oldState, electionTimeoutMs, voterInfo());
            }

    }


    /**
     * 将状态转换为Leader
     */
    private void convertToLeader() {
        VoterState oldState = voterState.getState();
        voterState.convertToLeader();
        leaderUri = state.getLocalUri();
        actor.send("Leader", "setActive", true, term);
        logger.info("Convert voter state from {} to LEADER, {}.", oldState, voterInfo());
    }

    private long randomInterval(long interval) {
        return interval + Math.round(ThreadLocalRandom.current().nextDouble(-1 * RAND_INTERVAL_RANGE, RAND_INTERVAL_RANGE) * interval);
    }


    @ActorListener
    @ResponseManually
    public void asyncAppendEntries(@ActorMessage ActorMsg msg) {
        AsyncAppendEntriesRequest request = msg.getPayload();

        if (state.getRole() != RaftServer.Roll.VOTER || request.getTerm() < term) {
            actor.reply(msg, new AsyncAppendEntriesResponse(false, request.getPrevLogIndex() + 1,
                    term, request.getEntries().size()));
            return;
        }
        if (checkTerm(request.getTerm()) && !Objects.equals(leaderUri, request.getLeader())) {
            this.leaderUri = request.getLeader();
        }

        actor.send("Follower", "asyncAppendEntries", msg);
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
                throw new IllegalStateException(String.format("Change voter state from %s to %s is not allowed!", state, VoterState.CANDIDATE));
            }
        }

        private void convertToPreVoting() {
            if (state == VoterState.PRE_VOTING || state == VoterState.FOLLOWER) {
                state = VoterState.PRE_VOTING;
            } else {
                throw new IllegalStateException(String.format("Change voter state from %s to %s is not allowed!", state, VoterState.PRE_VOTING));
            }
        }

        public VoterState getState() {
            return state;
        }

    }

    public Actor getActor() {
        return actor;
    }

    private void checkElectionTimeout() {
        try {
            if (voterState.getState() == VoterState.FOLLOWER && System.currentTimeMillis() - lastHeartbeat > electionTimeoutMs) {
                convertToPreVoting();
                // 如果不开启PreVote，直接转换成候选人
                if(!config.<Boolean>get("enable_pre_vote")) {
                    convertToCandidate();
                }
                nextElectionTime = System.currentTimeMillis() + electionTimeoutMs;
            }

            if ((voterState.getState() == VoterState.PRE_VOTING || voterState.getState() == VoterState.CANDIDATE) && System.currentTimeMillis() > nextElectionTime) {

                startElection(false);
            }

        } catch (Throwable t) {
            logger.warn("CheckElectionTimeout Exception, {}: ", voterInfo(), t);
        }
    }

    private boolean isSingleNodeCluster() {
        return !state.getConfigState().isJointConsensus() &&
                state.getConfigState().voters().size() == 1 &&
                state.getConfigState().voters().contains(state.getLocalUri());
    }

    private void updateVotes(AtomicBoolean isWinTheElection, AtomicInteger votesGrantedInNewConfig, AtomicInteger votesGrantedInOldConfig, URI destination) {
        ConfigState configState = state.getConfigState();
        if (configState.getConfigNew().contains(destination)) {
            votesGrantedInNewConfig.incrementAndGet();
        }
        if (configState.getConfigOld().contains(destination)) {
            votesGrantedInOldConfig.incrementAndGet();
        }

        boolean win;

        if (configState.isJointConsensus()) {
            win = votesGrantedInNewConfig.get() >= configState.getConfigNew().size() / 2 + 1 &&
                    votesGrantedInOldConfig.get() >= configState.getConfigOld().size() / 2 + 1;
        } else {
            win = votesGrantedInNewConfig.get() >= configState.getConfigNew().size() / 2 + 1;
        }
        if (win && isWinTheElection.compareAndSet(false, true)) {
            if (voterState.getState() == VoterState.PRE_VOTING) {
                convertToCandidate();
                startElection(false);
            } else if (voterState.getState() == VoterState.CANDIDATE) {
                convertToLeader();
            }
        }
    }

    /**
     * 发起选举。
     * 0. 角色转变为候选人
     * 1. 自增当前任期号：term = term + 1；
     * 2. 给自己投票；
     * 3. 重置选举计时器：lastHeartBeat = now，生成一个随机的新的选举超时时间（RAFT的推荐值为150~300ms）。
     * 4. 向其他Voter发送RequestVote请求
     * 4.1. 如果收到了来自大多数服务器的投票：成为LEADER
     * 4.2. 如果收到了来自新领导人的asyncAppendEntries请求（heartbeat）：转换状态为FOLLOWER
     * 4.3. 如果选举超时：开始新一轮的选举
     */
    private void startElection(boolean fromPreferredLeader) {

        nextElectionTime = Long.MAX_VALUE;
        votedFor = state.getLocalUri();
        boolean isPreVote = voterState.getState() == VoterState.PRE_VOTING;
        if (!isPreVote) {
            this.term ++;
            logger.info("Start election, {}", voterInfo());
        } else {
            logger.info("Start pre vote, {}", voterInfo());
        }

        long lastLogIndex = journal.maxIndex() - 1;
        int lastLogTerm = journal.getTerm(lastLogIndex);

        RequestVoteRequest request = new RequestVoteRequest(term, state.getLocalUri(), lastLogIndex, lastLogTerm, fromPreferredLeader, isPreVote);
        List<URI> destinations = state.getConfigState().voters().stream()
                .filter(uri -> !uri.equals(this.state.getLocalUri())).collect(Collectors.toList());

        final AtomicBoolean isWinTheElection = new AtomicBoolean(false);
        final AtomicInteger votesGrantedInNewConfig = new AtomicInteger(0);
        final AtomicInteger votesGrantedInOldConfig = new AtomicInteger(0);

        updateVotes(isWinTheElection, votesGrantedInNewConfig, votesGrantedInOldConfig, this.state.getLocalUri());

        if (!isWinTheElection.get()) {
            final AtomicInteger pendingRequests = new AtomicInteger(destinations.size());
            for (URI destination : destinations) {
                actor.<RequestVoteResponse>sendThen("Rpc","requestVote", new RpcMsg<>(destination, request))
                        .thenAccept(response -> {
                            if (null != response) {
                                logger.info("Request vote result {}, dest uri: {}, {}...",
                                        response.isVoteGranted(),
                                        destination,
                                        voterInfo());
                                if (response.isVoteGranted()) {
                                    updateVotes(isWinTheElection, votesGrantedInNewConfig, votesGrantedInOldConfig, destination);
                                }
                            }
                        })
                        .exceptionally(e -> {
                            logger.warn("Request vote exception: {}!", e.getMessage());
                            return null;
                        }).thenRun(() -> {
                            if (pendingRequests.decrementAndGet() == 0 && !isWinTheElection.get()) {
                                electionTimeoutMs = config.<Long>get("election_timeout_ms") + randomInterval(config.<Long>get("election_timeout_ms"));
                                nextElectionTime = System.currentTimeMillis() + electionTimeoutMs;
                            }
                        });
            }
        }
    }
    private String voterInfo() {
        return String.format("voterState: %s, currentTerm: %d, minIndex: %d, " +
                        "maxIndex: %d, commitIndex: %d, lastApplied: %d, uri: %s",
                voterState.getState(), this.term, journal.minIndex(),
                journal.maxIndex(), journal.commitIndex(), state.lastApplied(), state.getLocalUri().toString());
    }
    @ActorSubscriber
    private void onStart(ServerContext context) {

        if (isSingleNodeCluster()) {
            convertToPreVoting();
        } else {
            convertToFollower();
        }
        actor.addScheduler(config.<Long>get("heartbeat_interval_ms"), TimeUnit.MILLISECONDS, "checkElectionTimeout", this::checkElectionTimeout);
    }

}
