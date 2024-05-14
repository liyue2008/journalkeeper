package io.journalkeeper.core.raft;

import io.journalkeeper.base.ReplicableIterator;
import io.journalkeeper.core.api.*;
import io.journalkeeper.core.entry.internal.*;
import io.journalkeeper.core.server.VoterConfigManager;
import io.journalkeeper.core.state.ConfigState;
import io.journalkeeper.core.state.Snapshot;
import io.journalkeeper.core.transaction.JournalTransactionManager;
import io.journalkeeper.exceptions.IndexUnderflowException;
import io.journalkeeper.exceptions.NotLeaderException;
import io.journalkeeper.rpc.client.*;
import io.journalkeeper.rpc.server.*;
import io.journalkeeper.utils.actor.Actor;
import io.journalkeeper.utils.actor.ActorMsg;
import io.journalkeeper.utils.actor.annotation.*;
import io.journalkeeper.utils.config.Config;
import io.journalkeeper.utils.event.Event;
import io.journalkeeper.utils.event.EventType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static io.journalkeeper.core.api.RaftJournal.INTERNAL_PARTITION;
import static io.journalkeeper.core.entry.internal.InternalEntryType.TYPE_UPDATE_VOTERS_S1;
import static io.journalkeeper.core.entry.internal.InternalEntryType.TYPE_UPDATE_VOTERS_S2;

public class VoterActor {
    public final static float RAND_INTERVAL_RANGE = 0.5F;
    private static final Logger logger = LoggerFactory.getLogger( VoterActor.class);
    private final Actor actor = Actor.builder("Voter").setHandlerInstance(this).build();
    private final VoterStateMachine voterState = new VoterStateMachine();
    private final RaftJournal journal;
    private final RaftState state;
    private final Config config;
    
    // Election Only
    private long lastHeartbeat;  // 上一次从Leader收到的心跳时间
    private long electionTimeoutMs; // 选举超时时长
    private long nextElectionTime; // 下一次发起选举的时间
    private URI votedFor = null; // 投票给谁
    private int term = 0;
    private URI leaderUri = null; // 当前Leader
    
    // Follower Only
    private long leaderMaxIndex = -1L;  // Leader 日志当前的最大位置
    
    // Leader Only
    private final JournalEntryParser journalEntryParser;
    private final JournalTransactionManager journalTransactionManager;
    private boolean isWritable = true; // Leader 是否可写
    private long disableWriteTimeout = 0L; // 禁用写操作的超时时间
    private boolean isAnnounced = false; // 发布Leader announcement 被多数确认，正式行使leader职权。

    private final List<WaitingResponse> waitingResponses = new LinkedList<>(); // 处理中的待响应的update请求
    private final List<ReplicationDestination> replicationDestinations = new ArrayList<>(); // Followers
    private final VoterConfigManager voterConfigManager;
    VoterActor(JournalEntryParser journalEntryParser, JournalTransactionManager journalTransactionManager, RaftJournal journal, RaftState state, Config config) {
        this.journalEntryParser = journalEntryParser;
        this.journalTransactionManager = journalTransactionManager;
        this.journal = journal;
        this.state = state;
        this.config = config;
        this.voterConfigManager = new VoterConfigManager(journalEntryParser);

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

        // 如果不是预投票，检查并更新term
        if (!request.isPreVote()) {
            if (checkTerm(request.getTerm())) {
                currentTerm = this.term;
            }
        }
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


    private void setLeaderUri(URI leaderUri) {
        if (!Objects.equals(this.leaderUri, leaderUri)) {
            this.leaderUri = leaderUri;
            actor.pub("onLeaderChange", leaderUri);        }

    }

    @ActorListener
    private URI getLeaderUri() {
        return leaderUri;
    }

    @ActorListener
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
        logger.info("Reject vote request from candidate {}, cause: [{}], {}.", candidate, rejectMessage, voterInfo());
        return new RequestVoteResponse(term, false);
    }

    @ActorListener
    private void convertToFollower() {
        VoterState oldState = voterState.getState();
        voterState.convertToFollower();
        this.electionTimeoutMs = config.<Long>get("election_timeout_ms") + randomInterval(config.get("election_timeout_ms"));
        logger.info("Convert voter state from {} to FOLLOWER, electionTimeout: {}.", oldState, electionTimeoutMs);
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
        voterState.convertToPreVoting();
        logger.info("Convert voter state from {} to PRE_VOTING, electionTimeout: {}, {}.", oldState, electionTimeoutMs, voterInfo());
        if(isSingleNodeCluster()){
            convertToCandidate();
        }
    }

    /**
     * 将状态转换为Leader
     */
    private void convertToLeader() {
        VoterState oldState = voterState.getState();
        voterState.convertToLeader();
        leaderUri = state.getLocalUri();
        this.replicationDestinations.forEach(ReplicationDestination::reset);
        announceLeader();

        logger.info("Convert voter state from {} to LEADER, {}.", oldState, voterInfo());

    }

    private long randomInterval(long interval) {
        return interval + Math.round(ThreadLocalRandom.current().nextDouble(-1 * RAND_INTERVAL_RANGE, RAND_INTERVAL_RANGE) * interval);
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

    // Scheduler function
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
        long lastLogIndex = journal.maxIndex() - 1;
        int lastLogTerm = journal.getTerm(lastLogIndex);
        int requestTerm = this.term + 1;
        if (!isPreVote) {
            this.term ++;
            logger.info("Start election, {}", voterInfo());
        } else {
            logger.info("Start pre vote, {}", voterInfo());
        }



        RequestVoteRequest request = new RequestVoteRequest(requestTerm, state.getLocalUri(), lastLogIndex, lastLogTerm, fromPreferredLeader, isPreVote);
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
                                if (!checkTerm(response.getTerm())  && response.isVoteGranted()) {
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
        
        // Leader
        if (config.<Boolean>get("enable_check_quorum")) {
            actor.addScheduler(config.get("check_quorum_timeout_ms"), TimeUnit.MILLISECONDS, "checkQuorum", this::checkQuorum);
        }
        actor.addScheduler(config.get("heartbeat_interval_ms"), TimeUnit.MILLISECONDS, "replication", this::replication);
        actor.addScheduler(config.get("heartbeat_interval_ms"), TimeUnit.MILLISECONDS, "commit", this::commit);
    }


    // Follower
    @ActorListener
    @ResponseManually
    public void asyncAppendEntries(@ActorMessage ActorMsg msg) {
        AsyncAppendEntriesRequest request = msg.getPayload();

//        If RPC request or response contains term T > currentTerm:
//        set currentTerm = T, convert to follower
        if (request.getTerm() > term) {
            logger.info("Set current term from {} to {}, {}.", this.term, term, voterInfo());
            this.term = request.getTerm();
            this.votedFor = null;
            convertToFollower();
            setLeaderUri(request.getLeader());
        }
        // Reply false if term < currentTerm
        if (request.getTerm() < term) {
            actor.reply(msg, new AsyncAppendEntriesResponse(false, request.getPrevLogIndex() + 1,
                    term, request.getEntries().size()));
            return;
        }

        if (voterState.getState() != VoterState.FOLLOWER && request.getLeader().equals(this.votedFor) && this.term == request.getTerm()) {
            this.votedFor = null;
            convertToFollower();
            setLeaderUri(request.getLeader());
        }

        lastHeartbeat = System.currentTimeMillis();

        boolean notHeartBeat = null != request.getEntries() && !request.getEntries().isEmpty();
        // Reply false if log does not contain an entry at prevLogIndex
        // whose term matches prevLogTerm
        final long startIndex = request.getPrevLogIndex() + 1;
        final List<byte[]> entries = request.getEntries();
        if (notHeartBeat &&
                (request.getPrevLogIndex() < journal.minIndex() - 1 ||
                        request.getPrevLogIndex() >= journal.maxIndex() ||
                        journal.getTerm(request.getPrevLogIndex()) != request.getPrevLogTerm())
        ) {
            actor.reply(msg, new AsyncAppendEntriesResponse(false, startIndex,
                    request.getTerm(), request.getEntries().size()));
            return;
        }



        // 如果要删除部分未提交的日志，并且待删除的这部分存在配置变更日志，则需要回滚配置
        actor.sendThen("State", "maybeRollbackConfig", startIndex)
                // 3. If an existing entry conflicts with a new one (same index
                // but different terms), delete the existing entry and all that
                // follow it (§5.3)
                //4. Append any new entries not already in the log
                .thenCompose(ignored -> actor.sendThen("Journal","compareOrAppendRaw", entries, request.getPrevLogIndex() + 1))
                .thenCompose(ignored -> actor.sendThen("State", "maybeUpdateNonLeaderConfig", entries))
                //5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
                .thenCompose(ignored -> actor.sendThen("Journal", "commit", request.getLeaderCommit()))
                .thenRun(() -> {
                    if (leaderMaxIndex < request.getMaxIndex()) {
                        leaderMaxIndex = request.getMaxIndex();
                    }
                    actor.reply(msg, new AsyncAppendEntriesResponse(true, request.getPrevLogIndex() + 1,
                            request.getTerm(), request.getEntries().size()));
                })
                .exceptionally(t -> {
                    actor.reply(msg, new AsyncAppendEntriesResponse(t));
                    return null;
                });

    }

    // Leader Only
    
    @ActorListener
    @ResponseManually
    private void updateClusterState(@ActorMessage ActorMsg msg) {
        // 1. Leader.updateClusterState
        //      1.1. Journal.append
        // 2.
        //      2.1 Journal.flush
        //      2.2 Leader.replication
        // 3. (RPC) Follower.asyncAppendEntries
        // 4. Leader.commit
        // 5. Journal.commit
        // 6. State.applyEntries
        // 7. Leader.callback
        UpdateClusterStateRequest request = msg.getPayload();
        if (!(voterState.getState() == VoterState.LEADER && isAnnounced)) {
            actor.reply(msg, new UpdateClusterStateResponse(new NotLeaderException(this.leaderUri)));
        }
        if (!checkWriteable()) {
            actor.reply(msg, new UpdateClusterStateResponse(new IllegalStateException("Server disabled temporarily.")));
        }
        if (request.getResponseConfig() == ResponseConfig.RECEIVE) {
            actor.reply(msg, new UpdateClusterStateResponse());
        }

        updateVoterConfig(request);

        List<JournalEntry> journalEntries = requestToJournalEntries(request);

        actor.<Long>sendThen("Journal", "append", journalEntries)
                .thenApply(position -> new WaitingResponse(msg, position - journalEntries.size() + 1, position + 1, config.get("rpc_timeout_ms"), actor))
                .thenAccept(this.waitingResponses::add)
                .thenRun(() -> onJournalAppend(request.getResponseConfig()));

    }

    private void updateVoterConfig(UpdateClusterStateRequest request) {
        UpdateRequest updateRequest;
        if (request.getRequests().size() == 1 && (updateRequest = request.getRequests().get(0)).getPartition() == INTERNAL_PARTITION) {
            InternalEntryType entryType = InternalEntriesSerializeSupport.parseEntryType(updateRequest.getEntry());
            if (entryType == TYPE_UPDATE_VOTERS_S1) {
                UpdateVotersS1Entry updateVotersS1Entry = InternalEntriesSerializeSupport.parse(updateRequest.getEntry());
                actor.send("State", "toJointConsensus", updateVotersS1Entry.getConfigOld(), updateVotersS1Entry.getConfigNew());
                for (URI uri : updateVotersS1Entry.getConfigNew()) {
                    if (! uri.equals(state.getLocalUri()) && // uri was not me
                            replicationDestinations.stream().noneMatch(r -> r.getUri().equals(uri))) { // and not included in the old followers collection
                        replicationDestinations.add(new ReplicationDestination(uri, journal.maxIndex(), config.get("heartbeat_interval_ms"), config.get("replication_batch_size")));
                    }
                }
            } else if(entryType == TYPE_UPDATE_VOTERS_S2) {
                actor.<List<URI>>sendThen("State", "toNewConfig")
                        .thenAccept(voters -> this.replicationDestinations.removeIf(r -> !voters.contains(r.getUri())));
            }
        }
    }

    private void onJournalAppend(ResponseConfig responseConfig) {
        if (voterState.getState() != VoterState.LEADER) {
            return;
        }
        switch (responseConfig) {
            case PERSISTENCE:
                actor.send("Journal", "flush");
                break;
            case REPLICATION:
                this.replication();
                break;
            case ALL:
                actor.send("Journal", "flush");
                this.replication();
                break;
            default:
                // nothing to do.
        }
    }

    /**
     * 对于每一个AsyncAppendRequest RPC请求，当收到成功响应的时需要更新repStartIndex、matchIndex和commitIndex。
     * 由于接收者按照日志的索引位置串行处理请求，一般情况下，收到的响应也是按照顺序返回的，但是考虑到网络延时和数据重传，
     * 依然不可避免乱序响应的情况。LEADER在处理响应时需要遵循：
     * <p>
     * 1. 对于所有响应，先比较返回值中的term是否与当前term一致，如果不一致说明任期已经变更，丢弃响应，
     * 2. LEADER 反复重试所有term一致的超时和失败请求（考虑到性能问题，可以在每次重试前加一个时延）；
     * 3. 对于返回失败的请求，如果这个请求是所有在途请求中日志位置最小的（repStartIndex == logIndex），
     * 说明接收者的日志落后于repStartIndex，这时LEADER需要回退，再次发送AsyncAppendRequest RPC请求，
     * 直到找到FOLLOWER与LEADER相同的位置。
     * 4. 对于成功的响应，需要按照日志索引位置顺序处理。规定只有返回值中的logIndex与repStartIndex相等时，
     * 才更新repStartIndex和matchIndex，否则反复重试直到满足条件；
     * 5. 如果存在一个索引位置N，这个N是所有满足如下所有条件位置中的最大值，则将commitIndex更新为N。
     * 5.1 超过半数的matchIndex都大于等于N
     * 5.2 N > commitIndex
     * 5.3 log[N].term == currentTerm
     */
    @ActorListener
    private void commit() {
        if (voterState.getState() != VoterState.LEADER) {
            return;
        }
        boolean isAnyFollowerNextIndexUpdated = false;
        if (
                journal.commitIndex() < journal.maxIndex() && (
                        replicationDestinations.isEmpty() ||  (isAnyFollowerNextIndexUpdated = this.replicationDestinations.stream()
                                .anyMatch(r -> !r.isCommitted()))
                )) {
            long N = calculateN(isAnyFollowerNextIndexUpdated);
            if (N > journal.commitIndex() && getTerm(N - 1) == this.term) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Set commitIndex {} to {}, {}.", journal.commitIndex(), N, voterInfo());
                }
                actor.send("Journal", "commit", N);
            }
        }
    }
    
    private int getTerm(long index) {
        try {
            return journal.getTerm(index);
        } catch (IndexUnderflowException e) {
            NavigableMap<Long, Snapshot> snapshots = state.getSnapshots();
            if (index + 1 == snapshots.firstKey()) {
                return snapshots.firstEntry().getValue().lastIncludedTerm();
            } else {
                throw e;
            }
        }
    }

    private long calculateN(boolean isAnyFollowerNextIndexUpdated) {
        long N = 0L;
        if (this.replicationDestinations.isEmpty()) {
            N = journal.maxIndex();
        } else {
            if (isAnyFollowerNextIndexUpdated) {
                if (state.isJointConsensus()) {
                    long[] sortedMatchIndexInOldConfig = replicationDestinations.stream()
                            .filter(follower -> state.getConfigOld().contains(follower.getUri()))
                            .mapToLong(ReplicationDestination::getMatchIndex)
                            .sorted().toArray();
                    long nInOldConfig = sortedMatchIndexInOldConfig.length > 0 ?
                            sortedMatchIndexInOldConfig[sortedMatchIndexInOldConfig.length / 2] : journal.maxIndex();

                    long[] sortedMatchIndexInNewConfig = replicationDestinations.stream()
                            .filter(follower -> state.getConfigNew().contains(follower.getUri()))
                            .mapToLong(ReplicationDestination::getMatchIndex)
                            .sorted().toArray();
                    long nInNewConfig = sortedMatchIndexInNewConfig.length > 0 ?
                            sortedMatchIndexInNewConfig[sortedMatchIndexInNewConfig.length / 2] : journal.maxIndex();

                    N = Math.min(nInNewConfig, nInOldConfig);

                } else {
                    long[] sortedMatchIndex = replicationDestinations.stream()
                            .mapToLong(ReplicationDestination::getMatchIndex)
                            .sorted().toArray();
                    if (sortedMatchIndex.length > 0) {
                        N = sortedMatchIndex[sortedMatchIndex.length / 2];
                    }
                }

            }
        }

        return N;
    }
    @ActorSubscriber
    private void onStateChange(StateResult stateResult) {
        if (voterState.getState() != VoterState.LEADER) {
            return;
        }
        if(config.get("enable_events")) {
            OnStateChangeEvent event = new OnStateChangeEvent(state.lastApplied());
            byte [] serializedEvent =  InternalEntriesSerializeSupport.serialize(event);
            actor.send("EventBus", "fireEvent", new Event(EventType.ON_STATE_CHANGE, serializedEvent));
        }
        Iterator<WaitingResponse> iterator = waitingResponses.iterator();
        while (iterator.hasNext()) {
            WaitingResponse waitingResponse = iterator.next();
            if (waitingResponse.positionMatch(stateResult.getLastApplied())) {
                waitingResponse.putResult(stateResult.getUserResult());
                waitingResponse.countdownReplication();
                iterator.remove();
                break;
            }
        }
    }

    @ActorListener
    private void onJournalFlush(long journalFlushIndex) {
        if (voterState.getState() != VoterState.LEADER) {
            return;
        }
        this.waitingResponses.removeIf(waitingResponse -> waitingResponse.getToPosition() <= journalFlushIndex && waitingResponse.countdownFlush());
    }

    @ActorSubscriber(topic = "onJournalCommit")
    private void replication() {
        if (voterState.getState() != VoterState.LEADER) {
            return;
        }
        this.replicationDestinations.forEach(ReplicationDestination::onJournalCommit);
        this.replicationDestinations.forEach(ReplicationDestination::replication);
    }
    @ActorScheduler
    private void removeTimeoutResponses() {
        this.waitingResponses.removeIf(WaitingResponse::isTimeout);
    }

    private List<JournalEntry> requestToJournalEntries(UpdateClusterStateRequest request) {
        List<JournalEntry> journalEntries = new ArrayList<>(request.getRequests().size());
        for (UpdateRequest serializedUpdateRequest : request.getRequests()) {
            JournalEntry entry;

            if (request.isIncludeHeader()) {
                entry = journalEntryParser.parse(serializedUpdateRequest.getEntry());
            } else {
                entry = journalEntryParser.createJournalEntry(serializedUpdateRequest.getEntry());
            }
            entry.setPartition(serializedUpdateRequest.getPartition());
            entry.setBatchSize(serializedUpdateRequest.getBatchSize());
            entry.setTerm(this.term);


            if (request.getTransactionId() != null) {
                entry = journalTransactionManager.wrapTransactionalEntry(entry, request.getTransactionId(), journalEntryParser);
            }
            journalEntries.add(entry);
        }
        return journalEntries;
    }

    private boolean isUpdateVoterRequest(UpdateClusterStateRequest request) {
        UpdateRequest updateRequest;

        if (request.getRequests().size() == 1 && (updateRequest = request.getRequests().get(0)).getPartition() == INTERNAL_PARTITION) {
            InternalEntryType entryType = InternalEntriesSerializeSupport.parseEntryType(updateRequest.getEntry());
            return entryType == TYPE_UPDATE_VOTERS_S1 | entryType == TYPE_UPDATE_VOTERS_S2;
        }
        return false;
    }

    @ActorListener
    private CreateTransactionResponse createTransaction(CreateTransactionRequest request) {
        // TODO
        return null;
    }

    @ActorListener
    private CompleteTransactionResponse completeTransaction(CompleteTransactionRequest request) {
        // TODO
        return null;
    }

    @ActorListener
    private GetOpeningTransactionsResponse getOpeningTransactions() {
        // TODO
        return null;
    }

    private boolean checkWriteable() {
        if (isWritable) {
            return true;
        } else {
            if (System.currentTimeMillis() > disableWriteTimeout) {
                isWritable = true;
                return true;
            } else {
                return false;
            }
        }
    }

    @ActorListener
    private DisableLeaderWriteResponse disableLeaderWrite(DisableLeaderWriteRequest request) {
        if (voterState.getState() != VoterState.LEADER) {
            return new DisableLeaderWriteResponse(new NotLeaderException(this.leaderUri));
        }
        long timeoutMs = request.getTimeoutMs();
        int term = request.getTerm();
        if (this.term != term) {
            return  new DisableLeaderWriteResponse(new IllegalStateException(
                    String.format("Term not matched! Term in leader: %d, term in request: %d", this.term, term)));
        }
        this.isWritable = false;
        this.disableWriteTimeout = System.currentTimeMillis() + timeoutMs;
        return new DisableLeaderWriteResponse(this.term);
    }


    private void checkQuorum() {
        if (voterState.getState() != VoterState.LEADER) {
            return;
        }

        if (replicationDestinations.isEmpty()) {
            return;
        }
        long[] sortedHeartbeatResponseTimes = replicationDestinations.stream().mapToLong(ReplicationDestination::getLastHeartbeatResponseTime)
                .sorted().toArray();

        long leaderShipDeadLineMs =
                (sortedHeartbeatResponseTimes[sortedHeartbeatResponseTimes.length / 2]) + config.<Long>get("check_quorum_timeout_ms");
        long now = System.currentTimeMillis();
        if (leaderShipDeadLineMs > 0 && now > leaderShipDeadLineMs) {
            logger.info("Leader check quorum failed, convert myself to follower, {}.", voterInfo());
            convertToFollower();
        }
    }

    private void announceLeader() {
        isAnnounced = false;
        byte[] payload = InternalEntriesSerializeSupport.serialize(new LeaderAnnouncementEntry(this.term, state.getLocalUri()));
        JournalEntry journalEntry = journalEntryParser.createJournalEntry(payload);
        journalEntry.setTerm(this.term);
        journalEntry.setPartition(INTERNAL_PARTITION);
        actor.send("Journal", "append", ActorMsg.Response.IGNORE, Collections.singletonList(journalEntry));
    }

    @ActorListener(topic = "checkLeadership")
    private CheckLeadershipResponse clientCheckLeadership() {
        if (voterState.getState() == VoterState.LEADER && isAnnounced) {
            return new CheckLeadershipResponse();
        } else {
            return new CheckLeadershipResponse(new NotLeaderException(this.leaderUri));
        }
    }

    @ActorSubscriber(topic = "onInternalEntryApply")
    private void onLeaderAnnouncementEntryApplied(InternalEntryType type, byte [] internalEntry) {
        if (type == InternalEntryType.TYPE_LEADER_ANNOUNCEMENT) {
            LeaderAnnouncementEntry leaderAnnouncementEntry = InternalEntriesSerializeSupport.parse(internalEntry);

            if (voterState.getState() == VoterState.LEADER && !isAnnounced && leaderAnnouncementEntry.getTerm() == this.term) {
                this.isAnnounced = true;
                logger.info("Leader announcement applied! Leader: {}, term: {}.", state.getLocalUri(), term);
            }
        }
    }





    @ActorSubscriber
    private void onStateRecovered() {
        this.replicationDestinations.addAll(state.getConfigState().voters().stream()
                .filter(uri -> !uri.equals(state.getLocalUri()))
                .map(uri -> new ReplicationDestination(uri, journal.maxIndex(), config.get("heartbeat_interval_ms"), config.get("replication_batch_size")))
                .collect(Collectors.toList()));
    }


    @ResponseManually
    @ActorListener
    public void queryClusterState(@ActorMessage ActorMsg msg) {
        QueryStateRequest request = msg.getPayload();
        if (voterState.getState() == VoterState.LEADER) {
            actor.sendThen("State", "queryServerState", request)
                    .thenAccept(resp -> actor.reply(msg, resp));
        } else {
            actor.reply(msg, new QueryStateResponse(new NotLeaderException(this.leaderUri)));
        }
    }

    private static class WaitingResponse implements Comparable<WaitingResponse>{
        private final ActorMsg requestMsg;
        private final ResponseConfig responseConfig;
        private final long fromPosition;
        private final long toPosition;
        private int flushCountDown, replicationCountDown;
        private final long timestamp;
        private final long rpcTimeoutMs;
        private final List<byte[]> results;
        private final Actor actor;


        public WaitingResponse(ActorMsg requestMsg, long fromPosition, long toPosition, long rpcTimeoutMs, Actor actor) {
            this.rpcTimeoutMs = rpcTimeoutMs;
            this.actor = actor;
            UpdateClusterStateRequest request = requestMsg.getPayload();
            this.requestMsg = new ActorMsg(requestMsg.getSequentialId(), requestMsg.getSender(), requestMsg.getReceiver(), requestMsg.getTopic());
            this.responseConfig = request.getResponseConfig();
            this.fromPosition = fromPosition;
            this.toPosition = toPosition;
            this.timestamp = System.currentTimeMillis();
            int count = request.getRequests().size();
            this.results = new ArrayList<>(count);
            this.flushCountDown = count;
            this.replicationCountDown = count;
        }

        public ActorMsg getRequestMsg() {
            return requestMsg;
        }

        public ResponseConfig getResponseConfig() {
            return responseConfig;
        }

        public long getFromPosition() {
            return fromPosition;
        }

        public long getToPosition() {
            return toPosition;
        }

        @Override
        public int compareTo(WaitingResponse o) {
            return Long.compare(fromPosition, o.fromPosition);
        }
        private boolean countdownFlush() {
            if (--flushCountDown == 0) {
                return maybeReply();
            }
            return false;
        }

        private boolean countdownReplication() {
            if (--replicationCountDown == 0) {
                return maybeReply();
            }
            return false;
        }
        private void putResult(byte[] result) {
            results.add(result);
        }

        private boolean maybeReply() {
            if (shouldReply()) {
                actor.reply(requestMsg, new UpdateClusterStateResponse(results, fromPosition));
                return true;
            }
            return false;
        }

        private boolean shouldReply () {
            switch (responseConfig) {
                case PERSISTENCE:
                    if (flushCountDown == 0) {
                        return true;
                    }
                case REPLICATION:
                    if (replicationCountDown == 0) {
                        return true;
                    }
                case ALL:
                    if (flushCountDown == 0 && replicationCountDown == 0) {
                        return true;
                    }
                default:
                    return false;
            }
        }

        private boolean positionMatch(long position) {
            return position >= fromPosition && position < toPosition;
        }


        public boolean isTimeout() {
            long deadline = System.currentTimeMillis() - this.rpcTimeoutMs;

            if (timestamp < deadline) {
                actor.reply(requestMsg, new UpdateClusterStateResponse(new TimeoutException()));
                return true;
            }
            return false;
        }
    }

    public Actor getActor() {
        return actor;
    }

    @ActorSubscriber
    private void onLeaderChange(URI leaderUri) {
        this.leaderUri = leaderUri;
    }

    private class ReplicationDestination {

        private final URI uri;


        private final int replicationBatchSize;
        private boolean installSnapshotInProgress = false;

        /**
         * 需要发给它的下一个日志条目的索引（初始化为领导人上一条日志的索引值 +1）
         */
        private long nextIndex;
        /**
         * 已经复制到该服务器的日志的最高索引值（从 0 开始递增）
         */
        private long matchIndex = 0L;

        /**
         * 上次从FOLLOWER收到心跳（asyncAppendEntries）成功响应的时间戳
         */
        private long lastHeartbeatResponseTime;
        private long lastHeartbeatRequestTime = 0L;
        private final long heartbeatIntervalMs;
        private boolean committed = true;
        private boolean waitingForResponse = false;

        void reset() {
            lastHeartbeatResponseTime = System.currentTimeMillis();
            lastHeartbeatRequestTime = lastHeartbeatResponseTime;
            committed = true;
            waitingForResponse = false;
            installSnapshotInProgress = false;
        }

        ReplicationDestination(URI uri, long nextIndex, long heartbeatIntervalMs, int replicationBatchSize) {
            this.uri = uri;
            this.nextIndex = nextIndex;
            this.heartbeatIntervalMs = heartbeatIntervalMs;
            this.lastHeartbeatResponseTime = 0L;
            this.replicationBatchSize = replicationBatchSize;
        }


        void replication() {
            long maxIndex;

            if (waitingForResponse || (nextIndex >= (maxIndex = journal.maxIndex()) // NOT 还有需要复制的数据
                    &&
                    System.currentTimeMillis() - lastHeartbeatRequestTime < heartbeatIntervalMs // NOT 距离上次复制/心跳已经超过一个心跳超时了
            )) {
                return;
            }

            if (installSnapshotInProgress) {
                return;
            }


            // 如果有必要，先安装第一个快照
            Map.Entry<Long, Snapshot> fistSnapShotEntry = state.getSnapshots().firstEntry();
            if (installSnapshot(fistSnapShotEntry)) {
                return;
            }

            // 读取需要复制的Entry
            List<byte[]> entries;
            if (nextIndex < maxIndex) { // 复制
                entries = journal.readRaw(nextIndex, this.replicationBatchSize);
            } else { // 心跳
                entries = Collections.emptyList();
            }

            // 构建请求并发送
            AsyncAppendEntriesRequest request =
                    new AsyncAppendEntriesRequest(term, state.getLocalUri(),
                            nextIndex - 1, this.getPreLogTerm(nextIndex),
                            entries, state.commitIndex(), maxIndex);

            waitingForResponse = true;
            actor.<AsyncAppendEntriesResponse>sendThen("Rpc", "asyncAppendEntries", new RpcMsg<>(this.uri, request))
                    .thenAccept(resp -> handleAppendEntriesResponse(resp, entries.size(), fistSnapShotEntry.getKey()))
                    .exceptionally(e -> {
                        logger.warn("Replication execution exception, from {} to {}, cause: {}.", state.getLocalUri(), uri, null == e.getCause() ? e.getMessage() : e.getCause().getMessage());
                        return null;
                    })
                    .whenComplete((c, r) -> {
                        waitingForResponse = false;
                    });
            lastHeartbeatRequestTime = System.currentTimeMillis();
        }


        private void handleAppendEntriesResponse(AsyncAppendEntriesResponse response, int entrySize, long startIndex) {
            if (checkTerm(response.getTerm())) {
                return;
            }

            if(!response.success()) {
                return;
            }
            // 成功收到响应响应
            lastHeartbeatResponseTime = System.currentTimeMillis();
            if (response.isSuccess()) { // 复制成功
                if (entrySize > 0) {
                    nextIndex += entrySize;
                    matchIndex = nextIndex;
                    committed = false;
                    actor.send("Voter","commit");
                }
            } else {
                // 不匹配，回退
                int rollbackSize = (int) Math.min(replicationBatchSize, nextIndex - startIndex);
                nextIndex -= rollbackSize;
            }
        }

        private boolean installSnapshot(Map.Entry<Long, Snapshot> snapShotEntry) {
            long snapshotIndex = snapShotEntry.getKey();
            Snapshot snapshot = snapShotEntry.getValue();
            if (nextIndex >= snapshotIndex) {
                return false;
            }
            installSnapshotInProgress = true;

            try {
                logger.info("Install snapshot to {} ...", this.getUri());

                int offset = 0;
                ReplicableIterator iterator = snapshot.iterator();
                while (iterator.hasMoreTrunks()) {
                    byte[] trunk = iterator.nextTrunk();
                    InstallSnapshotRequest request = new InstallSnapshotRequest(
                            term, state.getLocalUri(), snapshot.lastIncludedIndex(), snapshot.lastIncludedTerm(),
                            offset, trunk, !iterator.hasMoreTrunks()
                    );
                    boolean lastRequest = !iterator.hasMoreTrunks();
                    actor.<InstallSnapshotResponse>sendThen("Rpc", "installSnapshot", new RpcMsg<>(this.uri, request))
                            .whenComplete((response, exception) -> handleInstallSnapshotResponse(response, exception, lastRequest, snapshotIndex));
                    offset += trunk.length;
                }
            } catch (IOException t) {
                logger.warn("Install snapshot to {} failed!", this.getUri(), t);
                installSnapshotInProgress = false;
            }
            return true;
        }

        private void handleInstallSnapshotResponse(InstallSnapshotResponse response, Throwable exception, boolean last, long snapshotIndex) {
            if (null != exception) {
                logger.warn("Install snapshot execution exception, from {} to {}, cause: {}.", state.getLocalUri(), uri, null == exception.getCause()? exception.getMessage() : exception.getCause().getMessage());
                installSnapshotInProgress = false;
            } else {
                if (!response.success()) {
                    logger.warn("Install snapshot to {} failed! Cause: {}.", this.getUri(), response.errorString());
                    installSnapshotInProgress = false;
                } else {
                    if (last) {
                        nextIndex = snapshotIndex;
                        logger.info("Install snapshot to {} success.", this.getUri());
                    }
                }
            }
            if (last) {
                installSnapshotInProgress = false;
            }
        }


        private int getPreLogTerm(long currentLogIndex) {
            if (currentLogIndex > journal.minIndex()) {
                return journal.getTerm(currentLogIndex - 1);
            } else if (currentLogIndex == journal.minIndex() && state.getSnapshots().containsKey(currentLogIndex)) {
                return state.getSnapshots().get(currentLogIndex).lastIncludedTerm();
            } else if (currentLogIndex == 0) {
                return -1;
            } else {
                throw new IndexUnderflowException();
            }
        }

        URI getUri() {
            return uri;
        }

        long getNextIndex() {
            return nextIndex;
        }

        long getMatchIndex() {
            return matchIndex;
        }

        long getLastHeartbeatResponseTime() {
            return lastHeartbeatResponseTime;
        }

        long getLastHeartbeatRequestTime() {
            return lastHeartbeatRequestTime;
        }

        public boolean isCommitted() {
            return committed;
        }

        @Override
        public String toString() {
            return "{" +
                    "uri=" + uri +
                    ", nextIndex=" + nextIndex +
                    ", matchIndex=" + matchIndex +
                    '}';
        }

        public void onJournalCommit() {
            this.committed = journal.commitIndex() >= matchIndex;
        }


    }

    

}
