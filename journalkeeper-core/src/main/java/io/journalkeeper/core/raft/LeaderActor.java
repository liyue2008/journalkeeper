package io.journalkeeper.core.raft;

import io.journalkeeper.core.api.*;
import io.journalkeeper.core.entry.internal.InternalEntriesSerializeSupport;
import io.journalkeeper.core.entry.internal.InternalEntryType;
import io.journalkeeper.core.state.ConfigState;
import io.journalkeeper.core.state.Snapshot;
import io.journalkeeper.core.transaction.JournalTransactionManager;
import io.journalkeeper.exceptions.IndexUnderflowException;
import io.journalkeeper.exceptions.NotLeaderException;
import io.journalkeeper.rpc.client.*;
import io.journalkeeper.rpc.server.DisableLeaderWriteRequest;
import io.journalkeeper.rpc.server.DisableLeaderWriteResponse;
import io.journalkeeper.utils.actor.*;
import io.journalkeeper.utils.config.Config;
import io.journalkeeper.utils.state.StateServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.*;
import java.util.concurrent.TimeoutException;

import static io.journalkeeper.core.api.RaftJournal.INTERNAL_PARTITION;
import static io.journalkeeper.core.entry.internal.InternalEntryType.TYPE_UPDATE_VOTERS_S1;
import static io.journalkeeper.core.entry.internal.InternalEntryType.TYPE_UPDATE_VOTERS_S2;


public class LeaderActor {

    private static final Logger logger = LoggerFactory.getLogger( LeaderActor.class );

    private final JournalEntryParser journalEntryParser;
    private final JournalTransactionManager journalTransactionManager = null;

    private final Actor actor = new Actor("Leader");
    private final RaftJournal journal;
    private final RaftState state;

    private final RaftVoter voter;

    private final Config config;

    private boolean isWritable = true;
    private long disableWriteTimeout = 0L;

    private final List<WaitingResponse> waitingResponses = new LinkedList<>();

    private final List<ReplicationDestination> replicationDestinations = new ArrayList<>();


    public LeaderActor(JournalEntryParser journalEntryParser, RaftJournal journal, RaftState state, RaftVoter voter, Config config) {
        this.journalEntryParser = journalEntryParser;
        this.journal = journal;
        this.state = state;
        this.voter = voter;
        this.config = config;
        actor.setHandlerInstance(this);
    }

    @ActorListener
    private void updateClusterState(ActorMsg msg) {
        // 1. Leader.updateClusterState
        //      1.1. Journal.append
        // 2.
        //      2.1 Journal.flush
        //      2.2 Leader.replication
        // 3. (RPC) Follower.asyncAppendEntries
        // 4. Leader.commit
        UpdateClusterStateRequest request = msg.getPayload();
        if (voter.getVoterState() != VoterState.LEADER) {
            actor.reply(msg, new UpdateClusterStateResponse(new NotLeaderException(getClusterLeader())));
        }
        if (!checkWriteable()) {
            actor.reply(msg, new UpdateClusterStateResponse(new IllegalStateException("Server disabled temporarily.")));
        }
        if (request.getResponseConfig() == ResponseConfig.RECEIVE) {
            actor.reply(msg, new UpdateClusterStateResponse());
        }

        if (isUpdateVoterRequest(request)) {
            // TODO
            // return;
        }

        List<JournalEntry> journalEntries = requestToJournalEntries(request);

        actor.<Long>sendThen("Journal", "append", journalEntries)
                .thenAccept(position -> {
                            this.waitingResponses.add(
                                    new WaitingResponse(msg, position - journalEntries.size() + 1, position + 1, config.get("rpc_timeout_ms"), actor)
                            );
                            switch (request.getResponseConfig()) {
                                case PERSISTENCE:
                                    actor.send("Journal", "flush", null);
                                    break;
                                case REPLICATION:
                                    actor.send("Leader", "replication", null);
                                    break;
                                case ALL:
                                    actor.send("Journal", "flush", null);
                                    actor.send("Leader", "replication", null);
                                    break;
                                default:
                                    // nothing to do.
                            }
                        }
                );

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
    @ActorScheduler(interval = 100L)
    private void commit() {
        boolean isAnyFollowerNextIndexUpdated = false;
        if (
                journal.commitIndex() < journal.maxIndex() && (
                        replicationDestinations.isEmpty() ||  (isAnyFollowerNextIndexUpdated = this.replicationDestinations.stream()
                .noneMatch(ReplicationDestination::isCommitted))
                )) {
            long N = calcuateN(isAnyFollowerNextIndexUpdated);
            if (N > journal.commitIndex() && getTerm(N - 1) == getCurrentTerm()) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Set commitIndex {} to {}, {}.", journal.commitIndex(), N, voterInfo());
                }
                actor.send("Journal", "commit", N);
//                onCommitted();
            }
        }
    }

    private String voterInfo() {
        return String.format("voterState: %s, currentTerm: %d, minIndex: %d, " +
                        "maxIndex: %d, commitIndex: %d, lastApplied: %d, uri: %s",
                VoterState.LEADER, getCurrentTerm(), journal.minIndex(),
                journal.maxIndex(), journal.commitIndex(), state.lastApplied(), state.getLocalUri().toString());
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

    private long calcuateN(boolean isAnyFollowerNextIndexUpdated) {
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
    @ActorListener(consumer = true, payload = true)
    private void onStateChange(StateResult stateResult) {
        this.waitingResponses.removeIf(waitingResponse -> waitingResponse.positionMatch(stateResult.getLastApplied()) && waitingResponse.countdownReplication());
    }

    @ActorListener(consumer = true, payload = true)
    private void onJournalFlush(long journalFlushIndex) {
        this.waitingResponses.removeIf(waitingResponse -> waitingResponse.getToPosition() <= journalFlushIndex && waitingResponse.countdownFlush());
    }

    @ActorScheduler(interval = 100)
    private void replication() {
        this.replicationDestinations.forEach(ReplicationDestination::replication);
    }
    @ActorListener
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
            entry.setTerm(getCurrentTerm());


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

    @ActorListener(payload = true, response = true)
    private CreateTransactionResponse createTransaction(CreateTransactionRequest request) {
        // TODO
        return null;
    }

    @ActorListener(payload = true, response = true)
    private CompleteTransactionResponse completeTransaction(CompleteTransactionRequest request) {
        // TODO
        return null;
    }

    @ActorListener(payload = true, response = true)
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

    @ActorListener(payload = true, response = true)
    private DisableLeaderWriteResponse disableLeaderWrite(DisableLeaderWriteRequest request) {
        if (voter.getVoterState() != VoterState.LEADER) {
            return new DisableLeaderWriteResponse(new NotLeaderException(getClusterLeader()));
        }
        long timeoutMs = request.getTimeoutMs();
        int term = request.getTerm();
        if (getCurrentTerm() != term) {
            return  new DisableLeaderWriteResponse(new IllegalStateException(
                    String.format("Term not matched! Term in leader: %d, term in request: %d", getCurrentTerm(), term)));
        }
        this.isWritable = false;
        this.disableWriteTimeout = System.currentTimeMillis() + timeoutMs;
        return new DisableLeaderWriteResponse(getCurrentTerm());
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
            this.requestMsg = new ActorMsg(requestMsg.getSequentialId(), requestMsg.getSender(), requestMsg.getReceiver(), requestMsg.getTopic(), null);
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

    private int getCurrentTerm() {
        return state.getTerm();
    }

    private URI getClusterLeader() {
        return state.getLeaderUri();
    }

}
