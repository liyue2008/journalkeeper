package io.journalkeeper.core.raft;

import io.journalkeeper.core.api.*;
import io.journalkeeper.core.entry.internal.InternalEntriesSerializeSupport;
import io.journalkeeper.core.entry.internal.InternalEntryType;
import io.journalkeeper.core.transaction.JournalTransactionManager;
import io.journalkeeper.exceptions.NotLeaderException;
import io.journalkeeper.metric.JMetric;
import io.journalkeeper.rpc.client.*;
import io.journalkeeper.rpc.server.DisableLeaderWriteRequest;
import io.journalkeeper.rpc.server.DisableLeaderWriteResponse;
import io.journalkeeper.utils.actor.*;
import io.journalkeeper.utils.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;

import static io.journalkeeper.core.api.RaftJournal.INTERNAL_PARTITION;
import static io.journalkeeper.core.entry.internal.InternalEntryType.TYPE_UPDATE_VOTERS_S1;
import static io.journalkeeper.core.entry.internal.InternalEntryType.TYPE_UPDATE_VOTERS_S2;


public class LeaderActor {

    private static final Logger logger = LoggerFactory.getLogger( LeaderActor.class );

    private final JournalEntryParser journalEntryParser;
    private final JournalTransactionManager journalTransactionManager = null;

    private final Actor actor = new Actor("Leader");

    private final RaftState state;

    private final RaftVoter voter;

    private final Config config;

    private boolean isWritable = true;

    private final List<WaitingResponse> waitingResponses = new LinkedList<>();

    private final List<ReplicationDestination> replicationDestinations = new ArrayList<>();


    public LeaderActor(JournalEntryParser journalEntryParser, RaftState state, RaftVoter voter, Config config) {
        this.journalEntryParser = journalEntryParser;
        this.state = state;
        this.voter = voter;
        this.config = config;
        actor.setHandlerInstance(this);
    }
    @ActorListener
    private void updateClusterState(ActorMsg msg) {
        UpdateClusterStateRequest request = msg.getPayload();
        if (voter.getVoterState() != VoterState.LEADER) {
            actor.reply(msg, new UpdateClusterStateResponse(new NotLeaderException(getClusterLeader())));
        }
        if (!isWritable) {
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
                .thenAccept(position ->
                        this.waitingResponses.add(
                                new WaitingResponse(msg, position - journalEntries.size() + 1, position + 1, config.get("rpc_timeout_ms"), actor)
                        )
                );
    }
    @ActorListener(consumer = true, payload = true)
    private void onStateChange(StateResult stateResult) {
        this.waitingResponses.removeIf(waitingResponse -> waitingResponse.positionMatch(stateResult.getLastApplied()) && waitingResponse.countdownReplication());
    }

    @ActorListener(consumer = true, payload = true)
    private void onJournalFlush(long position) {
        this.waitingResponses.removeIf(waitingResponse -> waitingResponse.getToPosition() <= position && waitingResponse.countdownFlush());
    }

    @ActorListener(payload = true, consumer = true)
    private void onJournalAppend(long position) {
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

    private DisableLeaderWriteResponse disableLeaderWrite(DisableLeaderWriteRequest request) {
        // TODO
        return null;
    }

    private static class UpdateStateRequestResponse {
        private final UpdateClusterStateRequest request;
        private final ResponseFuture responseFuture;
        private final long start = System.nanoTime();

        UpdateStateRequestResponse(UpdateClusterStateRequest request, JMetric metric) {
            this.request = request;
            this.responseFuture = new ResponseFuture(request.getResponseConfig(), request.getRequests().size());
            if (null != metric) {
                responseFuture.getResponseFuture()
                        .thenRun(() -> metric.mark(() -> System.nanoTime() - start, () -> request.getRequests().stream().mapToLong(r -> r.getEntry().length).sum()));
            }

        }

        UpdateClusterStateRequest getRequest() {
            return request;
        }

        ResponseFuture getResponseFuture() {
            return this.responseFuture;
        }
    }

    private static class ResponseFuture {
        private final CompletableFuture<UpdateClusterStateResponse> responseFuture;
        private final CompletableFuture<UpdateClusterStateResponse> flushFuture;
        private final CompletableFuture<UpdateClusterStateResponse> replicationFuture;
        private int flushCountDown, replicationCountDown;
        private List<byte[]> results;

        ResponseFuture(ResponseConfig responseConfig, int count) {
            this.flushFuture = new CompletableFuture<>();
            this.replicationFuture = new CompletableFuture<>();

            switch (responseConfig) {
                case PERSISTENCE:
                    responseFuture = flushFuture;
                    break;
                case ALL:
                    responseFuture = new CompletableFuture<>();
                    this.replicationFuture.whenComplete((replicationResponse, e) -> {
                        if (null == e) {
                            if (replicationResponse.success()) {
                                this.flushFuture.whenComplete((flushResponse, t) -> {
                                    if (null == t) {
                                        if (flushResponse.success()) {
                                            // 如果都成功，优先使用replication response，因为里面有执行状态机的返回值。
                                            responseFuture.complete(replicationResponse);
                                        } else {
                                            // replication 成功，flush 失败，返回失败的flush response。
                                            responseFuture.complete(flushResponse);
                                        }
                                    } else {
                                        responseFuture.complete(new UpdateClusterStateResponse(t));
                                    }
                                });
                            } else {
                                responseFuture.complete(replicationResponse);
                            }
                        } else {
                            responseFuture.complete(new UpdateClusterStateResponse(e));
                        }
                    });
                    break;
                default:
                    responseFuture = replicationFuture;
                    break;

            }

            flushCountDown = count;
            replicationCountDown = count;
            results = new ArrayList<>(count);
        }

        CompletableFuture<UpdateClusterStateResponse> getResponseFuture() {
            return responseFuture;
        }

        void countDownFlush() {
            if (--flushCountDown == 0) {
                flushFuture.complete(new UpdateClusterStateResponse(Collections.emptyList(), -1L));
            }
        }

        void putResult(byte[] result, long lastApplied) {
            results.add(result);
            if (--replicationCountDown == 0) {
                replicationFuture.complete(new UpdateClusterStateResponse(results, lastApplied));
            }
        }

        void completedExceptionally(Throwable throwable) {
            responseFuture.completeExceptionally(throwable);
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
