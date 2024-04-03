package io.journalkeeper.core.raft;

import io.journalkeeper.rpc.RpcAccessPointFactory;
import io.journalkeeper.utils.actor.ActorBase;
import io.journalkeeper.utils.actor.ActorMsg;
import io.journalkeeper.utils.actor.PostOffice;
import io.journalkeeper.rpc.client.*;
import io.journalkeeper.rpc.server.*;
import io.journalkeeper.utils.event.EventWatcher;
import io.journalkeeper.utils.spi.ServiceSupport;
import io.journalkeeper.utils.state.StateServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;


public class ServerRpcActor extends ActorBase implements ServerRpc {
    private static final Logger logger = LoggerFactory.getLogger( ServerRpcActor.class );
    private final URI uri;

    private final Properties properties;
    private RpcAccessPointFactory rpcAccessPointFactory;
    private ServerRpcAccessPoint serverRpcAccessPoint;
    private StateServer rpcServer = null;


    private StateServer.ServerState serverState = StateServer.ServerState.CREATED;

    protected ServerRpcActor(PostOffice postOffice, URI uri, Properties properties) {
        super(DEFAULT_CAPACITY, postOffice);
        this.uri = uri;
        this.properties  = properties;
        setDefaultResponseHandler(this::onResponse);
    }


    @Override
    public String addr() {
        return "ServerRpc";
    }

    @Override
    public URI serverUri() {
        return this.uri;
    }
    @SuppressWarnings("rawtypes")
    private final Map<Object, CompletableFuture> responseFutures = new ConcurrentHashMap<>();

    @SuppressWarnings("unchecked")

    private void onResponse(ActorMsg request, Object response) {
        @SuppressWarnings("rawtypes")
        CompletableFuture future = responseFutures.remove(request);
        if (future == null) {
            logger.warn("No response future for request: {}", request);
            return;
        }
        future.complete(response);
    }
    @Override
    public CompletableFuture<UpdateClusterStateResponse> updateClusterState(UpdateClusterStateRequest request) {
        return forwardRequest(request);
    }

    @Override
    public CompletableFuture<QueryStateResponse> queryClusterState(QueryStateRequest request) {
        return forwardRequest(request);
    }

    @Override
    public CompletableFuture<QueryStateResponse> queryServerState(QueryStateRequest request) {
        return forwardRequest(request);
    }

    @Override
    public CompletableFuture<LastAppliedResponse> lastApplied() {
        return forwardRequest();
    }

    private  <T> CompletableFuture<T>  forwardRequest(){
        return forwardRequest(null, null, null);
    }
    private  <T> CompletableFuture<T>  forwardRequest(Object request) {
        return forwardRequest(request, null, null);

    }
    private <T> CompletableFuture<T> forwardRequest(Object request, String addr, String topic) {

        CompletableFuture<T> future = new CompletableFuture<>();
        if (addr == null) {
            addr = "RaftServer";
        }
        if (null == request) {
            request = new Object();
        }
        if (topic == null) {
            topic = request.getClass().getSimpleName();

            if (topic.endsWith("Request")){
                topic = topic.substring(0, topic.length() - 7);
            }
        }


        responseFutures.put(request, future);
        send(addr, topic, request);
        return future;
    }

    @Override
    public CompletableFuture<QueryStateResponse> querySnapshot(QueryStateRequest request) {
        return forwardRequest(request);
    }

    @Override
    public CompletableFuture<GetServersResponse> getServers() {
        return forwardRequest();
    }

    @Override
    public CompletableFuture<GetServerStatusResponse> getServerStatus() {
        return forwardRequest();
    }

    @Override
    public CompletableFuture<AddPullWatchResponse> addPullWatch() {
        return forwardRequest();

    }

    @Override
    public CompletableFuture<RemovePullWatchResponse> removePullWatch(RemovePullWatchRequest request) {
        return forwardRequest(request);

    }

    @Override
    public CompletableFuture<UpdateVotersResponse> updateVoters(UpdateVotersRequest request) {
        return forwardRequest(request);

    }

    @Override
    public CompletableFuture<PullEventsResponse> pullEvents(PullEventsRequest request) {
        return forwardRequest(request);

    }

    @Override
    public CompletableFuture<ConvertRollResponse> convertRoll(ConvertRollRequest request) {
        return forwardRequest(request);

    }

    @Override
    public CompletableFuture<CreateTransactionResponse> createTransaction(CreateTransactionRequest request) {
        return forwardRequest(request);

    }

    @Override
    public CompletableFuture<CompleteTransactionResponse> completeTransaction(CompleteTransactionRequest request) {
        return forwardRequest(request);

    }

    @Override
    public CompletableFuture<GetOpeningTransactionsResponse> getOpeningTransactions() {
        return forwardRequest();

    }

    @Override
    public CompletableFuture<GetSnapshotsResponse> getSnapshots() {
        return forwardRequest();

    }

    @Override
    public CompletableFuture<CheckLeadershipResponse> checkLeadership() {
        return forwardRequest();

    }

    @Override
    public void watch(EventWatcher eventWatcher) {
        send("EventBus", "watch", eventWatcher);
    }

    @Override
    public void unWatch(EventWatcher eventWatcher) {
        send("EventBus", "unWatch", eventWatcher);
    }

    @Override
    public void stop() {
        // nothing to do...
    }

    @Override
    public CompletableFuture<AsyncAppendEntriesResponse> asyncAppendEntries(AsyncAppendEntriesRequest request) {
        CompletableFuture<AsyncAppendEntriesResponse> future = new CompletableFuture<>();
        responseFutures.put(request, future);
        send("RaftServer", "asyncAppendEntries", request);
        return future;
    }

    @Override
    public CompletableFuture<RequestVoteResponse> requestVote(RequestVoteRequest request) {
        CompletableFuture<RequestVoteResponse> future = new CompletableFuture<>();
        responseFutures.put(request, future);
        send("RaftServer", "requestVote", request);
        return future;
    }

    @Override
    public CompletableFuture<GetServerEntriesResponse> getServerEntries(GetServerEntriesRequest request) {
        CompletableFuture<GetServerEntriesResponse> future = new CompletableFuture<>();
        responseFutures.put(request, future);
        send("RaftServer", "getServerEntries", request);
        return future;
    }

    @Override
    public CompletableFuture<GetServerStateResponse> getServerState(GetServerStateRequest request) {
        CompletableFuture<GetServerStateResponse> future = new CompletableFuture<>();
        responseFutures.put(request, future);
        send("RaftServer", "getServerState", request);
        return future;
    }

    @Override
    public CompletableFuture<DisableLeaderWriteResponse> disableLeaderWrite(DisableLeaderWriteRequest request) {
        CompletableFuture<DisableLeaderWriteResponse> future = new CompletableFuture<>();
        responseFutures.put(request, future);
        send("RaftServer", "disableLeaderWrite", request);
        return future;
    }

    @Override
    public CompletableFuture<InstallSnapshotResponse> installSnapshot(InstallSnapshotRequest request) {
        CompletableFuture<InstallSnapshotResponse> future = new CompletableFuture<>();
        responseFutures.put(request, future);
        send("RaftServer", "installSnapshot", request);
        return future;
    }

    private void start(ActorMsg msg) {
        if (this.serverState != StateServer.ServerState.CREATED) {
            reply(msg,false);
        }
        try {
            this.serverState = StateServer.ServerState.STARTING;

            this.rpcAccessPointFactory = ServiceSupport.load(RpcAccessPointFactory.class);
            this.serverRpcAccessPoint = rpcAccessPointFactory.createServerRpcAccessPoint(properties);

            this.rpcServer = rpcAccessPointFactory.bindServerService(this);
            this.rpcServer.start();
            this.serverState = StateServer.ServerState.RUNNING;
            reply(msg, true);
        } catch (Exception e) {
            logger.error("Failed to start server RPC service.", e);
            reply(msg, false);
        }

    }

    private void stop(ActorMsg msg) {
        if (this.serverState == StateServer.ServerState.RUNNING) {
            reply(msg, false);
        }
        try {
            this.serverState = StateServer.ServerState.STOPPING;
            if (rpcServer != null) {
                rpcServer.stop();
            }
            this.serverState = StateServer.ServerState.STOPPED;
            reply(msg, true);
        } catch (Exception e) {
            reply(msg, false);
        }
    }
}
