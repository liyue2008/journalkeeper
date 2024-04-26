package io.journalkeeper.core.raft;

import io.journalkeeper.rpc.RpcAccessPointFactory;
import io.journalkeeper.rpc.client.*;
import io.journalkeeper.rpc.server.*;
import io.journalkeeper.utils.actor.*;
import io.journalkeeper.utils.actor.annotation.ActorListener;
import io.journalkeeper.utils.event.EventWatcher;
import io.journalkeeper.utils.spi.ServiceSupport;
import io.journalkeeper.utils.state.StateServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;


public class ServerRpcActor implements ServerRpc {
    private static final Logger logger = LoggerFactory.getLogger( ServerRpcActor.class );
    private URI uri;

    private final Properties properties;
    private RpcAccessPointFactory rpcAccessPointFactory;
    private ServerRpcAccessPoint serverRpcAccessPoint;
    private StateServer rpcServer = null;
    private final Actor actor = Actor.builder("ServerRpc").setHandlerInstance(this).build();

    private StateServer.ServerState serverState = StateServer.ServerState.CREATED;

    protected ServerRpcActor( Properties properties) {
        this.properties  = properties;
    }


    public Actor getActor() {
        return actor;
    }

    @Override
    public URI serverUri() {
        return this.uri;
    }

    @Override
    public CompletableFuture<UpdateClusterStateResponse> updateClusterState(UpdateClusterStateRequest request) {
        return forwardRequest(request, "Leader");
    }

    @Override
    public CompletableFuture<QueryStateResponse> queryClusterState(QueryStateRequest request) {
        return forwardRequest(request, "State", "queryClusterState");
    }

    @Override
    public CompletableFuture<QueryStateResponse> queryServerState(QueryStateRequest request) {
        return forwardRequest(request, "State", "queryServerState");

    }

    @Override
    public CompletableFuture<LastAppliedResponse> lastApplied() {
        return forwardRequest(null, "State");
    }

    private  <T> CompletableFuture<T>  forwardRequest(String addr, String topic){
        return forwardRequest(null, addr, topic);
    }
    private  <T> CompletableFuture<T>  forwardRequest(String topic){
        return forwardRequest(null, null, topic);
    }
    private  <T> CompletableFuture<T>  forwardRequest(Object request) {
        return forwardRequest(request, null, null);

    }
    private  <T> CompletableFuture<T>  forwardRequest(Object request, String addr) {
        return forwardRequest(request, addr, null);
    }
    private <T> CompletableFuture<T> forwardRequest(Object request, String addr, String topic) {

        if (addr == null) {
            addr = "RaftServer";
        }
        if (topic == null) {
            topic = request.getClass().getSimpleName();

            if (topic.endsWith("Request")){
                topic = topic.substring(0, topic.length() - 7);
            }
            topic = firstCharToLowerCase(topic);
        }

        if (null == request) {
            return actor.sendThen(addr, topic);
        }else {
            return actor.sendThen(addr, topic, request);
        }
    }

    private String firstCharToLowerCase(String str) {
        if(str == null || str.isEmpty()) {
            return str;
        } else {
            return Character.toLowerCase(str.charAt(0)) + str.substring(1);
        }
    }
    @Override
    public CompletableFuture<QueryStateResponse> querySnapshot(QueryStateRequest request) {
        return forwardRequest(request,"State", "querySnapshot");
    }

    @Override
    public CompletableFuture<GetServersResponse> getServers() {
        return forwardRequest(null, "State", "getServers");
    }

    @Override
    public CompletableFuture<GetServerStatusResponse> getServerStatus() {
        return forwardRequest("getServerStatus");
    }

    @Override
    public CompletableFuture<AddPullWatchResponse> addPullWatch() {
        return forwardRequest(null, "EventBus");

    }

    @Override
    public CompletableFuture<RemovePullWatchResponse> removePullWatch(RemovePullWatchRequest request) {
        return forwardRequest(request, "EventBus");

    }

    @Override
    public CompletableFuture<UpdateVotersResponse> updateVoters(UpdateVotersRequest request) {
        return forwardRequest(request);

    }

    @Override
    public CompletableFuture<PullEventsResponse> pullEvents(PullEventsRequest request) {
        return forwardRequest(request, "EventBus");

    }

    @Override
    public CompletableFuture<ConvertRollResponse> convertRoll(ConvertRollRequest request) {
        return forwardRequest(request, "State");

    }

    @Override
    public CompletableFuture<CreateTransactionResponse> createTransaction(CreateTransactionRequest request) {
        return forwardRequest(request, "Leader");

    }

    @Override
    public CompletableFuture<CompleteTransactionResponse> completeTransaction(CompleteTransactionRequest request) {
        return forwardRequest(request, "Leader");

    }

    @Override
    public CompletableFuture<GetOpeningTransactionsResponse> getOpeningTransactions() {
        return forwardRequest(null, "Leader");

    }

    @Override
    public CompletableFuture<GetSnapshotsResponse> getSnapshots() {
        return forwardRequest(null , "State");

    }

    @Override
    public CompletableFuture<CheckLeadershipResponse> checkLeadership() {
        return forwardRequest("checkLeadership");

    }

    @Override
    public void watch(EventWatcher eventWatcher) {
        actor.send("EventBus", "watch", eventWatcher);
    }

    @Override
    public void unWatch(EventWatcher eventWatcher) {
        actor.send("EventBus", "unWatch", eventWatcher);
    }

    @Override
    public CompletableFuture<AsyncAppendEntriesResponse> asyncAppendEntries(AsyncAppendEntriesRequest request) {
        return forwardRequest(request);

    }

    @Override
    public CompletableFuture<RequestVoteResponse> requestVote(RequestVoteRequest request) {
        return forwardRequest(request);

    }

    @Override
    public CompletableFuture<GetServerEntriesResponse> getServerEntries(GetServerEntriesRequest request) {
        return forwardRequest(request);

    }

    @Override
    public CompletableFuture<GetServerStateResponse> getServerState(GetServerStateRequest request) {
        return forwardRequest(request, "State");

    }

    @Override
    public CompletableFuture<DisableLeaderWriteResponse> disableLeaderWrite(DisableLeaderWriteRequest request) {
        return forwardRequest(request, "Leader");

    }

    @Override
    public CompletableFuture<InstallSnapshotResponse> installSnapshot(InstallSnapshotRequest request) {
        return forwardRequest(request);
    }

    @ActorListener
    private void start(URI uri) {
        if (this.serverState != StateServer.ServerState.CREATED) {
            return;
        }
        try {
            this.uri = uri;
            this.serverState = StateServer.ServerState.STARTING;

            this.rpcAccessPointFactory = ServiceSupport.load(RpcAccessPointFactory.class);
            this.serverRpcAccessPoint = rpcAccessPointFactory.createServerRpcAccessPoint(properties);

            this.rpcServer = rpcAccessPointFactory.bindServerService(this);
            this.rpcServer.start();
            this.serverState = StateServer.ServerState.RUNNING;
        } catch (Exception e) {
            logger.error("Failed to start server RPC service.", e);
        }

    }

    @ActorListener
    public void stop() {
        if (this.serverState != StateServer.ServerState.RUNNING) {
            return;
        }
        try {
            this.serverState = StateServer.ServerState.STOPPING;
            if (rpcServer != null) {
                rpcServer.stop();
            }
            this.serverState = StateServer.ServerState.STOPPED;
        } catch (Exception e) {
            logger.error("Failed to stop server RPC service.", e);
        }
    }

    public ServerRpcAccessPoint getServerRpcAccessPoint() {
        return serverRpcAccessPoint;
    }

}
