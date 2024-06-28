package io.journalkeeper.core.raft;

import io.journalkeeper.core.api.ClusterConfiguration;
import io.journalkeeper.core.resilience.InFlightRequestRateLimiter;
import io.journalkeeper.rpc.RpcAccessPointFactory;
import io.journalkeeper.rpc.client.*;
import io.journalkeeper.rpc.server.*;
import io.journalkeeper.utils.actor.*;
import io.journalkeeper.utils.actor.annotation.ActorSubscriber;
import io.journalkeeper.utils.event.EventWatcher;
import io.journalkeeper.utils.spi.ServiceSupport;
import io.journalkeeper.utils.state.StateServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;


public class ServerRpcActor implements ServerRpc {
    private static final Logger logger = LoggerFactory.getLogger( ServerRpcActor.class );
    private URI uri;

    private final RpcAccessPointFactory rpcAccessPointFactory;
    private StateServer rpcServer = null;
    private final Actor actor = Actor.builder()
            .addr("ServerRpc")
            .setHandlerInstance(this)
            .addTopicQueue("updateClusterState", 1024)
            .addTopicQueue("asyncAppendEntries", 1024)
            .build();

    private final InFlightRequestRateLimiter commonLimiter;
    private final Map<String, InFlightRequestRateLimiter> rateLimiterMap;

    private StateServer.ServerState serverState = StateServer.ServerState.CREATED;

    protected ServerRpcActor() {
        this.rpcAccessPointFactory = ServiceSupport.load(RpcAccessPointFactory.class);
        this.rateLimiterMap = createRateLimiterMap();
        this.commonLimiter = new InFlightRequestRateLimiter(1024);

    }

    private Map<String, InFlightRequestRateLimiter> createRateLimiterMap() {
        InFlightRequestRateLimiter updateLimiter = new InFlightRequestRateLimiter(1024);
        InFlightRequestRateLimiter queryLimiter = new InFlightRequestRateLimiter(1024);
        Map<String, InFlightRequestRateLimiter> rateLimiterMap = new ConcurrentHashMap<>();
        rateLimiterMap.put("updateClusterState", updateLimiter);
        rateLimiterMap.put("asyncAppendEntries", updateLimiter);
        rateLimiterMap.put("queryClusterState", queryLimiter);
        rateLimiterMap.put("queryServerState", queryLimiter);
        rateLimiterMap.put("getServerEntries", queryLimiter);
        return rateLimiterMap;
    }

    private InFlightRequestRateLimiter getRateLimiter(String topic) {
        return rateLimiterMap.getOrDefault(topic, commonLimiter);
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

        return forwardRequest(request, "Voter");
    }

    @Override
    public CompletableFuture<QueryStateResponse> queryClusterState(QueryStateRequest request) {
        return forwardRequest(request, "Voter", "queryClusterState");
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
    private  <T> CompletableFuture<T>  forwardRequest(Object request, String addr) {
        return forwardRequest(request, addr, null);
    }
    private <T> CompletableFuture<T> forwardRequest(Object request, String addr, String topic) {
        CompletableFuture<T> future;

        if (serverState != StateServer.ServerState.RUNNING) {
            future = new CompletableFuture<>();
            future.completeExceptionally(new IllegalStateException("Server is not running"));
            return future;
        }
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

        InFlightRequestRateLimiter rateLimiter = getRateLimiter(topic);
        try {
            rateLimiter.acquire();
            if (null == request) {
                future = actor.sendThen(addr, topic, ActorRejectPolicy.BLOCK);
            } else {
                future = actor.sendThen(addr, topic, ActorRejectPolicy.BLOCK, request);
            }
            future.whenComplete((r, e) -> rateLimiter.release());
            return future;
        } catch (InterruptedException e) {
            future = new CompletableFuture<>();
            future.completeExceptionally(e);
            return future;
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
        if (serverState != StateServer.ServerState.RUNNING) {
            CompletableFuture<GetServersResponse> future = new CompletableFuture<>();
            future.completeExceptionally(new IllegalStateException("Server is not running"));
            return future;
        }
        final ClusterConfiguration clusterConfiguration = new ClusterConfiguration();
        return CompletableFuture.allOf(
                actor.<URI>sendThen("Voter", "getLeaderUri", ActorRejectPolicy.BLOCK).thenAccept(clusterConfiguration::setLeader),
                actor.<List<URI>>sendThen("State", "getVoters", ActorRejectPolicy.BLOCK).thenAccept(clusterConfiguration::setVoters)
        ).thenApply(any -> new GetServersResponse(clusterConfiguration));
    }

    @Override
    public CompletableFuture<GetServerStatusResponse> getServerStatus() {

        return forwardRequest("Voter", "getServerStatus");
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
        return forwardRequest(request,"Voter");

    }

    @Override
    public CompletableFuture<PullEventsResponse> pullEvents(PullEventsRequest request) {
        return forwardRequest(request, "EventBus");

    }

    @Override
    public CompletableFuture<ConvertRollResponse> convertRoll(ConvertRollRequest request) {
        return forwardRequest(request, "Voter");

    }

    @Override
    public CompletableFuture<CreateTransactionResponse> createTransaction(CreateTransactionRequest request) {
        return forwardRequest(request, "Voter");

    }

    @Override
    public CompletableFuture<CompleteTransactionResponse> completeTransaction(CompleteTransactionRequest request) {
        return forwardRequest(request, "Voter");

    }

    @Override
    public CompletableFuture<GetOpeningTransactionsResponse> getOpeningTransactions() {
        return forwardRequest("Voter", "getOpeningTransactions");

    }

    @Override
    public CompletableFuture<GetSnapshotsResponse> getSnapshots() {
        return forwardRequest("Voter", "getSnapshots");

    }

    @Override
    public CompletableFuture<CheckLeadershipResponse> checkLeadership() {
        return forwardRequest("Voter", "checkLeadership");

    }

    @Override
    public void watch(EventWatcher eventWatcher) {
        actor.send("EventBus", "watch", ActorMsg.Response.DEFAULT, ActorRejectPolicy.BLOCK, eventWatcher);
    }

    @Override
    public void unWatch(EventWatcher eventWatcher) {
        actor.send("EventBus", "unWatch", ActorMsg.Response.DEFAULT, ActorRejectPolicy.BLOCK,eventWatcher);
    }

    @Override
    public CompletableFuture<AsyncAppendEntriesResponse> asyncAppendEntries(AsyncAppendEntriesRequest request) {
        return forwardRequest(request, "Voter");

    }

    @Override
    public CompletableFuture<RequestVoteResponse> requestVote(RequestVoteRequest request) {
        return forwardRequest(request, "Voter");

    }

    @Override
    public CompletableFuture<GetServerEntriesResponse> getServerEntries(GetServerEntriesRequest request) {
        return forwardRequest(request, "Voter");

    }

    @Override
    public CompletableFuture<GetServerStateResponse> getServerState(GetServerStateRequest request) {
        return forwardRequest(request, "State");

    }

    @Override
    public CompletableFuture<DisableLeaderWriteResponse> disableLeaderWrite(DisableLeaderWriteRequest request) {
        return forwardRequest(request, "Voter");

    }

    @Override
    public CompletableFuture<InstallSnapshotResponse> installSnapshot(InstallSnapshotRequest request) {
        return forwardRequest(request, "State");
    }

    @ActorSubscriber(topic = "onStart")
    private void start(ServerContext context) {
        if (this.serverState != StateServer.ServerState.CREATED) {
            return;
        }
        try {
            this.uri = context.getState().getLocalUri();
            this.serverState = StateServer.ServerState.STARTING;
            this.rpcServer = rpcAccessPointFactory.bindServerService(this);
            this.rpcServer.start();
            this.serverState = StateServer.ServerState.RUNNING;
        } catch (Exception e) {
            logger.error("Failed to start server RPC service.", e);
        }

    }

    @ActorSubscriber(topic = "onStop")
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


}
