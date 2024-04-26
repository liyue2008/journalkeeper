package io.journalkeeper.core.raft;

import io.journalkeeper.rpc.server.ServerRpc;
import io.journalkeeper.rpc.server.ServerRpcAccessPoint;
import io.journalkeeper.utils.actor.Actor;
import io.journalkeeper.utils.actor.ActorMsg;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class RpcActor {
    private static final Logger logger = LoggerFactory.getLogger( RpcActor.class );
    private final Actor actor = Actor.builder("Rpc")
            .setDefaultHandlerFunction(this::send)
            .build();
    private final Map<URI, ServerRpc> remoteServers = new HashMap<>();
    private final ServerRpcAccessPoint serverRpcAccessPoint;

    public RpcActor(ServerRpcAccessPoint serverRpcAccessPoint) {
       this.serverRpcAccessPoint = serverRpcAccessPoint;
    }

    public Actor getActor() {
        return actor;
    }


    private ServerRpc getServerRpc(URI uri) {
        // FIXME：需要一个清理remoteServers的机制
        return remoteServers.computeIfAbsent(uri, serverRpcAccessPoint::getServerRpcAgent);
    }

    private void send(ActorMsg actorMsg) {
        try {
            RpcMsg<?> rpcMsg = actorMsg.getPayload();
            URI uri = rpcMsg.getUri();
            ServerRpc serverRpc = getServerRpc(uri);

            String topic = actorMsg.getTopic();
            Method method = serverRpc.getClass().getMethod(topic, rpcMsg.getRequest().getClass());
            @SuppressWarnings("rawtypes") CompletableFuture future = (CompletableFuture) method.invoke(serverRpc, rpcMsg.getRequest());
            //noinspection unchecked
            future.thenAccept(response -> actor.reply(actorMsg, response));

        } catch (Exception e) {
            actor.reply(actorMsg, e);
            logger.warn("actorMsg:" + actorMsg, e);
        }
    }

}
