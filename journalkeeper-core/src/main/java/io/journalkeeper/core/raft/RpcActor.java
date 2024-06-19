package io.journalkeeper.core.raft;

import io.journalkeeper.rpc.BaseResponse;
import io.journalkeeper.rpc.RpcAccessPointFactory;
import io.journalkeeper.rpc.server.ServerRpc;
import io.journalkeeper.rpc.server.ServerRpcAccessPoint;
import io.journalkeeper.utils.actor.Actor;
import io.journalkeeper.utils.actor.ActorMsg;
import io.journalkeeper.utils.spi.ServiceSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.net.URI;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

public class RpcActor {
    private final Actor actor = Actor.builder().addr("Rpc")
            .setDefaultHandlerFunction(this::send)
            .build();
    private final ServerRpcAccessPoint serverRpcAccessPoint;

    public RpcActor(Properties properties) {
        RpcAccessPointFactory rpcAccessPointFactory = ServiceSupport.load(RpcAccessPointFactory.class);
       this.serverRpcAccessPoint = rpcAccessPointFactory.createServerRpcAccessPoint(properties);
    }

    public Actor getActor() {
        return actor;
    }


    private ServerRpc getServerRpc(URI uri) {
        return serverRpcAccessPoint.getServerRpcAgent(uri);
    }

    private void send(ActorMsg actorMsg) {
        try {
            RpcMsg<?> rpcMsg = actorMsg.getPayload();
            URI uri = rpcMsg.getUri();
            ServerRpc serverRpc = getServerRpc(uri);

            String topic = actorMsg.getTopic();
            Method method = serverRpc.getClass().getDeclaredMethod(topic, rpcMsg.getRequest().getClass());
            @SuppressWarnings("unchecked") CompletableFuture<? extends BaseResponse> future = (CompletableFuture<? extends BaseResponse>) method.invoke(serverRpc, rpcMsg.getRequest());
            future.thenAccept(response -> actor.reply(actorMsg, response));

        } catch (Exception e) {
            actor.replyException(actorMsg, e);
        }
    }

}
