package io.journalkeeper.utils.actor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;

class ActorResponseSupport {

    static final String RESPONSE = "response";

    private static final Logger logger = LoggerFactory.getLogger( ActorResponseSupport.class );

    private BiConsumer<ActorMsg, ActorResponse> defaultResponseHandler;

    private final Map<String /* topic */, BiConsumer<ActorMsg, ActorResponse>> responseHandlers;

    private final Map<Long, ActorMsg> ongoingRequests = new HashMap<>();

    private Object handlerInstance;

    @SuppressWarnings("rawtypes")
    private final Map<ActorMsg, CompletableFuture> responseFutures = new ConcurrentHashMap<>();

    private final ActorOutbox outbox;
    ActorResponseSupport(ActorInbox inbox, ActorOutbox outbox) {
        responseHandlers = new HashMap<>();
        this.outbox = outbox;
        inbox.addTopicHandlerFunction(RESPONSE, this::processResponse);
    }

    <T> CompletableFuture<T> send(String addr, String topic){
        return send(addr, topic, new Object[]{});
    }
    <T> CompletableFuture<T> send(String addr, String topic, Object... payloads){
        CompletableFuture<T> future = new CompletableFuture<>();
        ActorMsg request = this.outbox.send(addr, topic, payloads);
        responseFutures.put(request, future);
        return future;
    }

    void addTopicHandlerFunction(String topic, BiConsumer<ActorMsg, ActorResponse> handler) {
        responseHandlers.put(topic, handler);
    }

    void setHandlerInstance(Object handlerInstance) {
        this.handlerInstance = handlerInstance;
    }

    void replyException(ActorMsg request, Throwable throwable) {
        this.outbox.send(request.getSender(), RESPONSE, new ActorResponse(request.getTopic(), request.getSequentialId(), throwable));
    }

    void reply(ActorMsg request, Object result) {
        this.outbox.send(request.getSender(), RESPONSE, new ActorResponse(request.getTopic(), request.getSequentialId(), result));
    }

    void setDefaultHandlerFunction(BiConsumer<ActorMsg, ActorResponse> handler) {
        this.defaultResponseHandler = handler;
    }

    private void processResponse(ActorMsg actorMsg) {
        ActorResponse response = actorMsg.getPayload();
        ActorMsg request = ongoingRequests.remove(response.getSequentialId());
        if (request != null) {
            // 调用future
            if (responseFutures.containsKey(request)) {
                //noinspection unchecked
                responseFutures.remove(request).complete(response.getResult());
                return;
            }
            // 显式注册的
            BiConsumer<ActorMsg, ActorResponse> handler = responseHandlers.get(response.getTopic());
            if (handler != null) {
                handler.accept(request, response);
                return;
            }
            if (handlerInstance != null) {
                try {
                    Method method = handlerInstance.getClass().getMethod(actorMsg.getTopic() + "Response", ActorMsg.class, ActorResponse.class);
                    method.invoke(handlerInstance, request, response);
                    return;
                } catch (NoSuchMethodException ignored) {
                    // nothing to do
                } catch (IllegalAccessException | InvocationTargetException e) {
                    logger.warn("Invoke response handler failed, method: {}!", actorMsg.getTopic() + "Response", e);
                    return;
                }
            }
            if (this.defaultResponseHandler != null) {
                this.defaultResponseHandler.accept(request, response);

            } else {
                logger.warn("No ongoing request for response: {}", response);
            }
        }
    }
}
