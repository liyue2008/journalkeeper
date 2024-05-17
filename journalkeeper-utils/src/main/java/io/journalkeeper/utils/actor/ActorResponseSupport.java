package io.journalkeeper.utils.actor;

import io.journalkeeper.utils.actor.annotation.ActorListener;
import io.journalkeeper.utils.actor.annotation.ActorResponseListener;
import io.journalkeeper.utils.actor.annotation.ActorScheduler;
import io.journalkeeper.utils.actor.annotation.ActorSubscriber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

class ActorResponseSupport {

    static final String RESPONSE = "actor_response";

    private static final Logger logger = LoggerFactory.getLogger( ActorResponseSupport.class );

    private Consumer<ActorResponse> defaultResponseHandler;

    private final Map<String /* topic */, Consumer<ActorResponse>> responseHandlers;

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
        ActorMsg request = this.outbox.createMsg(addr, topic, ActorMsg.Response.REQUIRED, payloads);
        responseFutures.put(request, future);
        this.outbox.send(request);
        return future;
    }

    void addTopicHandlerFunction(String topic, Consumer<ActorResponse> handler) {
        responseHandlers.put(topic, handler);
    }

    void setHandlerInstance(Object handlerInstance) {
        this.handlerInstance = handlerInstance;
        this.annotationListeners = ActorUtils.scanActionListeners(handlerInstance, ActorResponseListener.class);
    }

    void replyException(ActorMsg request, Throwable throwable) {
        this.outbox.send(request.getSender(), RESPONSE, new ActorResponse(request, throwable));
    }

    void reply(ActorMsg request, Object result) {
        this.outbox.send(request.getSender(), RESPONSE, new ActorResponse(request, result));
    }

    void setDefaultHandlerFunction(Consumer<ActorResponse> handler) {
        this.defaultResponseHandler = handler;
    }

    private Map<String, Method> annotationListeners = new HashMap<>();


    private void processResponse(ActorResponse response) {
        ActorMsg request = response.getRequest();
        if (request != null) {
            // 调用future
            if (responseFutures.containsKey(request)) {

                //noinspection rawtypes
                CompletableFuture future = responseFutures.remove(request);
                if (response.getThrowable() != null) {
                    future.completeExceptionally(response.getThrowable());
                } else {
                    //noinspection unchecked
                    future.complete(response.getResult());
                }
                return;
            }
            // 显式注册的
            Consumer<ActorResponse> handler = responseHandlers.get(request.getTopic());
            if (handler != null) {
                handler.accept(response);
                return;
            }
            if (handlerInstance != null) {
                try {
                    // 注解注册的
                    if (null != annotationListeners && annotationListeners.containsKey(request.getTopic())){
                        // 通过注解注册的方法
                        Method method = annotationListeners.get(request.getTopic());
                        method.setAccessible(true);
                        method.invoke(handlerInstance, response);
                        return;
                    }
                    // 默认的响应方法
                    Method method = handlerInstance.getClass().getDeclaredMethod(request.getTopic() + "Response", ActorResponse.class);
                    method.setAccessible(true);
                    method.invoke(handlerInstance, response);
                    return;
                } catch (NoSuchMethodException ignored) {
                    // nothing to do
                } catch (IllegalAccessException | InvocationTargetException e) {
                    logger.warn("Invoke response handler failed, method: {}!", request.getTopic() + "Response", e);
                    return;
                }
            }
            if (this.defaultResponseHandler != null) {
                this.defaultResponseHandler.accept(response);

            } else {
                logger.warn("No ongoing request for response: {}", response);
            }
        }
    }
}
