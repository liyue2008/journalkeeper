package io.journalkeeper.utils.actor;

import java.lang.reflect.Method;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Actor model
 */
public class Actor {
    private final String addr;
    private final ActorInbox inbox;
    private final ActorOutbox outbox;
    private final ActorResponseSupport responseSupport;

    public Actor(String addr) {

        this.addr = addr;
        this.outbox = new ActorOutbox(addr);
        this.inbox = new ActorInbox(addr, outbox);
        this.responseSupport = new ActorResponseSupport(inbox, outbox);

    }

    public String getAddr() {
        return addr;
    }

    public <T, U> void addTopicHandlerFunction(String topic, BiConsumer<T, U> handler) {
        inbox.addTopicHandlerFunction(topic, handler);
    }

    public void addTopicHandlerFunction(String topic,Object instance, Method method) {
        inbox.addTopicHandlerFunction(topic, instance, method);
    }

    public <T> void addTopicHandlerFunction(String topic, Consumer<T> handler) {
        inbox.addTopicHandlerFunction(topic, handler);
    }

    public void setDefaultHandlerFunction(Consumer<ActorMsg> handler) {
        inbox.setDefaultHandlerFunction(handler);
    }

    public void setHandlerInstance(Object handlerInstance) {
        inbox.setHandlerInstance(handlerInstance);
    }

    public ActorMsg pubMsg(String topic, Object payload) {
        return send(PostOffice.PUB_ADDR, topic, payload);
    }

    public ActorMsg send(String addr, String topic) {
        return outbox.send(addr, topic);
    }
    public ActorMsg send(String addr, String topic, Object... payloads) {
        return outbox.send(addr, topic, payloads);
    }
    public <T> CompletableFuture<T> sendThen(String addr, String topic) {
        return responseSupport.send(addr, topic);
    }

    public <T> CompletableFuture<T> sendThen(String addr, String topic, Object... payloads) {
        return responseSupport.send(addr, topic, payloads);
    }

    public void addTopicResponseHandlerFunction(String topic, BiConsumer<ActorMsg, ActorResponse> handler) {
        responseSupport.addTopicHandlerFunction(topic, handler);
    }

    public void setResponseHandlerInstance(Object handlerInstance) {
        responseSupport.setHandlerInstance(handlerInstance);
    }

    public void reply(ActorMsg request, Object result) {
        responseSupport.reply(request, result);
    }

    public void setDefaultResponseHandlerFunction(BiConsumer<ActorMsg, ActorResponse> handler) {
        responseSupport.setDefaultHandlerFunction(handler);

    }

    ActorInbox getInbox() {
        return inbox;
    }

    ActorOutbox getOutbox() {
        return outbox;
    }
}
