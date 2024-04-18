package io.journalkeeper.utils.actor;

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

    public void addTopicHandlerFunction(String topic, Consumer<ActorMsg> handler) {
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

    public ActorMsg send(String addr, String topic, Object payload) {
        return outbox.send(addr, topic, payload);
    }

    public <T> CompletableFuture<T> sendThen(String addr, String topic, Object payload) {
        return responseSupport.send(addr, topic, payload);
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
