package io.journalkeeper.utils.actor;

import io.journalkeeper.utils.actor.annotation.ActorListener;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.*;

/**
 * Actor model
 * Actor 维护自己的私有状态，可以在认为安全的情况下，通过只读接口暴露部分状态给外部调用。
 * Actor 可以发送消息，接收其他Actor的消息。
 * Actor 消息处理都是单线程的。
 * 要尽量避免在处理消息的方法中有长时间的IO操作。
 */
public class Actor {
    // 地址
    private final String addr;
    // 收件箱，所有收到的消息放入收件箱暂存，然后单线程顺序处理。
    private final ActorInbox inbox;
    // 发件箱，所有发出去的消息放入发件箱暂存，然后邮递员会将消息分发给对应地址
    private final ActorOutbox outbox;
    // 对请求/响应模式的封装支持
    private final ActorResponseSupport responseSupport;

    private boolean stopped = false;
    private Actor(String addr) {
        this.addr = addr;
        this.outbox = new ActorOutbox(addr);
        this.inbox = new ActorInbox(addr, outbox);
        this.responseSupport = new ActorResponseSupport(inbox, outbox);
    }

    /**
     * 获取Actor地址
     * @return 地址
     */
    public String getAddr() {
        return addr;
    }
    /**
     * 添加没有参数的消息处理函数
     * @param topic 消息主题
     * @param handler 消息处理函数
     */
    private void addTopicHandlerFunction(String topic, Runnable handler) {
        inbox.addTopicHandlerFunction(topic, handler);
    }
    /**
     * 添加没有参数的消息处理函数，函数返回值将作为响应消息发送给发送者
     * @param topic 消息主题
     * @param handler 消息处理函数
     * @param <R> 返回值类型
     */
    private <R> void addTopicHandlerFunction(String topic, Supplier<R> handler) {
        inbox.addTopicHandlerFunction(topic, handler);
    }
    /**
     * 添加1个参数的消息处理函数，函数返回值将作为响应消息发送给发送者
     * @param topic 消息主题
     * @param handler 消息处理函数
     * @param <T> 参数的类型
     * @param <R> 返回值类型
     */
    private <T, R> void addTopicHandlerFunction(String topic, Function<T, R> handler) {
        inbox.addTopicHandlerFunction(topic, handler);
    }
    /**
     * 添加有2个参数的消息处理函数，函数返回值将作为响应消息发送给发送者
     * @param topic 消息主题
     * @param handler 消息处理函数
     * @param <T> 第一个参数类型
     * @param <U> 第二个参数类型
     * @param <R> 返回值类型
     */
    private <T, U, R> void addTopicHandlerFunction(String topic, BiFunction<T, U, R> handler) {
        inbox.addTopicHandlerFunction(topic, handler);
    }

    /**
     * 添加有2个参数的消息处理函数
     * @param topic 消息主题
     * @param handler 消息处理函数
     * @param <T> 第一个参数类型
     * @param <U> 第二个参数类型
     */
    private <T, U> void addTopicHandlerFunction(String topic, BiConsumer<T, U> handler) {
        inbox.addTopicHandlerFunction(topic, handler);
    }

    /**
     * 添加有1个参数的消息处理函数
     * @param topic 消息主题
     * @param handler 消息处理函数
     * @param <T> 参数类型。
     */
    private <T> void addTopicHandlerFunction(String topic, Consumer<T> handler) {
        inbox.addTopicHandlerFunction(topic, handler);
    }

    public void addScheduler(long interval, TimeUnit timeUnit, String topic, Runnable runnable) {
        inbox.receive(new ActorMsg(0L, addr,addr,"@addTopicHandlerFunction", topic, runnable));
        send("Scheduler", "addTask", new ScheduleTask(timeUnit, interval, this.addr, topic));
    }

    public void removeScheduler(String topic) {
        send("Scheduler", "removeTask", addr, topic);
        inbox.receive(new ActorMsg(0L, addr,addr,"@removeTopicHandlerFunction", topic));

    }
    /**
     * 设置默认消息处理函数。所有未处理的消息都由这个函数处理。
     * @param handler 默认的消息处理函数
     */
    private void setDefaultHandlerFunction(Consumer<ActorMsg> handler) {
        inbox.setDefaultHandlerFunction(handler);
    }

    private void setHandlerInstance(Object handlerInstance) {
        inbox.setHandlerInstance(handlerInstance);
    }

    public ActorMsg pub(String topic, Object payload) {
        return send(PostOffice.PUB_ADDR, topic, payload);
    }
    public ActorMsg pub(String topic, Object... payloads) {
        return send(PostOffice.PUB_ADDR, topic, payloads);
    }

    public ActorMsg send(String addr, String topic) {
        return outbox.send(addr, topic);
    }
    ActorMsg send(String addr, String topic, ActorMsg.Response response) {
        return outbox.send(addr, topic, response);
    }
    public ActorMsg send(String addr, String topic, ActorMsg.Response response, Object... payloads) {
        return outbox.send(addr, topic, response, payloads);
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

    private void addTopicResponseHandlerFunction(String topic, Consumer<ActorResponse> handler) {
        responseSupport.addTopicHandlerFunction(topic, handler);
    }

    private void setResponseHandlerInstance(Object handlerInstance) {
        responseSupport.setHandlerInstance(handlerInstance);
    }

    public void reply(ActorMsg request, Object result) {
        responseSupport.reply(request, result);
    }

    public void replyException(ActorMsg request, Throwable throwable) {
        responseSupport.replyException(request, throwable);
    }

    private void setDefaultResponseHandlerFunction(Consumer<ActorResponse> handler) {
        responseSupport.setDefaultHandlerFunction(handler);

    }

    ActorInbox getInbox() {
        return inbox;
    }

    ActorOutbox getOutbox() {
        return outbox;
    }

    public static Builder builder(String addr) {
        return new Builder(addr);
    }

    void stop() {
        stopped = true;
        outbox.stop();
    }

    boolean outboxCleared() {
        return outbox.cleared();
    }

    boolean inboxCleared() {
        return inbox.cleared();
    }

    // Builder
    public static class Builder {
        private final Actor actor;
        private Builder(String addr) {
            this.actor = new Actor(addr);
        }

        // add all methods start with add or set to the builder
        public Builder addTopicHandlerFunction(String topic, Runnable handler) {
            actor.addTopicHandlerFunction(topic, handler);
            return this;
        }

        public <R> Builder addTopicHandlerFunction(String topic, Supplier<R> handler) {
            actor.addTopicHandlerFunction(topic, handler);
            return this;
        }

        public <T, R> Builder addTopicHandlerFunction(String topic, Function<T, R> handler) {
            actor.addTopicHandlerFunction(topic, handler);
            return this;
        }

        public <T, U, R> Builder addTopicHandlerFunction(String topic, BiFunction<T, U, R> handler) {
            actor.addTopicHandlerFunction(topic, handler);
            return this;
        }

        public <T, U> Builder addTopicHandlerFunction(String topic, BiConsumer<T, U> handler) {
            actor.addTopicHandlerFunction(topic, handler);
            return this;
        }

        public <T> Builder addTopicHandlerFunction(String topic, Consumer<T> handler) {
            actor.addTopicHandlerFunction(topic, handler);
            return this;
        }

        public Builder setDefaultHandlerFunction(Consumer<ActorMsg> handler) {
            actor.setDefaultHandlerFunction(handler);
            return this;
        }

        public Builder setHandlerInstance(Object handlerInstance) {
            actor.setHandlerInstance(handlerInstance);
            return this;
        }

        public Builder addResponseHandlerFunction(String topic, Consumer<ActorResponse> handler) {
            actor.addTopicResponseHandlerFunction(topic, handler);
            return this;
        }

        public Builder setResponseHandlerInstance(Object handlerInstance) {
            actor.setResponseHandlerInstance(handlerInstance);
            return this;
        }

        public Builder setDefaultResponseHandlerFunction(Consumer<ActorResponse> handler) {
            actor.setDefaultResponseHandlerFunction(handler);
            return this;
        }
        public Actor build() {
            return this.actor;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Actor actor = (Actor) o;
        return Objects.equals(addr, actor.addr);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(addr);
    }
}


