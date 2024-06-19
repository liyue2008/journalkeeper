package io.journalkeeper.utils.actor;

import java.util.HashMap;
import java.util.Map;
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
@SuppressWarnings("UnusedReturnValue")
public class Actor {
    // 地址
    private final String addr;
    // 收件箱，所有收到的消息放入收件箱暂存，然后单线程顺序处理。
    private final ActorInbox inbox;
    // 发件箱，所有发出去的消息放入发件箱暂存，然后邮递员会将消息分发给对应地址
    private final ActorOutbox outbox;
    // 对请求/响应模式的封装支持
    private final ActorResponseSupport responseSupport;

    public boolean isPrivatePostman() {
        return privatePostman;
    }

    // 是否独占线程，独占线程有更好的性能
    private final boolean privatePostman;

    private Actor(String addr, int inboxCapacity, int outboxCapacity, Map<String, Integer> outboxQueueMpa, boolean privatePostman) {
        this.addr = addr;
        this.outbox = new ActorOutbox(outboxCapacity, addr, outboxQueueMpa);
        this.inbox = new ActorInbox(inboxCapacity, addr, outbox);
        this.responseSupport = new ActorResponseSupport(inbox, outbox);
        this.privatePostman = privatePostman;
    }

    public int getInboxQueueSize() {
        return inbox.getQueueSize();
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

    public void runDelay(long delay, TimeUnit timeUnit,  Runnable runnable) {
        String topic = runnable.toString();
        inbox.receive(new ActorMsg(0L, addr,addr,"@addTopicHandlerFunction", topic, runnable));
        send("Scheduler", "addDelayTask", new DelayTask(timeUnit, delay, this.addr, topic));
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
        return send(PubSubActor.ADDR, topic, payload);
    }
    public ActorMsg pub(String topic) {
        return send(PubSubActor.ADDR, topic);
    }
    public ActorMsg pub(String topic, Object... payloads) {
        return send(PubSubActor.ADDR, topic, payloads);
    }

    public CompletableFuture<Void> pubThen(String topic) {
        return this.<CompletableFuture<Void>>sendThen(PubSubActor.ADDR, topic).thenCompose(f -> f);
    }
    public CompletableFuture<Void> pubThen(String topic, Object... payloads) {
        return this.<CompletableFuture<Void>>sendThen(PubSubActor.ADDR, topic, payloads).thenCompose(f -> f);
    }

    public ActorMsg send(String addr, String topic) {
        return send(addr, topic, ActorMsg.Response.DEFAULT);
    }
    public ActorMsg send(String addr, String topic, ActorMsg.Response response) {
        return send(addr, topic, response, new Object[]{});
    }
    public ActorMsg send(String addr, String topic, Object... payloads) {
        return send(addr, topic,ActorMsg.Response.DEFAULT, payloads);
    }
    public ActorMsg send(String addr, String topic, ActorMsg.Response response, Object... payloads) {
        return outbox.send(addr, topic, response, payloads);
    }
    public ActorMsg send(String addr, String topic, ActorMsg.Response response, ActorRejectPolicy rejectPolicy, Object... payloads) {
        return outbox.send(addr, topic, response, rejectPolicy, payloads);
    }

    public <T> CompletableFuture<T> sendThen(String addr, String topic) {
        return sendThen(addr, topic, ActorRejectPolicy.EXCEPTION);
    }
    public <T> CompletableFuture<T> sendThen(String addr, String topic, ActorRejectPolicy rejectPolicy) {
        return sendThen(addr, topic, rejectPolicy, new Object[]{});
    }

    public <T> CompletableFuture<T> sendThen(String addr, String topic, Object... payloads) {
        return sendThen(addr, topic, ActorRejectPolicy.EXCEPTION,payloads);
    }

    public <T> CompletableFuture<T> sendThen(String addr, String topic, ActorRejectPolicy rejectPolicy, Object... payloads) {
        return responseSupport.send(addr, topic, rejectPolicy, payloads);
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

    public static Builder builder() {
        return new Builder();
    }

    boolean outboxCleared() {
        return outbox.cleared();
    }

    boolean inboxCleared() {
        return inbox.cleared();
    }

    // Builder
    public static class Builder {
        private String addr;
        private final Map<String, Runnable> topicHandlerRunnableMap = new HashMap<>(); // <topic, handler>
        private final Map<String, Supplier<?>> topicHandlerSupplierMap = new HashMap<>(); // <topic, supplier>
        private final Map<String, Function<?, ?>> topicHandlerFunctionMap = new HashMap<>(); // <topic, handler>
        private final Map<String, BiFunction<?, ?, ?>> topicHandlerBiFunctionMap = new HashMap<>(); // <topic, handler>
        private final Map<String, BiConsumer<?, ?>> topicHandlerBiConsumerMap = new HashMap<>(); // <topic, handler>
        private final Map<String, Consumer<?>> topicHandlerConsumerMap = new HashMap<>(); // <topic, handler>
        private final Map<String, Consumer<ActorResponse>> topicHandlerResponseConsumerMap = new HashMap<>();
        private Consumer<ActorMsg> defaultHandlerFunction = null; // <topic, handler>
        private Object handlerInstance = null;
        private Object responseHandlerInstance = null;
        private int inboxCapacity = -1;
        private int outBoxCapacity = -1;
        private final Map<String, Integer> outboxQueueMap = new HashMap<>();
        private Consumer<ActorResponse> defaultResponseHandlerFunction = null;
        private boolean privatePostman = false;
        private Builder() {}

        public Builder addTopicHandlerFunction(String topic, Runnable handler) {
            this.topicHandlerRunnableMap.put(topic, handler);
            return this;
        }

        public <R> Builder addTopicHandlerFunction(String topic, Supplier<R> handler) {
            this.topicHandlerSupplierMap.put(topic, handler);
            return this;
        }

        public <T, R> Builder addTopicHandlerFunction(String topic, Function<T, R> handler) {
            this.topicHandlerFunctionMap.put(topic, handler);
            return this;
        }

        public <T, U, R> Builder addTopicHandlerFunction(String topic, BiFunction<T, U, R> handler) {
            this.topicHandlerBiFunctionMap.put(topic, handler);
            return this;
        }

        public <T, U> Builder addTopicHandlerFunction(String topic, BiConsumer<T, U> handler) {
            this.topicHandlerBiConsumerMap.put(topic, handler);
            return this;
        }

        public <T> Builder addTopicHandlerFunction(String topic, Consumer<T> handler) {
            this.topicHandlerConsumerMap.put(topic, handler);
            return this;
        }

        public Builder setDefaultHandlerFunction(Consumer<ActorMsg> handler) {
            this.defaultHandlerFunction = handler;
            return this;
        }

        public Builder setHandlerInstance(Object handlerInstance) {
            this.handlerInstance = handlerInstance;
            return this;
        }

        public Builder addResponseHandlerFunction(String topic, Consumer<ActorResponse> handler) {
            this.topicHandlerResponseConsumerMap.put(topic, handler);
            return this;
        }

        public Builder setResponseHandlerInstance(Object handlerInstance) {
            this.responseHandlerInstance = handlerInstance;
            return this;
        }

        public Builder setDefaultResponseHandlerFunction(Consumer<ActorResponse> handler) {
            this.defaultResponseHandlerFunction = handler;
            return this;
        }

        public Builder privatePostman(boolean privatePostman) {
            this.privatePostman = privatePostman;
            return this;
        }
        public Builder inboxCapacity(int inboxCapacity) {
            this.inboxCapacity = inboxCapacity;
            return this;
        }
        public Builder outBoxCapacity(int outBoxCapacity) {
            this.outBoxCapacity = outBoxCapacity;
            return this;
        }

        public Builder addTopicQueue(String topic) {
            this.outboxQueueMap.put(topic, -1);
            return this;
        }
        public Builder addTopicQueue(String topic, int queueCapacity) {
            this.outboxQueueMap.put(topic, queueCapacity);
            return this;
        }

        public Builder addr(String addr) {
            this.addr = addr;
            return this;
        }
        public Actor build() {
            Actor actor = new Actor(addr, inboxCapacity, outBoxCapacity, outboxQueueMap, privatePostman);
            this.topicHandlerRunnableMap.forEach(actor::addTopicHandlerFunction);
            this.topicHandlerSupplierMap.forEach(actor::addTopicHandlerFunction);
            this.topicHandlerFunctionMap.forEach(actor::addTopicHandlerFunction);
            this.topicHandlerBiFunctionMap.forEach(actor::addTopicHandlerFunction);
            this.topicHandlerBiConsumerMap.forEach(actor::addTopicHandlerFunction);
            this.topicHandlerConsumerMap.forEach(actor::addTopicHandlerFunction);
            if (this.defaultHandlerFunction != null) {
                actor.setDefaultHandlerFunction(this.defaultHandlerFunction);
            }
            if (this.handlerInstance != null) {
                actor.setHandlerInstance(this.handlerInstance);
            }
            this.topicHandlerResponseConsumerMap.forEach(actor::addTopicResponseHandlerFunction);
            if (this.responseHandlerInstance != null) {
                actor.setResponseHandlerInstance(this.responseHandlerInstance);
            }
            if (this.defaultResponseHandlerFunction != null) {
                actor.setDefaultResponseHandlerFunction(this.defaultResponseHandlerFunction);
            }

            return actor;
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


