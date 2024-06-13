package io.journalkeeper.utils.actor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

class ActorOutbox {
    private static final Logger logger = LoggerFactory.getLogger( ActorOutbox.class );

    private final AtomicLong msgId = new AtomicLong(0);

    private final BlockingQueue<ActorMsg> msgQueue;

    private final Map<String, BlockingQueue<ActorMsg>> topicQueueMap;

    private final String myAddr;

    final static int DEFAULT_CAPACITY = 10000;

    private final ThreadLocal<ActorThreadContext> contextThreadLocal = new ThreadLocal<>();

    ActorOutbox(int capacity, String myAddr, Map<String, Integer> outboxQueueMpa) {
        this.msgQueue = new LinkedBlockingQueue<>(capacity < 0 ? DEFAULT_CAPACITY : capacity);
        this.myAddr = myAddr;
        this.topicQueueMap = new HashMap<>();
        if (null != outboxQueueMpa) {
            for (Map.Entry<String, Integer> entry : outboxQueueMpa.entrySet()) {
                topicQueueMap.put(entry.getKey(), new LinkedBlockingQueue<>(entry.getValue() < 0 ? DEFAULT_CAPACITY : entry.getValue()));
            }
        }
    }
    ActorMsg send(String addr, String topic, Object... payloads) {
        return send(addr, topic, ActorMsg.Response.DEFAULT, payloads);
    }
    ActorMsg send(String addr, String topic, ActorMsg.Response response, Object... payloads){
        return send(createMsg(addr, topic,response, payloads), ActorRejectPolicy.EXCEPTION);
    }

    ActorMsg send(String addr, String topic, ActorMsg.Response response, ActorRejectPolicy rejectPolicy, Object... payloads){
        return send(createMsg(addr, topic,response, payloads), rejectPolicy);
    }

    ActorMsg send(ActorMsg actorMsg) {
        return send(actorMsg, ActorRejectPolicy.EXCEPTION);
    }
    ActorMsg send(ActorMsg actorMsg, ActorRejectPolicy rejectPolicy) {
        try {
            BlockingQueue<ActorMsg> queue = topicQueueMap.getOrDefault(actorMsg.getTopic(), msgQueue);
            ActorMsg ret = actorMsg;
            switch (rejectPolicy) {
                case EXCEPTION:
                    queue.add(actorMsg);
                    break;
                case DROP:
                    ret = queue.offer(actorMsg) ? actorMsg : null;
                    break;
                case BLOCK:

                    ActorThreadContext context = contextThreadLocal.get();
                    if (null != context && context.isPostmanThread()) {
                        throw new IllegalAccessError("can not use BLOCK in postman thread.");
                    }
                    queue.put(actorMsg);
                    break;
                default:
                    throw new IllegalArgumentException("unknown rejectPolicy: " + rejectPolicy);
            }
            ring();
            return ret;
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    ActorMsg createMsg(String addr, String topic, ActorMsg.Response response, Object... payloads){
        return new ActorMsg(msgId.getAndIncrement(), myAddr, addr, topic,response, payloads);
    }




    boolean consumeOneMsg(Consumer<ActorMsg> consumer) {
        ActorMsg msg = msgQueue.peek();
        if (msg != null) {
            try {
                consumer.accept(msg);
                msgQueue.poll();
                return true;
            } catch (IllegalStateException t) {
                logger.debug("Target inbox queue full，retry later, msg: {}", msg, t);
            }
        }
        return false;
    }

    private Object ring;
    void setRing(Object ring) {
        this.ring = ring;

    }

    private void ring() {
        if(ring != null) {
            //noinspection SynchronizeOnNonFinalField
            synchronized (ring) {
                ring.notify();
            }
        }
    }

    boolean cleared() {
        return this.msgQueue.isEmpty();
    }
}
