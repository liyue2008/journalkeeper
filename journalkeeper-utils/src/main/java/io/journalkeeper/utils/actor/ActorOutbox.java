package io.journalkeeper.utils.actor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

@SuppressWarnings("UnusedReturnValue")
class ActorOutbox {
    private static final Logger logger = LoggerFactory.getLogger( ActorOutbox.class );

    private final AtomicLong msgId = new AtomicLong(0);

    private final Map<String, BlockingQueue<ActorMsg>> topicQueueMap;

    private final String myAddr;

    private final ThreadLocal<ActorThreadContext> contextThreadLocal = new ThreadLocal<>();

    private final boolean enableMetric;

    private final int capacity;

    ActorOutbox(int capacity, String myAddr, Map<String, Integer> topicQueueMap, boolean enableMetric) {
        this.capacity = capacity;
        this.myAddr = myAddr;
        this.topicQueueMap = new ConcurrentHashMap<>();
        if (null != topicQueueMap) {
            for (Map.Entry<String, Integer> entry : topicQueueMap.entrySet()) {
                this.topicQueueMap.put(entry.getKey(), new LinkedBlockingQueue<>(entry.getValue() < 0 ? capacity : entry.getValue()));
            }
        }
        this.enableMetric = enableMetric;
    }
    ActorMsg send(String addr, String topic, ActorMsg.Response response, Object... payloads){
        return send(createMsg(addr, topic,response,ActorRejectPolicy.EXCEPTION, payloads));
    }

    ActorMsg send(String addr, String topic, ActorMsg.Response response, ActorRejectPolicy rejectPolicy, Object... payloads){
        return send(createMsg(addr, topic,response , rejectPolicy, payloads)) ;
    }

    ActorMsg send(ActorMsg actorMsg) {
        try {
            ActorRejectPolicy rejectPolicy = actorMsg.getContext().getRejectPolicy();
            BlockingQueue<ActorMsg> queue = topicQueueMap.computeIfAbsent(actorMsg.getQueueName(), queueName -> new LinkedBlockingQueue<>(capacity));
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
            if (actorMsg.getContext().getMetric() != null) {
                actorMsg.getContext().getMetric().onOutboxIn(actorMsg.getQueueName(), queue.size());
            }
            ring();

            return ret;
        } catch (IllegalStateException e) {
            throw new ActorQueueFullException(e);
        }catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    ActorMsg createMsg(String addr, String topic, ActorMsg.Response response, ActorRejectPolicy rejectPolicy, Object... payloads){
        return new ActorMsg(msgId.getAndIncrement(), myAddr, addr, topic, new ActorMsgCtx(response, ActorMsg.Type.REQUEST, rejectPolicy, this.enableMetric), payloads);
    }

    ActorMsg createResponse(ActorMsg request, Object result, Throwable throwable) {
        return new ActorMsg(msgId.getAndIncrement(), myAddr, request, result, throwable);
    }



    boolean consumeOneMsg(Consumer<ActorMsg> consumer) {
        boolean hasMessage = false;
        for (BlockingQueue<ActorMsg> queue : topicQueueMap.values()) {
            ActorMsg msg = queue.peek();
            if (msg != null) {
                try {
                    consumer.accept(msg);
                    queue.poll();
                    hasMessage = true;
                } catch (IllegalStateException t) {
                    logger.debug("Target inbox queue full，retry later, msg: {}", msg, t);
                }
            }
        }
        return hasMessage;
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
        return this.topicQueueMap.values().stream().allMatch(BlockingQueue::isEmpty);
    }
}
