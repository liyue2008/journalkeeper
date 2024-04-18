package io.journalkeeper.utils.actor;

import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

class ActorOutbox {

    private final AtomicLong msgId = new AtomicLong(0);

    private final Queue<ActorMsg> msgQueue;

    private final String myAddr;

    final static int DEFAULT_CAPACITY = 1000;

    ActorOutbox(String myAddr) {
        this(DEFAULT_CAPACITY, myAddr);
    }
    ActorOutbox(int capacity, String myAddr) {
        this.msgQueue = new ArrayBlockingQueue<>(capacity);
        this.myAddr = myAddr;
    }

    ActorMsg send(String addr, String topic, Object payload){
        ActorMsg actorMsg = new ActorMsg(msgId.getAndIncrement(), myAddr, addr, topic, payload);
        msgQueue.add(actorMsg);
        ring();
        return actorMsg;
    }


    boolean consumeOneMsg(Consumer<ActorMsg> consumer) {
        ActorMsg msg = msgQueue.peek();
        if (msg != null) {
            consumer.accept(msg);
            msgQueue.poll();
            return true;
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

}
