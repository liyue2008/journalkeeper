package io.journalkeeper.utils.actor;

import java.util.ArrayList;
import java.util.List;

public class Postman implements Runnable {
    private final PostOffice postOffice;

    private final Object ring = new Object();

    private final List<ActorInbox> inboxList = new ArrayList<>();

    private final List<ActorOutbox> outboxList = new ArrayList<>();

    private boolean isRunning = false;

    Postman(PostOffice postOffice) {
        this.postOffice = postOffice;
    }

    void addInbox(ActorInbox inbox) {
        if (isRunning) {
            throw new IllegalStateException("Thread already started");
        }
        inboxList.add(inbox);
        inbox.setRing(ring);
    }

    void addOutbox(ActorOutbox outbox) {
        if (isRunning) {
            throw new IllegalStateException("Thread already started");
        }
        outboxList.add(outbox);
        outbox.setRing(ring);
    }

    private boolean stopFlag = false;

    public void stop() {
        stopFlag = true;
        synchronized (ring) {
            ring.notify();
        }
    }
    @Override
    public void run() {
        isRunning = true;
        while (!stopFlag) {
            boolean hasMessage = false;

            for (ActorInbox inbox : inboxList) {
                if (inbox.processOneMsg()) {
                    hasMessage = true;
                }
            }
            for (ActorOutbox outbox: outboxList) {
                if (outbox.consumeOneMsg(postOffice::send)) {
                    hasMessage = true;
                }
            }
            if (!hasMessage) {
                synchronized (ring) {
                    try {
                        ring.wait(100);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
        }
    }
}
