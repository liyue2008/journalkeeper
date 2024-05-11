package io.journalkeeper.utils.actor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Postman implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger( Postman.class );
    private final PostOffice postOffice;

    private final Object ring = new Object();

    private final List<ActorInbox> inboxList;

    private final List<ActorOutbox> outboxList;

    private Postman(PostOffice postOffice, List<ActorInbox> inboxList, List<ActorOutbox> outboxList) {
        this.postOffice = postOffice;
        this.inboxList = Collections.unmodifiableList(inboxList);
        this.outboxList = Collections.unmodifiableList(outboxList);
        inboxList.forEach(inbox -> inbox.setRing(ring));
        outboxList.forEach(outbox -> outbox.setRing(ring));
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
                        ring.wait(10);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
        }
        logger.info("{} stopped.", Thread.currentThread().getName());
    }



    static Builder builder() {
        return new Builder();
    }
    static class Builder {

        private final List<ActorInbox> inboxList = new ArrayList<>();

        private final List<ActorOutbox> outboxList = new ArrayList<>();
        private PostOffice postOffice;

        Builder postOffice(PostOffice postOffice) {
            this.postOffice = postOffice;
            return this;
        }

        void addInbox(ActorInbox inbox) {
            inboxList.add(inbox);
        }

        void addOutbox(ActorOutbox outbox) {
            outboxList.add(outbox);
        }
        Postman build() {
            return new Postman(postOffice, inboxList, outboxList);
        }
    }
}
