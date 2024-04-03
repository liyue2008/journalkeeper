package io.journalkeeper.utils.actor;

import java.util.HashMap;
import java.util.Map;

public class PostOffice{

    private final Map<String, Actor> inboxMap = new HashMap<>();
    public void regInbox(String addr, Actor actor) {
        inboxMap.put(addr, actor);
    }


    public void send(ActorMsg msg) {
        Actor receiver = inboxMap.get(msg.getReceiver());
        if (receiver == null) {
            throw new IllegalArgumentException("Receiver not found!");
        }
        if (!receiver.receive(msg)) {
            throw new IllegalStateException("Receiver queue is full!");
        }
    }
}
