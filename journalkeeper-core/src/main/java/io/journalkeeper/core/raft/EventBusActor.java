package io.journalkeeper.core.raft;

import io.journalkeeper.utils.actor.ActorBase;
import io.journalkeeper.utils.actor.ActorMsg;
import io.journalkeeper.utils.actor.PostOffice;
import io.journalkeeper.utils.event.EventBus;

public class EventBusActor  extends ActorBase {
    private final EventBus eventBus;



    public EventBusActor(PostOffice postOffice, EventBus eventBus) {
        super(DEFAULT_CAPACITY, postOffice);
        this.eventBus = eventBus;
    }

    public void watch(ActorMsg msg) {
        eventBus.watch(msg.getPayload());

    }

    public void unWatch(ActorMsg msg) {
        eventBus.unWatch(msg.getPayload());
    }

    @Override
    public String addr() {
        return "EventBus";
    }
}
