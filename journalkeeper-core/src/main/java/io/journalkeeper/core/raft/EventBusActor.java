package io.journalkeeper.core.raft;

import io.journalkeeper.core.api.StateResult;
import io.journalkeeper.core.entry.internal.InternalEntriesSerializeSupport;
import io.journalkeeper.core.entry.internal.OnStateChangeEvent;
import io.journalkeeper.rpc.client.*;
import io.journalkeeper.utils.actor.*;
import io.journalkeeper.utils.actor.annotation.ActorListener;
import io.journalkeeper.utils.event.Event;
import io.journalkeeper.utils.event.EventBus;
import io.journalkeeper.utils.event.EventType;

public class EventBusActor {
    private final EventBus eventBus;
    private final Actor actor = Actor.builder().addr("EventBus").setHandlerInstance(this).build();




    public EventBusActor() {

        this.eventBus = new EventBus();
    }

    public void watch(ActorMsg msg) {
        eventBus.watch(msg.getPayload());

    }

    public void unWatch(ActorMsg msg) {
        eventBus.unWatch(msg.getPayload());
    }
    @ActorListener
    private AddPullWatchResponse addPullWatch() {
        // TODO
        return null;

    }

    @ActorListener
    private RemovePullWatchResponse removePullWatch(RemovePullWatchRequest request) {
        // TODO

        return null;
    }

    @ActorListener
    private PullEventsResponse pullEvents(PullEventsRequest request) {
        // TODO

        return null;
    }

    @ActorListener
    private void onStateChange(StateResult stateResult) {
        OnStateChangeEvent event = new OnStateChangeEvent(stateResult.getLastApplied());
        byte [] serializedEvent =  InternalEntriesSerializeSupport.serialize(event);
        eventBus.fireEvent(new Event(EventType.ON_STATE_CHANGE, serializedEvent));
    }
    @ActorListener
    private void fireEvent(Event event) {
        eventBus.fireEvent(event);
    }
    public Actor getActor() {
        return actor;
    }

    public EventBus getEventBus() {
        return eventBus;
    }
}
