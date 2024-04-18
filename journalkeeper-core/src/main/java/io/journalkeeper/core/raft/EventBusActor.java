package io.journalkeeper.core.raft;

import io.journalkeeper.core.api.StateResult;
import io.journalkeeper.core.entry.internal.InternalEntriesSerializeSupport;
import io.journalkeeper.core.entry.internal.OnStateChangeEvent;
import io.journalkeeper.rpc.client.*;
import io.journalkeeper.utils.actor.*;
import io.journalkeeper.utils.event.Event;
import io.journalkeeper.utils.event.EventBus;
import io.journalkeeper.utils.event.EventType;

public class EventBusActor {
    private final EventBus eventBus;
    private final Actor actor = new Actor("EventBus");




    public EventBusActor(EventBus eventBus) {

        this.eventBus = eventBus;
        actor.setHandlerInstance(this);
    }

    public void watch(ActorMsg msg) {
        eventBus.watch(msg.getPayload());

    }

    public void unWatch(ActorMsg msg) {
        eventBus.unWatch(msg.getPayload());
    }
    @ActorListener(payload = true, response = true)
    private AddPullWatchResponse addPullWatch() {
        // TODO
        return null;

    }

    @ActorListener(payload = true, response = true)
    private RemovePullWatchResponse removePullWatch(RemovePullWatchRequest request) {
        // TODO

        return null;
    }

    @ActorListener(payload = true, response = true)
    private PullEventsResponse pullEvents(PullEventsRequest request) {
        // TODO

        return null;
    }

    @ActorListener(payload = true, consumer = true)
    private void onStateChange(StateResult stateResult) {
        OnStateChangeEvent event = new OnStateChangeEvent(stateResult.getLastApplied());
        byte [] serializedEvent =  InternalEntriesSerializeSupport.serialize(event);
        eventBus.fireEvent(new Event(EventType.ON_STATE_CHANGE, serializedEvent));
    }
    public Actor getActor() {
        return actor;
    }
}
