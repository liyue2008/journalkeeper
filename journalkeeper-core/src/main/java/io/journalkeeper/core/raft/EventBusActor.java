package io.journalkeeper.core.raft;

import io.journalkeeper.core.api.StateResult;
import io.journalkeeper.core.entry.internal.InternalEntriesSerializeSupport;
import io.journalkeeper.core.entry.internal.OnStateChangeEvent;
import io.journalkeeper.rpc.client.*;
import io.journalkeeper.utils.actor.*;
import io.journalkeeper.utils.actor.annotation.ActorListener;
import io.journalkeeper.utils.actor.annotation.ActorSubscriber;
import io.journalkeeper.utils.event.Event;
import io.journalkeeper.utils.event.EventBus;
import io.journalkeeper.utils.event.EventType;
import io.journalkeeper.utils.event.EventWatcher;

import java.util.List;

public class EventBusActor {
    private final EventBus eventBus;
    private final Actor actor = Actor.builder().addr("EventBus").setHandlerInstance(this).build();




    public EventBusActor() {

        this.eventBus = new EventBus();
    }


    @ActorListener
    private void watch(EventWatcher eventWatcher) {
        eventBus.watch(eventWatcher);
    }
    @ActorListener
    private void unWatch(EventWatcher eventWatcher) {
        eventBus.unWatch(eventWatcher);
    }

    @ActorListener
    private AddPullWatchResponse addPullWatch() {
        return new AddPullWatchResponse(eventBus.addPullWatch(), eventBus.pullIntervalMs());
    }

    @ActorListener
    private RemovePullWatchResponse removePullWatch(RemovePullWatchRequest request) {
        eventBus.removePullWatch(request.getPullWatchId());
        return new RemovePullWatchResponse();
    }

    @ActorListener
    private PullEventsResponse pullEvents(PullEventsRequest request) {
        if (request.getAckSequence() >= 0) {
            eventBus.ackPullEvents(request.getPullWatchId(), request.getAckSequence());
        }
        return new PullEventsResponse(eventBus.pullEvents(request.getPullWatchId()));
    }

    @ActorSubscriber
    private void onStateChange(List<StateResult> stateResults) {
        for(StateResult stateResult : stateResults) {
            OnStateChangeEvent event = new OnStateChangeEvent(stateResult.getLastApplied());
            byte[] serializedEvent = InternalEntriesSerializeSupport.serialize(event);
            eventBus.fireEvent(new Event(EventType.ON_STATE_CHANGE, serializedEvent));
        }
    }
    @ActorListener
    private void fireEvent(Event event) {
        eventBus.fireEvent(event);
    }
    @ActorListener
    private void fireEvents(List<Event> events) {
        events.forEach(eventBus::fireEvent);
    }
    public Actor getActor() {
        return actor;
    }

    public EventBus getEventBus() {
        return eventBus;
    }
}
