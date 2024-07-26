package io.journalkeeper.core.raft;

import io.journalkeeper.core.api.EntryFuture;
import io.journalkeeper.core.api.JournalEntry;
import io.journalkeeper.core.api.RaftJournal;
import io.journalkeeper.core.event.EventSupport;
import io.journalkeeper.core.state.ApplyReservedEntryInterceptor;
import io.journalkeeper.rpc.client.*;
import io.journalkeeper.utils.actor.*;
import io.journalkeeper.utils.actor.annotation.ActorListener;
import io.journalkeeper.utils.actor.annotation.ActorSubscriber;
import io.journalkeeper.utils.event.Event;
import io.journalkeeper.utils.event.EventBus;
import io.journalkeeper.utils.event.PullEvent;

import java.util.ArrayList;
import java.util.List;

import static io.journalkeeper.core.event.EventSupport.EVENT_PARTITION;


public class EventBusActor implements ApplyReservedEntryInterceptor{
    private final EventBus eventBus;
    private final Actor actor;
    private long appliedEventIndex = 0L;
    private final RaftJournal raftJournal;
    private static final int PULL_BATCH_SIZE = 1024;


    public EventBusActor(RaftJournal raftJournal) {
        this.raftJournal = raftJournal;

        this.eventBus = new EventBus();
        actor = Actor.builder().addr("EventBus").setHandlerInstance(this).build();
    }
    @ActorSubscriber
    private void onStart(ServerContext context) {
        actor.send("State", "addInterceptor", this);
    }

    @ActorListener
    private void recoverEventIndex(long index) {
        this.appliedEventIndex = index;
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
        long startIndex = request.getIndex() < 0 ? appliedEventIndex : request.getIndex();
        long index = startIndex;
        List<PullEvent> events = new ArrayList<>();
        while (index < Math.min(appliedEventIndex, startIndex + PULL_BATCH_SIZE)) {

            JournalEntry journalEntry = raftJournal.readByPartition(EVENT_PARTITION, index);
            List<Event> eventBatch =EventSupport.journalEntryToEvents(journalEntry);

            for (Event event : eventBatch) {
                events.add(new PullEvent(event.getEventType(),index++, event.getEventData()));
            }
        }
        return new PullEventsResponse(appliedEventIndex, events);
    }

    public Actor getActor() {
        return actor;
    }

    public EventBus getEventBus() {
        return eventBus;
    }

    @Override
    public void applyReservedEntry(JournalEntry entryHeader, EntryFuture entryFuture) {
        if (entryHeader.getPartition() == EVENT_PARTITION) {
            appliedEventIndex += entryHeader.getBatchSize();
        }
    }
}
