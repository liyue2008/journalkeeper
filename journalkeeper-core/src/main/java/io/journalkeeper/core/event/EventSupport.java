package io.journalkeeper.core.event;

import io.journalkeeper.core.api.JournalEntry;
import io.journalkeeper.core.api.ResponseConfig;
import io.journalkeeper.rpc.client.UpdateClusterStateRequest;
import io.journalkeeper.rpc.client.UpdateClusterStateResponse;
import io.journalkeeper.utils.actor.Actor;
import io.journalkeeper.utils.event.Event;
import io.journalkeeper.utils.event.EventInterceptor;
import io.journalkeeper.utils.event.Fireable;
import io.journalkeeper.utils.spi.ServiceSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class EventSupport implements Fireable {
    private static final Logger logger = LoggerFactory.getLogger(EventSupport.class);
    private final Actor actor;
    public final static int EVENT_PARTITION = 30033;
    private final Collection<EventInterceptor> interceptors;

    public EventSupport(Actor actor) {
        this.actor = actor;
        interceptors = ServiceSupport.loadAll(EventInterceptor.class);

    }


    public void fireEvent(Event event) {
        fireEvents(Collections.singletonList(event));
    }
    public void fireEvents(List<Event> events) {


        if (null == events) {
            return;
        }

        Iterator<Event> eventIterator = events.iterator();
        while (eventIterator.hasNext()) {
            Event event = eventIterator.next();
            for (EventInterceptor interceptor : interceptors) {
                if (!interceptor.onEvent(event, this)) {
                    logger.info("Event canceled by an interceptor, type: {}, data: {}"
                            , event.getEventType(), event.getEventData());
                    eventIterator.remove();
                    break;
                }
            }
        }

        if (events.isEmpty()) {
            return;
        }

        byte [] eventsRaw = encodeEvents(events);
        UpdateClusterStateRequest updateClusterStateRequest = new UpdateClusterStateRequest(eventsRaw, EVENT_PARTITION, events.size(), false, ResponseConfig.REPLICATION);
        actor.<UpdateClusterStateResponse>sendThen("Voter", "updateClusterState", updateClusterStateRequest)
                .whenComplete((r, t) -> {
                    if (null != t) {
                        logger.warn("Fire event exception: ", t);
                    } else if (!r.success()) {
                        logger.warn("Fire event error: {}", r.errorString());
                    }
                });
    }

    private static byte [] encodeEvents(List<Event> events) {
        byte [] eventsRaw = new byte[Integer.BYTES * events.size() * 2 + events.stream().map(Event::getEventData).mapToInt(i -> i.length).sum()];
        ByteBuffer byteBuffer = ByteBuffer.wrap(eventsRaw);
        for(Event event : events) {
            byteBuffer.putInt(event.getEventType());
            byteBuffer.putInt(event.getEventData().length);
            byteBuffer.put(event.getEventData());
        }
        return eventsRaw;
    }


    private static List<Event>  decodeEvents(byte[] bytes, int offset){

        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes, offset, bytes.length - offset);
        List<Event> events = new java.util.ArrayList<>();
        while (byteBuffer.hasRemaining()) {
            int eventType = byteBuffer.getInt();
            int eventDataLength = byteBuffer.getInt();
            byte[] eventData = new byte[eventDataLength];
            byteBuffer.get(eventData);
            events.add(new Event(eventType, eventData));
        }
        return events;
    }

    public static List<Event> journalEntryToEvents(JournalEntry journalEntry) {
        return decodeEvents(journalEntry.getPayload().getBytes(), journalEntry.getOffset());
    }


}
