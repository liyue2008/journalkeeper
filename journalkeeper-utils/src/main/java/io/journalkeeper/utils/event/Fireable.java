package io.journalkeeper.utils.event;

import java.util.List;

public interface Fireable {
    void fireEvent(Event event);
    default void fireEvents(List<Event> events) {
        events.forEach(this::fireEvent);
    }
}
