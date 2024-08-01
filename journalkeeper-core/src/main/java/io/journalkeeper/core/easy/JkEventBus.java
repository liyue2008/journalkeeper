package io.journalkeeper.core.easy;

import io.journalkeeper.core.api.RaftClient;
import io.journalkeeper.core.entry.internal.InternalEntriesSerializeSupport;
import io.journalkeeper.core.entry.internal.OnLeaderChangeEvent;
import io.journalkeeper.core.entry.internal.OnStateChangeEvent;
import io.journalkeeper.core.serialize.JavaSerializeExtensionPoint;
import io.journalkeeper.core.serialize.SerializeExtensionPoint;
import io.journalkeeper.utils.event.EventType;
import io.journalkeeper.utils.event.EventWatcher;
import io.journalkeeper.utils.spi.ServiceSupport;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

public class JkEventBus {
    private final RaftClient raftClient;
    private final SerializeExtensionPoint serializer = ServiceSupport.tryLoad(SerializeExtensionPoint.class).orElse(new JavaSerializeExtensionPoint());

    private final Set<Consumer<OnLeaderChangeEvent>> leaderChangeListeners = new HashSet<>();
    private final Set<Consumer<OnStateChangeEvent>> stateChangeListeners = new HashSet<>();
    private final Set<Consumer<?>> userEventListeners = new HashSet<>();
    private final AtomicInteger listenerCounter = new AtomicInteger(0);
    private final EventWatcher eventWatcher ;

    public JkEventBus(RaftClient raftClient) {
        this.raftClient = raftClient;
        this.eventWatcher = event -> {
            switch (event.getEventType()) {
                case EventType.ON_LEADER_CHANGE:
                    synchronized (leaderChangeListeners) {
                        if (!leaderChangeListeners.isEmpty()) {
                            OnLeaderChangeEvent leaderChangeEvent = InternalEntriesSerializeSupport.parse(event.getEventData());
                            this.leaderChangeListeners.forEach(listener -> listener.accept(leaderChangeEvent));
                        }
                    }
                    break;
                case EventType.ON_STATE_CHANGE:
                    synchronized (stateChangeListeners) {
                        if (!stateChangeListeners.isEmpty()) {
                            OnStateChangeEvent onStateChangeEvent = InternalEntriesSerializeSupport.parse(event.getEventData());
                            this.stateChangeListeners.forEach(listener -> listener.accept(onStateChangeEvent));
                        }
                    }
                    break;

                case EventType.ON_USER_EVENT:
                    synchronized (userEventListeners) {
                        if (!userEventListeners.isEmpty()) {
                            this.userEventListeners.forEach(listener -> listener.accept(serializer.parse(event.getEventData())));
                        }
                    }
                    break;


                default:
                    // nothing to do
            }
        };
    }

    private void reWatch() {
        this.raftClient.unWatch(this.eventWatcher);
        this.raftClient.watch(this.eventWatcher);
    }

    public void addLeaderChangeListener(Consumer<OnLeaderChangeEvent> listener) {
        synchronized (leaderChangeListeners) {
            if (leaderChangeListeners.add(listener) && listenerCounter.getAndIncrement() == 0) {
                this.raftClient.watch(eventWatcher);
            }
        }
    }

    public void removeLeaderChangeListener(Consumer<OnLeaderChangeEvent> listener) {
        synchronized (leaderChangeListeners) {
            if (leaderChangeListeners.remove(listener) && listenerCounter.decrementAndGet() == 0) {
                this.raftClient.unWatch(eventWatcher);
            }
        }
    }

    public void addStateChangeListener(Consumer<OnStateChangeEvent> listener) {
        synchronized (stateChangeListeners) {
            if (stateChangeListeners.add(listener) && listenerCounter.getAndIncrement() == 0) {
                this.raftClient.watch(eventWatcher);
            }
        }
    }

    public void removeStateChangeListener(Consumer<OnStateChangeEvent> listener) {
        synchronized (stateChangeListeners) {
            if (stateChangeListeners.remove(listener) && listenerCounter.decrementAndGet() == 0) {
                this.raftClient.unWatch(eventWatcher);
            }
        }
    }

    public <T> void  addUserEventListener(Consumer<T> listener) {
        synchronized (userEventListeners) {
            if (userEventListeners.add(listener) && listenerCounter.getAndIncrement() == 0) {
                this.raftClient.watch(eventWatcher);
            }
        }
    }

    public <T> void removeUserEventListener(Consumer<T> listener) {
        synchronized (userEventListeners) {
            if (userEventListeners.remove(listener) && listenerCounter.decrementAndGet() == 0) {
                this.raftClient.unWatch(eventWatcher);
            }
        }
    }


    public void close() {
        if (listenerCounter.get() > 0) {
            this.raftClient.unWatch(eventWatcher);
            this.stateChangeListeners.clear();
            this.leaderChangeListeners.clear();
        }

    }

    private static class Event {
        private final Object eventData;

        public Event(Object eventData) {
            this.eventData = eventData;
        }

        @SuppressWarnings("unchecked")
        public <T> T get() {
            return (T) eventData;

        }
    }



    private static class EventListener implements Consumer<Event> {
        private final Consumer<?> listener;

        private EventListener(Consumer<?> listener) {
            this.listener = listener;
        }

        @Override
        public void accept(Event event) {
            listener.accept(event.get());
        }

        @Override
        public boolean equals(Object object) {
            if (this == object) return true;
            if (object == null || getClass() != object.getClass()) return false;
            EventListener that = (EventListener) object;
            return Objects.equals(listener, that.listener);
        }

        @Override
        public int hashCode() {
            return Objects.hash(listener);
        }
    }


}
