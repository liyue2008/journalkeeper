/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.journalkeeper.sql.client;

import io.journalkeeper.base.Serializer;
import io.journalkeeper.utils.event.Event;
import io.journalkeeper.utils.event.EventType;
import io.journalkeeper.utils.event.EventWatcher;

import java.util.Objects;

/**
 * EventWatcherAdapter
 * author: gaohaoxiang
 * date: 2019/6/11
 */
// TODO 适配
public class EventWatcherAdapter implements EventWatcher {

    private byte[] key;
    private SQLEventListener listener;
    private final Serializer<SQLEvent> eventSerializer;


    public EventWatcherAdapter(SQLEventListener listener, Serializer<SQLEvent> eventSerializer) {
        this.listener = listener;
        this.eventSerializer = eventSerializer;
    }


    @Override
    public void onEvent(Event event) {
        if (event.getEventType() != EventType.ON_STATE_CHANGE) {
            return;
        }

        SQLEvent sqlEvent = eventSerializer.parse(event.getEventData());
        listener.onEvent(sqlEvent);

    }

    @Override
    public int hashCode() {
        return listener.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof EventWatcherAdapter)) {
            return false;
        }

        return ((EventWatcherAdapter) obj).getListener().equals(listener) &&
                (key == null && ((EventWatcherAdapter) obj).getKey() == null || Objects.deepEquals(((EventWatcherAdapter) obj).getKey(), key));
    }

    public byte[] getKey() {
        return key;
    }

    public SQLEventListener getListener() {
        return listener;
    }
}