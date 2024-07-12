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
package io.journalkeeper.core.entry.internal;

public enum InternalEntryType {
    TYPE_LEADER_ANNOUNCEMENT(0),
    TYPE_CREATE_SNAPSHOT(1),
    TYPE_SCALE_PARTITIONS(2),
    TYPE_UPDATE_VOTERS_S1(3),
    TYPE_UPDATE_VOTERS_S2(4),
    TYPE_UPDATE_OBSERVERS(5),
    TYPE_SET_PREFERRED_LEADER(6),
    TYPE_RECOVER_SNAPSHOT(7),

    TYPE_ON_STATE_CHANGE_EVENT(8),

    TYPE_ON_LEADER_CHANGE_EVENT(9)

    ;

    private final int value;

    InternalEntryType(int value) {
        this.value = value;
    }

    public static InternalEntryType valueOf(final int value) {
        for (InternalEntryType type : InternalEntryType.values()) {
            if (type.value() == value) {
                return type;
            }
        }
        throw new IllegalArgumentException("Unknown entry type: " + value);
    }

    public int value() {
        return value;
    }

}
