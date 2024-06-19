package io.journalkeeper.core.entry.internal;

import static io.journalkeeper.core.entry.internal.InternalEntryType.TYPE_ON_STATE_CHANGE_EVENT;

public class OnStateChangeEvent extends InternalEntry {

    private final Long lastApplied;

    public OnStateChangeEvent(Long lastApplied) {
        super(TYPE_ON_STATE_CHANGE_EVENT);
        this.lastApplied = lastApplied;
    }

    public OnStateChangeEvent(int version, Long lastApplied) {
        super(TYPE_ON_STATE_CHANGE_EVENT, version);
        this.lastApplied = lastApplied;
    }

    public Long getLastApplied() {
        return lastApplied;
    }
}