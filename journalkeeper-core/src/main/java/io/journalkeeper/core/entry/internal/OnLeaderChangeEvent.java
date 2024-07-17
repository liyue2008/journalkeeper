package io.journalkeeper.core.entry.internal;

import java.net.URI;

import static io.journalkeeper.core.entry.internal.InternalEntryType.TYPE_ON_LEADER_CHANGE_EVENT;

public class OnLeaderChangeEvent extends InternalEntry {

    private final URI leader;
    private final int term;


    public OnLeaderChangeEvent(URI leader, int term) {
        super(TYPE_ON_LEADER_CHANGE_EVENT);
        this.leader = leader;
        this.term = term;
    }

    public OnLeaderChangeEvent(URI leader, int term, int version) {
        super(TYPE_ON_LEADER_CHANGE_EVENT, version);
        this.leader = leader;
        this.term = term;
    }

    public URI getLeader() {
        return leader;
    }

    public int getTerm() {
        return term;
    }

    @Override
    public String toString() {
        return "OnLeaderChangeEvent{" +
                "leader=" + leader +
                ", term=" + term +
                '}';
    }
}
