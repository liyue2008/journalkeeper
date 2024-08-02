package io.journalkeeper.core.easy;

import io.journalkeeper.core.api.State;
import io.journalkeeper.core.api.StateFactory;

public class JkStateFactory implements StateFactory {
    @Override
    public State createState() {
        JkStateImpl state = new JkStateImpl();
        onStateCreated(state);
        return state;
    }

    protected void onStateCreated(JkState state) {

    }
}
