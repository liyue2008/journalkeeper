package io.journalkeeper.utils.state;

import java.util.*;

public class StateMachine {
    private final Map<String, Set<String>> stats;
    private String currentState;

    private StateMachine(String initstate, Map<String, Set<String>> stats) {
        if (null == initstate) {
            throw new IllegalArgumentException("initState can not be null.");
        }

        if (null == stats || stats.isEmpty()) {
            throw new IllegalArgumentException("states can not be null or empty.");
        }

        if (!stats.containsKey(initstate)) {
            throw new IllegalArgumentException("initState " + initstate + " not exists in stats.");
        }
        this.currentState = initstate;
        this.stats = stats;
    }

    /**
     * Converts the current state to the specified new state.
     *
     * @param  newState  the new state to convert to
     * @throws IllegalStateException if the current state cannot be converted to the new state
     */
    public synchronized void convertTo(String newState) {
        String oldState = currentState;
        Set<String> validPreStates = stats.get(newState);

        if (null != validPreStates && !validPreStates.contains(oldState)) {
            throw new IllegalStateException("Current state " + oldState + " can not convert to " + newState);
        }
        currentState = newState;
    }

    public String current() {
        return currentState;
    }
    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String initState = null;
        private Map<String, Set<String>> stats = new HashMap<>();
        private Builder() {}

        public Builder initState(String initState) {
            this.initState = initState;
            addState(initState);
            return this;
        }

        public Builder initState(String initState, Set<String> preStates) {
            this.initState = initState;
            addState(initState, preStates);
            return this;
        }

        public Builder addState(String state, Set<String> preStates) {
            this.stats.put(state, null == preStates ? null : Collections.unmodifiableSet(preStates));
            return this;
        }

        public Builder addState(String state) {
            addState(state, null);
            return this;
        }

        public StateMachine build() {
            return new StateMachine(initState, stats);
        }
    }
}
