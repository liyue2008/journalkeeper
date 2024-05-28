package io.journalkeeper.utils.state;

import java.util.*;

public class StateMachine< T extends  Enum<T>> {
    private final Map<T, Set<T>> stats;
    private T currentState;

    private StateMachine(T initstate, Map<T, Set<T>> stats) {
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
    public synchronized void convertTo(T newState) {
        T oldState = currentState;
        Set<T> validPreStates = stats.get(newState);

        if (null != validPreStates && !validPreStates.contains(oldState)) {
            throw new IllegalStateException("Current state " + oldState + " can not convert to " + newState);
        }
        currentState = newState;
    }

    public T current() {
        return currentState;
    }
    public static <B extends Enum<B>> Builder<B> builder() {
        return new Builder<>();
    }

    public static class Builder<B extends Enum<B>> {
        private B initState = null;
        private Map<B, Set<B>> stats = new HashMap<>();
        private Builder() {}

        public Builder<B> initState(B initState) {
            this.initState = initState;
            addState(initState);
            return this;
        }

        public Builder<B> initState(B initState, Set<B> preStates) {
            this.initState = initState;
            addState(initState, preStates);
            return this;
        }

        public Builder<B> addState(B state, Set<B> preStates) {
            this.stats.put(state, null == preStates ? null : Collections.unmodifiableSet(preStates));
            return this;
        }

        public Builder<B> addState(B state) {
            addState(state, null);
            return this;
        }

        public StateMachine<B> build() {
            return new StateMachine<>(initState, stats);
        }
    }
}
