package io.journalkeeper.core.easy;

import io.journalkeeper.core.serialize.WrappedState;
import io.journalkeeper.core.serialize.WrappedStateResult;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

public abstract class JkState implements WrappedState<JkRequest, JkResponse, JkRequest, JkResponse> {

    private final Map<String, Function<JkRequest, JkResponse >> queryCommandHandlers = new HashMap<>();

    private final Map<String, BiFunction<JkRequest,JkFireable, JkResponse >> executeCommandHandlers = new HashMap<>();

    public <P, R>  void registerQueryCommandHandler(String command, Function<P, R> handler) {
        queryCommandHandlers.put(command, request -> new JkResponse(command, handler.apply(request.getParameter())));
    }

    public <P, R> void registerExecuteCommandHandler(String command, BiFunction<P,JkFireable, R> handler) {
        executeCommandHandlers.put(command, (request, fireable) -> new JkResponse(command, handler.apply(request.getParameter(), fireable)));
    }
    @Override
    public final JkResponse query(JkRequest query) {
        String command = query.getCommand();
        Function<JkRequest, JkResponse> handler = queryCommandHandlers.get(command);
        try {
            if (handler == null) {
                return new JkResponse(command, new HandlerNotFoundException(command));
            } else {
                return handler.apply(query);
            }
        } catch (Exception e) {
            return new JkResponse(command, e);
        }
    }

    @Override
    public WrappedStateResult<JkResponse> executeAndNotify(JkRequest entry) {
        String command = entry.getCommand();
        EventHolder fireable = new EventHolder();
        BiFunction<JkRequest, JkFireable, JkResponse> handler = executeCommandHandlers.get(command);
        try {
            if (handler == null) {
                return new WrappedStateResult<>(new JkResponse(command, new HandlerNotFoundException(command)), null);
            } else {
                return new WrappedStateResult<>(
                        handler.apply(entry, fireable),
                        fireable.getEventData());

            }
        } catch (Exception e) {
            return  new WrappedStateResult<>(new JkResponse(command, e), null);
        }
    }

    @Override
    public final JkResponse execute(JkRequest entry) {
        throw new UnsupportedOperationException();
    }

    private static class EventHolder implements JkFireable {
        private final Map<String, String> eventData = new HashMap<>();


        @Override
        public void fireEvent(Map<String, String> event) {
            eventData.putAll(event);
        }

        public Map<String, String> getEventData() {
            return eventData;
        }
    }
}

