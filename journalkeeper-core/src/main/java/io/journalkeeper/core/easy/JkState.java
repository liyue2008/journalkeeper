package io.journalkeeper.core.easy;

import io.journalkeeper.core.serialize.WrappedState;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public abstract class JkState implements WrappedState<JkRequest, JkResponse, JkRequest, JkResponse> {

    private final Map<String, Function<JkRequest, JkResponse >> queryCommandHandlers = new HashMap<>();

    private final Map<String, Function<JkRequest, JkResponse>> executeCommandHandlers = new HashMap<>();

    public <P, R>  void registerQueryCommandHandler(String command, Function<P, R> handler) {
        queryCommandHandlers.put(command, request -> new JkResponse(command, handler.apply(request.getParameter())));
    }

    public <P, R> void registerExecuteCommandHandler(String command, Function<P, R> handler) {
        executeCommandHandlers.put(command, request -> new JkResponse(command, handler.apply(request.getParameter())));
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
    public final JkResponse execute(JkRequest entry) {
        String command = entry.getCommand();
        Function<JkRequest, JkResponse> handler = executeCommandHandlers.get(command);
        try {
            if (handler == null) {
                return new JkResponse(command, new HandlerNotFoundException(command));
            } else {
                return handler.apply(entry);
            }
        } catch (Exception e) {
            return new JkResponse(command, e);
        }
    }
}

