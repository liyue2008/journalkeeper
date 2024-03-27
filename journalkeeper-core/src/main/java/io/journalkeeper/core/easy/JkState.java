package io.journalkeeper.core.easy;

import io.journalkeeper.core.api.RaftJournal;
import io.journalkeeper.core.api.State;
import io.journalkeeper.core.api.StateResult;
import io.journalkeeper.core.serialize.JavaSerializeExtensionPoint;
import io.journalkeeper.core.serialize.SerializeExtensionPoint;
import io.journalkeeper.utils.spi.ServiceSupport;

import java.util.HashMap;
import java.util.Map;
import java.util.function.*;


public abstract class JkState implements State {

    private final SerializeExtensionPoint serializer = ServiceSupport.tryLoad(SerializeExtensionPoint.class).orElse(new JavaSerializeExtensionPoint());
    private final Map<String, Function<JkRequest, JkResponse >> queryCommandHandlers = new HashMap<>();

    private final Map<String, BiFunction<JkRequest,JkFireable, JkResponse >> executeCommandHandlers = new HashMap<>();

    public <P, R>  void registerQueryCommandHandler(Function<P, R> handler) {
        registerQueryCommandHandler(JkClient.DEFAULT_COMMAND, handler);
    }
    
    public <R>  void registerQueryCommandHandler(Supplier<R> handler) {
        registerQueryCommandHandler(JkClient.DEFAULT_COMMAND, handler);
    }
    
    public <P, R> void registerExecuteCommandHandler(BiFunction<P,JkFireable, R>  handler) {
        registerExecuteCommandHandler(JkClient.DEFAULT_COMMAND, handler);
    }
    
    public <P, R> void registerExecuteCommandHandler(Function<P, R>  handler) {
        registerExecuteCommandHandler(JkClient.DEFAULT_COMMAND, handler);
    }
    
    public <P> void registerExecuteCommandHandler(BiConsumer<P,JkFireable> handler) {
        registerExecuteCommandHandler(JkClient.DEFAULT_COMMAND, handler);
    }
    
    public <P> void registerExecuteCommandHandler(Consumer<P> handler) {
        registerExecuteCommandHandler(JkClient.DEFAULT_COMMAND, handler);
    }
    public <P, R>  void registerQueryCommandHandler(String command, Function<P, R> handler) {
        queryCommandHandlers.put(command, request -> new JkResponse(command, handler.apply(request.getParameter())));
    }

    public <R>  void registerQueryCommandHandler(String command, Supplier<R> handler) {
        queryCommandHandlers.put(command, request -> new JkResponse(command, handler.get()));
    }

    public <P, R> void registerExecuteCommandHandler(String command, BiFunction<P,JkFireable, R>  handler) {
        executeCommandHandlers.put(command, (request, fireable) -> new JkResponse(command, handler.apply(request.getParameter(), fireable)));
    }

    public <P, R> void registerExecuteCommandHandler(String command, Function<P, R>  handler) {
        executeCommandHandlers.put(command, (request, fireable) -> new JkResponse(command, handler.apply(request.getParameter())));
    }

    public <P> void registerExecuteCommandHandler(String command, BiConsumer<P,JkFireable> handler) {
        executeCommandHandlers.put(command, (request, fireable) -> {
            handler.accept(request.getParameter(), fireable);
            return new JkResponse(command,null);
        });
    }

    public <P> void registerExecuteCommandHandler(String command, Consumer<P> handler) {
        executeCommandHandlers.put(command, (request, fireable) -> {
            handler.accept(request.getParameter());
            return new JkResponse(command, null);
        });
    }
    @Override
    public final byte[] query(byte[] query, RaftJournal journal) {
        JkRequest queryRequest = serializer.parse(query);
        String command = queryRequest.getCommand();
        Function<JkRequest, JkResponse> handler = queryCommandHandlers.get(command);
        try {
            if (handler == null) {
                return serializer.serialize(new JkResponse(command, new HandlerNotFoundException(command)));
            } else {
                return serializer.serialize(handler.apply(queryRequest));
            }
        } catch (Exception e) {
            return serializer.serialize(new JkResponse(command, e));
        }
    }

    @Override
    public StateResult execute(byte[] entry, int partition, long index, int batchSize, RaftJournal journal) {
        JkRequest request = serializer.parse(entry);
        String command = request.getCommand();
        EventHolder fireable = new EventHolder();

        BiFunction<JkRequest, JkFireable, JkResponse> handler = executeCommandHandlers.get(command);
        try {
            if (handler == null) {
                return new StateResult(
                        serializer.serialize(new JkResponse(command, new HandlerNotFoundException(command)))
                );
            } else {
                return new StateResult(
                        serializer.serialize(handler.apply(serializer.parse(entry),fireable)),
                        serializer.serialize(fireable.getEventData()));

            }
        } catch (Exception e) {
            return new StateResult(
                    serializer.serialize(new JkResponse(command, e)));
        }
    }

    private static class EventHolder implements JkFireable {
        private Object eventData = null;


        @Override
        public void fireEvent(Object event) {
            eventData = event;
        }

        public Object getEventData() {
            return eventData;
        }
    }
}

