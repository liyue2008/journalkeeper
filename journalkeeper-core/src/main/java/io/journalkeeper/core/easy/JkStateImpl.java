package io.journalkeeper.core.easy;

import io.journalkeeper.core.api.RaftJournal;
import io.journalkeeper.core.api.State;
import io.journalkeeper.core.api.StateResult;
import io.journalkeeper.core.serialize.JavaSerializeExtensionPoint;
import io.journalkeeper.core.serialize.SerializeExtensionPoint;
import io.journalkeeper.utils.spi.ServiceSupport;

import java.io.Flushable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.function.*;


public  class JkStateImpl implements State, JkState , Flushable {

    private final SerializeExtensionPoint serializer = ServiceSupport.tryLoad(SerializeExtensionPoint.class).orElse(new JavaSerializeExtensionPoint());
    private final Map<String, Function<JkRequest, JkResponse >> queryCommandHandlers = new HashMap<>();

    private final Map<String, BiFunction<JkRequest, StateContext, JkResponse >> executeCommandHandlers = new HashMap<>();
    private final List<BiConsumer<Path, Properties>> recoverHandlers = new ArrayList<>();

    private final List<Flushable> flushableList = new ArrayList<>();
    private Path statePath = null;
    private Properties properties = null;

    @Override
    public <P, R>  void registerQueryCommandHandler(Function<P, R> handler) {
        registerQueryCommandHandler(JkClient.DEFAULT_COMMAND, handler);
    }
    
    @Override
    public <R>  void registerQueryCommandHandler(Supplier<R> handler) {
        registerQueryCommandHandler(JkClient.DEFAULT_COMMAND, handler);
    }
    
    @Override
    public <P, R> void registerExecuteCommandHandler(BiFunction<P, StateContext, R> handler) {
        registerExecuteCommandHandler(JkClient.DEFAULT_COMMAND, handler);
    }
    
    @Override
    public <P, R> void registerExecuteCommandHandler(Function<P, R> handler) {
        registerExecuteCommandHandler(JkClient.DEFAULT_COMMAND, handler);
    }
    
    @Override
    public <P> void registerExecuteCommandHandler(BiConsumer<P, StateContext> handler) {
        registerExecuteCommandHandler(JkClient.DEFAULT_COMMAND, handler);
    }
    
    @Override
    public <P> void registerExecuteCommandHandler(Consumer<P> handler) {
        registerExecuteCommandHandler(JkClient.DEFAULT_COMMAND, handler);
    }
    @Override
    public <P, R>  void registerQueryCommandHandler(String command, Function<P, R> handler) {
        queryCommandHandlers.put(command, request -> new JkResponse(command, handler.apply(request.getParameter())));
    }

    @Override
    public <R>  void registerQueryCommandHandler(String command, Supplier<R> handler) {
        queryCommandHandlers.put(command, request -> new JkResponse(command, handler.get()));
    }

    @Override
    public <P, R> void registerExecuteCommandHandler(String command, BiFunction<P, StateContext, R> handler) {
        executeCommandHandlers.put(command, (request, fireable) -> new JkResponse(command, handler.apply(request.getParameter(), fireable)));
    }

    @Override
    public <P, R> void registerExecuteCommandHandler(String command, Function<P, R> handler) {
        executeCommandHandlers.put(command, (request, fireable) -> new JkResponse(command, handler.apply(request.getParameter())));
    }

    @Override
    public <P> void registerExecuteCommandHandler(String command, BiConsumer<P, StateContext> handler) {
        executeCommandHandlers.put(command, (request, fireable) -> {
            handler.accept(request.getParameter(), fireable);
            return new JkResponse(command,null);
        });
    }

    @Override
    public <P> void registerExecuteCommandHandler(String command, Consumer<P> handler) {
        executeCommandHandlers.put(command, (request, fireable) -> {
            handler.accept(request.getParameter());
            return new JkResponse(command, null);
        });
    }

    @Override
    public void registerRecoverHandler(BiConsumer<Path, Properties> handler) {
        this.recoverHandlers.add(handler);
    }

    @Override
    public void registerFlushable(Flushable flushable) {
        this.flushableList.add(flushable);
    }

    @Override
    public final void recover(Path path, Properties properties) throws IOException {
        this.statePath = path;
        this.properties = properties;
        this.recoverHandlers.forEach(handler -> handler.accept(path, properties));
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
        JkStateContext context = new JkStateContext(statePath, properties);

        BiFunction<JkRequest, StateContext, JkResponse> handler = executeCommandHandlers.get(command);
        try {
            if (handler == null) {
                return new StateResult(
                        serializer.serialize(new JkResponse(command, new HandlerNotFoundException(command)))
                );
            } else {
                return new StateResult(
                        serializer.serialize(handler.apply(serializer.parse(entry),context)),
                        serializer.serialize(context.getEventData()));

            }
        } catch (Exception e) {
            return new StateResult(
                    serializer.serialize(new JkResponse(command, e)));
        }
    }

    @Override
    public void flush() throws IOException {
        for (Flushable flushable : this.flushableList) {
            flushable.flush();
        }
    }

    private static class JkStateContext implements StateContext {
        private Object eventData = null;
        private final Path statePath;
        private final Properties properties;

        public JkStateContext(Path statePath, Properties properties) {
            this.statePath = statePath;
            this.properties = properties;
        }

        @Override
        public void fireEvent(Object event) {
            if (null == eventData) {
                eventData = event;
            } else {
                throw new IllegalStateException("event data is not null, can not fire more than one event");
            }
        }

        @Override
        public Path getStatePath() {
            return statePath;
        }



        @Override
        public Properties getProperties() {
            return properties;
        }

        public Object getEventData() {
            return eventData;
        }
    }
}

