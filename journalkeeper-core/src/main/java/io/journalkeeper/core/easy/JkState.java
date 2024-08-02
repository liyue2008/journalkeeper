package io.journalkeeper.core.easy;

import java.io.Flushable;
import java.nio.file.Path;
import java.util.Properties;
import java.util.function.*;

public interface JkState {
    <P, R> void registerQueryCommandHandler(Function<P, R> handler);

    <R> void registerQueryCommandHandler(Supplier<R> handler);

    <P, R> void registerExecuteCommandHandler(BiFunction<P, StateContext, R> handler);

    <P, R> void registerExecuteCommandHandler(Function<P, R> handler);

    <P> void registerExecuteCommandHandler(BiConsumer<P, StateContext> handler);

    <P> void registerExecuteCommandHandler(Consumer<P> handler);

    <P, R> void registerQueryCommandHandler(String command, Function<P, R> handler);

    <R> void registerQueryCommandHandler(String command, Supplier<R> handler);

    <P, R> void registerExecuteCommandHandler(String command, BiFunction<P, StateContext, R> handler);

    <P, R> void registerExecuteCommandHandler(String command, Function<P, R> handler);

    <P> void registerExecuteCommandHandler(String command, BiConsumer<P, StateContext> handler);

    <P> void registerExecuteCommandHandler(String command, Consumer<P> handler);

    void registerRecoverHandler(BiConsumer<Path, Properties> handler);

    void registerFlushable(Flushable flushable);
}
