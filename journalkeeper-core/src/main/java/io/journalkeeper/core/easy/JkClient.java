package io.journalkeeper.core.easy;

import io.journalkeeper.core.api.*;
import io.journalkeeper.core.api.transaction.TransactionContext;
import io.journalkeeper.core.api.transaction.TransactionId;
import io.journalkeeper.core.entry.internal.InternalEntriesSerializeSupport;
import io.journalkeeper.core.entry.internal.OnLeaderChangeEvent;
import io.journalkeeper.core.serialize.JavaSerializeExtensionPoint;
import io.journalkeeper.core.serialize.SerializeExtensionPoint;
import io.journalkeeper.utils.event.EventType;
import io.journalkeeper.utils.event.EventWatcher;
import io.journalkeeper.utils.spi.ServiceSupport;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

// TODO: 支持事务
public class JkClient implements ClusterReadyAware, ServerConfigAware, Closeable {
    private final RaftClient raftClient;

    public  static final String DEFAULT_COMMAND = "DEFAULT_COMMAND";

    private final SerializeExtensionPoint serializer = ServiceSupport.tryLoad(SerializeExtensionPoint.class).orElse(new JavaSerializeExtensionPoint());

    public JkClient(RaftClient raftClient) {
        this.raftClient = raftClient;
    }


    public <P,R> CompletableFuture<R> update ( P parameter) {
        return update(DEFAULT_COMMAND, parameter);
    }

    public <R> CompletableFuture<R> update (String command) {

        return update(command, null);
    }
    /**
     * 写入操作命令变更状态。集群保证按照提供的顺序写入，保证原子性，服务是线性的，任一时间只能有一个update操作被执行。
     * 日志在集群中复制到大多数节点，并在状态机执行后返回。
     *
     * @param command 更新命令
     * @param parameter 命令参数
     * @return 更新结果
     * @param <P> 命令参数类型
     * @param <R> 返回结果类型
     */
    public <P,R> CompletableFuture<R> update (String command, P parameter) {
        return this.raftClient.update(serializer.serialize(new JkRequest(command, parameter)))
                .thenApply(serializer::<JkResponse>parse)
                .thenApply(response -> {
                    if (response.isSuccess()) {
                        return response.getResult();
                    } else {
                        throw new CompletionException(response.getException());
                    }
                });
    }

    public <P,R> CompletableFuture<R> query (P parameter) {
        return query(DEFAULT_COMMAND, parameter);
    }

    public <R> CompletableFuture<R> query (String command) {
        return query(command, null);
    }
    /**
     * 查询集群当前的状态，即日志在状态机中执行完成后产生的数据。该服务保证强一致性，保证读到的状态总是集群的最新状态。
     * @param command 查询命令
     * @param parameter 命令参数
     * @return  查询结果
     * @param <P> 命令参数类型
     * @param <R> 查询结果类型
     */
    public <P,R> CompletableFuture<R> query (String command, P parameter) {
        return this.raftClient.query(serializer.serialize(new JkRequest(command, parameter)))
                .thenApply(serializer::<JkResponse>parse)
                .thenApply(response -> {
                    if (response.isSuccess()) {
                        return response.getResult();
                    } else {
                        throw new CompletionException(response.getException());
                    }
                });
    }

    public <R> CompletableFuture<R> query (String command, QueryConsistency consistency) {
        return query(command, null, consistency);
    }

    public <R> CompletableFuture<R> query (QueryConsistency consistency) {
        return query(DEFAULT_COMMAND, null, consistency);
    }

    public <P,R> CompletableFuture<R> query (P parameter, QueryConsistency consistency) {
        return query(DEFAULT_COMMAND, parameter, consistency);
    }
    /**
     * 查询集群当前的状态，即日志在状态机中执行完成后产生的数据。该服务保证强一致性，保证读到的状态总是集群的最新状态。
     * @param command 查询命令
     * @param parameter 命令参数
     * @param consistency 查询一致性。 See {@link QueryConsistency}
     * @return  查询结果
     * @param <P> 命令参数类型
     * @param <R> 查询结果类型
     */
    public <P,R> CompletableFuture<R> query (String command, P parameter, QueryConsistency consistency) {
        return this.raftClient.query(serializer.serialize(new JkRequest(command, parameter)), consistency)
                .thenApply(serializer::<JkResponse>parse)
                .thenApply(response -> {
                    if (response.isSuccess()) {
                        return response.getResult();
                    } else {
                        throw new CompletionException(response.getException());
                    }
                });
    }

    /**
     * 开启一个新事务，并返回事务ID。
     *
     * @return 事务ID
     */
    public CompletableFuture<TransactionContext> createTransaction() {
        return raftClient.createTransaction();
    }

    /**
     * 开启一个新事务，并返回事务ID。
     *
     * @param context 事务上下文
     * @return 事务ID
     */
    public CompletableFuture<TransactionContext> createTransaction(Map<String, String> context) {
        return raftClient.createTransaction(context);
    }

    /**
     * 结束事务，可能是提交或者回滚事务。
     *
     * @param transactionId 事务ID
     * @param commitOrAbort true：提交事务，false：回滚事务。
     * @return 执行成功返回null，失败抛出异常。
     */
    public CompletableFuture<Void> completeTransaction(TransactionId transactionId, boolean commitOrAbort) {
        return raftClient.completeTransaction(transactionId, commitOrAbort);
    }

    /**
     * 查询进行中的事务。
     *
     * @return 进行中的事务ID列表。
     */
    public CompletableFuture<Collection<TransactionContext>> getOpeningTransactions() {
        return raftClient.getOpeningTransactions();
    }

    /**
     * 写入操作日志变更状态。集群保证按照提供的顺序写入，保证原子性，服务是线性的，任一时间只能有一个update操作被执行。
     * 日志在集群中复制到大多数节点，并在状态机执行后返回。
     * 此方法等效于：update(transactionId, updateRequest, false, responseConfig);
     *
     * @param transactionId 事务ID
     * @param command 更新命令
     * @param parameter 命令参数
     * @return 执行成功返回null，失败抛出异常。
     * @param <P> 命令参数类型
     */
    public <P> CompletableFuture<Void> update(TransactionId transactionId, String command, P parameter) {
        return raftClient.update(
                transactionId,
                new UpdateRequest(serializer.serialize(new JkRequest(command, parameter)))
        );
    }
    @Override
    public void waitForClusterReady(long maxWaitMs) throws TimeoutException {
        this.raftClient.waitForClusterReady(maxWaitMs);
    }

    @Override
    public void updateServers(List<URI> servers) {
        this.raftClient.updateServers(servers);
    }

    public void addLeaderChangeListener(Consumer<OnLeaderChangeEvent> listener) {
        synchronized (leaderChangeListeners) {
            if (leaderChangeListeners.add(listener) && listenerCounter.getAndIncrement() == 0) {
                this.raftClient.watch(eventWatcher);
            }
        }
    }

    public void removeLeaderChangeListener(Consumer<OnLeaderChangeEvent> listener) {
        synchronized (leaderChangeListeners) {
            if (leaderChangeListeners.remove(listener) && listenerCounter.decrementAndGet() == 0) {
                this.raftClient.unWatch(eventWatcher);
            }
        }
    }

    public <T> void watch(Consumer<T> listener) {
        synchronized (stateChangeListeners) {
            if (stateChangeListeners.add(new EventListener(listener)) && listenerCounter.getAndIncrement() == 0) {
                this.raftClient.watch(eventWatcher);
            }
        }
    }

    public <T> void unwatch(Consumer<T> listener) {
        synchronized (stateChangeListeners) {
            if (stateChangeListeners.remove(new EventListener(listener)) && listenerCounter.decrementAndGet() == 0) {
                this.raftClient.unWatch(eventWatcher);
            }
        }
    }

    private final Set<Consumer<OnLeaderChangeEvent>> leaderChangeListeners = new HashSet<>();
    private final Set<Consumer<Event>> stateChangeListeners = new HashSet<>();
    private final AtomicInteger listenerCounter = new AtomicInteger(0);
    private final EventWatcher eventWatcher = event -> {
        switch (event.getEventType()) {
            case EventType.ON_LEADER_CHANGE:
                synchronized (leaderChangeListeners) {
                    if (!leaderChangeListeners.isEmpty()) {
                        OnLeaderChangeEvent leaderChangeEvent = InternalEntriesSerializeSupport.parse(event.getEventData());
                        this.leaderChangeListeners.forEach(listener -> listener.accept(leaderChangeEvent));
                    }
                }
                break;
            case EventType.ON_STATE_CHANGE:
                synchronized (stateChangeListeners) {
                    if (!stateChangeListeners.isEmpty()) {
                        Object eventData = serializer.parse(event.getEventData());
                        this.stateChangeListeners.forEach(listener -> listener.accept(new Event(eventData)));
                    }
                }
                break;
            default:
                // nothing to do
        }
    };
    @Override
    public void close() throws IOException {
        if (listenerCounter.get() > 0) {
            this.raftClient.unWatch(eventWatcher);
            this.stateChangeListeners.clear();
            this.leaderChangeListeners.clear();
        }

    }

    private static class Event {
        private final Object eventData;

        public Event(Object eventData) {
            this.eventData = eventData;
        }

        public <T> T get() {
            //noinspection unchecked
            return (T) eventData;

        }
    }

    private static class EventListener implements Consumer<JkClient.Event> {
    private final Consumer<?> listener;

        private EventListener(Consumer<?> listener) {
            this.listener = listener;
        }

        @Override
        public void accept(JkClient.Event event) {
            listener.accept(event.get());
        }

        @Override
        public boolean equals(Object object) {
            if (this == object) return true;
            if (object == null || getClass() != object.getClass()) return false;
            EventListener that = (EventListener) object;
            return Objects.equals(listener, that.listener);
        }

        @Override
        public int hashCode() {
            return Objects.hash(listener);
        }
    }



}
