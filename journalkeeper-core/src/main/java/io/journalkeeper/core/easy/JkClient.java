package io.journalkeeper.core.easy;

import io.journalkeeper.core.api.ClusterReadyAware;
import io.journalkeeper.core.api.QueryConsistency;
import io.journalkeeper.core.api.ServerConfigAware;
import io.journalkeeper.core.api.transaction.TransactionContext;
import io.journalkeeper.core.api.transaction.TransactionId;
import io.journalkeeper.core.serialize.WrappedRaftClient;
import io.journalkeeper.utils.event.EventWatcher;
import io.journalkeeper.utils.event.Watchable;

import java.net.URI;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeoutException;

public class JkClient  implements Watchable, ClusterReadyAware, ServerConfigAware {
    private final WrappedRaftClient<JkRequest, JkResponse, JkRequest, JkResponse> raftClient;

    public JkClient(WrappedRaftClient<JkRequest, JkResponse, JkRequest, JkResponse> raftClient) {
        this.raftClient = raftClient;
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
        return this.raftClient.update(new JkRequest(command, parameter))
                .thenApply(response -> {
                    if (response.isSuccess()) {
                        return response.getResult();
                    } else {
                        throw new CompletionException(response.getException());
                    }
                });
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
        return this.raftClient.query(new JkRequest(command, parameter))
                .thenApply(response -> {
                    if (response.isSuccess()) {
                        return response.getResult();
                    } else {
                        throw new CompletionException(response.getException());
                    }
                });
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
        return this.raftClient.query(new JkRequest(command, parameter), consistency)
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
                new JkRequest(command, parameter)
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

    @Override
    public void watch(EventWatcher eventWatcher) {
        this.raftClient.watch(eventWatcher);
    }

    @Override
    public void unWatch(EventWatcher eventWatcher) {
        this.raftClient.unWatch(eventWatcher);
    }
}
