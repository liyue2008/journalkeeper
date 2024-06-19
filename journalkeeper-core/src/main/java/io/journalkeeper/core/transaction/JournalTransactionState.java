/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.journalkeeper.core.transaction;

import io.journalkeeper.core.api.JournalEntry;
import io.journalkeeper.core.api.JournalEntryParser;
import io.journalkeeper.core.api.RaftJournal;
import io.journalkeeper.core.api.UpdateRequest;
import io.journalkeeper.core.api.transaction.JournalKeeperTransactionContext;
import io.journalkeeper.core.api.transaction.UUIDTransactionId;
import io.journalkeeper.exceptions.TransactionException;
import io.journalkeeper.rpc.client.ClientServerRpc;
import io.journalkeeper.rpc.client.UpdateClusterStateRequest;
import io.journalkeeper.rpc.client.UpdateClusterStateResponse;
import io.journalkeeper.utils.actor.Actor;
import io.journalkeeper.utils.state.ServerStateMachine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static io.journalkeeper.core.transaction.JournalTransactionManager.TRANSACTION_PARTITION_COUNT;
import static io.journalkeeper.core.transaction.JournalTransactionManager.TRANSACTION_PARTITION_START;

/**
 * 事务状态，非持久化
 *
 * @author LiYue
 * Date: 2019/10/22
 */
class JournalTransactionState extends ServerStateMachine {
    private static final Logger logger = LoggerFactory.getLogger(JournalTransactionState.class);
    private static final long RETRY_COMPLETE_TRANSACTION_INTERVAL_MS = 10000L;
    private final RaftJournal journal;
    private final Map<Integer /* partition */, TransactionEntryType /* last transaction entry type */> partitionStatusMap;
    private final Map<UUID /* transaction id */, TransactionState /* transaction state */> openingTransactionMap;
    private final ClientServerRpc server;
    private final TransactionEntrySerializer transactionEntrySerializer = new TransactionEntrySerializer();
    private final AtomicInteger nextFreePartition = new AtomicInteger(TRANSACTION_PARTITION_START);
    private final DelayQueue<CompleteTransactionRetry> retryCompleteTransactions = new DelayQueue<>();
    private final ScheduledExecutorService scheduledExecutor;
    private final long transactionTimeoutMs;
    private ScheduledFuture<?> retryCompleteTransactionScheduledFuture = null;
    private ScheduledFuture<?> checkOutdatedTransactionsScheduledFuture = null;
    private final Actor actor;

    JournalTransactionState(RaftJournal journal, long transactionTimeoutMs, ClientServerRpc server, ScheduledExecutorService scheduledExecutor) {
        super(false);
        this.journal = journal;
        this.transactionTimeoutMs = transactionTimeoutMs;
        this.server = server;
        this.scheduledExecutor = scheduledExecutor;
        this.partitionStatusMap = new HashMap<>(TRANSACTION_PARTITION_COUNT);
        this.openingTransactionMap = new HashMap<>(TRANSACTION_PARTITION_COUNT);
        this.actor = null;
    }

    JournalTransactionState(RaftJournal journal, long transactionTimeoutMs, Actor actor) {
        super(false);
        this.journal = journal;
        this.transactionTimeoutMs = transactionTimeoutMs;
        this.server = null;
        this.scheduledExecutor = null;
        this.actor = actor;
        this.partitionStatusMap = new HashMap<>(TRANSACTION_PARTITION_COUNT);
        this.openingTransactionMap = new HashMap<>(TRANSACTION_PARTITION_COUNT);
    }

    @Override
    protected void doStart() {
        super.doStart();
        recoverTransactionState();
        if (null != actor) {
            actor.addScheduler(RETRY_COMPLETE_TRANSACTION_INTERVAL_MS, TimeUnit.MILLISECONDS, "retryCompleteTransactions", this::retryCompleteTransactions);
            actor.addScheduler(transactionTimeoutMs, TimeUnit.MILLISECONDS, "checkOutdatedTransactions", this::abortOutdatedTransactions);
        }
        if (null != scheduledExecutor) {
            retryCompleteTransactionScheduledFuture = scheduledExecutor.scheduleWithFixedDelay(
                    this::retryCompleteTransactions,
                    RETRY_COMPLETE_TRANSACTION_INTERVAL_MS,
                    RETRY_COMPLETE_TRANSACTION_INTERVAL_MS,
                    TimeUnit.MILLISECONDS
            );
            checkOutdatedTransactionsScheduledFuture = scheduledExecutor.scheduleWithFixedDelay(
                    this::abortOutdatedTransactions,
                    transactionTimeoutMs,
                    transactionTimeoutMs,
                    TimeUnit.MILLISECONDS
            );
        }
    }

    @Override
    protected void doStop() {
        if (null != retryCompleteTransactionScheduledFuture) {
            retryCompleteTransactionScheduledFuture.cancel(false);
        }
        if (null != checkOutdatedTransactionsScheduledFuture) {
            checkOutdatedTransactionsScheduledFuture.cancel(false);
        }
        super.doStop();
    }

    private void abortOutdatedTransactions() {
        long currentTimestamp = System.currentTimeMillis();
        openingTransactionMap.forEach((transactionId, state) -> {
            int partition = state.getPartition();
            long i = journal.maxIndex(partition);
            while (--i >= journal.minIndex(partition)) {
                JournalEntry journalEntry = journal.readByPartition(partition, i);
                TransactionEntry transactionEntry = transactionEntrySerializer.parse(journalEntry.getPayload().getBytes());

                if (!transactionId.equals(transactionEntry.getTransactionId())) {
                    break;
                }

                if (transactionEntry.getType() == TransactionEntryType.TRANSACTION_START) {
                    long transactionCreateTimestamp = transactionEntry.getTimestamp();
                    if (transactionCreateTimestamp + transactionTimeoutMs < currentTimestamp) {
                        logger.info("Abort outdated transaction: {}.", transactionId);
                        writeTransactionCompleteEntry(transactionId, false, partition);
                    }
                    break;
                }
            }

        });
    }

    private void retryCompleteTransactions() {
        CompleteTransactionRetry retry;
        while ((retry = retryCompleteTransactions.poll()) != null) {
            completeTransaction(retry.getTransactionId(), true, retry.getPartition());
        }
    }

    private void recoverTransactionState() {
        for (int i = 0; i < TRANSACTION_PARTITION_COUNT; i++) {
            int partition = TRANSACTION_PARTITION_START + i;
            if (journal.getPartitions().contains(partition) && journal.maxIndex(partition) > 0) {
                logger.info("Recover transaction partition {}...", partition);
                long index = journal.maxIndex(partition);
                long minIndexOfPartition = journal.minIndex(partition);
                boolean lastEntryOfTheTransaction = true;
                while (index-- > minIndexOfPartition) {
                    JournalEntry journalEntry = journal.readByPartition(partition, index);
                    TransactionEntry transactionEntry = transactionEntrySerializer.parse(journalEntry.getPayload().getBytes());
                    if (lastEntryOfTheTransaction) {
                        partitionStatusMap.put(partition, transactionEntry.getType());

                        // retry pre committed transaction
                        if (transactionEntry.getType() == TransactionEntryType.TRANSACTION_PRE_COMPLETE) {
                            retryCompleteTransactions.put(
                                    new CompleteTransactionRetry(transactionEntry.getTransactionId(), partition)
                            );
                        }
                        lastEntryOfTheTransaction = false;
                    }

                    if (transactionEntry.getType() == TransactionEntryType.TRANSACTION_START) {
                        openingTransactionMap.put(
                                transactionEntry.getTransactionId(),
                                new TransactionState(partition, new JournalKeeperTransactionContext(
                                        new UUIDTransactionId(transactionEntry.getTransactionId()),
                                        transactionEntry.getContext(),
                                        transactionEntry.getTimestamp()
                                ))
                        );
                        break;
                    }
                }
            } else {
                partitionStatusMap.put(partition, TransactionEntryType.TRANSACTION_COMPLETE);
            }
        }
    }

    int nextFreePartition() {
        int partition = nextPartition();
        int start = partition;
        synchronized (partitionStatusMap) {
            while (partitionStatusMap.getOrDefault(partition, TransactionEntryType.TRANSACTION_COMPLETE) != TransactionEntryType.TRANSACTION_COMPLETE) {
                partition = nextPartition();
                if (partition == start) {
                    throw new TransactionException("No free transaction partition!");
                }
            }
            return partition;
        }
    }


    private int nextPartition() {
        int partition = nextFreePartition.getAndIncrement();
        nextFreePartition.compareAndSet(
                TRANSACTION_PARTITION_START + TRANSACTION_PARTITION_COUNT,
                TRANSACTION_PARTITION_START
        );

        return partition;
    }

    void applyEntry(TransactionEntry entry, int partition, Map<UUID, CompletableFuture<Void>> pendingCompleteTransactionFutures) {
        if (partition < TRANSACTION_PARTITION_START || partition >= TRANSACTION_PARTITION_START + TRANSACTION_PARTITION_COUNT) {
            logger.warn("Ignore transaction entry, cause: partition {} is not a transaction partition.", partition);
            return;
        }

        partitionStatusMap.put(partition, entry.getType());

        switch (entry.getType()) {
            case TRANSACTION_START:
                openingTransactionMap.put(
                        entry.getTransactionId(),
                        new TransactionState(partition, new JournalKeeperTransactionContext(
                                new UUIDTransactionId(entry.getTransactionId()),
                                entry.getContext(),
                                entry.getTimestamp()
                        ))
                );
                break;
            case TRANSACTION_PRE_COMPLETE:
                completeTransaction(entry.getTransactionId(), entry.isCommitOrAbort(), partition);
                break;
            case TRANSACTION_COMPLETE:
                openingTransactionMap.remove(entry.getTransactionId());
                partitionStatusMap.remove(partition);
                CompletableFuture<Void> future = pendingCompleteTransactionFutures.remove(entry.getTransactionId());
                if (null != future) {
                    future.complete(null);
                }
                break;
        }


    }

    /**
     * 执行状态机阶段，针对“TRANSACTION_PRE_COMPLETE”的日志，需要执行：
     * <p>
     * 1. 如果操作是回滚，直接写入TRANSACTION_COMPLETE日志；
     * 2. 如果操作是提交：
     * 3. 这个事务中所有消息涉及到的每个分区：读出该分区的所有事务日志，组合成一个BatchEntry写入对应分区。
     * 4. 上步骤中每条日志都写入成功后，写入TRANSACTION_COMPLETE日志，提交成功。
     *
     * @param transactionId 事务ID。
     * @param commitOrAbort true：提条事务，false：回滚事务。
     */
    private void completeTransaction(UUID transactionId, boolean commitOrAbort, int partition) {

        if (commitOrAbort) {
            TransactionEntry transactionEntry;
            int entryCount = 0;
            long i = journal.maxIndex(partition);
            Map<Integer /* partition */, List<TransactionEntry> /* transaction entries */> transactionEntriesByPartition
                    = new HashMap<>();
            long minIndex = journal.minIndex(partition);
            do {
                JournalEntry journalEntry = journal.readByPartition(partition, --i);
                transactionEntry = transactionEntrySerializer.parse(journalEntry.getPayload().getBytes());

                if (!transactionId.equals(transactionEntry.getTransactionId()) ||
                        transactionEntry.getType() == TransactionEntryType.TRANSACTION_START) {
                    break;
                }

                if (transactionEntry.getType() == TransactionEntryType.TRANSACTION_ENTRY) {
                    entryCount++;
                    List<TransactionEntry> list =
                            transactionEntriesByPartition.computeIfAbsent(transactionEntry.getPartition(), p -> new LinkedList<>());
                    list.add(transactionEntry);
                }

            } while (i > minIndex);

            final AtomicInteger unFinishedRequests = new AtomicInteger(entryCount);

            List<CompletableFuture<?>> futures = new ArrayList<>(entryCount);

            for (Map.Entry<Integer, List<TransactionEntry>> me : transactionEntriesByPartition.entrySet()) {
                int bizPartition = me.getKey();
                List<TransactionEntry> transactionEntries = me.getValue();

                for (TransactionEntry te : transactionEntries) {
                    if (null != server) {
                        futures.add(
                                server
                                        .updateClusterState(
                                                new UpdateClusterStateRequest(
                                                        new UpdateRequest(
                                                                te.getEntry(), bizPartition, te.getBatchSize()
                                                        )
                                                )
                                        )
                                        .exceptionally(UpdateClusterStateResponse::new)
                                        .thenAccept(response -> {
                                            if (response.success()) {
                                                unFinishedRequests.decrementAndGet();
                                            } else {
                                                logger.warn("Transaction commit {} failed! Cause: {}.",
                                                        transactionId,
                                                        response.errorString());
                                            }
                                        })
                        );
                    }

                    if (null != actor) {
                        futures.add(
                                actor
                                        .<UpdateClusterStateResponse>sendThen("Voter", "updateClusterState", new UpdateClusterStateRequest(
                                                new UpdateRequest(
                                                        te.getEntry(), bizPartition, te.getBatchSize()
                                                )
                                        ))
                                        .exceptionally(UpdateClusterStateResponse::new)
                                        .thenAccept(response -> {
                                            if (response.success()) {
                                                unFinishedRequests.decrementAndGet();
                                            } else {
                                                logger.warn("Transaction commit {} failed! Cause: {}.",
                                                        transactionId ,
                                                        response.errorString());
                                            }
                                        })
                        );
                    }
                }
            }
            CompletableFuture
                    .allOf(futures.toArray(new CompletableFuture[entryCount]))
                    .thenRun(() -> {
                        if (unFinishedRequests.get() > 0) {
                            retryCompleteTransactions.add(
                                    new CompleteTransactionRetry(transactionId, partition)
                            );
                        } else {
                            writeTransactionCompleteEntry(transactionId, true, partition);
                        }
                    });

        } else {
            writeTransactionCompleteEntry(transactionId, false, partition);
        }

    }

    JournalEntry wrapTransactionalEntry(JournalEntry entry, UUID transactionId, JournalEntryParser journalEntryParser) {
        int transactionPartition = getPartition(transactionId);
        if (transactionPartition > 0) {
            int bizPartition = entry.getPartition();
            int batchSize = entry.getBatchSize();
            int term = entry.getTerm();
            TransactionEntry transactionEntry = new TransactionEntry(transactionId, bizPartition, batchSize, entry.getPayload().getBytes());
            byte[] serializedTransactionEntry = transactionEntrySerializer.serialize(transactionEntry);
            JournalEntry wrappedEntry = journalEntryParser.createJournalEntry(serializedTransactionEntry);
            wrappedEntry.setPartition(transactionPartition);
            wrappedEntry.setTerm(term);

            return wrappedEntry;

        } else {
            throw new TransactionException(
                    String.format("Transaction %s is not open!", transactionId.toString())
            );
        }
    }

    private void writeTransactionCompleteEntry(UUID transactionId, boolean commitOrAbort, int partition) {
        TransactionEntry entry = new TransactionEntry(transactionId, TransactionEntryType.TRANSACTION_COMPLETE, commitOrAbort);
        byte[] serializedEntry = transactionEntrySerializer.serialize(entry);
        if (null != server) {
            server.updateClusterState(new UpdateClusterStateRequest(
                            new UpdateRequest(
                                    serializedEntry, partition, 1
                            )
                    ))
                    .exceptionally(UpdateClusterStateResponse::new)
                    .thenAccept(response -> {
                        if (response.success()) {
                            logger.info("Transaction {} {}.", transactionId.toString(), commitOrAbort ? "committed" : "aborted");
                        } else {
                            logger.warn("Transaction {} {} failed! Cause: {}.",
                                    transactionId.toString(),
                                    commitOrAbort ? "commit" : "abort",
                                    response.errorString());
                        }
                    });
        }
        if (null != actor) {
            actor.<UpdateClusterStateResponse>sendThen("Voter", "updateClusterState", new UpdateClusterStateRequest(
                    new UpdateRequest(
                            serializedEntry, partition, 1
                    )
            )).exceptionally(UpdateClusterStateResponse::new)
                    .thenAccept(response -> {
                        if (response.success()) {
                            logger.info("Transaction {} {}.", transactionId.toString(), commitOrAbort ? "committed" : "aborted");
                        } else {
                            logger.warn("Transaction {} {} failed! Cause: {}.",
                                    transactionId.toString(),
                                    commitOrAbort ? "commit" : "abort",
                                    response.errorString());
                        }
                    });
        }
    }

    Collection<JournalKeeperTransactionContext> getOpeningTransactions() {
        return openingTransactionMap.values().stream().map(TransactionState::getContext).collect(Collectors.toSet());
    }

    int getPartition(UUID transactionId) {
        TransactionState transactionState;
        return ((transactionState = openingTransactionMap
                .get(transactionId))) != null ? transactionState.getPartition() : -1;
    }

    void ensureTransactionOpen(UUID transactionId) {
        if (!openingTransactionMap.containsKey(transactionId)) {
            throw new IllegalStateException(
                    String.format("Transaction %s is not open!", transactionId.toString())
            );
        }
    }

    boolean isTransactionPartition(int partition) {
        return partition >= TRANSACTION_PARTITION_START && partition < TRANSACTION_PARTITION_START + TRANSACTION_PARTITION_COUNT;
    }

    private static class CompleteTransactionRetry implements Delayed {
        private final UUID transactionId;
        private final int partition;
        private final long expireTimeMs;

        CompleteTransactionRetry(UUID transactionId, int partition) {
            this.transactionId = transactionId;
            this.partition = partition;
            this.expireTimeMs = System.currentTimeMillis() + RETRY_COMPLETE_TRANSACTION_INTERVAL_MS;
        }

        public UUID getTransactionId() {
            return transactionId;
        }

        public int getPartition() {
            return partition;
        }


        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            CompleteTransactionRetry that = (CompleteTransactionRetry) o;
            return partition == that.partition &&
                    transactionId.equals(that.transactionId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(transactionId, partition);
        }

        public long getExpireTimeMs() {
            return expireTimeMs;
        }

        @Override
        public long getDelay(TimeUnit unit) {
            return unit.convert(this.expireTimeMs - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
        }

        @Override
        public int compareTo(Delayed o) {
            return (int) (this.getDelay(TimeUnit.MILLISECONDS) - o.getDelay(TimeUnit.MILLISECONDS));
        }
    }

    private static class TransactionState {
        private final int partition;
        private final JournalKeeperTransactionContext context;

        public TransactionState(int partition, JournalKeeperTransactionContext context) {
            this.partition = partition;
            this.context = context;
        }

        public int getPartition() {
            return partition;
        }

        public JournalKeeperTransactionContext getContext() {
            return context;
        }
    }
}
