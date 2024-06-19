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

import io.journalkeeper.core.api.*;
import io.journalkeeper.core.api.transaction.JournalKeeperTransactionContext;
import io.journalkeeper.core.api.transaction.UUIDTransactionId;
import io.journalkeeper.exceptions.JournalException;
import io.journalkeeper.exceptions.TransactionException;
import io.journalkeeper.rpc.client.UpdateClusterStateRequest;
import io.journalkeeper.rpc.client.UpdateClusterStateResponse;
import io.journalkeeper.utils.actor.Actor;
import io.journalkeeper.utils.state.ServerStateMachine;

import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author LiYue
 * Date: 2019/10/22
 */
public class JournalTransactionManager extends ServerStateMachine {
    public static final int TRANSACTION_PARTITION_START = 30000;
    public static final int TRANSACTION_PARTITION_COUNT = 32;

    private final Actor actor;
    private final JournalTransactionState transactionState;
    private final Map<UUID, CompletableFuture<Void>> pendingCompleteTransactionFutures = new ConcurrentHashMap<>();
    private final TransactionEntrySerializer transactionEntrySerializer = new TransactionEntrySerializer();



    public JournalTransactionManager(RaftJournal journal, Actor actor, long transactionTimeoutMs) {
        this.actor = actor;
        this.transactionState = new JournalTransactionState(journal, transactionTimeoutMs, actor);
    }

    @Override
    protected void doStart() {
        super.doStart();
        this.transactionState.start();
    }

    @Override
    protected void doStop() {
        this.transactionState.stop();
        super.doStop();
    }

    public CompletableFuture<JournalKeeperTransactionContext> createTransaction(Map<String, String> context) {
        int partition = transactionState.nextFreePartition();
        UUID transactionId = UUID.randomUUID();
        TransactionEntry entry = new TransactionEntry(transactionId, context);
        final long timestamp = entry.getTimestamp();
        byte[] serializedEntry = transactionEntrySerializer.serialize(entry);
        CompletableFuture<UpdateClusterStateResponse> future = null;
        UpdateClusterStateRequest request = new UpdateClusterStateRequest(new UpdateRequest(serializedEntry, partition, 1));

        return actor.<UpdateClusterStateResponse>sendThen("Voter", "updateClusterState", request)
                .thenApply(response -> {
                    if (response.success()) {
                        return new JournalKeeperTransactionContext(
                                new UUIDTransactionId(transactionId), context, timestamp
                        );
                    } else {
                        throw new JournalException(response.errorString());
                    }
                });
    }

    public CompletableFuture<Void> completeTransaction(UUID transactionId, boolean completeOrAbort) {
        int partition = getTransactionPartition(transactionId);
        ensureTransactionOpen(transactionId);
        TransactionEntry entry = new TransactionEntry(transactionId, TransactionEntryType.TRANSACTION_PRE_COMPLETE, completeOrAbort);
        byte[] serializedEntry = transactionEntrySerializer.serialize(entry);
        CompletableFuture<Void> future = new CompletableFuture<>();
        pendingCompleteTransactionFutures.put(transactionId, future);
        UpdateClusterStateRequest request = new UpdateClusterStateRequest(new UpdateRequest(serializedEntry, partition, 1));

        actor.<UpdateClusterStateResponse>sendThen("Voter", "updateClusterState", request)
                .thenAccept(response -> {
                    if (!response.success()) {
                        CompletableFuture<Void> retFuture = pendingCompleteTransactionFutures.remove(transactionId);
                        if (null != retFuture) {
                            retFuture.completeExceptionally(new TransactionException(response.errorString()));
                        }
                    }
                });
        return future;
    }

    public JournalEntry wrapTransactionalEntry(JournalEntry entry, UUID transactionId, JournalEntryParser journalEntryParser) {
        return transactionState.wrapTransactionalEntry(entry, transactionId, journalEntryParser);
    }

    private void ensureTransactionOpen(UUID transactionId) {
        transactionState.ensureTransactionOpen(transactionId);
    }

    private int getTransactionPartition(UUID transactionId) {
        return transactionState.getPartition(transactionId);
    }

    public Collection<JournalKeeperTransactionContext> getOpeningTransactions() {
        return transactionState.getOpeningTransactions();
    }

    public void applyEntry(JournalEntry entryHeader, EntryFuture entryFuture) {
        int partition = entryHeader.getPartition();
        if (transactionState.isTransactionPartition(partition)) {
            TransactionEntry transactionEntry = transactionEntrySerializer.parse(entryFuture.get());
            transactionState.applyEntry(transactionEntry, partition, pendingCompleteTransactionFutures);
        }
    }
}
