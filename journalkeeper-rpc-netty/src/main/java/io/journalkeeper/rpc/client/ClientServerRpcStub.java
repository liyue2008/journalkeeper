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
 * <p>
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
package io.journalkeeper.rpc.client;

import io.journalkeeper.exceptions.RequestTimeoutException;
import io.journalkeeper.rpc.BaseResponse;
import io.journalkeeper.rpc.codec.RpcTypes;
import io.journalkeeper.rpc.remoting.transport.Transport;
import io.journalkeeper.rpc.remoting.transport.TransportClient;
import io.journalkeeper.rpc.remoting.transport.TransportState;
import io.journalkeeper.rpc.remoting.transport.exception.TransportException;
import io.journalkeeper.rpc.utils.CommandSupport;

import java.net.InetSocketAddress;
import java.net.URI;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 客户端桩
 * @author LiYue
 * Date: 2019-03-30
 */

public class ClientServerRpcStub implements ClientServerRpc {
    protected final TransportClient transportClient;
    protected final InetSocketAddress inetSocketAddress;
    protected final URI uri;
    protected Transport transport;
    protected final AtomicBoolean lastRequestSuccess = new AtomicBoolean(true);
    protected final int version;

    public ClientServerRpcStub(TransportClient transportClient, URI uri, InetSocketAddress inetSocketAddress, int version) {
        this.transportClient = transportClient;
        this.uri = uri;
        this.version = version;
        this.inetSocketAddress = inetSocketAddress;
    }


    @Override
    public URI serverUri() {
        return uri;
    }

    protected <Q, R extends BaseResponse> CompletableFuture<R> sendRequest(Q request, int rpcType) {
        try {
            if (!isAlive()) {
                closeTransport();
                transport = createTransport();
            }
            CompletableFuture<R> future = CommandSupport.sendRequest(request, rpcType, transport, uri, version);

            future.whenCompleteAsync((response, exception) -> {
                if (null != exception) {
                    // 如果发生异常，
                    stop();
                    lastRequestSuccess.set(false);
                } else {
                    lastRequestSuccess.set(true);
                }
            });
            return future.exceptionally(e -> {
                try {
                    throw e;
                } catch (TransportException.RequestTimeoutException te) {
                    throw new RequestTimeoutException(te);
                } catch (TransportException e1) {
                    throw new io.journalkeeper.exceptions.TransportException(e1);
                } catch (Throwable t) {
                    throw new CompletionException(t);
                }
            });
        } catch (Throwable t) {
            CompletableFuture<R> completableFuture = new CompletableFuture<>();
            completableFuture.completeExceptionally(new io.journalkeeper.exceptions.TransportException(t));
            return completableFuture;
        }

    }

    @Override
    public CompletableFuture<UpdateClusterStateResponse> updateClusterState(UpdateClusterStateRequest request) {
        return sendRequest(request, RpcTypes.UPDATE_CLUSTER_STATE_REQUEST);
    }

    @Override
    public CompletableFuture<QueryStateResponse> queryClusterState(QueryStateRequest request) {
        return sendRequest(request, RpcTypes.QUERY_CLUSTER_STATE_REQUEST);
    }

    @Override
    public CompletableFuture<QueryStateResponse> queryServerState(QueryStateRequest request) {
        return sendRequest(request, RpcTypes.QUERY_SERVER_STATE_REQUEST);
    }

    @Override
    public CompletableFuture<LastAppliedResponse> lastApplied() {
        return sendRequest(null, RpcTypes.LAST_APPLIED_REQUEST);

    }

    @Override
    public CompletableFuture<QueryStateResponse> querySnapshot(QueryStateRequest request) {
        return sendRequest(request, RpcTypes.QUERY_SNAPSHOT_REQUEST);
    }

    @Override
    public CompletableFuture<GetServersResponse> getServers() {
        return sendRequest(null, RpcTypes.GET_SERVERS_REQUEST);
    }

    @Override
    public CompletableFuture<GetServerStatusResponse> getServerStatus() {
        return sendRequest(null, RpcTypes.GET_SERVER_STATUS_REQUEST);
    }

    @Override
    public CompletableFuture<AddPullWatchResponse> addPullWatch() {
        return sendRequest(null, RpcTypes.ADD_PULL_WATCH_REQUEST);
    }

    @Override
    public CompletableFuture<RemovePullWatchResponse> removePullWatch(RemovePullWatchRequest request) {
        return sendRequest(request, RpcTypes.REMOVE_PULL_WATCH_REQUEST);
    }

    @Override
    public CompletableFuture<UpdateVotersResponse> updateVoters(UpdateVotersRequest request) {
        return sendRequest(request, RpcTypes.UPDATE_VOTERS_REQUEST);
    }

    @Override
    public CompletableFuture<PullEventsResponse> pullEvents(PullEventsRequest request) {
        return sendRequest(request, RpcTypes.PULL_EVENTS_REQUEST);
    }

    @Override
    public CompletableFuture<ConvertRollResponse> convertRoll(ConvertRollRequest request) {
        return sendRequest(request, RpcTypes.CONVERT_ROLL_REQUEST);
    }

    @Override
    public CompletableFuture<CreateTransactionResponse> createTransaction(CreateTransactionRequest request) {
        return sendRequest(request, RpcTypes.CREATE_TRANSACTION_REQUEST);
    }

    @Override
    public CompletableFuture<CompleteTransactionResponse> completeTransaction(CompleteTransactionRequest request) {
        return sendRequest(request, RpcTypes.COMPLETE_TRANSACTION_REQUEST);
    }

    @Override
    public CompletableFuture<GetOpeningTransactionsResponse> getOpeningTransactions() {
        return sendRequest(null, RpcTypes.GET_OPENING_TRANSACTIONS_REQUEST);
    }

    @Override
    public CompletableFuture<GetSnapshotsResponse> getSnapshots() {
        return sendRequest(null, RpcTypes.GET_SNAPSHOTS_REQUEST);
    }

    @Override
    public CompletableFuture<CheckLeadershipResponse> checkLeadership() {
        return sendRequest(null, RpcTypes.CHECK_LEADERSHIP_REQUEST);
    }


    private synchronized Transport createTransport() {
        return transportClient.createTransport(inetSocketAddress);
    }

    private synchronized void closeTransport() {
        if (null != transport) {
            transport.stop();
            transport = null;
        }
    }

    private boolean isAlive() {
        return null != transport && transport.state() == TransportState.CONNECTED;
    }

    @Override
    public void stop() {
        closeTransport();
    }
}
