/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.journalkeeper.rpc.client;

import io.journalkeeper.core.api.ResponseConfig;

import java.util.UUID;

/**
 * RPC方法
 * {@link ClientServerRpc#queryServerState(QueryStateRequest) queryServerState}
 * 请求参数。
 * @author LiYue
 * Date: 2019-03-14
 */
public class UpdateClusterStateRequest {
    private final byte [] entry;
    private final int partition;
    private final int batchSize;
    private final boolean includeHeader;
    private final ResponseConfig responseConfig;
    private final UUID transactionId;

    public UpdateClusterStateRequest(byte [] entry, int partition, int batchSize) {
        this(entry, partition, batchSize, false, ResponseConfig.REPLICATION);
    }

    public UpdateClusterStateRequest(byte[] entry, int partition, int batchSize, boolean includeHeader, ResponseConfig responseConfig) {
        this(null, entry, partition, batchSize, includeHeader, responseConfig);
    }

    public UpdateClusterStateRequest(UUID transactionId, byte[] entry, int partition, int batchSize, boolean includeHeader) {
        this(transactionId, entry, partition, batchSize, includeHeader, ResponseConfig.REPLICATION);
    }

    public UpdateClusterStateRequest(UUID transactionId, byte[] entry, int partition, int batchSize, boolean includeHeader, ResponseConfig responseConfig) {
        this.transactionId = transactionId;
        this.batchSize = batchSize;
        this.responseConfig = responseConfig;
        this.entry = entry;
        this.partition = partition;
        this.includeHeader = includeHeader;
    }

    /**
     * 序列化后待执行的操作。entry将被：
     * 1. 写入Journal；
     * 2. 被状态机执行，变更系统状态；
     * @return 序列化后的entry。
     */
    public byte [] getEntry() {
        return entry;
    }

    /**
     * 响应配置，定义返回响应的时机
     * @return 响应配置
     */
    public ResponseConfig getResponseConfig() {
        return responseConfig;
    }

    public int getPartition() {
        return partition;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public boolean isIncludeHeader() {
        return includeHeader;
    }

    public UUID getTransactionId() {
        return transactionId;
    }
}
