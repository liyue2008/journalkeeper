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
package io.journalkeeper.coordinating.client;

import io.journalkeeper.coordinating.client.exception.CoordinatingClientException;
import io.journalkeeper.coordinating.state.domain.StateTypes;
import io.journalkeeper.coordinating.state.domain.WriteRequest;
import io.journalkeeper.core.easy.JkClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

/**
 * CoordinatingClient
 * author: gaohaoxiang
 *
 * date: 2019/6/4
 */
public class CoordinatingClient {

    protected static final Logger logger = LoggerFactory.getLogger(CoordinatingClient.class);

    private final JkClient client;

    public CoordinatingClient(JkClient client) {
        this.client = client;

    }

    public CompletableFuture<Boolean> set(byte[] key, byte[] value) {
        return client.<WriteRequest, Boolean>update(StateTypes.SET.getType(), new WriteRequest( key, value))
                .exceptionally(cause -> {
                    throw convertException(cause);
                });
    }

    public CompletableFuture<byte[]> get(byte[] key) {
        return client.<byte[], byte[]>query(StateTypes.GET.getType(), key)
                .exceptionally(cause -> {
                    throw convertException(cause);
                });
    }

    public CompletableFuture<List<byte[]>> list(List<byte[]> keys) {
        return client.<List<byte[]>, List<byte[]>>query(StateTypes.LIST.getType(), new ArrayList<>(keys))
                .exceptionally(cause -> {
                    throw convertException(cause);
                });
    }

    public CompletableFuture<Boolean> compareAndSet(byte[] key, byte[] expect, byte[] value) {
        return client.<WriteRequest, Boolean>update(StateTypes.COMPARE_AND_SET.getType(), new WriteRequest( key, expect, value))
                .exceptionally(cause -> {
                    throw convertException(cause);
                });
    }

    public CompletableFuture<Boolean> remove(byte[] key) {
        return client.<WriteRequest, Boolean>update(StateTypes.REMOVE.getType(), new WriteRequest(key))
                .exceptionally(cause -> {
                    throw convertException(cause);
                });
    }

    public CompletableFuture<Boolean> exist(byte[] key) {
        return client.<WriteRequest, Boolean>update(StateTypes.EXIST.getType(), new WriteRequest(key))
                .exceptionally(cause -> {
                    throw convertException(cause);
                });
    }

    public void watch(CoordinatingEventListener listener) {
        client.watch(listener);
    }

    public void unwatch(CoordinatingEventListener listener) {
        client.unwatch(listener);
    }

    public void waitClusterReady(Long maxWaitMs) throws TimeoutException {
        this.client.waitForClusterReady(maxWaitMs);
    }

    public void stop() {
    }

    protected CoordinatingClientException convertException(Throwable cause) {
        if (cause instanceof CoordinatingClientException) {
            return (CoordinatingClientException) cause;
        } else if (cause instanceof ExecutionException) {
            return new CoordinatingClientException(cause.getCause());
        } else {
            throw new CoordinatingClientException(cause);
        }
    }


}