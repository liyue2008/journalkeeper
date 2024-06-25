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
package io.journalkeeper.rpc.remoting.transport.command.support;

import io.journalkeeper.rpc.remoting.transport.RequestBarrier;
import io.journalkeeper.rpc.remoting.transport.ResponseFuture;
import io.journalkeeper.rpc.remoting.transport.Transport;
import io.journalkeeper.rpc.remoting.transport.command.Command;
import io.journalkeeper.rpc.remoting.transport.command.Header;
import io.journalkeeper.rpc.remoting.transport.config.TransportConfig;
import io.journalkeeper.utils.threads.NamedThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 响应处理器
 * author: gaohaoxiang
 * <p>
 * date: 2018/8/24
 */
public class ResponseHandler {

    protected static final Logger logger = LoggerFactory.getLogger(ResponseHandler.class);

    private final TransportConfig config;
    private final RequestBarrier barrier;
    private final ExecutorService asyncExecutorService;

    public ResponseHandler(TransportConfig transportConfig, RequestBarrier barrier) {
        this.config = transportConfig;
        this.barrier = barrier;
        this.asyncExecutorService = newAsyncExecutorService();
    }

    public void handle(Transport transport, Command command) {
        Header header = command.getHeader();
        // 超时被删除了
        final ResponseFuture responseFuture = barrier.get(header.getRequestId());
        if (responseFuture == null) {
            logger.info("request is timeout {}", header);
            return;
        }
        // 设置应答
        responseFuture.setResponse(command);
        // 异步调用
        if (responseFuture.getCallback() != null) {
            boolean success = false;
            ExecutorService executor = this.asyncExecutorService;
            if (executor != null) {
                try {
                    executor.execute(() -> {
                        try {
                            responseFuture.onSuccess();
                        } catch (Throwable e) {
                            logger.error("execute callback error.", e);
                        } finally {
                            responseFuture.release();
                        }
                    });
                    success = true;
                } catch (Throwable e) {
                    logger.error("execute callback error.", e);
                }
            }

            if (!success) {
                try {
                    responseFuture.onSuccess();
                } catch (Throwable e) {
                    logger.error("execute callback error.", e);
                } finally {
                    responseFuture.release();
                }
            }
        } else {
            // 释放资源，不回调
            if (!responseFuture.release()) {
                // 已经被释放了
                return;
            }
        }
        barrier.remove(header.getRequestId());
    }

    protected ExecutorService newAsyncExecutorService() {
        return Executors.newFixedThreadPool(config.getCallbackThreads(), new NamedThreadFactory("JournalKeeper-Async-Callback"));
    }

    public void stop() {
        asyncExecutorService.shutdown();
        this.barrier.clear();
    }
}