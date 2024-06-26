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
package io.journalkeeper.rpc.remoting.transport;

import io.journalkeeper.rpc.remoting.transport.command.Command;
import io.journalkeeper.rpc.remoting.transport.command.CommandCallback;
import io.journalkeeper.rpc.remoting.transport.exception.TransportException;

import java.net.SocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

/**
 * 通信
 * author: gaohaoxiang
 *
 * date: 2018/8/13
 */
public interface Transport {

    /**
     * 同步发送，需要应答
     *
     * @param command 命令
     * @return 应答命令
     * @throws TransportException 传输异常
     */
    default Command sync(Command command) throws TransportException {
        return sync(command, 0);
    }

    /**
     * 同步发送，需要应答
     *
     * @param command 命令
     * @param timeout 超时
     * @return 应答命令
     * @throws TransportException 传输异常
     */
    Command sync(Command command, long timeout) throws TransportException;

    /**
     * 异步发送，需要应答
     *
     * @param command  命令
     * @param callback 回调
     * @throws TransportException 传输异常
     */
    default void async(Command command, CommandCallback callback) throws TransportException {
        async(command, 0, callback);
    }

    /**
     * 异步发送，需要应答
     *
     * @param command  命令
     * @param timeout  超时
     * @param callback 回调
     * @throws TransportException 传输异常
     */
    void async(Command command, long timeout, CommandCallback callback) throws TransportException;

    /**
     * 异步发送，需要应答
     *
     * @param command  命令
     * @return 异步执行结果Future
     * @throws TransportException 传输异常
     */
    default CompletableFuture<Command> async(Command command) throws TransportException {
        return async(command, 0);
    }

    /**
     * 异步发送，需要应答
     *
     * @param command  命令
     * @param timeout  超时
     * @return 异步执行结果Future
     * @throws TransportException 传输异常
     */
    CompletableFuture<Command> async(Command command, long timeout) throws TransportException;

    /**
     * 单向发送，不需要应答
     *
     * @param command 命令
     * @throws TransportException 传输异常
     */
    default void oneway(Command command) throws TransportException {
        oneway(command, 0);
    }

    /**
     * 单向发送，需要应答
     *
     * @param command 命令
     * @param timeout 超时
     * @throws TransportException 传输异常
     */
    void oneway(Command command, long timeout) throws TransportException;

    /**
     * 应答
     *
     * @param request  请求
     * @param response 响应
     * @throws TransportException 传输异常
     */
    default void acknowledge(Command request, Command response) throws TransportException {
        acknowledge(request, response, null);
    }

    /**
     * 应答
     *
     * @param request  请求
     * @param response 响应
     * @param callback 回调
     * @throws TransportException 传输异常
     */
    void acknowledge(Command request, Command response, CommandCallback callback) throws TransportException;

    /**
     * 获取远端地址
     *
     * @return 远端地址
     */
    SocketAddress remoteAddress();

    /**
     * 属性
     * @return 传输属性
     */
    TransportAttribute attr();

    /**
     * 设置属性
     * @param attribute 属性
     */
    void attr(TransportAttribute attribute);

    /**
     * 状态
     * @return 传输状态
     */
    TransportState state();

    /**
     * 停止
     */
    void stop();
}