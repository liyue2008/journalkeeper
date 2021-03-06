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
package io.journalkeeper.rpc.remoting.transport.command.handler;

import io.journalkeeper.rpc.remoting.transport.command.Command;

/**
 * 命令处理器工厂类
 */
public interface CommandHandlerFactory {

    /**
     * 获取处理器，心跳命令，不支持的命令也需要返回默认处理器
     *
     * @param command 命令
     * @return 对应的CommandHandler
     */
    CommandHandler getHandler(Command command);

}