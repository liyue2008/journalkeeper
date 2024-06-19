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

import io.journalkeeper.rpc.remoting.transport.Transport;
import io.journalkeeper.rpc.remoting.transport.command.Command;
import io.journalkeeper.rpc.remoting.transport.command.handler.CommandHandler;
import io.journalkeeper.rpc.remoting.transport.command.handler.ExceptionHandler;
import io.journalkeeper.rpc.remoting.transport.command.handler.filter.CommandHandlerFilter;
import io.journalkeeper.rpc.remoting.transport.command.handler.filter.CommandHandlerFilterFactory;
import io.journalkeeper.rpc.remoting.transport.command.handler.filter.CommandHandlerInvocation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * 命令执行线程
 * author: gaohaoxiang
 * <p>
 * date: 2018/8/14
 */
public class CommandExecuteTask implements Runnable {

    protected static final Logger logger = LoggerFactory.getLogger(CommandExecuteTask.class);

    private final Transport transport;
    private final Command command;
    private final CommandHandler commandHandler;
    private final CommandHandlerFilterFactory commandHandlerFilterFactory;
    private final ExceptionHandler exceptionHandler;

    public CommandExecuteTask(Transport transport, Command command, CommandHandler commandHandler, CommandHandlerFilterFactory commandHandlerFilterFactory, ExceptionHandler exceptionHandler) {
        this.transport = transport;
        this.command = command;
        this.commandHandler = commandHandler;
        this.commandHandlerFilterFactory = commandHandlerFilterFactory;
        this.exceptionHandler = exceptionHandler;
    }

    @Override
    public void run() {
        try {
            List<CommandHandlerFilter> commandHandlerFilters = commandHandlerFilterFactory.getFilters();
            CommandHandlerInvocation commandHandlerInvocation = new CommandHandlerInvocation(transport, command, commandHandler, commandHandlerFilters);
            Command response = commandHandlerInvocation.invoke();

            if (response != null) {
                transport.acknowledge(command, response);
            }
        } catch (Throwable t) {
            logger.error("command handler exception, tratnsport: {}, command: {}", transport, command, t);

            if (exceptionHandler != null) {
                exceptionHandler.handle(transport, command, t);
            }
        }
    }
}