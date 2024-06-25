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
package io.journalkeeper.rpc.remoting.transport.support;

import io.journalkeeper.rpc.remoting.concurrent.EventBus;
import io.journalkeeper.rpc.remoting.event.TransportEvent;
import io.journalkeeper.rpc.remoting.transport.RequestBarrier;
import io.journalkeeper.rpc.remoting.transport.TransportClient;
import io.journalkeeper.rpc.remoting.transport.TransportClientFactory;
import io.journalkeeper.rpc.remoting.transport.codec.Codec;
import io.journalkeeper.rpc.remoting.transport.command.handler.CommandHandlerFactory;
import io.journalkeeper.rpc.remoting.transport.command.handler.ExceptionHandler;
import io.journalkeeper.rpc.remoting.transport.command.handler.filter.CommandHandlerFilterFactory;
import io.journalkeeper.rpc.remoting.transport.command.support.DefaultCommandHandlerFilterFactory;
import io.journalkeeper.rpc.remoting.transport.command.support.RequestHandler;
import io.journalkeeper.rpc.remoting.transport.command.support.ResponseHandler;
import io.journalkeeper.rpc.remoting.transport.config.ClientConfig;

/**
 * 默认通信客户端工厂
 * author: gaohaoxiang
 * <p>
 * date: 2018/8/24
 */
public class DefaultTransportClientFactory implements TransportClientFactory {

    private final Codec codec;
    private final CommandHandlerFactory commandHandlerFactory;
    private final ExceptionHandler exceptionHandler;
    private final EventBus<TransportEvent> transportEventBus;

    public DefaultTransportClientFactory(Codec codec) {
        this(codec, (CommandHandlerFactory) null);
    }

    public DefaultTransportClientFactory(Codec codec, EventBus<TransportEvent> transportEventBus) {
        this(codec, null, null, transportEventBus);
    }

    public DefaultTransportClientFactory(Codec codec, CommandHandlerFactory commandHandlerFactory) {
        this(codec, commandHandlerFactory, null);
    }

    public DefaultTransportClientFactory(Codec codec, CommandHandlerFactory commandHandlerFactory, ExceptionHandler exceptionHandler) {
        this(codec, commandHandlerFactory, exceptionHandler, new EventBus<>("DefaultTransportClientEventBus"));
    }

    public DefaultTransportClientFactory(Codec codec, CommandHandlerFactory commandHandlerFactory, ExceptionHandler exceptionHandler, EventBus<TransportEvent> transportEventBus) {
        this.codec = codec;
        this.commandHandlerFactory = commandHandlerFactory;
        this.exceptionHandler = exceptionHandler;
        this.transportEventBus = transportEventBus;
    }

    @Override
    public TransportClient create(ClientConfig config) {
        CommandHandlerFilterFactory commandHandlerFilterFactory = new DefaultCommandHandlerFilterFactory();
        RequestBarrier requestBarrier = new RequestBarrier(config);
        RequestHandler requestHandler = new RequestHandler(commandHandlerFactory, commandHandlerFilterFactory, exceptionHandler);
        ResponseHandler responseHandler = new ResponseHandler(config, requestBarrier);
        DefaultTransportClient transportClient = new DefaultTransportClient(config, codec, requestBarrier, requestHandler, responseHandler, transportEventBus);
        return new FailoverTransportClient(transportClient, config, transportEventBus);
    }
}