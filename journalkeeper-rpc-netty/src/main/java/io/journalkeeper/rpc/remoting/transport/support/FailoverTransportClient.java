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
import io.journalkeeper.rpc.remoting.concurrent.EventListener;
import io.journalkeeper.rpc.remoting.event.TransportEvent;
import io.journalkeeper.rpc.remoting.transport.ChannelTransport;
import io.journalkeeper.rpc.remoting.transport.Transport;
import io.journalkeeper.rpc.remoting.transport.TransportClient;
import io.journalkeeper.rpc.remoting.transport.TransportClientSupport;
import io.journalkeeper.rpc.remoting.transport.config.TransportConfig;
import io.journalkeeper.rpc.remoting.transport.exception.TransportException;

import java.net.SocketAddress;

/**
 * FailoverTransportClient
 * author: gaohaoxiang
 * <p>
 * date: 2018/10/30
 */
public class FailoverTransportClient implements TransportClient {

    private final TransportClient delegate;
    private final TransportConfig config;
    private final EventBus<TransportEvent> transportEventBus;

    public FailoverTransportClient(TransportClient delegate, TransportConfig config, EventBus<TransportEvent> transportEventBus) {
        this.delegate = delegate;
        this.config = config;
        this.transportEventBus = transportEventBus;
    }

    @Override
    public Transport createTransport(String address) throws TransportException {
        return this.createTransport(address, -1);
    }

    @Override
    public Transport createTransport(String address, long connectionTimeout) throws TransportException {
        return this.createTransport(TransportClientSupport.createInetSocketAddress(address), connectionTimeout);
    }

    @Override
    public Transport createTransport(SocketAddress address) throws TransportException {
        return this.createTransport(address, -1);
    }

    @Override
    public Transport createTransport(SocketAddress address, long connectionTimeout) throws TransportException {
        ChannelTransport transport = (ChannelTransport) delegate.createTransport(address, connectionTimeout);
        return new FailoverChannelTransport(transport, address, connectionTimeout, delegate, config, transportEventBus);
    }

    @Override
    public void addListener(EventListener<TransportEvent> listener) {
        delegate.addListener(listener);
    }

    @Override
    public void removeListener(EventListener<TransportEvent> listener) {
        delegate.removeListener(listener);
    }

    @Override
    public void start() throws Exception {
        delegate.start();
    }

    @Override
    public void stop() {
        delegate.stop();
    }

    @Override
    public boolean isStarted() {
        return delegate.isStarted();
    }
}