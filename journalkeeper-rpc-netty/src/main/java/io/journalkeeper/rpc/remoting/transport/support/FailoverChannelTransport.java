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
import io.journalkeeper.rpc.remoting.event.TransportEventType;
import io.journalkeeper.rpc.remoting.transport.ChannelTransport;
import io.journalkeeper.rpc.remoting.transport.IpUtil;
import io.journalkeeper.rpc.remoting.transport.TransportAttribute;
import io.journalkeeper.rpc.remoting.transport.TransportClient;
import io.journalkeeper.rpc.remoting.transport.TransportState;
import io.journalkeeper.rpc.remoting.transport.command.Command;
import io.journalkeeper.rpc.remoting.transport.command.CommandCallback;
import io.journalkeeper.rpc.remoting.transport.config.TransportConfig;
import io.journalkeeper.rpc.remoting.transport.exception.TransportException;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.CompletableFuture;

/**
 * 故障切换通信
 * author: gaohaoxiang
 * <p>
 * date: 2018/9/3
 */
public class FailoverChannelTransport implements ChannelTransport {

    private static final int RETRY_DELAY = 1000;

    protected static final Logger logger = LoggerFactory.getLogger(FailoverChannelTransport.class);

    private volatile ChannelTransport delegate;
    private SocketAddress address;
    private final long connectionTimeout;
    private final TransportClient transportClient;
    private final EventBus<TransportEvent> transportEventBus;
    private volatile long lastReconnect;

    public FailoverChannelTransport(ChannelTransport delegate, SocketAddress address, long connectionTimeout, TransportClient transportClient, TransportConfig config, EventBus<TransportEvent> transportEventBus) {
        this.delegate = delegate;
        this.address = address;
        this.connectionTimeout = connectionTimeout;
        this.transportClient = transportClient;
        this.transportEventBus = transportEventBus;
    }

    @Override
    public Channel getChannel() {
        return delegate.getChannel();
    }

    @Override
    public Command sync(Command command, long timeout) throws TransportException {

        if (!checkChannel()) {
            throw TransportException.RequestErrorException.build(IpUtil.toAddress(delegate.getChannel().remoteAddress()));
        }
        return delegate.sync(command, timeout);
    }



    @Override
    public void async(final Command command, final long timeout, final CommandCallback callback) throws TransportException {

        if (!checkChannel()) {
            callback.onException(command, TransportException.RequestErrorException.build(IpUtil.toAddress(delegate.getChannel().remoteAddress())));
            return;
        }
        delegate.async(command, timeout, callback);
    }



    @Override
    public CompletableFuture<Command> async(Command command, long timeout) throws TransportException {
        return delegate.async(command, timeout);
    }


    @Override
    public void oneway(Command command, long timeout) throws TransportException {
        delegate.oneway(command, timeout);
    }


    @Override
    public void acknowledge(Command request, Command response, CommandCallback callback) throws TransportException {
        delegate.acknowledge(request, response, callback);
    }

    @Override
    public SocketAddress remoteAddress() {
        return delegate.remoteAddress();
    }

    @Override
    public TransportAttribute attr() {
        return delegate.attr();
    }

    @Override
    public void attr(TransportAttribute attribute) {
        delegate.attr(attribute);
    }

    @Override
    public TransportState state() {
        return delegate.state();
    }

    @Override
    public void stop() {
        delegate.stop();
    }


    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    protected boolean checkChannel() {
        if (isChannelActive()) {
            return true;
        }
        return tryReconnect();
    }

    protected boolean tryReconnect() {
        if (!isNeedReconnect()) {
            return false;
        }
        synchronized (this) {
            // 判断重连间隔
            if (isNeedReconnect()) {
                return reconnect();
            } else {
                return false;
            }
        }
    }

    protected boolean isChannelActive() {
        return delegate.getChannel().isActive();
    }

    protected boolean isNeedReconnect() {
        return System.currentTimeMillis() - lastReconnect > RETRY_DELAY;
    }

    protected boolean reconnect() {
        try {
            if (address instanceof InetSocketAddress) {
                address = new InetSocketAddress(((InetSocketAddress) address).getHostString(), ((InetSocketAddress) address).getPort());
            }
            ChannelTransport newTransport = (ChannelTransport) transportClient.createTransport(address, connectionTimeout);
            ChannelTransport delegate = this.delegate;
            this.delegate = newTransport;
            try {
                delegate.stop();
            } catch (Throwable t) {
                logger.warn("stop transport exception, transport: {}", newTransport, t);
            }
            logger.info("reconnect transport success, transport: {}", newTransport);
            transportEventBus.add(new TransportEvent(TransportEventType.RECONNECT, newTransport));
            return true;
        } catch (Throwable t) {
            logger.debug("reconnect transport exception, address: {}", address, t);
//            logger.warn("reconnect transport exception, address: {}", address);
            return false;
        } finally {
            lastReconnect = System.currentTimeMillis();
        }
    }
}