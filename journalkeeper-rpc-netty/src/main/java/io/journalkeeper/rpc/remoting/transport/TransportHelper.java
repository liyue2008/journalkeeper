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

import io.journalkeeper.rpc.remoting.transport.support.DefaultChannelTransport;
import io.netty.channel.Channel;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;

/**
 * 通信管理器
 * author: gaohaoxiang
 * <p>
 * date: 2018/8/14
 */
public class TransportHelper {

    private static final AttributeKey<ChannelTransport> TRANSPORT_CACHE_ATTR = AttributeKey.valueOf("TRANSPORT_CACHE");

    public static ChannelTransport getOrNewTransport(Channel channel, RequestBarrier requestBarrier) {
        Attribute<ChannelTransport> attr = channel.attr(TRANSPORT_CACHE_ATTR);
        ChannelTransport transport = attr.get();

        if (transport == null) {
            transport = newTransport(channel, requestBarrier);
            attr.set(transport);
        }

        return transport;
    }

    public static ChannelTransport newTransport(Channel channel, RequestBarrier requestBarrier) {
        return new DefaultChannelTransport(channel, requestBarrier);
    }

    public static void setTransport(Channel channel, ChannelTransport transport) {
        channel.attr(TRANSPORT_CACHE_ATTR).set(transport);
    }

    public static ChannelTransport getTransport(Channel channel) {
        return channel.attr(TRANSPORT_CACHE_ATTR).get();
    }
}