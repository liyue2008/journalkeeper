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
package io.journalkeeper.rpc.remoting.event;


import io.journalkeeper.rpc.remoting.transport.Transport;

/**
 * 通信事件
 * author: gaohaoxiang
 * <p>
 * date: 2018/8/15
 */
public class TransportEvent {

    // 类型
    private final TransportEventType type;
    // 通道
    private final Transport transport;

    public TransportEvent(TransportEventType type, Transport transport) {
        this.type = type;
        this.transport = transport;
    }

    public TransportEventType getType() {
        return this.type;
    }

    public Transport getTransport() {
        return this.transport;
    }
}