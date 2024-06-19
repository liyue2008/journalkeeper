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


import io.journalkeeper.rpc.remoting.service.LifeCycle;

import java.net.InetSocketAddress;

/**
 * 通信服务
 * author: gaohaoxiang
 * <p>
 * date: 2018/8/13
 */
public interface TransportServer extends LifeCycle {

    InetSocketAddress getSocketAddress();

    @SuppressWarnings("SameReturnValue")
    boolean isSSLServer();
}