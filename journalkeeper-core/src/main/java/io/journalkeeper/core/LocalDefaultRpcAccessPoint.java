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
package io.journalkeeper.core;

import io.journalkeeper.rpc.client.ClientServerRpc;
import io.journalkeeper.rpc.client.ClientServerRpcAccessPoint;
import io.journalkeeper.rpc.server.ServerRpc;

import java.net.URI;

/**
 * 优先访问本地的ClientServerRpc接入点
 * @author LiYue
 * Date: 2019-03-25
 */
public class LocalDefaultRpcAccessPoint implements ClientServerRpcAccessPoint {
    private final ServerRpc server;
    private final ClientServerRpcAccessPoint clientServerRpcAccessPoint;

    public LocalDefaultRpcAccessPoint(ServerRpc server, ClientServerRpcAccessPoint clientServerRpcAccessPoint) {
        this.server = server;
        this.clientServerRpcAccessPoint = clientServerRpcAccessPoint;
    }

    @Override
    public ClientServerRpc getClintServerRpc(URI uri) {
        if (null == uri) {
            throw new IllegalArgumentException("URI can not be null!");
        }
        return uri.equals(server.serverUri()) ? server : clientServerRpcAccessPoint.getClintServerRpc(uri);
    }

    @Override
    public void stop() {
        clientServerRpcAccessPoint.stop();
    }

}
