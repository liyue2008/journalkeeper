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
package io.journalkeeper.rpc.remoting.transport.config;

/**
 * 通信服务配置
 * author: gaohaoxiang
 * <p>
 * date: 2018/8/13
 */
public class ClientConfig extends TransportConfig {

    private boolean preferIPv6 = false;
    // 连接超时(毫秒)
    private int connectionTimeout = 1000;

    public ClientConfig() {
    }

    public boolean getPreferIPv6() {
        return preferIPv6;
    }

    public void setPreferIPv6(boolean preferIPv6) {
        this.preferIPv6 = preferIPv6;
    }

    public int getConnectionTimeout() {
        return connectionTimeout;
    }

    public void setConnectionTimeout(int connectionTimeout) {
        this.connectionTimeout = connectionTimeout;
    }
}