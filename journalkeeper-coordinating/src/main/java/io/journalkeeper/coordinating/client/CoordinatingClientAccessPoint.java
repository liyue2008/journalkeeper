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
package io.journalkeeper.coordinating.client;

import io.journalkeeper.core.BootStrap;
import io.journalkeeper.core.api.RaftClient;
import io.journalkeeper.core.easy.JkClient;

import java.net.URI;
import java.util.List;
import java.util.Properties;

/**
 * CoordinatingClientAccessPoint
 * author: gaohaoxiang
 *
 * date: 2019/6/10
 */
public class CoordinatingClientAccessPoint {

    private final Properties config;

    public CoordinatingClientAccessPoint(Properties config) {
        this.config = config;
    }

    public CoordinatingClient createClient(List<URI> servers) {
        BootStrap bootStrap = BootStrap.builder().servers(servers).properties(config).build();
        RaftClient client =
                bootStrap.getClient();
        return new CoordinatingClient(new JkClient(client));
    }
}