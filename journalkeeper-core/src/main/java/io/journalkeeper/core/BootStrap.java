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

import io.journalkeeper.core.api.AdminClient;
import io.journalkeeper.core.api.ClusterAccessPoint;
import io.journalkeeper.core.api.JournalEntryParser;
import io.journalkeeper.core.api.RaftClient;
import io.journalkeeper.core.api.RaftServer;
import io.journalkeeper.core.api.StateFactory;
import io.journalkeeper.core.client.ClientRpc;
import io.journalkeeper.core.client.DefaultAdminClient;
import io.journalkeeper.core.client.DefaultRaftClient;
import io.journalkeeper.core.client.LocalClientRpc;
import io.journalkeeper.core.client.RemoteClientRpc;
import io.journalkeeper.core.entry.DefaultJournalEntryParser;
import io.journalkeeper.core.raft.RaftServerActor;
import io.journalkeeper.rpc.RpcAccessPointFactory;
import io.journalkeeper.rpc.RpcException;
import io.journalkeeper.rpc.client.ClientServerRpcAccessPoint;
import io.journalkeeper.utils.retry.ExponentialRetryPolicy;
import io.journalkeeper.utils.retry.RetryPolicy;
import io.journalkeeper.utils.spi.ServiceSupport;

import java.net.URI;
import java.util.List;
import java.util.Properties;

/**
 * @author LiYue
 * Date: 2019-03-25
 */
public class BootStrap implements ClusterAccessPoint {
    private final static int SCHEDULE_EXECUTOR_QUEUE_SIZE = 128;

    private final StateFactory stateFactory;

    public Properties getProperties() {
        return properties;
    }

    private final Properties properties;
    private final RaftServer.Roll roll;
    private final RpcAccessPointFactory rpcAccessPointFactory;
    private final List<URI> servers;
    private final RaftServerActor server;
    private final JournalEntryParser journalEntryParser;
    private final RetryPolicy remoteRetryPolicy =
            new ExponentialRetryPolicy(10L, 3000L, 10);
    private RaftClient client = null;
    private AdminClient adminClient = null;
    private RaftClient localClient = null;
    private AdminClient localAdminClient = null;



    private BootStrap(RaftServer.Roll roll, List<URI> servers, StateFactory stateFactory,
                      JournalEntryParser journalEntryParser,
                      Properties properties) {
        this.stateFactory = stateFactory;
        if (properties == null) {
            properties = new Properties();
        }
        this.properties = properties;
        this.roll = roll;
        this.rpcAccessPointFactory = ServiceSupport.load(RpcAccessPointFactory.class);

        if (null == journalEntryParser) {
            journalEntryParser = new DefaultJournalEntryParser();
        }
        this.journalEntryParser = journalEntryParser;

        this.server = createServer();
        this.servers = servers;
    }

    private RaftServerActor createServer() {

        if (null != roll) {
            return new RaftServerActor(roll, stateFactory, journalEntryParser, properties);
        }
        return null;
    }

    @Override
    public RaftClient getRaftClient() {
        if (null == client) {
            RemoteClientRpc clientRpc = createRemoteClientRpc();
            client = new DefaultRaftClient(clientRpc);
        }
        return client;
    }

    @Override
    public RaftClient getLocalRaftClient() {

        if (null == localClient) {
            LocalClientRpc clientRpc = createLocalClientRpc();
            localClient = new DefaultRaftClient(clientRpc);
        }
        return localClient;
    }

    private LocalClientRpc createLocalClientRpc() {

        if (this.server != null) {
            return new LocalClientRpc(server.getServerRpc(), remoteRetryPolicy);
        } else {
            throw new IllegalStateException("No local server!");
        }
    }

    private RemoteClientRpc createRemoteClientRpc() {

        ClientServerRpcAccessPoint clientServerRpcAccessPoint = rpcAccessPointFactory.createClientServerRpcAccessPoint(this.properties);
        RemoteClientRpc clientRpc;
        if (this.server == null) {
            clientRpc = new RemoteClientRpc(getServersForClient(), clientServerRpcAccessPoint, remoteRetryPolicy);
        } else {
            clientServerRpcAccessPoint = new LocalDefaultRpcAccessPoint(server.getServerRpc(), clientServerRpcAccessPoint);
            clientRpc = new RemoteClientRpc(getServersForClient(), clientServerRpcAccessPoint, remoteRetryPolicy);
            clientRpc.setPreferredServer(server.serverUri());
        }
        return clientRpc;
    }

    public void shutdown() {

        if (null != client) {
            client.stop();
        }
        if (null != adminClient) {
            adminClient.stop();
        }

        if (null != server) {
            server.stop();

        }
    }

    @Override
    public RaftServer getServer() {
        return server;
    }

    @Override
    public AdminClient getAdminClient() {
        if (null == adminClient) {
            ClientRpc clientRpc = createRemoteClientRpc();
            adminClient = new DefaultAdminClient(clientRpc);
        }
        return adminClient;
    }

    @Override
    public AdminClient getLocalAdminClient() {
        if (null == localAdminClient) {
            ClientRpc clientRpc = createLocalClientRpc();
            localAdminClient = new DefaultAdminClient(clientRpc);
        }
        return localAdminClient;
    }


    private List<URI> getServersForClient() {
        if (null == server) {
            return servers;
        } else {
            try {
                return server.getServerRpc().getServers().get().getClusterConfiguration().getVoters();
            } catch (Throwable e) {
                throw new RpcException(e);
            }
        }
    }

    public JournalEntryParser getJournalEntryParser() {
        return this.journalEntryParser;
    }

    public static Builder builder() {
        return new Builder();

    }

    public static class Builder {
        private RaftServer.Roll roll;
        private StateFactory stateFactory;
        private JournalEntryParser journalEntryParser;
        private Properties properties;
        private List<URI> servers;

        private Builder() {

        }

        public Builder roll(RaftServer.Roll roll) {
            this.roll = roll;
            return this;
        }

        public Builder servers(List<URI> servers) {
            this.servers = servers;
            return this;
        }

        public Builder stateFactory(StateFactory stateFactory) {
            this.stateFactory = stateFactory;
            return this;
        }

        public Builder journalEntryParser(JournalEntryParser journalEntryParser) {
            this.journalEntryParser = journalEntryParser;
            return this;
        }

        public Builder properties(Properties properties) {
            this.properties = properties;
            return this;
        }

        public BootStrap build() {
            return new BootStrap(roll, servers, stateFactory, journalEntryParser, properties);
        }
    }
}
