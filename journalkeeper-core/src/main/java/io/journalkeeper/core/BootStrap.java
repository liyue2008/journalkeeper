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
import io.journalkeeper.utils.threads.NamedThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * @author LiYue
 * Date: 2019-03-25
 */
public class BootStrap implements ClusterAccessPoint {
    private static final Logger logger = LoggerFactory.getLogger(BootStrap.class);
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
    private final boolean isExecutorProvided;
    private ScheduledExecutorService serverScheduledExecutor, clientScheduledExecutor;
    private ExecutorService serverAsyncExecutor, clientAsyncExecutor;
    private RaftClient client = null;
    private AdminClient adminClient = null;
    private RaftClient localClient = null;
    private AdminClient localAdminClient = null;



    private BootStrap(RaftServer.Roll roll, List<URI> servers, StateFactory stateFactory,
                      JournalEntryParser journalEntryParser,
                      ExecutorService clientAsyncExecutor,
                      ScheduledExecutorService clientScheduledExecutor,
                      ExecutorService serverAsyncExecutor,
                      ScheduledExecutorService serverScheduledExecutor,
                      Properties properties) {
        this.stateFactory = stateFactory;
        if (properties == null) {
            properties = new Properties();
        }
        this.properties = properties;
        this.roll = roll;
        this.rpcAccessPointFactory = ServiceSupport.load(RpcAccessPointFactory.class);
        this.isExecutorProvided = (
                clientAsyncExecutor != null ||
                        clientScheduledExecutor != null ||
                        serverAsyncExecutor != null ||
                        serverScheduledExecutor != null);
        if (null == journalEntryParser) {
            journalEntryParser = new DefaultJournalEntryParser();
        }
        this.journalEntryParser = journalEntryParser;
        this.clientAsyncExecutor = clientAsyncExecutor;
        this.serverAsyncExecutor = serverAsyncExecutor;
        this.clientScheduledExecutor = clientScheduledExecutor;
        this.serverScheduledExecutor = serverScheduledExecutor;
        this.server = createServer();
        this.servers = servers;
    }

    private RaftServerActor createServer() {
        if (null == serverScheduledExecutor && !isExecutorProvided) {
            this.serverScheduledExecutor = Executors.newScheduledThreadPool(SCHEDULE_EXECUTOR_QUEUE_SIZE, new NamedThreadFactory("JournalKeeper-Server-Scheduled-Executor"));
        }
        if (null == serverAsyncExecutor && !isExecutorProvided) {
            this.serverAsyncExecutor = Executors.newCachedThreadPool(new NamedThreadFactory("JournalKeeper-Server-Async-Executor"));
        }

        if (null != roll) {
            return new RaftServerActor(roll, stateFactory, journalEntryParser, properties);
//            return new Server(roll, stateFactory, journalEntryParser, serverScheduledExecutor, serverAsyncExecutor, properties);
        }
        return null;
    }

    @Override
    public RaftClient getRaftClient() {
        if (null == client) {
            RemoteClientRpc clientRpc = createRemoteClientRpc();
            client = new DefaultRaftClient(clientRpc, properties);
        }
        return client;
    }

    @Override
    public RaftClient getLocalRaftClient() {

        if (null == localClient) {
            LocalClientRpc clientRpc = createLocalClientRpc();
            localClient = new DefaultRaftClient(clientRpc, properties);
        }
        return localClient;
    }

    private LocalClientRpc createLocalClientRpc() {
        if (null == clientAsyncExecutor && !isExecutorProvided) {
            this.clientAsyncExecutor = Executors.newCachedThreadPool(new NamedThreadFactory("JournalKeeper-Client-Async-Executor"));
        }
        if (null == clientScheduledExecutor && !isExecutorProvided) {
            this.clientScheduledExecutor = Executors.newScheduledThreadPool(SCHEDULE_EXECUTOR_QUEUE_SIZE, new NamedThreadFactory("JournalKeeper-Client-Scheduled-Executor"));
        }

        if (this.server != null) {
            return new LocalClientRpc(server.getServerRpc(), remoteRetryPolicy, clientScheduledExecutor);
        } else {
            throw new IllegalStateException("No local server!");
        }
    }

    private RemoteClientRpc createRemoteClientRpc() {
        if (null == clientAsyncExecutor && !isExecutorProvided) {
            this.clientAsyncExecutor = Executors.newCachedThreadPool(new NamedThreadFactory("JournalKeeper-Client-Async-Executor"));
        }
        if (null == clientScheduledExecutor && !isExecutorProvided) {
            this.clientScheduledExecutor = Executors.newScheduledThreadPool(SCHEDULE_EXECUTOR_QUEUE_SIZE, new NamedThreadFactory("JournalKeeper-Client-Scheduled-Executor"));
        }

        ClientServerRpcAccessPoint clientServerRpcAccessPoint = rpcAccessPointFactory.createClientServerRpcAccessPoint(this.properties);
        RemoteClientRpc clientRpc;
        if (this.server == null) {
            clientRpc = new RemoteClientRpc(getServersForClient(), clientServerRpcAccessPoint, remoteRetryPolicy, clientAsyncExecutor, clientScheduledExecutor);
        } else {
            clientServerRpcAccessPoint = new LocalDefaultRpcAccessPoint(server.getServerRpc(), clientServerRpcAccessPoint);
            clientRpc = new RemoteClientRpc(getServersForClient(), clientServerRpcAccessPoint, remoteRetryPolicy, clientAsyncExecutor, clientScheduledExecutor);
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

        if (!isExecutorProvided) {
            shutdownExecutorService(serverScheduledExecutor);
            shutdownExecutorService(serverAsyncExecutor);
            shutdownExecutorService(clientScheduledExecutor);
            shutdownExecutorService(clientAsyncExecutor);
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
            adminClient = new DefaultAdminClient(clientRpc, properties);
        }
        return adminClient;
    }

    @Override
    public AdminClient getLocalAdminClient() {
        if (null == localAdminClient) {
            ClientRpc clientRpc = createLocalClientRpc();
            localAdminClient = new DefaultAdminClient(clientRpc, properties);
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

    private void shutdownExecutorService(ExecutorService executor) {
        if (null != executor) {
            executor.shutdown();
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
        private ExecutorService clientAsyncExecutor;
        private ScheduledExecutorService clientScheduledExecutor;
        private ExecutorService serverAsyncExecutor;
        private ScheduledExecutorService serverScheduledExecutor;
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

        public Builder clientAsyncExecutor(ExecutorService clientAsyncExecutor) {
            this.clientAsyncExecutor = clientAsyncExecutor;
            return this;
        }

        public Builder clientScheduledExecutor(ScheduledExecutorService clientScheduledExecutor) {
            this.clientScheduledExecutor = clientScheduledExecutor;
            return this;
        }

        public Builder serverAsyncExecutor(ExecutorService serverAsyncExecutor) {
            this.serverAsyncExecutor = serverAsyncExecutor;
            return this;
        }

        public Builder serverScheduledExecutor(ScheduledExecutorService serverScheduledExecutor) {
            this.serverScheduledExecutor = serverScheduledExecutor;
            return this;
        }

        public Builder properties(Properties properties) {
            this.properties = properties;
            return this;
        }

        public BootStrap build() {
            return new BootStrap(roll, servers, stateFactory, journalEntryParser, clientAsyncExecutor, clientScheduledExecutor, serverAsyncExecutor, serverScheduledExecutor, properties);
        }
    }
}
