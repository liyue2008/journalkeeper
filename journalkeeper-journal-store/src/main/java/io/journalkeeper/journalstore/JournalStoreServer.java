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
package io.journalkeeper.journalstore;

import io.journalkeeper.core.BootStrap;
import io.journalkeeper.core.api.AdminClient;
import io.journalkeeper.core.api.JournalEntryParser;
import io.journalkeeper.core.api.RaftServer;
import io.journalkeeper.core.entry.DefaultJournalEntryParser;
import io.journalkeeper.utils.state.StateServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

/**
 * @author LiYue
 * Date: 2019-05-09
 */
public class JournalStoreServer implements StateServer {
    private static final Logger logger = LoggerFactory.getLogger(JournalStoreServer.class);
    private final BootStrap bootStrap;

    public JournalStoreServer(Properties properties) {
        this(RaftServer.Roll.VOTER, new DefaultJournalEntryParser(), properties);
    }

    public JournalStoreServer(RaftServer.Roll roll, JournalEntryParser journalEntryParser, Properties properties) {
        bootStrap = BootStrap.builder()
                .roll(roll)
                .stateFactory(new JournalStoreStateFactory(journalEntryParser))
                .journalEntryParser(journalEntryParser)
                .properties(properties)
                .build();
    }

    public JournalStoreServer(
            RaftServer.Roll roll,
            JournalEntryParser journalEntryParser,
            ExecutorService clientAsyncExecutor,
            ScheduledExecutorService clientScheduledExecutor,
            ExecutorService serverAsyncExecutor,
            ScheduledExecutorService serverScheduledExecutor,
            Properties properties) {
        bootStrap = BootStrap.builder()
                .roll(roll)
                .stateFactory(new JournalStoreStateFactory(journalEntryParser))
                .journalEntryParser(journalEntryParser)
                .clientAsyncExecutor(clientAsyncExecutor)
                .clientScheduledExecutor(clientScheduledExecutor)
                .serverAsyncExecutor(serverAsyncExecutor)
                .serverScheduledExecutor(serverScheduledExecutor)
                .properties(properties).build();

    }

    public void init(URI uri, List<URI> voters) throws IOException {
        bootStrap.getServer().init(uri, voters);
    }

    public void init(URI uri, List<URI> voters, Set<Integer> partitions) throws IOException {
        bootStrap.getServer().init(uri, voters, partitions);
    }

    public void init(URI uri, List<URI> voters, Set<Integer> partitions, URI preferredleader) throws IOException {
        bootStrap.getServer().init(uri, voters, partitions, preferredleader);
    }

    public boolean isInitialized() {
        return bootStrap.getServer().isInitialized();
    }

    public void recover() throws IOException {
        bootStrap.getServer().recover();
    }


    @Override
    public void start() {
        bootStrap.getServer().start();
    }

    @Override
    public void stop() {
        bootStrap.shutdown();
    }

    @Override
    public ServerState serverState() {
        return bootStrap.getServer().serverState();
    }

    public JournalStoreClient createClient() {
        return new JournalStoreClient(bootStrap.getRaftClient(), bootStrap.getJournalEntryParser());
    }

    public AdminClient getAdminClient() {
        return bootStrap.getAdminClient();
    }

    public JournalStoreClient createLocalClient() {
        return new JournalStoreClient(bootStrap.getLocalRaftClient(), bootStrap.getJournalEntryParser());
    }

    public AdminClient getLocalAdminClient() {
        return bootStrap.getLocalAdminClient();
    }

    public URI serverUri() {
        return bootStrap.getServer().serverUri();
    }
}
