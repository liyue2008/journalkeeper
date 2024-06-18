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

import io.journalkeeper.core.api.AdminClient;
import io.journalkeeper.core.api.JournalEntry;
import io.journalkeeper.core.api.JournalEntryParser;
import io.journalkeeper.core.api.ResponseConfig;
import io.journalkeeper.core.api.UpdateRequest;
import io.journalkeeper.core.api.transaction.TransactionContext;
import io.journalkeeper.core.entry.DefaultJournalEntryParser;
import io.journalkeeper.core.entry.JournalEntryParseSupport;
import io.journalkeeper.exceptions.ServerBusyException;
import io.journalkeeper.rpc.client.UpdateClusterStateRequest;
import io.journalkeeper.utils.format.Format;
import io.journalkeeper.utils.net.NetworkingUtils;
import io.journalkeeper.utils.test.ByteUtils;
import io.journalkeeper.utils.test.TestPathUtils;
import io.journalkeeper.utils.threads.NamedThreadFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.internal.util.collections.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.journalkeeper.core.api.RaftJournal.DEFAULT_PARTITION;

/**
 * @author LiYue
 * Date: 2019-04-25
 */
public class JournalStoreTest {
    private static final Logger logger = LoggerFactory.getLogger(JournalStoreTest.class);
    private Path base = null;

    @Before
    public void before() throws IOException {
        base = TestPathUtils.prepareBaseDir();
    }

    @After
    public void after() {
        TestPathUtils.destroyBaseDir(base.toFile());
    }

    @Test
    public void writeReadTest() throws Exception {
        // 单分区和多分区
        List<Set<Integer>> partitionsList = Arrays.asList(
                Sets.newSet(0, 1, 2, 3, 4)
        );

        // Raft 节点数量
        List<Integer> nodesList = Arrays.asList(
                1, 3
        );

        // 响应类型

        List<ResponseConfig> responseConfigList = Arrays.asList(
          ResponseConfig.RECEIVE, ResponseConfig.PERSISTENCE, ResponseConfig.REPLICATION, ResponseConfig.ALL
        );

        Properties properties = new Properties();
//        properties.setProperty("enable_metric", String.valueOf(true));
//        properties.setProperty("print_metric_interval_sec", String.valueOf(5));
        properties.setProperty("performance_mode", "true");

        for (Set<Integer> partitions : partitionsList) {
            for (Integer nodes : nodesList) {
                for (ResponseConfig responseConfig : responseConfigList) {
                    writeReadTest(nodes, partitions, 1024, 10, 10L * 1024 * 1024, false, responseConfig, true, properties);
                    after();
                    before();
                    writeReadTest(nodes, partitions, 1024, 10, 10L * 1024 * 1024, true, responseConfig, true, properties);
                    after();
                    before();
                }
            }
        }

    }
    @Ignore
    @Test
    public void tempTest() throws Exception {
        // 单分区和多分区
        List<Set<Integer>> partitionsList = Arrays.asList(
                Sets.newSet(0, 1, 2, 3, 4)
        );

        Properties properties = new Properties();
        properties.setProperty("enable_metric", String.valueOf(true));
        properties.setProperty("print_metric_interval_sec", String.valueOf(5));
        properties.setProperty("performance_mode", "true");
        writeReadTest(3, Sets.newSet(0, 1, 2, 3, 4), 1024, 10, 10L * 1024 * 1024, true, ResponseConfig.REPLICATION, true, properties);
    }

    @Test
    public void maxPositionTest() throws IOException, ExecutionException, InterruptedException {
        List<JournalStoreServer> servers = createServers(1, base);
        try {
            JournalStoreClient client = servers.get(0).createClient();
            Map<Integer, Long> maxIndices = client.maxIndices().get();
            Assert.assertEquals(0L, (long) maxIndices.get(0));
        } finally {
            stopServers(servers);
        }
    }

    @Test
    public void scalePartitionTest() throws IOException, ExecutionException, InterruptedException {
        List<JournalStoreServer> servers = createServers(1, base);
        JournalStoreClient client = servers.get(0).createClient();
        Set<Integer> readPartitions = client.listPartitions().get();
        Assert.assertEquals(Collections.singleton(DEFAULT_PARTITION), readPartitions);
        stopServers(servers);
        after();
        before();

        Set<Integer> partitions = Stream.of(2, 3, 4, 99, 8).collect(Collectors.toSet());
        servers = createServers(1, base, partitions);

        client = servers.get(0).createClient();
        client.waitForClusterReady();
        readPartitions = client.listPartitions().get();
        Assert.assertEquals(partitions, readPartitions);
        partitions = Stream.of(2, 3, 7, 8).collect(Collectors.toSet());
        AdminClient adminClient = servers.get(0).getAdminClient();
        adminClient.scalePartitions(partitions).get();
        Set<Integer> readIntPartitions = client.listPartitions().get();
        Assert.assertEquals(partitions, readIntPartitions);
        stopServers(servers);

        after();
        before();

        partitions = Stream.of(2, 3, 4, 99, 8).collect(Collectors.toSet());
        URI uri = URI.create("jk://localhost:" + NetworkingUtils.findRandomOpenPortOnAllLocalInterfaces());
        Path workingDir = base.resolve("server-recover");
        Properties properties = new Properties();
        properties.setProperty("working_dir", workingDir.toString());
        JournalStoreServer journalStoreServer = new JournalStoreServer(properties);
        journalStoreServer.init(uri, Collections.singletonList(uri), partitions);
        journalStoreServer.recover();
        journalStoreServer.start();

        client = journalStoreServer.createLocalClient();
        client.waitForClusterReady();
        readPartitions = client.listPartitions().get();
        Assert.assertEquals(partitions, readPartitions);

        journalStoreServer.stop();

        journalStoreServer = new JournalStoreServer(properties);
        journalStoreServer.recover();
        journalStoreServer.start();

        client = journalStoreServer.createLocalClient();
        client.waitForClusterReady();
        readPartitions = client.listPartitions().get();
        Assert.assertEquals(partitions, readPartitions);

        journalStoreServer.stop();


    }

    @Test
    public void queryIndexTest() throws IOException, ExecutionException, InterruptedException, TimeoutException {
        JournalEntryParser journalEntryParser = new DefaultJournalEntryParser();
        JournalStoreServer server = createServers(1, base).get(0);

        JournalStoreClient client = server.createLocalClient();
        client.waitForClusterReady();

        long[] timestamps = new long[]{10, 11, 12, 100, 100, 101, 105, 110, 2000};
        long[] indices = new long[timestamps.length];
        for (int i = 0; i < timestamps.length; i++) {
            long timestamp = timestamps[i];
            byte[] payload = new byte[128];
            JournalEntry entry = journalEntryParser.createJournalEntry(payload);
            byte[] rawEntry = entry.getSerializedBytes();
            JournalEntryParseSupport.setLong(ByteBuffer.wrap(rawEntry),
                    JournalEntryParseSupport.TIMESTAMP, timestamp);

            long index = client.append(0, 1, rawEntry, true, ResponseConfig.REPLICATION).get();
            indices[i] = index;
        }

        Assert.assertEquals(indices[0], client.queryIndex(0, 8L).get().longValue());
        Assert.assertEquals(indices[0], client.queryIndex(0, 10L).get().longValue());
        Assert.assertEquals(indices[2], client.queryIndex(0, 80L).get().longValue());
        Assert.assertEquals(indices[3], client.queryIndex(0, 100L).get().longValue());
        Assert.assertEquals(indices[8], client.queryIndex(0, 2000L).get().longValue());
        Assert.assertEquals(indices[8], client.queryIndex(0, 9000L).get().longValue());

        server.stop();


    }
    @Ignore
    @Test
    public void writePerformanceTest() throws Exception {
        Set<Integer> partitions = Sets.newSet(0, 1, 2, 3, 4);
        Properties properties = new Properties();
        properties.setProperty("cache_requests", String.valueOf(10L * 1024));
        properties.setProperty("enable_events", String.valueOf(false));
        properties.setProperty("print_state_interval_sec", String.valueOf(5));
        properties.setProperty("enable_metric", String.valueOf(true));
        properties.setProperty("print_metric_interval_sec", String.valueOf(5));
        properties.setProperty("replication_batch_size", String.valueOf( 1024));

        writeReadTest(2, partitions,1024, 10, 2L * 1024 * 1024 * 1024, true, ResponseConfig.RECEIVE, false, properties);
    }

    /**
     * 读写测试
     * @param nodes 节点数量
     * @param partitions 分区
     * @param entrySize 每条数据大小
     * @param batchSize 每批数据条数
     * @param totalBytes 总计写入字节数
     * @param responseConfig 响应类型
     */
    private void writeReadTest(int nodes, Set<Integer> partitions, int entrySize, int batchSize, long totalBytes, boolean async, ResponseConfig responseConfig, boolean enableRead, Properties properties) throws Exception {
        logger.info("Start write read test, nodes: {}, partitions: {}, entrySize: {}, batchSize: {}, totalBytes: {}, response config: {}, async: {}.",
                nodes, partitions,entrySize, batchSize,Format.formatSize(totalBytes), responseConfig, async);
        List<JournalStoreServer> servers = createServers(nodes, base, partitions,properties);
        JournalEntryParser journalEntryParser = new DefaultJournalEntryParser();
        List<Integer> partitionList = new ArrayList<>(partitions);
        byte[] rawEntryBytes = ByteUtils.createFixedSizeBytes(entrySize);

        try {
            JournalStoreClient client;
            if(servers.size() == 1) {
                client = servers.get(0).createLocalClient();
            } else {
                client = servers.get(0).createClient();
            }
            AdminClient adminClient = servers.get(0).getAdminClient();
            client.waitForClusterReady();
            long startApplied = adminClient.getServerStatus(adminClient.getClusterConfiguration().get().getLeader()).get().getLastApplied();
            List<UpdateRequest> entries = new ArrayList<>(batchSize);
            for (int i = 0; i < batchSize; i++) {
                byte [] entry = journalEntryParser.createJournalEntry(rawEntryBytes).getSerializedBytes();
                int partition = partitionList.get(i % partitionList.size());
                UpdateRequest updateRequest = new UpdateRequest(entry, partition, 1);
                entries.add(updateRequest);
            }
            UpdateClusterStateRequest request = new UpdateClusterStateRequest(entries, true, responseConfig);
            long bytesOfRequest = request.getRequests().stream().mapToLong(r -> r.getEntry().length).sum();
            Thread.sleep(100);

            long t0 = System.nanoTime();

            long currentBytes = 0L;
            long writeCount = 0L;

            while (currentBytes < totalBytes) {
                long start = System.nanoTime();
                CompletableFuture<List<Long>> resultFuture = client.append(entries, true, responseConfig);
                if(!async && responseConfig != ResponseConfig.ONE_WAY) {
                    resultFuture.get();
                }
//                logger.info("takes: {} ns.", System.nanoTime() - start);
                currentBytes += bytesOfRequest;
                writeCount += batchSize;
            }

            long t1 = System.nanoTime();
            long takesMs = (t1 - t0) / 1000000;
            if(takesMs == 0) {
                takesMs = 1;
            }
            logger.info("Write finished, total write {}. " +
                            "Write takes: {}ms, {}ps, tps: {}.",
                    Format.formatSize(currentBytes),
                    takesMs,
                    Format.formatSize(1000L * currentBytes / takesMs),
                    1000L * writeCount / takesMs);
            URI leaderUri = adminClient.getClusterConfiguration().get().getLeader();

            if(async) {
                while (adminClient.getServerStatus(leaderUri).get().getLastApplied() < startApplied + writeCount) {
                    Thread.sleep(1);
                }
            }

            t1 = System.nanoTime();
            takesMs = (t1 - t0) / 1000000;
            if(takesMs == 0) {
                takesMs = 1;
            }
            logger.info("All entries applied, " +
                            "takes: {}ms, {}ps, tps: {}.",
                    takesMs,
                    Format.formatSize(1000L * currentBytes / takesMs),
                    1000L * writeCount / takesMs);

            // read
            if (enableRead) {
                t0 = System.nanoTime();
                Map<Integer, Long> maxIndices = client.maxIndices().get();
                for (int partition : partitions) {
                    long maxIndex = maxIndices.get(partition);
                    for (int i = 0; i < maxIndex; i += batchSize) {
                        int batchReadCount = Math.min((int) (maxIndex - i), batchSize);
                        List<JournalEntry> raftEntries = client.get(partition, i, batchReadCount).get();
                        for (JournalEntry journalEntry : raftEntries) {
                            Assert.assertEquals(partition, journalEntry.getPartition());
                            Assert.assertEquals(1, journalEntry.getBatchSize());
                            Assert.assertEquals(0, journalEntry.getOffset());
                            Assert.assertArrayEquals(rawEntryBytes, journalEntry.getPayload().getBytes());
                        }
                    }
                }
                t1 = System.nanoTime();
                takesMs = (t1 - t0) / 1000000;
                if(takesMs == 0) {
                    takesMs = 1;
                }
                logger.info("Read " +
                                "takes: {}ms, {}ps, tps: {}.",
                        takesMs,
                        Format.formatSize(1000L * currentBytes / takesMs),
                        1000L * writeCount / takesMs);
            }
        } finally {
            stopServers(servers);

        }

    }

    @Test
    public void batchWriteReadTest() throws Exception {
        int nodes = 3;
        Integer[] partitions = new Integer[]{0, 1, 2, 3, 4};
        int entrySize = 1024;
        int batchSize = 12;
        int batchCount = 1024;
        List<JournalStoreServer> servers = createServers(nodes, base, new HashSet<>(Arrays.asList(partitions)));
        try {
            JournalStoreClient client = servers.get(0).createClient();
            client.waitForClusterReady();

            List<byte[]> bytes = ByteUtils.createRandomSizeByteList(entrySize, batchCount);
            List<UpdateRequest> requests = new ArrayList<>(bytes.size());
            for (int i = 0; i < bytes.size(); i++) {
                int partition = partitions[i % partitions.length];
                requests.add(new UpdateRequest(bytes.get(i), partition, batchSize));
            }
            long t0 = System.nanoTime();
            List<Long> indices = client.append(requests).get();
            long t1 = System.nanoTime();
            logger.info("Replication finished. " +
                            "Write takes: {}ms, {}ps, tps: {}.",
                    (t1 - t0) / 1000000,
                    Format.formatSize(1000000000L * partitions.length * entrySize * batchCount / (t1 - t0)),
                    1000000000L * partitions.length * batchCount / (t1 - t0));


            // read

            t0 = System.nanoTime();

            for (int i = 0; i < batchCount; i++) {
                int partition = partitions[i % partitions.length];
                List<JournalEntry> raftEntries = client.get(partition, indices.get(i), batchSize).get();
                Assert.assertEquals(1, raftEntries.size());
                JournalEntry entry = raftEntries.get(0);
                Assert.assertEquals(partition, entry.getPartition());
                Assert.assertEquals(batchSize, entry.getBatchSize());
                Assert.assertEquals(0, entry.getOffset());
                Assert.assertArrayEquals(bytes.get(i), entry.getPayload().getBytes());
            }

            t1 = System.nanoTime();
            logger.info("Read finished. " +
                            "Takes: {}ms {}ps, tps: {}.",
                    (t1 - t0) / 1000000,
                    Format.formatSize(1000000000L * partitions.length * entrySize * batchCount / (t1 - t0)),
                    1000000000L * partitions.length * batchCount / (t1 - t0));
        } finally {
            stopServers(servers);

        }

    }

    @Test
    public void allResponseTest() throws Exception {
        int nodes = 5;
        int entrySize = 1024;
        int count = 512;
        List<JournalStoreServer> servers = createServers(nodes, base);
        try {
            JournalStoreClient client = servers.get(0).createClient();
            client.waitForClusterReady();
            logger.info("Cluster is ready.");
            byte[] rawEntries = ByteUtils.createFixedSizeBytes(entrySize);
            CompletableFuture[] futures = new CompletableFuture[count];
            for (int i = 0; i < count; i++) {
                futures[i] = client.append(0, 1, rawEntries, ResponseConfig.ALL);
            }

            CompletableFuture.allOf(futures).get();

            for (int i = 0; i < count; i++) {
                List<JournalEntry> raftEntries = client.get(0, i, 1).get();
                Assert.assertEquals(raftEntries.size(), 1);
                JournalEntry entry = raftEntries.get(0);
                Assert.assertEquals(0, entry.getPartition());
                Assert.assertEquals(1, entry.getBatchSize());
                Assert.assertEquals(0, entry.getOffset());
                Assert.assertArrayEquals(rawEntries, entry.getPayload().getBytes());
            }

        } finally {
            stopServers(servers);

        }

    }

    @Test
    public void transactionCommitTest() throws Exception {
        final int nodes = 3;
        final int entrySize = 1024;
        final int entryCount = 3;
        final Set<Integer> partitions = Collections.unmodifiableSet(
                new HashSet<>(Arrays.asList(0, 1, 2, 3, 4))
        );
        final Map<String, String> context = new HashMap<>();
        context.put("key", "val");

        List<byte[]> rawEntries = ByteUtils.createFixedSizeByteList(entrySize, entryCount);
        List<JournalStoreServer> servers = createServers(nodes, base, partitions);
        JournalStoreClient client = new JournalStoreClient(servers.stream().map(JournalStoreServer::serverUri).collect(Collectors.toList()), new Properties());
        client.waitForClusterReady();

        // Create transaction
        TransactionContext transactionContext = client.createTransaction(context).get();
        Assert.assertNotNull(transactionContext);
        Assert.assertNotNull(transactionContext.transactionId());
        Assert.assertEquals(context, transactionContext.context());

        // Send some transactional messages
        CompletableFuture[] futures = new CompletableFuture[rawEntries.size()];
        for (int i = 0; i < rawEntries.size(); i++) {
            int partition = i % partitions.size();
            futures[i] = client.append(transactionContext.transactionId(), rawEntries.get(i), partition, 1);
        }
        CompletableFuture.allOf(futures).get();

        // Verify no message is readable until the transaction was committed.
        Map<Integer, Long> maxIndices = client.maxIndices().get();
        for (Long maxIndex : maxIndices.values()) {
            Assert.assertEquals(0L, (long) maxIndex);
        }

        Collection<TransactionContext> openingTransactions = client.getOpeningTransactions().get();
        Assert.assertTrue(openingTransactions.contains(transactionContext));

        // Commit the transaction
        client.completeTransaction(transactionContext.transactionId(), true).get();

        openingTransactions = client.getOpeningTransactions().get();
        Assert.assertFalse(openingTransactions.contains(transactionContext));

        // Verify messages
        for (int i = 0; i < rawEntries.size(); i++) {
            int partition = i % partitions.size();
            int index = i / partitions.size();
            List<JournalEntry> journalEntries = client.get(partition, index, 1).get();
            Assert.assertEquals(1, journalEntries.size());
            JournalEntry journalEntry = journalEntries.get(0);
            byte[] readEntry = journalEntry.getPayload().getBytes();

            Assert.assertArrayEquals(rawEntries.get(i), readEntry);

        }

        stopServers(servers);

    }

    @Test
    public void transactionAbortTest() throws Exception {
        final int nodes = 3;
        final int entrySize = 1024;
        final int entryCount = 3;
        final Set<Integer> partitions = Collections.unmodifiableSet(
                new HashSet<>(Arrays.asList(0, 1, 2, 3, 4))
        );

        List<byte[]> rawEntries = ByteUtils.createFixedSizeByteList(entrySize, entryCount);
        List<JournalStoreServer> servers = createServers(nodes, base, partitions);
        JournalStoreClient client = new JournalStoreClient(servers.stream().map(JournalStoreServer::serverUri).collect(Collectors.toList()), new Properties());
        client.waitForClusterReady();

        // Create transaction
        TransactionContext transactionContext = client.createTransaction(null).get();
        Assert.assertNotNull(transactionContext);
        Assert.assertNotNull(transactionContext.transactionId());

        // Send some transactional messages
        CompletableFuture[] futures = new CompletableFuture[rawEntries.size()];
        for (int i = 0; i < rawEntries.size(); i++) {
            int partition = i % partitions.size();
            futures[i] = client.append(transactionContext.transactionId(), rawEntries.get(i), partition, 1);
        }
        CompletableFuture.allOf(futures).get();

        // Verify no message is readable until the transaction was committed.
        Map<Integer, Long> maxIndices = client.maxIndices().get();
        for (Long maxIndex : maxIndices.values()) {
            Assert.assertEquals(0L, (long) maxIndex);
        }

        Collection<TransactionContext> openingTransactions = client.getOpeningTransactions().get();
        Assert.assertTrue(openingTransactions.contains(transactionContext));

        // Commit the transaction
        client.completeTransaction(transactionContext.transactionId(), false).get();

        openingTransactions = client.getOpeningTransactions().get();
        Assert.assertFalse(openingTransactions.contains(transactionContext));

        // Verify messages
        maxIndices = client.maxIndices().get();
        for (Long maxIndex : maxIndices.values()) {
            Assert.assertEquals(0L, (long) maxIndex);
        }

        stopServers(servers);

    }


    @Test
    public void transactionFailSafeTest() throws Exception {
        final int nodes = 3;
        final int entrySize = 1024;
        final int entryCount = 10;
        final Map<String, String> context = new HashMap<>();
        context.put("key0", "val0");
        context.put("key1", "val1");
        context.put("key2", "val2");

        final Set<Integer> partitions = Collections.unmodifiableSet(
                new HashSet<>(Arrays.asList(0, 1, 2, 3, 4))
        );
        List<byte[]> rawEntries = ByteUtils.createFixedSizeByteList(entrySize, entryCount);
        List<JournalStoreServer> servers = createServers(nodes, base, partitions);
        JournalStoreClient client = new JournalStoreClient(servers.stream().map(JournalStoreServer::serverUri).collect(Collectors.toList()), new Properties());
        client.waitForClusterReady();

        // Create transaction
        TransactionContext transactionContext = client.createTransaction(context).get();
        Assert.assertNotNull(transactionContext);
        Assert.assertNotNull(transactionContext.transactionId());
        Assert.assertEquals(context, transactionContext.context());

        // Send some transactional messages
        CompletableFuture[] futures = new CompletableFuture[rawEntries.size() / 2];
        int i = 0;
        for (; i < rawEntries.size() / 2; i++) {
            int partition = i % partitions.size();
            futures[i] = client.append(transactionContext.transactionId(), rawEntries.get(i), partition, 1);
        }
        CompletableFuture.allOf(futures).get();

        // Stop current leader and wait until new leader is ready.
        AdminClient adminClient = servers.get(0).getAdminClient();
        URI leaderUri = adminClient.getClusterConfiguration().get().getLeader();
        JournalStoreServer leaderServer = servers.stream().filter(server -> leaderUri.equals(server.serverUri())).findAny().orElse(null);
        Assert.assertNotNull(leaderServer);

        leaderServer.stop();
        servers.remove(leaderServer);
        client.waitForClusterReady();

        // Send some transactional messages
        futures = new CompletableFuture[rawEntries.size() - rawEntries.size() / 2];
        for (; i < rawEntries.size(); i++) {
            int partition = i % partitions.size();
            futures[i - rawEntries.size() / 2] = client.append(transactionContext.transactionId(), rawEntries.get(i), partition, 1);
        }
        CompletableFuture.allOf(futures).get();


        // Verify no message is readable until the transaction was committed.
        Map<Integer, Long> maxIndices = client.maxIndices().get();
        for (Long maxIndex : maxIndices.values()) {
            Assert.assertEquals(0L, (long) maxIndex);
        }

        Collection<TransactionContext> openingTransactions = client.getOpeningTransactions().get();
        Assert.assertTrue(openingTransactions.contains(transactionContext));

        // Commit the transaction
        client.completeTransaction(transactionContext.transactionId(), true).get();

        openingTransactions = client.getOpeningTransactions().get();
        Assert.assertFalse(openingTransactions.contains(transactionContext));

        // Verify messages
        for (i = 0; i < rawEntries.size(); i++) {
            int partition = i % partitions.size();
            int index = i / partitions.size();
            List<JournalEntry> journalEntries = client.get(partition, index, 1).get();
            Assert.assertEquals(1, journalEntries.size());
            JournalEntry journalEntry = journalEntries.get(0);
            byte[] readEntry = journalEntry.getPayload().getBytes();

            Assert.assertArrayEquals(rawEntries.get(i), readEntry);

        }
        stopServers(servers);
    }

    @Test
    public void transactionTimeoutTest() throws Exception {
        final int nodes = 3;
        final int entrySize = 1024;
        final int entryCount = 3;
        final long transactionTimeoutMs = 5000L;
        final Set<Integer> partitions = Collections.unmodifiableSet(
                new HashSet<>(Arrays.asList(0, 1, 2, 3, 4))
        );
        List<byte[]> rawEntries = ByteUtils.createFixedSizeByteList(entrySize, entryCount);
        Properties properties = new Properties();
        properties.put("transaction_timeout_ms", String.valueOf(transactionTimeoutMs));
        List<JournalStoreServer> servers = createServers(nodes, base, partitions, properties);
        JournalStoreClient client = new JournalStoreClient(servers.stream().map(JournalStoreServer::serverUri).collect(Collectors.toList()), new Properties());
        client.waitForClusterReady();

        // Create transaction
        TransactionContext transactionContext = client.createTransaction(null).get();
        Assert.assertNotNull(transactionContext);

        // Send some transactional messages
        CompletableFuture<?>[] futures = new CompletableFuture[rawEntries.size()];
        for (int i = 0; i < rawEntries.size(); i++) {
            int partition = i % partitions.size();
            futures[i] = client.append(transactionContext.transactionId(), rawEntries.get(i), partition, 1);
        }
        CompletableFuture.allOf(futures).get();

        // wait for transaction timeout
        logger.info("Wait {} ms for transaction timeout...", transactionTimeoutMs * 2);
        Thread.sleep(transactionTimeoutMs * 2);

        Collection<TransactionContext> openingTransactions = client.getOpeningTransactions().get();
        Assert.assertFalse(openingTransactions.contains(transactionContext));

        stopServers(servers);

    }


    private void asyncWrite(Set<Integer> partitions, int batchSize, int batchCount, JournalStoreClient client, byte[] rawEntries) throws InterruptedException {
        ExecutorService executors = Executors.newFixedThreadPool(10, new NamedThreadFactory("ClientRetryThreads"));
        CountDownLatch latch = new CountDownLatch(partitions.size() * batchCount);
        final List<Throwable> exceptions = Collections.synchronizedList(new LinkedList<>());
        long t0 = System.nanoTime();
        // write
        for (int partition : partitions) {
            for (int i = 0; i < batchCount; i++) {
                asyncAppend(client, rawEntries, partition, batchSize, latch, executors, exceptions);
            }
        }
        long t1 = System.nanoTime();
        logger.info("Async write finished. " +
                        "Takes: {}ms {}ps.",
                (t1 - t0) / 1000000,
                Format.formatSize(1000000000L * partitions.size() * rawEntries.length * batchCount / (t1 - t0)));

        while (!latch.await(1, TimeUnit.SECONDS)) {
            Thread.yield();
        }
        logger.warn("Async write exceptions: {}.", exceptions.size());
        exceptions.stream().limit(10).forEach(e -> logger.warn("Exception: ", e));
        Assert.assertEquals(0, exceptions.size());

    }

    private void asyncAppend(JournalStoreClient client, byte[] rawEntries, int partition, int batchSize, CountDownLatch latch, ExecutorService executorService, List<Throwable> exceptions) {
        client.append(partition, batchSize, rawEntries, ResponseConfig.REPLICATION)
                .whenCompleteAsync((v, e) -> {

                    if (e instanceof CompletionException && e.getCause() instanceof ServerBusyException) {
                        logger.info("AbstractServer busy!");
                        Thread.yield();
                        asyncAppend(client, rawEntries, partition, batchSize, latch, executorService, exceptions);

                    } else {
                        if (null != e) {
                            logger.warn("Exception: ", e);
                            exceptions.add(e);
                        }
                        latch.countDown();
                    }
                });
    }

    private void syncWrite(Set<Integer> partitions, int batchSize, int batchCount, JournalStoreClient client, byte[] rawEntries) throws InterruptedException, ExecutionException {
        // write
        for (int partition : partitions) {
            for (int i = 0; i < batchCount; i++) {
                client.append(partition, batchSize, rawEntries, ResponseConfig.REPLICATION).get();
            }
        }
    }

    private void stopServers(List<JournalStoreServer> servers) {
        logger.info("Stop servers...");
        for (JournalStoreServer server : servers) {
            try {
                server.stop();
            } catch (Throwable t) {
                logger.warn("Stop server {} exception: ", server.serverUri(), t);
            }
        }
        logger.info("All servers were stopped.");
    }

    private List<JournalStoreServer> createServers(int nodes, Path path) throws IOException {
        return createServers(nodes, path, Collections.singleton(0));
    }

    private List<JournalStoreServer> createServers(int nodes, Path path, Set<Integer> partitions) throws IOException {
        return createServers(nodes, path, partitions, null);
    }

    private List<JournalStoreServer> createServers(int nodes, Path path, Set<Integer> partitions, Properties props) throws IOException {
//        logger.info("Create {} nodes servers", nodes);
        List<URI> serverURIs = new ArrayList<>(nodes);
        List<Properties> propertiesList = new ArrayList<>(nodes);
        for (int i = 0; i < nodes; i++) {
            URI uri = URI.create("jk://localhost:" + NetworkingUtils.findRandomOpenPortOnAllLocalInterfaces());
            serverURIs.add(uri);
            Path workingDir = path.resolve("server" + i);
            Properties properties = new Properties();
            properties.setProperty("working_dir", workingDir.toString());
            properties.setProperty("snapshot_step", "0");
            properties.setProperty("disable_logo", "true");
            properties.setProperty("server_name", String.valueOf(i));



            if (null != props) {
                properties.putAll(props);
            }
//            properties.setProperty("print_metric_interval_sec", String.valueOf(5));
//            properties.setProperty("enable_metric", String.valueOf(true));
            properties.setProperty("print_state_interval_sec", String.valueOf(5));
//            properties.setProperty("persistence.journal.file_data_size", String.valueOf(128 * 1024));
//            properties.setProperty("persistence.index.file_data_size", String.valueOf(16 * 1024));
            propertiesList.add(properties);
        }
        return createServers(serverURIs, propertiesList, partitions);
    }

    private List<JournalStoreServer> createServers(List<URI> serverURIs, List<Properties> propertiesList, Set<Integer> partitions) throws IOException {
        List<JournalStoreServer> journalStoreServers = new ArrayList<>(serverURIs.size());
        for (int i = 0; i < serverURIs.size(); i++) {
            JournalStoreServer journalStoreServer = new JournalStoreServer(propertiesList.get(i));
            journalStoreServers.add(journalStoreServer);
            journalStoreServer.init(serverURIs.get(i), serverURIs, partitions);
            journalStoreServer.recover();
            journalStoreServer.start();
        }
        return journalStoreServers;
    }
}
