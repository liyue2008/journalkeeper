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
package io.journalkeeper.core.server;

import io.journalkeeper.core.BootStrap;
import io.journalkeeper.core.api.*;
import io.journalkeeper.core.easy.JkClient;
import io.journalkeeper.core.monitor.SimpleMonitorCollector;
import io.journalkeeper.core.state.KvStateFactory;
import io.journalkeeper.monitor.MonitorCollector;
import io.journalkeeper.monitor.MonitoredServer;
import io.journalkeeper.monitor.ServerMonitorInfo;
import io.journalkeeper.utils.spi.ServiceSupport;
import io.journalkeeper.utils.test.TestPathUtils;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/**
 * @author LiYue
 * Date: 2019-04-08
 */
public class KvTest {
    private static final Logger logger = LoggerFactory.getLogger(KvTest.class);

    @Test
    public void singleNodeTest() throws Exception {
        setGetTest("singleNodeTest", 1);
    }

    @Test
    public void tripleNodesTest() throws Exception {
        setGetTest("tripleNodesTest",3);
    }

    @Test
    public void fiveNodesTest() throws Exception {
        setGetTest("fiveNodesTest",5);
    }

    @Test
    public void sevenNodesTest() throws Exception {
        setGetTest("sevenNodesTest", 7);
    }

    @Test
    public void singleNodeRecoverTest() throws Exception {
        Path path = TestPathUtils.prepareBaseDir("singleNodeTest");
        BootStrap kvServer = createServers(1, path).get(0);
        JkClient kvClient = new JkClient(kvServer.getRaftClient());
        Assert.assertNull(kvClient.update("SET","key value", ResponseConfig.ALL).get());
        Assert.assertEquals("value", kvClient.query("GET", "key").get());
        kvServer.shutdown();

        kvServer = recoverServer("server0", path);
        kvServer.getAdminClient().waitForClusterReady(0L);
        kvClient = new JkClient(kvServer.getRaftClient());

        Assert.assertEquals("value", kvClient.query("GET", "key").get());
        kvServer.shutdown();
        TestPathUtils.destroyBaseDir(path.toFile());

    }

    @Test
    public void singleNodeAvailabilityTest() throws IOException, InterruptedException, ExecutionException, TimeoutException {
        availabilityTest(1);
    }

    @Test
    public void tripleNodesAvailabilityTest() throws IOException, InterruptedException, ExecutionException, TimeoutException {
        availabilityTest(3);
    }

    /**
     * 创建N个server，依次停掉每个server，再依次启动，验证集群可用性
     */
    private void availabilityTest(int nodes) throws IOException, InterruptedException, ExecutionException, TimeoutException {
        logger.info("{} nodes availability test.", nodes);
        Path path = TestPathUtils.prepareBaseDir("availabilityTest" + nodes);
        List<URI> serverURIs = new ArrayList<>(nodes);
        List<Properties> propertiesList = new ArrayList<>(nodes);
        for (int i = 0; i < nodes; i++) {
            URI uri = URI.create("local://test" + i);
            serverURIs.add(uri);
            Path workingDir = path.resolve("server" + i);
            Properties properties = new Properties();
            properties.setProperty("working_dir", workingDir.toString());
            properties.setProperty("persistence.journal.file_data_size", String.valueOf(128 * 1024));
            properties.setProperty("persistence.index.file_data_size", String.valueOf(16 * 1024));
            properties.setProperty("disable_logo", "true");
            properties.setProperty("server_name", String.valueOf(i));

            propertiesList.add(properties);
        }
        List<BootStrap> kvServers = createServers(serverURIs, propertiesList, RaftServer.Roll.VOTER, true);
        int keyNum = 0;
        while (!kvServers.isEmpty()) {
            JkClient kvClient = new JkClient(kvServers.get(0).getRaftClient());
            if (kvServers.size() > nodes / 2) {
                kvClient.update("SET", "key" + keyNum + " value" + keyNum, ResponseConfig.ALL).get();
            }


            BootStrap toBeRemoved = kvServers.get(0);

            logger.info("Shutting down server: {}.", toBeRemoved.getServer().serverUri());
            toBeRemoved.shutdown();
            kvServers.remove(toBeRemoved);
            if (kvServers.size() > nodes / 2) {
                // 等待新的Leader选出来
                logger.info("Wait for new leader...");
                AdminClient adminClient = kvServers.get(0).getAdminClient();
                adminClient.waitForClusterReady(0L);
                Assert.assertEquals("value" + keyNum, new JkClient(kvServers.get(0).getRaftClient()).query("GET", "key" + keyNum).get());
                keyNum++;
            }
        }

        for (int j = 0; j < nodes; j++) {

            BootStrap kvServer = recoverServer(propertiesList.get(j));
            kvServers.add(kvServer);
            if (kvServers.size() > nodes / 2) {
                // 等待新的Leader选出来
                logger.info("Wait for new leader...");
                AdminClient adminClient = kvServers.get(0).getAdminClient();
                adminClient.waitForClusterReady(0L);
                for (int i = 0; i < keyNum; i++) {
                    Assert.assertEquals("value" + i, new JkClient(kvServers.get(0).getRaftClient()).query("GET", "key" + i).get());
                }
            }
        }

        stopServers(kvServers);
        TestPathUtils.destroyBaseDir(path.toFile());
    }

    /**
     * 验证禁用PreVote时，选举是否成功
     */
    @Test
    public void disablePreVoteTest() throws IOException, InterruptedException, ExecutionException, TimeoutException {
        final int nodes = 3;
        logger.info("{} nodes disable pre vote test.", nodes);
        Path path = TestPathUtils.prepareBaseDir("disablePreVote" + nodes);
        List<URI> serverURIs = new ArrayList<>(nodes);
        List<Properties> propertiesList = new ArrayList<>(nodes);
        for (int i = 0; i < nodes; i++) {
            URI uri = URI.create("local://test" + i);
            serverURIs.add(uri);
            Path workingDir = path.resolve("server" + i);
            Properties properties = new Properties();
            properties.setProperty("working_dir", workingDir.toString());
            properties.setProperty("persistence.journal.file_data_size", String.valueOf(128 * 1024));
            properties.setProperty("persistence.index.file_data_size", String.valueOf(16 * 1024));
            properties.setProperty("disable_logo", "true");
            properties.setProperty("enable_pre_vote", "false");
            propertiesList.add(properties);
        }
        List<BootStrap> kvServers = createServers(serverURIs, propertiesList, RaftServer.Roll.VOTER, true);
        int keyNum = 0;
        while (!kvServers.isEmpty()) {
            JkClient kvClient = new JkClient(kvServers.get(0).getRaftClient());
            if (kvServers.size() > nodes / 2) {
                kvClient.update("SET", "key" + keyNum + " value" + keyNum);
            }


            BootStrap toBeRemoved = kvServers.get(0);

            logger.info("Shutting down server: {}.", toBeRemoved.getServer().serverUri());
            toBeRemoved.shutdown();
            kvServers.remove(toBeRemoved);
            if (kvServers.size() > nodes / 2) {
                // 等待新的Leader选出来
                logger.info("Wait for new leader...");
                AdminClient adminClient = kvServers.get(0).getAdminClient();
                adminClient.waitForClusterReady(0L);
                Assert.assertEquals("value" + keyNum, new JkClient(kvServers.get(0).getRaftClient()).query("GET", "key" + keyNum).get());
                keyNum++;
            }
        }

        for (int j = 0; j < nodes; j++) {

            BootStrap kvServer = recoverServer(propertiesList.get(j));
            kvServers.add(kvServer);
            if (kvServers.size() > nodes / 2) {
                // 等待新的Leader选出来
                logger.info("Wait for new leader...");
                AdminClient adminClient = kvServers.get(0).getAdminClient();
                adminClient.waitForClusterReady(0L);
                for (int i = 0; i < keyNum; i++) {
                    Assert.assertEquals("value" + i, new JkClient(kvServers.get(0).getRaftClient()).query("GET", "key" + i).get());
                }
            }
        }

        stopServers(kvServers);
        TestPathUtils.destroyBaseDir(path.toFile());
    }


    private BootStrap recoverServer(String serverPath, Path path) throws IOException {
        Path workingDir = path.resolve(serverPath);
        Properties properties = new Properties();
        properties.setProperty("working_dir", workingDir.toString());
        properties.setProperty("disable_logo", "true");
        properties.setProperty("persistence.journal.file_data_size", String.valueOf(128 * 1024));
        properties.setProperty("persistence.index.file_data_size", String.valueOf(16 * 1024));

        return recoverServer(properties);
    }

    private BootStrap recoverServer(Properties properties) throws IOException {
        BootStrap serverBootStrap =BootStrap.builder().roll(RaftServer.Roll.VOTER).stateFactory(new KvStateFactory()).properties(properties).build();
        serverBootStrap.getServer().recover();
        serverBootStrap.getServer().start();
        return serverBootStrap;
    }

    private void setGetTest(String testname, int nodes) throws IOException, ExecutionException, InterruptedException, TimeoutException {

        Path path = TestPathUtils.prepareBaseDir("SetGetTest-" + testname + "-" + nodes);
        List<BootStrap> kvServers = createServers(nodes, path);
        try {
            List<URI> servers = kvServers.stream().map(BootStrap::getServer).map(RaftServer::serverUri).collect(Collectors.toList());
            BootStrap clientBootStrap = BootStrap.builder().servers(servers).build();
            JkClient client = new JkClient(clientBootStrap.getRaftClient());


            Assert.assertNull(client.update("SET", "key1 hello!").get());
            Assert.assertNull(client.update("SET", "key2 world!").get());
            Assert.assertEquals("hello!", client.query("GET", "key1", QueryConsistency.STRICT).get());
            Assert.assertEquals(new HashSet<>(Arrays.asList("key1", "key2")),
                    new HashSet<>(Arrays.stream(client.<String>query("KEYS", QueryConsistency.STRICT).get().split(",")).map(String::trim).collect(Collectors.toList())));

            Assert.assertNull(client.update("DEL", "key2").get());
            Assert.assertNull(client.query("GET", "key2", QueryConsistency.STRICT).get());
            Assert.assertEquals("key1", client.query("KEYS", QueryConsistency.STRICT).get());
        } finally {
            stopServers(kvServers);
            TestPathUtils.destroyBaseDir(path.toFile());
        }
    }

    @Test
    public void localClientTest() throws IOException, ExecutionException, InterruptedException, TimeoutException {

        Path path = TestPathUtils.prepareBaseDir("LocalClientTest");
        List<BootStrap> kvServers = createServers(1, path);
        try {
            JkClient kvClient = new JkClient(kvServers.stream().findFirst().orElseThrow(RuntimeException::new).getLocalRaftClient());


            Assert.assertNull(kvClient.update("SET", "key1 hello!").get());
            Assert.assertNull(kvClient.update("SET", "key2 world!").get());
            Assert.assertEquals("hello!", kvClient.query("GET", "key1").get());
            Assert.assertEquals(new HashSet<>(Arrays.asList("key1", "key2")),
                    new HashSet<>(Arrays.stream(kvClient.<String>query("KEYS").get().split(",")).map(String::trim).collect(Collectors.toList())));

            Assert.assertNull(kvClient.update("DEL", "key2").get());
            Assert.assertNull(kvClient.query("GET", "key2").get());
            Assert.assertEquals("key1", kvClient.query("KEYS").get());
        } finally {
            stopServers(kvServers);
            TestPathUtils.destroyBaseDir(path.toFile());
        }
    }


    // 增加节点

    @Test
    public void addVotersTest() throws IOException, InterruptedException, ExecutionException, TimeoutException {
        final int newServerCount = 2;
        final int oldServerCount = 3;

        // 初始化并启动一个3个节点的集群
        Path path = TestPathUtils.prepareBaseDir("AddVotersTest-");
        List<BootStrap> oldServers = createServers(oldServerCount, path);

        JkClient kvClient = new JkClient(oldServers.get(0).getRaftClient());

        // 写入一些数据
        for (int i = 0; i < 10; i++) {
            Assert.assertNull(kvClient.update("SET", "key" + i + " " + i).get());
        }

        // 初始化另外2个新节点，先以OBSERVER方式启动
        List<BootStrap> newServers = new ArrayList<>(newServerCount);
        List<URI> oldConfig = oldServers.stream()
                .map(BootStrap::getServer)
                .map(RaftServer::serverUri)
                .collect(Collectors.toList());
        List<URI> newConfig = new ArrayList<>(oldServerCount + newServerCount);
        newConfig.addAll(oldConfig);

        logger.info("Create {} observers", newServerCount);
        List<URI> newServerUris = new ArrayList<>(newServerCount);
        for (int i = oldServers.size(); i < oldServers.size() + newServerCount; i++) {
            URI uri = URI.create("local://test" + i);
            newConfig.add(uri);
            Path workingDir = path.resolve("server" + i);
            Properties properties = new Properties();
            properties.setProperty("server_name", String.valueOf(i));
            properties.setProperty("working_dir", workingDir.toString());
            properties.setProperty("persistence.journal.file_data_size", String.valueOf(128 * 1024));
            properties.setProperty("persistence.index.file_data_size", String.valueOf(16 * 1024));
            properties.setProperty("disable_logo", "true");
//            properties.setProperty("enable_metric", "true");
//            properties.setProperty("print_metric_interval_sec", "3");
            properties.setProperty("observer.parents", String.join(",", oldConfig.stream().map(URI::toString).toArray(String[]::new)));
            BootStrap kvServer = BootStrap.builder().roll(RaftServer.Roll.OBSERVER)
                    .stateFactory(new KvStateFactory())
                    .properties(properties).build();
            newServers.add(kvServer);
            newServerUris.add(uri);
        }


        for (int i = 0; i < newServerCount; i++) {
            BootStrap newServer = newServers.get(i);
            URI uri = newServerUris.get(i);
            newServer.getServer().init(uri, newConfig);
            newServer.getServer().recover();
            newServer.getServer().start();
        }

        AdminClient oldAdminClient = BootStrap.builder().servers(oldConfig).build().getAdminClient();
        AdminClient newAdminClient = BootStrap.builder().servers(newConfig).build().getAdminClient();

        URI leaderUri = oldAdminClient.getClusterConfiguration().get().getLeader();
        while (null == leaderUri) {
            leaderUri = oldAdminClient.getClusterConfiguration().get().getLeader();
            Thread.sleep(100L);
        }
        long leaderApplied = oldAdminClient.getServerStatus(leaderUri).get().getLastApplied();

        // 等待2个节点拉取数据直到与现有集群同步
        logger.info("Wait for observers to catch up the cluster.");
        boolean synced = false;
        while (!synced) {
            synced = newServerUris.stream().allMatch(uri -> {
                try {
                    return newAdminClient.getServerStatus(uri).get().getLastApplied() >= leaderApplied;
                } catch (Throwable e) {
                    return false;
                }
            });
            Thread.sleep(100L);
        }

        // 转换成VOTER
        logger.info("Convert roll to voter of new servers.");

        for (URI uri : newServerUris) {
            newAdminClient.convertRoll(uri, RaftServer.Roll.VOTER).get();
            Assert.assertEquals(RaftServer.Roll.VOTER, newAdminClient.getServerStatus(uri).get().getRoll());
        }

        // 更新集群配置
        logger.info("Update cluster config...");
        boolean success = oldAdminClient.updateVoters(oldConfig, newConfig).get();

        // 验证集群配置
        Assert.assertTrue(success);

        // 等待2阶段变更都提交了
        synced = false;
        long newApplied = leaderApplied + 2;
        while (!synced) {
            synced = newServerUris.stream().allMatch(uri -> {
                try {
                    return newAdminClient.getServerStatus(uri).get().getLastApplied() >= newApplied;
                } catch (Throwable e) {
                    return false;
                }
            });
            Thread.sleep(100L);
        }
        // 验证所有节点都成功完成了配置变更
        for (URI uri : newConfig) {
            Assert.assertEquals(newConfig, newAdminClient.getClusterConfiguration(uri).get().getVoters());
            ServerStatus serverStatus = newAdminClient.getServerStatus(uri).get();
            Assert.assertEquals(RaftServer.Roll.VOTER, serverStatus.getRoll());
            if (leaderUri.equals(uri)) {
                Assert.assertEquals(VoterState.LEADER, serverStatus.getVoterState());
            } else {
                Assert.assertEquals(VoterState.FOLLOWER, serverStatus.getVoterState());
            }
        }


        // 读取数据，验证是否正确
        for (int i = 0; i < 10; i++) {
            Assert.assertEquals(String.valueOf(i), kvClient.query("GET", "key" + i).get());
        }

        oldAdminClient.stop();
        newAdminClient.stop();
        stopServers(newServers);
        stopServers(oldServers);
        TestPathUtils.destroyBaseDir(path.toFile());
    }

    // 替换节点

    // FIXME： 偶尔会失败，暂未找到原因。
    @Test
    public void replaceVotersTest() throws IOException, InterruptedException, ExecutionException, TimeoutException {
        final int serverCount = 3;
        final int replaceServerCount = 1;

        // 初始化并启动一个3个节点的集群
        Path path = TestPathUtils.prepareBaseDir("ReplaceVotersTest-");
        List<BootStrap> oldServers = createServers(serverCount, path);

        JkClient kvClient = new JkClient(oldServers.get(0).getRaftClient());

        // 写入一些数据
        for (int i = 0; i < 10; i++) {
            Assert.assertNull(kvClient.update("SET", "key" + i + " " + i).get());
        }


        List<BootStrap> newServers = new ArrayList<>(replaceServerCount);
        List<URI> oldConfig = oldServers.stream()
                .map(BootStrap::getServer)
                .map(RaftServer::serverUri)
                .collect(Collectors.toList());
        List<URI> newConfig = new ArrayList<>(serverCount);

        newConfig.addAll(oldConfig);

        newConfig.subList(0, replaceServerCount).clear();


        logger.info("Create {} observers", replaceServerCount);
        List<URI> newServerUris = new ArrayList<>(serverCount);
        for (int i = oldConfig.size(); i < oldConfig.size() + replaceServerCount; i++) {
            URI uri = URI.create("local://test" + i);
            Path workingDir = path.resolve("server" + i);
            Properties properties = new Properties();
            properties.setProperty("working_dir", workingDir.toString());
            properties.setProperty("persistence.journal.file_data_size", String.valueOf(128 * 1024));
            properties.setProperty("persistence.index.file_data_size", String.valueOf(16 * 1024));
            properties.setProperty("observer.parents", String.join(",", oldConfig.stream().map(URI::toString).toArray(String[]::new)));
            properties.setProperty("disable_logo", "true");
//            properties.setProperty("print_state_interval_sec", String.valueOf(5));

//            properties.setProperty("enable_metric", "true");
//            properties.setProperty("print_metric_interval_sec", "3");
            BootStrap kvServer = BootStrap.builder().roll(RaftServer.Roll.OBSERVER).stateFactory(new KvStateFactory()).properties(properties).build();
            newServers.add(kvServer);
            newServerUris.add(uri);
        }
        newConfig.addAll(newServerUris);

        for (int i = 0; i < replaceServerCount; i++) {
            BootStrap newServer = newServers.get(i);
            URI uri = newServerUris.get(i);
            newServer.getServer().init(uri, newConfig);
            newServer.getServer().recover();
            newServer.getServer().start();
        }


        AdminClient oldAdminClient = BootStrap.builder().servers(oldConfig).build().getAdminClient();
        AdminClient newAdminClient = BootStrap.builder().servers(newConfig).build().getAdminClient();


        URI leaderUri = oldAdminClient.getClusterConfiguration().get().getLeader();
        while (null == leaderUri) {
            leaderUri = oldAdminClient.getClusterConfiguration().get().getLeader();
            Thread.sleep(100L);
        }
        long leaderApplied = oldAdminClient.getServerStatus(leaderUri).get().getLastApplied();

        // 等待新节点拉取数据直到与现有集群同步
        logger.info("Wait for observers to catch up the cluster.");
        boolean synced = false;
        while (!synced) {
            synced = newServerUris.stream().allMatch(uri -> {
                try {
                    return newAdminClient.getServerStatus(uri).get().getLastApplied() >= leaderApplied;
                } catch (Throwable e) {
                    return false;
                }
            });
            Thread.sleep(100L);
        }
        // 转换成VOTER
        logger.info("Convert roll to voter of new servers.");

        for (URI uri : newServerUris) {
            newAdminClient.convertRoll(uri, RaftServer.Roll.VOTER).get();
            Assert.assertEquals(RaftServer.Roll.VOTER, newAdminClient.getServerStatus(uri).get().getRoll());
        }

        // 更新集群配置
        logger.info("Update cluster config...");
        boolean success = oldAdminClient.updateVoters(oldConfig, newConfig).get();

        // 验证集群配置
        Assert.assertTrue(success);

        // 等待2阶段变更都提交了
        synced = false;
        long newApplied = leaderApplied + 2;
        while (!synced) {
            synced = newServerUris.stream().allMatch(uri -> {
                try {
                    return newAdminClient.getServerStatus(uri).get().getLastApplied() >= newApplied;
                } catch (Throwable e) {
                    return false;
                }
            });
            Thread.sleep(100L);
        }


        // 停止已不在集群内的节点

        oldServers.removeIf(server -> {
            if (!newConfig.contains(server.getServer().serverUri())) {
                logger.info("Stop server: {}.", server.getServer().serverUri());
                server.shutdown();
                return true;
            } else {
                return false;
            }
        });


        // 可能发生选举，需要等待选举完成。
        newAdminClient.waitForClusterReady();

        JkClient newClient = new JkClient(newServers.get(0).getRaftClient());
//        leaderUri = newAdminClient.getClusterConfiguration().get().getLeader();


        // 验证所有节点都成功完成了配置变更
        for (URI uri : newConfig) {
            Assert.assertEquals(newConfig, newAdminClient.getClusterConfiguration(uri).get().getVoters());
            ServerStatus serverStatus = newAdminClient.getServerStatus(uri).get();
            Assert.assertEquals(RaftServer.Roll.VOTER, serverStatus.getRoll());
        }


        // 读取数据，验证是否正确
        for (int i = 0; i < 10; i++) {
            logger.info("Query {}...", "key" + i);
            Assert.assertEquals(String.valueOf(i), newClient.query("GET", "key" + i).get());
        }

        oldAdminClient.stop();
        newAdminClient.stop();
        stopServers(newServers);
        stopServers(oldServers);
        TestPathUtils.destroyBaseDir(path.toFile());

    }

    // 减少节点

    @Test
    public void removeVotersTest() throws IOException, InterruptedException, ExecutionException, TimeoutException {
        final int newServerCount = 3;
        final int oldServerCount = 5;

        // 初始化并启动一个5节点的集群
        Path path = TestPathUtils.prepareBaseDir("RemoveVotersTest");
        List<BootStrap> servers = createServers(oldServerCount, path);

        JkClient kvClient = new JkClient(servers.get(0).getRaftClient());

        // 写入一些数据
        for (int i = 0; i < 10; i++) {
            Assert.assertNull(kvClient.update("SET", "key" + i + " " + i).get());
        }

        List<URI> oldConfig = servers.stream()
                .map(BootStrap::getServer)
                .map(RaftServer::serverUri)
                .collect(Collectors.toList());
        List<URI> newConfig = new ArrayList<>(newServerCount);
        newConfig.addAll(oldConfig.subList(0, newServerCount));

        AdminClient oldAdminClient = BootStrap.builder().servers(oldConfig).build().getAdminClient();
        AdminClient newAdminClient = BootStrap.builder().servers(newConfig).build().getAdminClient();
        URI leaderUri = oldAdminClient.getClusterConfiguration().get().getLeader();
        long leaderApplied = oldAdminClient.getServerStatus(leaderUri).get().getLastApplied();
        // 更新集群配置
        logger.info("Update cluster config...");
        boolean success = oldAdminClient.updateVoters(oldConfig, newConfig).get();

        // 验证集群配置
        Assert.assertTrue(success);

        // 等待2阶段变更都提交了
        boolean synced = false;
        long newApplied = leaderApplied + 2;
        while (!synced) {
            synced = newConfig.stream().allMatch(uri -> {
                try {
                    return newAdminClient.getServerStatus(uri).get().getLastApplied() >= newApplied;
                } catch (Throwable e) {
                    return false;
                }
            });
            Thread.sleep(100L);
        }


        // 停止已不在集群内的节点

        servers.removeIf(server -> {
            if (!newConfig.contains(server.getServer().serverUri())) {
                logger.info("Stop server: {}.", server.getServer().serverUri());
                server.shutdown();
                return true;
            } else {
                return false;
            }
        });

        // 可能发生选举，需要等待选举完成。
        newAdminClient.waitForClusterReady();

        // 验证所有节点都成功完成了配置变更
        for (URI uri : newConfig) {
            Assert.assertEquals(newConfig, newAdminClient.getClusterConfiguration(uri).get().getVoters());
        }
        BootStrap clientBootStrap = BootStrap.builder().servers(newConfig).build();
        kvClient = new JkClient(clientBootStrap.getRaftClient());

        // 读取数据，验证是否正确
        for (int i = 0; i < 10; i++) {
            Assert.assertEquals(String.valueOf(i), kvClient.query("GET", "key" + i).get());
        }
        oldAdminClient.stop();
        newAdminClient.stop();
        stopServers(servers);
        TestPathUtils.destroyBaseDir(path.toFile());

    }

    @Test
    public void preferredLeaderTest() throws Exception {
        // 启动5个节点的集群
        int serverCount = 5;
        long timeoutMs = 60000L;
        logger.info("Creating {} nodes cluster...", serverCount);

        Path path = TestPathUtils.prepareBaseDir("PreferredLeaderTest");
        List<BootStrap> servers = createServers(serverCount, path);
        JkClient kvClient = new JkClient(servers.get(0).getRaftClient());

        logger.info("Write some data...");
        // 写入一些数据
        for (int i = 0; i < 10; i++) {
            Assert.assertNull(kvClient.update("SET", "key" + i + " " + i).get());
        }
        AdminClient adminClient = servers.get(0).getAdminClient();

        // 获取当前leader节点，设为推荐Leader，并停掉这个节点
        URI leaderUri = adminClient.getClusterConfiguration().get().getLeader();
        URI preferredLeader = leaderUri;

        Assert.assertNotNull(leaderUri);
        logger.info("Current leader is {}.", leaderUri);
        URI finalLeaderUri = leaderUri;
        Assert.assertTrue(servers.stream().map(BootStrap::getServer).anyMatch(server -> finalLeaderUri.equals(server.serverUri())));

        Properties properties = null;
        for (BootStrap server : servers) {
            if (leaderUri.equals(server.getServer().serverUri())) {
                logger.info("Stop server: {}.", server.getServer().serverUri());
                server.shutdown();
                properties = server.getProperties();
                break;
            }
        }

        servers.removeIf(server -> finalLeaderUri.equals(server.getServer().serverUri()));

        logger.info("Wait for new leader...");
        // 等待选出新的leader
        adminClient = servers.get(0).getAdminClient();

        adminClient.waitForClusterReady(0L);
        leaderUri = adminClient.getClusterConfiguration().get().getLeader();
        logger.info("Current leader is {}.", leaderUri);

        // 写入一些数据
        logger.info("Write some data...");
        kvClient = new JkClient(servers.get(0).getRaftClient());
        for (int i = 10; i < 20; i++) {
            Assert.assertNull(kvClient.update("SET", "key" + i + " " + i).get());
        }

        // 启动推荐Leader
        logger.info("Set", "preferred leader to {}.", preferredLeader);
        adminClient.setPreferredLeader(preferredLeader).get();

        // 重新启动Server
        logger.info("Restart server {}...", preferredLeader);
        BootStrap recoveredServer = recoverServer(properties);
        servers.add(recoveredServer);
        // 反复检查集群的Leader是否变更为推荐Leader
        logger.info("Checking preferred leader...");
        long t0 = System.currentTimeMillis();
        while (System.currentTimeMillis() - t0 < timeoutMs && !(preferredLeader.equals(adminClient.getClusterConfiguration().get().getLeader()))) {
            Thread.sleep(100L);
        }
        Assert.assertEquals(preferredLeader, adminClient.getClusterConfiguration().get().getLeader());

        // 设置推荐Leader为另一个节点
        URI newPreferredLeader = servers.stream()
                .map(BootStrap::getServer)
                .map(RaftServer::serverUri)
                .filter(uri -> !preferredLeader.equals(uri))
                .findAny().orElse(null);
        Assert.assertNotNull(newPreferredLeader);

        logger.info("Set", "preferred leader to {}.", newPreferredLeader);
        adminClient.setPreferredLeader(newPreferredLeader).get();

        // 反复检查集群的Leader是否变更为推荐Leader

        logger.info("Checking preferred leader...");
        t0 = System.currentTimeMillis();
        while (System.currentTimeMillis() - t0 < timeoutMs && !(newPreferredLeader.equals(adminClient.getClusterConfiguration().get().getLeader()))) {
            Thread.sleep(100L);
        }

        Assert.assertEquals(newPreferredLeader, adminClient.getClusterConfiguration().get().getLeader());


        stopServers(servers);
        TestPathUtils.destroyBaseDir(path.toFile());
    }

    @Test
    public void preVoteTest() throws Exception {
        // 启动5个节点的集群
        int serverCount = 3;
        logger.info("Creating {} nodes cluster...", serverCount);

        Path path = TestPathUtils.prepareBaseDir("preVoteTest");
        List<BootStrap> servers = createServers(serverCount, path);

        BootStrap clientBootStrap =
                BootStrap.builder().servers(
                        servers.stream().map(s -> s.getServer().serverUri()).collect(Collectors.toList())
                ).build();

        JkClient kvClient = new JkClient(clientBootStrap.getRaftClient());
        AdminClient adminClient = clientBootStrap.getAdminClient();
        kvClient.waitForClusterReady();

        logger.info("Write some data...");
        // 写入一些数据
        for (int i = 0; i < 10; i++) {
            Assert.assertNull(kvClient.update("SET", "key" + i + " " + i).get());
        }

        // 获取当前leader节点
        URI leaderUri = adminClient.getClusterConfiguration().get().getLeader();

        Assert.assertNotNull(leaderUri);
        logger.info("Current leader is {}.", leaderUri);

        // 从2个follower中挑选任意一个，停掉
        BootStrap toBeShutdown = servers.stream().filter(s -> !s.getServer().serverUri().equals(leaderUri)).findAny().orElse(null);
        Assert.assertNotNull(toBeShutdown);
        logger.info("Shutdown server: {}...", toBeShutdown.getServer().serverUri());
        toBeShutdown.shutdown();
        servers.remove(toBeShutdown);
        Properties propertiesOfShutdown = toBeShutdown.getProperties();

        // 记录leader的term，然后停掉leader
        BootStrap leader = servers.stream().filter(s -> s.getServer().serverUri().equals(leaderUri)).findAny().orElse(null);
        Assert.assertNotNull(leader);
        int term = ((Voter )((Server)leader.getServer()).getServer()).getTerm();
        logger.info("Shutdown leader: {}, term: {}...", leader.getServer().serverUri(), term);
        leader.shutdown();
        servers.remove(leader);
        Properties propertiesOfLeader = leader.getProperties();

        logger.info("Sleep 5 seconds...");
        Thread.sleep(5000L);

        // 重启刚刚2个节点

        logger.info("Restart server {}...", toBeShutdown.getServer().serverUri());
        servers.add(recoverServer(propertiesOfShutdown));

        logger.info("Restart server {}...", leader.getServer().serverUri());
        servers.add(recoverServer(propertiesOfLeader));
        logger.info("Waiting for cluster ready...");
        adminClient.waitForClusterReady();

        // 查看新的Leader上的term
        URI newLeaderUri = adminClient.getClusterConfiguration().get().getLeader();
        leader = servers.stream().filter(s -> s.getServer().serverUri().equals(newLeaderUri)).findAny().orElse(null);
        Assert.assertNotNull(leader);
        int newTerm = ((Voter )((Server)leader.getServer()).getServer()).getTerm();
        // 检查term是否只增加了1
        Assert.assertEquals(term + 1, newTerm);

        stopServers(servers);
        TestPathUtils.destroyBaseDir(path.toFile());
    }

    @Test
    public void monitorTest() throws Exception {

        int nodes = 5;
        Path path = TestPathUtils.prepareBaseDir("monitorTest");
        SimpleMonitorCollector simpleMonitorCollector = ServiceSupport.load(MonitorCollector.class, SimpleMonitorCollector.class);
        Assert.assertNotNull(simpleMonitorCollector);
        for (MonitoredServer monitoredServer : simpleMonitorCollector.getMonitoredServers()) {
            simpleMonitorCollector.removeServer(monitoredServer);
        }

        List<BootStrap> servers = createServers(nodes, path);
        Collection<MonitoredServer> monitoredServers = simpleMonitorCollector.getMonitoredServers();
        Assert.assertEquals(nodes, monitoredServers.size());
        Collection<ServerMonitorInfo> monitorInfos = simpleMonitorCollector.collectAll();
        Assert.assertEquals(nodes, monitorInfos.size());

//        for (ServerMonitorInfo monitorInfo : monitorInfos) {
//            logger.info("ServerMonitorInfo: {}.", monitorInfo);
//        }

        stopServers(servers);

    }

    private void stopServers(List<BootStrap> kvServers) {
        for (BootStrap serverBootStraps : kvServers) {
            try {
                serverBootStraps.shutdown();
            } catch (Throwable t) {
                logger.warn("Stop server {} exception:", serverBootStraps.getServer().serverUri(), t);
            }
        }
    }

    private List<BootStrap> createServers(int nodes, Path path) throws IOException, ExecutionException, InterruptedException, TimeoutException {
        return createServers(nodes, path, RaftServer.Roll.VOTER, true);
    }

    private List<BootStrap> createServers(int nodes, Path path, RaftServer.Roll roll, boolean waitForLeader) throws IOException, ExecutionException, InterruptedException, TimeoutException {
        logger.info("Create {} nodes servers", nodes);
        List<URI> serverURIs = new ArrayList<>(nodes);
        List<Properties> propertiesList = new ArrayList<>(nodes);
        for (int i = 0; i < nodes; i++) {
            URI uri = URI.create("local://test" + i);
            serverURIs.add(uri);
            Path workingDir = path.resolve("server" + i);
            Properties properties = new Properties();
            properties.setProperty("server_name", String.valueOf(i));
            properties.setProperty("working_dir", workingDir.toString());
            properties.setProperty("persistence.journal.file_data_size", String.valueOf(128 * 1024));
            properties.setProperty("persistence.index.file_data_size", String.valueOf(16 * 1024));
            properties.setProperty("disable_logo", "true");
            propertiesList.add(properties);
        }
        return createServers(serverURIs, propertiesList, roll, waitForLeader);

    }


    private List<BootStrap> createServers(List<URI> serverURIs, List<Properties> propertiesList, RaftServer.Roll roll, boolean waitForLeader) throws IOException, ExecutionException, InterruptedException, TimeoutException {

        List<BootStrap> serverBootStraps = new ArrayList<>(serverURIs.size());
        for (int i = 0; i < serverURIs.size(); i++) {
            BootStrap serverBootStrap = BootStrap.builder().roll(roll).stateFactory(new KvStateFactory()).properties(propertiesList.get(i)).build();            serverBootStraps.add(serverBootStrap);

            serverBootStrap.getServer().init(serverURIs.get(i), serverURIs);
            serverBootStrap.getServer().recover();
            serverBootStrap.getServer().start();
        }
        if (waitForLeader) {
            serverBootStraps.get(0).getAdminClient().waitForClusterReady();
        }
        return serverBootStraps;
    }
}
