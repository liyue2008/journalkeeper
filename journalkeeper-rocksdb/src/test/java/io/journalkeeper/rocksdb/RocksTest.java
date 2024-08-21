package io.journalkeeper.rocksdb;

import io.journalkeeper.core.BootStrap;
import io.journalkeeper.core.api.RaftServer;
import io.journalkeeper.core.easy.JkClient;
import io.journalkeeper.utils.net.NetworkingUtils;
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

public class RocksTest {

    private static final Logger logger = LoggerFactory.getLogger(RocksTest.class);

    @Test
    public void singleNodeTest() throws Exception {
        setGetTest("singleNodeTest", 1);
    }

    @Test
    public void tripleNodesTest() throws Exception {
        setGetTest("tripleNodesTest",3);
    }

    private void setGetTest(String testname, int nodes) throws IOException, ExecutionException, InterruptedException, TimeoutException {

        Path path = TestPathUtils.prepareBaseDir("SetGetTest-" + testname + "-" + nodes);
        List<BootStrap> kvServers = createServers(nodes, path);
        try {
            List<URI> servers = kvServers.stream().map(BootStrap::getServer).map(RaftServer::serverUri).collect(Collectors.toList());
            BootStrap clientBootStrap = BootStrap.builder().servers(servers).build();
            JkClient jkClient = new JkClient(clientBootStrap.getRaftClient());
            RocksClient client = new RocksClient(jkClient);


            client.put("key1", "hello!");
            client.put("key2", "world!");
            Assert.assertEquals("hello!", client.get("key1"));
            Assert.assertEquals("world!", client.get("key2"));

            client.del("key2");
            Assert.assertNull(client.get("key2"));
        } finally {
            stopServers(kvServers);
            TestPathUtils.destroyBaseDir(path.toFile());
        }
    }

    private List<BootStrap> createServers(int nodes, Path path) throws IOException {
        return createServers(nodes, path, RaftServer.Roll.VOTER, true);
    }

    private List<BootStrap> createServers(int nodes, Path path, RaftServer.Roll roll, boolean waitForLeader) throws IOException {
        logger.info("Create {} nodes servers", nodes);
        List<URI> serverURIs = new ArrayList<>(nodes);
        List<Properties> propertiesList = new ArrayList<>(nodes);
        for (int i = 0; i < nodes; i++) {
            URI uri = URI.create("jk://localhost:" + NetworkingUtils.findRandomOpenPortOnAllLocalInterfaces());
            serverURIs.add(uri);
            Properties properties = getProperties(path, i);
            propertiesList.add(properties);
        }
        return createServers(serverURIs, propertiesList, roll, waitForLeader);

    }

    private List<BootStrap> createServers(List<URI> serverURIs, List<Properties> propertiesList, RaftServer.Roll roll, boolean waitForLeader) {

        List<BootStrap> serverBootStraps = new ArrayList<>(serverURIs.size());
        for (int i = 0; i < serverURIs.size(); i++) {
            BootStrap serverBootStrap = BootStrap.builder().roll(roll).stateFactory(new RocksStateFactory()).properties(propertiesList.get(i)).build();
            serverBootStraps.add(serverBootStrap);

            serverBootStrap.getServer().init(serverURIs.get(i), serverURIs);
            serverBootStrap.getServer().recover();
            serverBootStrap.getServer().start();
        }
        if (waitForLeader) {
            serverBootStraps.get(0).getAdminClient().waitForClusterReady();
        }
        return serverBootStraps;
    }

    private static Properties getProperties(Path path, int i) {
        Path workingDir = path.resolve("server" + i);
        Properties properties = new Properties();
        properties.setProperty("server_name", String.valueOf(i));
        properties.setProperty("working_dir", workingDir.toString());
        properties.setProperty("persistence.journal.file_data_size", String.valueOf(128 * 1024));
        properties.setProperty("persistence.index.file_data_size", String.valueOf(16 * 1024));
        properties.setProperty("disable_logo", "true");
        return properties;
    }

    private void stopServers(List<BootStrap> kvServers) {
        kvServers.parallelStream().forEach(s -> {
            try {
                s.shutdown();
            } catch (Throwable t) {
                logger.warn("Stop server {} exception:", s.getServer().serverUri(), t);
            }
        });
    }
}
