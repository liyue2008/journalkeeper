package io.journalkeeper.core;

import io.journalkeeper.core.api.QueryConsistency;
import io.journalkeeper.core.api.RaftServer;
import io.journalkeeper.core.api.State;
import io.journalkeeper.core.api.StateFactory;
import io.journalkeeper.core.easy.JkClient;
import io.journalkeeper.core.easy.JkState;
import io.journalkeeper.utils.test.TestPathUtils;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/**
 * @author LiYue
 * Date: 2020/2/19
 */
public class ConsistencyTest {
    private static final Logger logger = LoggerFactory.getLogger(ConsistencyTest.class);
    private void stopServers(List<BootStrap> kvServers) {
        for (BootStrap serverBootStraps : kvServers) {
            try {
                serverBootStraps.shutdown();
            } catch (Throwable t) {
                logger.warn("Stop server {} exception:", serverBootStraps.getServer().serverUri(), t);
            }
        }
    }

    @Test
    public void testSequential() throws Exception {
        Path path = TestPathUtils.prepareBaseDir("TestSequential");
        List<BootStrap> serverBootStraps = createServers(3, path);
        try {
            for (int j = 0; j < 3; j++) {
                JkClient client = new JkClient(serverBootStraps.get(j).getClient());
                for (int i = 0; i < 100; i++) {
                    Integer value = client.<Integer, Integer>update(1).get();
                    Assert.assertEquals(value, client.<Integer>query(QueryConsistency.SEQUENTIAL).get());
                }
            }
        } finally {
            stopServers(serverBootStraps);
        }
    }

    @Test
    public void testAvailability() throws Exception {
        Path path = TestPathUtils.prepareBaseDir("TestSequential");
        List<BootStrap> serverBootStraps = createServers(3, path);
        List<URI> serverUris = serverBootStraps.stream().map(b -> b.getServer().serverUri()).collect(Collectors.toList());
        BootStrap clientBootStrap = BootStrap.builder().servers(serverUris).build();
        JkClient client = new JkClient(clientBootStrap.getClient());
        try {
            for (int i = 0; i < 100; i++) {
                client.<Integer, Integer>update(1).get();
            }
            List<BootStrap> tobeStopped = serverBootStraps.subList(0, 2);
            stopServers(tobeStopped);
            serverBootStraps.removeAll(tobeStopped);
            Assert.assertTrue(client.<Integer>query(QueryConsistency.NONE).get() >= 0);

        } finally {
            clientBootStrap.shutdown();
            stopServers(serverBootStraps);
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
            properties.setProperty("working_dir", workingDir.toString());
            properties.setProperty("persistence.journal.file_data_size", String.valueOf(128 * 1024));
            properties.setProperty("persistence.index.file_data_size", String.valueOf(16 * 1024));
            properties.setProperty("disable_logo", "true");

//            properties.setProperty("enable_metric", "true");
//            properties.setProperty("print_metric_interval_sec", "3");
            properties.setProperty("print_state_interval_sec", String.valueOf(5));

            propertiesList.add(properties);
        }
        return createServers(serverURIs, propertiesList, roll, waitForLeader);

    }


    private List<BootStrap> createServers(List<URI> serverURIs, List<Properties> propertiesList, RaftServer.Roll roll, boolean waitForLeader) throws IOException, ExecutionException, InterruptedException, TimeoutException {

        List<BootStrap> serverBootStraps = new ArrayList<>(serverURIs.size());
        for (int i = 0; i < serverURIs.size(); i++) {
            BootStrap serverBootStrap = BootStrap.builder().roll(roll).stateFactory(new ConsistencyStateFactory()).properties(propertiesList.get(i)).build();
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

    private static class ConsistencyState extends JkState {
        private int value = 0;
        public Integer doExecute(Integer entry) {
            return value += entry;
        }

        public Integer doQuery(Integer query) {
            return value;
        }

        public ConsistencyState () {
            super();
            registerQueryCommandHandler(this::doQuery);
            registerExecuteCommandHandler(this::doExecute);
        }
        @Override
        public void recover(Path path, Properties properties) throws IOException {

        }
    }

    private static class ConsistencyStateFactory implements StateFactory {

        @Override
        public State createState() {
            return new ConsistencyState();
        }
    }
}
