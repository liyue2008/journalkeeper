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
import io.journalkeeper.core.api.RaftServer;
import io.journalkeeper.core.api.SnapshotEntry;
import io.journalkeeper.core.api.SnapshotsEntry;
import io.journalkeeper.core.easy.JkClient;
import io.journalkeeper.core.state.KvStateFactory;
import io.journalkeeper.utils.files.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * RecoverSnapshotTest
 * author: gaohaoxiang
 * date: 2019/12/12
 */
public class RecoverSnapshotTest {

    private static final String ROOT = String.format("%s/export/recoverTest", System.getProperty("java.io.tmpdir"));

    @Before
    public void before() throws Exception {
        FileUtils.deleteFolder(new File(ROOT).toPath());
    }

    @After
    public void after() throws Exception {
        FileUtils.deleteFolder(new File(ROOT).toPath());
    }

    @Test
    public void singleTakeAndRecoverTest() throws Exception {
        URI uri = URI.create("local://test");
        File root = new File(ROOT);
        Properties properties = new Properties();
        properties.setProperty("working_dir", root.toString());

        BootStrap kvServer = BootStrap.builder().roll(RaftServer.Roll.VOTER).stateFactory(new KvStateFactory()).properties(properties).build();
        kvServer.getServer().init(uri, Collections.singletonList(uri));
        kvServer.getServer().recover();
        kvServer.getServer().start();
        kvServer.getAdminClient().waitForClusterReady(1000 * 5);

        JkClient client = new JkClient(kvServer.getRaftClient());
        Assert.assertNull(client.update("SET", "key_1 value_1").get());
        Assert.assertEquals("value_1", client.query("GET", "key_1").get());

        kvServer.getAdminClient().takeSnapshot().get();
        List<SnapshotEntry> snapshotEntries = kvServer.getAdminClient().getSnapshots().get().getSnapshots();
        Assert.assertEquals(snapshotEntries.size(), 2);
        long index = snapshotEntries.get(1).getIndex();

        Assert.assertNull(client.update("SET", "key_2 value_2").get());
        Assert.assertEquals("value_2", client.query("GET", "key_2").get());

        kvServer.getAdminClient().recoverSnapshot(index).get();
        snapshotEntries = kvServer.getAdminClient().getSnapshots().get().getSnapshots();
        Assert.assertEquals(snapshotEntries.size(), 3);
        index = snapshotEntries.get(2).getIndex();
        Assert.assertEquals("value_1", client.query("GET", "key_1").get());
        Assert.assertNull(client.query("GET", "key_2").get());

        kvServer.getAdminClient().recoverSnapshot(index).get();
        Assert.assertEquals(kvServer.getAdminClient().getSnapshots().get().getSnapshots().size(), 4);
        Assert.assertEquals("value_1", client.query("GET", "key_1").get());
        Assert.assertEquals("value_2", client.query("GET", "key_2").get());

        kvServer.getAdminClient().recoverSnapshot(0).get();
        Assert.assertNull(client.query("GET", "key_1").get());
        Assert.assertNull(client.query("GET", "key_2").get());

        kvServer.shutdown();
    }

    @Test
    public void clusterTakeAndRecoverTest() throws Exception {
        List<URI> uris = new ArrayList<>();
        List<BootStrap> servers = new ArrayList<>();
        List<JkClient> clients = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            URI uri = URI.create("local://test" + i);
            uris.add(uri);
        }

        for (int i = 0; i < 3; i++) {
            URI uri = URI.create("local://test" + i);
            File root = new File(ROOT);
            Properties properties = new Properties();
            properties.setProperty("working_dir", root.toString() + "/" + i);

            BootStrap kvServer = BootStrap.builder().roll(RaftServer.Roll.VOTER).stateFactory(new KvStateFactory()).properties(properties).build();
            kvServer.getServer().init(uri, uris);
            kvServer.getServer().recover();
            kvServer.getServer().start();
            servers.add(kvServer);
            clients.add(new JkClient(kvServer.getRaftClient()));
        }
        servers.get(0).getAdminClient().waitForClusterReady(1000 * 5);

        for (JkClient client : clients) {
            Assert.assertNull(client.update("SET", "key_1 value_1").get());
            Assert.assertEquals("value_1", client.query("GET", "key_1").get());
        }

        servers.get(0).getAdminClient().takeSnapshot().get();
        for (int i = 0; i < servers.size(); i++) {
            Assert.assertEquals(servers.get(i).getAdminClient().getSnapshots().get().getSnapshots().size(), 2);
        }
        long index = servers.get(0).getAdminClient().getSnapshots().get().getSnapshots().get(1).getIndex();

        for (JkClient client : clients) {
            Assert.assertNull(client.<String, String>update("SET", "key_2 value_2").get());
            Assert.assertEquals("value_2", client.<String, String>query("GET", "key_2").get());
        }

        servers.get(0).getAdminClient().recoverSnapshot(index).get();

        for (int i = 0; i < servers.size(); i++) {
            Assert.assertEquals(servers.get(i).getAdminClient().getSnapshots().get().getSnapshots().size(), 3);
        }
        index = servers.get(0).getAdminClient().getSnapshots().get().getSnapshots().get(2).getIndex();
        for (JkClient client : clients) {
            Assert.assertEquals("value_1", client.<String, String>query("GET", "key_1").get());
            Assert.assertNull(client.<String, String>query("GET", "key_2").get());
        }

        servers.get(0).getAdminClient().recoverSnapshot(index).get();

        for (int i = 0; i < servers.size(); i++) {
            Assert.assertEquals(servers.get(i).getAdminClient().getSnapshots().get().getSnapshots().size(), 4);
        }

        for (JkClient client : clients) {
            Assert.assertEquals("value_1", client.query("GET", "key_1").get());
            Assert.assertEquals("value_2", client.query("GET", "key_2").get());
        }

        servers.get(0).getAdminClient().recoverSnapshot(0).get();

        for (int i = 0; i < servers.size(); i++) {
            Assert.assertEquals(servers.get(i).getAdminClient().getSnapshots().get().getSnapshots().size(), 5);
        }

        for (JkClient client : clients) {
            Assert.assertNull(client.query("GET", "key_1").get());
            Assert.assertNull(client.query("GET", "key_2").get());
        }

        for (BootStrap server : servers) {
            server.shutdown();
        }
    }
}