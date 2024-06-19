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
package io.journalkeeper.examples.kv;

import io.journalkeeper.core.BootStrap;
import io.journalkeeper.core.easy.JkClient;
import io.journalkeeper.utils.net.NetworkingUtils;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class KvExampleApplicationMain {
    private static final Logger logger = LoggerFactory.getLogger(KvExampleApplicationMain.class);

    public static void main(String[] args) throws Exception {
        int nodes = 1;
        logger.info("Usage: java {} [nodes(default 3)]", KvExampleApplicationMain.class.getName());
        if (args.length > 0) {
            nodes = Integer.parseInt(args[0]);
        }
        logger.info("Nodes: {}", nodes);
        List<URI> serverURIs = new ArrayList<>(nodes);
        for (int i = 0; i < nodes; i++) {
            URI uri = URI.create("jk://localhost:" + NetworkingUtils.findRandomOpenPortOnAllLocalInterfaces());
            serverURIs.add(uri);
        }

        List<BootStrap> serverBootStraps = new ArrayList<>(serverURIs.size());

        KvStateFactory stateFactory = new KvStateFactory();
        for (int i = 0; i < serverURIs.size(); i++) {
            Path workingDir = Paths.get(System.getProperty("user.dir")).resolve("journalkeeper").resolve("server" + i);
            FileUtils.deleteDirectory(workingDir.toFile());
            Properties properties = new Properties();
            properties.put("working_dir", workingDir.toString());
            BootStrap bootStrap = BootStrap.builder().stateFactory(stateFactory).properties(properties).build();
            bootStrap.getServer().init(serverURIs.get(i), serverURIs);
            bootStrap.getServer().recover();
            bootStrap.getServer().start();
            serverBootStraps.add(bootStrap);
        }

        BootStrap clientBootStrap = BootStrap.builder().servers(serverURIs).build();

        JkClient kvClient = new JkClient(clientBootStrap.getRaftClient());
        kvClient.waitForClusterReady();
        logger.info("SET key1 hello!");
        String result = kvClient.<String,String>update("SET", "key1 hello!").get();
        logger.info(result);

        logger.info("SET key2 world!");
        result = kvClient.<String, String>update("SET", "key2 world!").get();
        logger.info(result);

        logger.info("GET key1");
        result = kvClient.<String, String>query("GET", "key1").get();
        logger.info(result);

        logger.info("KEYS");
        result = kvClient.<String>query("KEYS").get();
        logger.info(result);

        logger.info("DEL key2");
        result = kvClient.<String, String>update("DEL","key2").get();
        logger.info(result);

        logger.info("GET key2");
        result = kvClient.<String, String>query("GET","key2").get();
        logger.info(result);

        logger.info("KEYS");
        result = kvClient.<String>query("KEYS").get();
        logger.info(result);

        clientBootStrap.shutdown();
        serverBootStraps.forEach(BootStrap::shutdown);
    }

}
