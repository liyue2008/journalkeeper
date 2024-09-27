package io.journalkeeper.core;

import java.net.URI;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.journalkeeper.core.api.JournalEntryParser;
import io.journalkeeper.core.api.StateFactory;
import io.journalkeeper.core.api.RaftServer.Roll;
import io.journalkeeper.core.config.ServerConfigDeclaration;
import io.journalkeeper.core.discovery.DiscoverProvider;
import io.journalkeeper.utils.CloseSupport;
import io.journalkeeper.utils.config.Config;
import io.journalkeeper.utils.config.PropertiesConfigProvider;
import io.journalkeeper.utils.spi.ServiceSupport;

public class AutoDiscoverBootStrap {
    private static final Logger logger = LoggerFactory.getLogger(AutoDiscoverBootStrap.class);
    private final URI uri;
    private final JournalEntryParser journalEntryParser;
    private final StateFactory stateFactory;
    private final Properties properties;
    private final int voterCount;
    private final String token;
    private final long timeout_ms;
    private final Config config;
    private AutoDiscoverBootStrap(StateFactory stateFactory,
                      JournalEntryParser journalEntryParser,
                      Properties properties, URI uri) {
        
        this.uri = uri;
        this.properties = properties;
        this.stateFactory = stateFactory;
        this.journalEntryParser = journalEntryParser;

        this.config = new Config();
        ServerConfigDeclaration serverConfigDeclaration = new ServerConfigDeclaration();
        serverConfigDeclaration.declare(config);
        config.load(new PropertiesConfigProvider(properties));
        
        this.voterCount = config.get("discovery.voter_count");
        this.token = config.get("discovery.token");
        this.timeout_ms = config.<Long>get("discovery.timeout_sec") * 1000L;
    }

    public CompletableFuture<BootStrap> discover() {

        DiscoverProvider discoverProvider = ServiceSupport.load(DiscoverProvider.class, config.<String>get("discovery.provider"));
        discoverProvider.init(properties);
        discoverProvider.register(uri, token);
        
        return CompletableFuture.supplyAsync(() -> {
            long start = System.currentTimeMillis();
            List<URI> servers = null;
            while (System.currentTimeMillis() - start < timeout_ms) {   
                servers = discoverProvider.discoverNodes(token);
                if (servers.size() >= voterCount) {
                    break;          
                }
            }
            if(servers == null || servers.size() != voterCount) {
                throw new RuntimeException(String.format("发现节点数量不等于配置的节点数: %d != %d", servers.size(), voterCount));
            }

            logger.info("发现节点: {}", servers);
            CloseSupport.tryClose(discoverProvider);
            return servers;
        }).thenApply(servers -> BootStrap.builder()
            .roll(Roll.VOTER)
            .stateFactory(stateFactory)
            .journalEntryParser(journalEntryParser)
            .properties(properties)
            .servers(servers)
            .build()
            );
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private StateFactory stateFactory;
        private JournalEntryParser journalEntryParser;
        private Properties properties;
        private URI uri;

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

        public Builder uri(URI uri) {
            this.uri = uri;
            return this;
        }

        public AutoDiscoverBootStrap build() {
            return new AutoDiscoverBootStrap(stateFactory, journalEntryParser, properties, uri);
        }
    }
}
