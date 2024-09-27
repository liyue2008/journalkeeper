package io.journalkeeper.core.discovery;

import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import io.journalkeeper.utils.spi.Singleton;
@Singleton
public class InMemoryDiscoverProvider implements DiscoverProvider {
    private final Map<String /* token */, List<URI>> tokenToServers = new ConcurrentHashMap<>();

    @Override
    public void register(URI uri, String token) {
        synchronized (tokenToServers) {
            List<URI> servers = tokenToServers.computeIfAbsent(token, t -> new CopyOnWriteArrayList<>());
            servers.add(uri);
        }
    }

    @Override
    public List<URI> discoverNodes(String token) {
        return tokenToServers.getOrDefault(token, Collections.emptyList());
    }

    @Override
    public void init(Properties properties) {
    }

}
