package io.journalkeeper.core.raft;

import io.journalkeeper.utils.actor.PostOffice;
import io.journalkeeper.utils.config.Config;
import io.journalkeeper.utils.event.EventBus;

import java.util.Properties;

public class ServerContext {
    private final PostOffice postOffice;

    private final EventBus eventBus;

    private final Config config;

    private final Properties properties;

    public ServerContext(Properties properties) {
        this.properties = properties;
        this.postOffice = new PostOffice();
        this.eventBus = new EventBus();
        this.config = new Config();
        new EventBusActor(postOffice, eventBus);
    }

    public Config getConfig() {
        return config;
    }

    public PostOffice getPostOffice() {
        return postOffice;
    }

    public EventBus getEventBus() {
        return eventBus;
    }

    public Properties getProperties() {
        return properties;
    }
}
