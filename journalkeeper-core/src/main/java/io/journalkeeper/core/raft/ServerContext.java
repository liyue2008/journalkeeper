package io.journalkeeper.core.raft;

import io.journalkeeper.core.api.RaftJournal;
import io.journalkeeper.utils.actor.PostOffice;
import io.journalkeeper.utils.config.Config;
import io.journalkeeper.utils.config.PropertiesConfigProvider;
import io.journalkeeper.utils.event.EventBus;

import java.util.Properties;

public class ServerContext {
    private final PostOffice postOffice;

    private final EventBus eventBus;

    private final Config config;

    private final Properties properties;

    private final RaftState state;

    private final RaftJournal journal;

    private final RaftVoter voter;

    public ServerContext(Properties properties, Config config, RaftState state, RaftJournal journal, RaftVoter voter, EventBus eventBus, PostOffice postOffice) {
        this.properties = properties;
        this.config = config;
        this.state = state;
        this.journal = journal;
        this.voter = voter;

        this.eventBus = eventBus;
        this.postOffice = postOffice;


    }

    public Config getConfig() {
        return config;
    }

    public PostOffice getPostOffice() {
        return postOffice;
    }

    public Properties getProperties() {
        return properties;
    }

    public RaftState getState() {
        return state;
    }

    public RaftJournal getJournal() {
        return journal;
    }

    public RaftVoter getVoter() {
        return voter;
    }

    public EventBus getEventBus() {
        return eventBus;
    }
}
