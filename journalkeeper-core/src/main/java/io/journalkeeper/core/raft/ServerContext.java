package io.journalkeeper.core.raft;

import io.journalkeeper.core.api.RaftJournal;
import io.journalkeeper.core.api.ServerStatus;
import io.journalkeeper.core.monitor.MonitoredJournal;
import io.journalkeeper.core.monitor.MonitoredVoter;
import io.journalkeeper.utils.actor.PostOffice;
import io.journalkeeper.utils.config.Config;
import io.journalkeeper.utils.config.PropertiesConfigProvider;
import io.journalkeeper.utils.event.EventBus;
import io.journalkeeper.utils.state.StateServer;

import java.util.Properties;

public class ServerContext {
    private final PostOffice postOffice;

    private final EventBus eventBus;

    private final Config config;

    private final Properties properties;

    private final RaftState state;

    private final RaftJournal journal;

    private final MonitoredVoter monitoredVoter;

    private final MonitoredJournal monitoredJournal;

    private final StateServer runningState;


    public ServerContext(Properties properties, Config config, RaftState state, RaftJournal journal, MonitoredVoter monitoredVoter, MonitoredJournal monitoredJournal, EventBus eventBus, PostOffice postOffice, StateServer stateServer) {
        this.properties = properties;
        this.config = config;
        this.state = state;
        this.journal = journal;
        this.monitoredVoter = monitoredVoter;
        this.monitoredJournal = monitoredJournal;
        this.eventBus = eventBus;
        this.postOffice = postOffice;
        this.runningState = stateServer;


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

    public EventBus getEventBus() {
        return eventBus;
    }

    public MonitoredVoter getMonitoredVoter() {
        return monitoredVoter;
    }

    public StateServer getRunningState() {
        return runningState;
    }

    public MonitoredJournal getMonitoredJournal() {
        return monitoredJournal;
    }
}
