package io.journalkeeper.core.raft;

import io.journalkeeper.core.monitor.MonitoredVoter;
import io.journalkeeper.core.state.ConfigState;
import io.journalkeeper.monitor.*;
import java.net.URI;


public class RaftServerMonitorInfoProvider implements MonitoredServer {
    private final ServerContext context;

    public RaftServerMonitorInfoProvider(ServerContext context) {
        this.context = context;
    }

    @Override
    public URI uri() {
        return context.getState().getLocalUri();
    }

    @Override
    public ServerMonitorInfo collect() {
        RaftState state = context.getState();
        MonitoredVoter voter = context.getMonitoredVoter();

        ServerMonitorInfo serverMonitorInfo = new ServerMonitorInfo();
        serverMonitorInfo.setUri(state.getLocalUri());
        serverMonitorInfo.setState(context.getRunningState().serverState());
        serverMonitorInfo.setRoll(voter.roll());

        serverMonitorInfo.setLeader(voter.leaderUri());
        NodeMonitorInfo nodeMonitorInfo = collectNodeMonitorInfo(state.getConfigState());
        serverMonitorInfo.setNodes(nodeMonitorInfo);
        JournalMonitorInfo journalMonitorInfo = context.getMonitoredJournal().collectJournalMonitorInfo();
        journalMonitorInfo.setAppliedIndex(state.lastApplied());
        serverMonitorInfo.setJournal(journalMonitorInfo);

        DiskMonitorInfo diskMonitorInfo = context.getMonitoredJournal().collectDistMonitorInfo();
        serverMonitorInfo.setDisk(diskMonitorInfo);

        VoterMonitorInfo voterMonitorInfo = voter.collectVoterMonitorInfo();
        serverMonitorInfo.setVoter(voterMonitorInfo);

        return serverMonitorInfo;
    }

    private NodeMonitorInfo collectNodeMonitorInfo(ConfigState voterConfigurationStateMachine) {
        NodeMonitorInfo nodeMonitorInfo = null;
        if (null != voterConfigurationStateMachine) {
            nodeMonitorInfo = new NodeMonitorInfo();
            nodeMonitorInfo.setJointConsensus(voterConfigurationStateMachine.isJointConsensus());
            if (voterConfigurationStateMachine.isJointConsensus()) {
                nodeMonitorInfo.setNewConfig(voterConfigurationStateMachine.getConfigNew());
                nodeMonitorInfo.setOldConfig(voterConfigurationStateMachine.getConfigOld());
            } else {
                nodeMonitorInfo.setConfig(voterConfigurationStateMachine.getConfigNew());
            }
        }
        return nodeMonitorInfo;
    }

}
