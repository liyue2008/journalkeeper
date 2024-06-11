package io.journalkeeper.core.monitor;


import io.journalkeeper.core.api.RaftServer;
import io.journalkeeper.core.api.VoterState;
import io.journalkeeper.monitor.VoterMonitorInfo;

import java.net.URI;

public interface MonitoredVoter {
    VoterMonitorInfo collectVoterMonitorInfo();
    RaftServer.Roll roll();
    VoterState voterState();
    URI leaderUri();
}
