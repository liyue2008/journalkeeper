package io.journalkeeper.core.raft;

import io.journalkeeper.core.api.RaftServer;
import io.journalkeeper.core.state.Snapshot;

import java.net.URI;
import java.util.List;
import java.util.NavigableMap;

public interface RaftState {
    NavigableMap<Long, Snapshot> getSnapshots();

    boolean isInitialized();

    URI getLocalUri();
    URI getLeaderUri();
    int getTerm();
    RaftServer.Roll getRole();

    long commitIndex();

    Long lastApplied();

    List<URI> getConfigNew();
    List<URI> getConfigOld();
    List<URI> getConfigAll();
    boolean isJointConsensus();
    long getConfigEpoch();

}
