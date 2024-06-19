package io.journalkeeper.core.raft;

import io.journalkeeper.core.state.ConfigState;
import io.journalkeeper.core.state.Snapshot;

import java.net.URI;
import java.util.List;
import java.util.NavigableMap;

public interface RaftState {
    NavigableMap<Long, Snapshot> getSnapshots();

    boolean isInitialized();

    URI getLocalUri();

    long commitIndex();

    Long lastApplied();

    List<URI> getConfigNew();
    List<URI> getConfigOld();
    List<URI> getConfigAll();
    boolean isJointConsensus();
    long getConfigEpoch();

    URI getPreferredLeader();

    ConfigState getConfigState();

}
