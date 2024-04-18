package io.journalkeeper.core.raft;

import io.journalkeeper.core.api.VoterState;

public interface RaftVoter {

    VoterState getVoterState();
}
