package io.journalkeeper.core.raft;

import io.journalkeeper.core.api.RaftJournal;
import io.journalkeeper.core.api.RaftServer;
import io.journalkeeper.core.api.ServerStatus;
import io.journalkeeper.rpc.client.GetServerStatusResponse;
import io.journalkeeper.utils.actor.Actor;
import io.journalkeeper.utils.actor.annotation.ActorListener;
import io.journalkeeper.utils.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ObserverActor {
    private final Actor actor = Actor.builder("Observer").setHandlerInstance(this).build();
    private static final Logger logger = LoggerFactory.getLogger(ObserverActor.class);
    private final RaftJournal journal;
    private final RaftState state;
    private final Config config;

    ObserverActor(RaftJournal journal, RaftState state, Config config) {
        this.journal = journal;
        this.state = state;
        this.config = config;
    }

    @ActorListener
    private GetServerStatusResponse getServerStatus() {

        return new GetServerStatusResponse(new ServerStatus(
                RaftServer.Roll.VOTER,
                journal.minIndex(),
                journal.maxIndex(),
                journal.commitIndex(),
                state.lastApplied(),
                null));
    }

    Actor getActor() {
        return actor;
    }
}
