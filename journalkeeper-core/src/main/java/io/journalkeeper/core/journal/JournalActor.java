package io.journalkeeper.core.journal;

import io.journalkeeper.core.api.JournalEntry;
import io.journalkeeper.core.api.JournalEntryParser;
import io.journalkeeper.core.api.RaftJournal;
import io.journalkeeper.persistence.BufferPool;
import io.journalkeeper.persistence.PersistenceFactory;
import io.journalkeeper.rpc.server.AsyncAppendEntriesRequest;
import io.journalkeeper.rpc.server.AsyncAppendEntriesResponse;
import io.journalkeeper.utils.actor.Actor;
import io.journalkeeper.utils.actor.ActorListener;
import io.journalkeeper.utils.actor.ActorMsg;
import io.journalkeeper.utils.config.Config;
import io.journalkeeper.utils.spi.ServiceSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.Set;


public class JournalActor {
private static final Logger logger = LoggerFactory.getLogger( JournalActor.class );
    private final PersistenceFactory persistenceFactory;
    private final Journal journal;

    private final BufferPool bufferPool;

    private final Properties properties;

    private final Config config;

    private final Actor actor = new Actor("Journal");

    public JournalActor(JournalEntryParser journalEntryParser, Config config, Properties properties) {
        this.config = config;
        this.properties = properties;
        persistenceFactory = ServiceSupport.load(PersistenceFactory.class);
        bufferPool = ServiceSupport.load(BufferPool.class);
        this.journal = new Journal(persistenceFactory, bufferPool, journalEntryParser);
        this.actor.setHandlerInstance(this);
    }

    public RaftJournal getRaftJournal() {
        return journal;
    }

    @ActorListener
    private void recover(RecoverJournalRequest request) {
        boolean success = false;
        try {
            doRecover(request.getPartitions(), request.getJournalSnapshot(), request.getCommitIndex());
            success = true;
        } catch (Exception e) {
            logger.error("Recover Journal failed! Path: {}", config.get("working_dir"), e);
        } finally {
            actor.send("RaftServer", "recovered", success);
        }
    }
    private void doRecover(Set<Integer> partitions, JournalSnapshot journalSnapshot, long commitIndex) throws IOException {
        journal.recover(config.get("working_dir"), commitIndex, journalSnapshot, properties);
        journal.rePartition(partitions);
    }

    public static class RecoverJournalRequest{
        private final Set<Integer> partitions;
        private final JournalSnapshot journalSnapshot;
        private final long commitIndex;

        public RecoverJournalRequest(Set<Integer> partitions, JournalSnapshot journalSnapshot, long commitIndex) {
            this.partitions = partitions;
            this.journalSnapshot = journalSnapshot;
            this.commitIndex = commitIndex;
        }

        public Set<Integer> getPartitions() {
            return partitions;
        }

        public JournalSnapshot getJournalSnapshot() {
            return journalSnapshot;
        }

        public long getCommitIndex() {
            return commitIndex;
        }
    }

    @ActorListener(response = true, payload = true)
    private long append(List<JournalEntry> journalEntries) {
        if (journalEntries.size() == 1) {
            return journal.append(journalEntries.get(0));
        } else {
            List<Long> positions = journal.append(journalEntries);
            return positions.get(positions.size() - 1);
        }
    }
    @ActorListener(payload = true)
    private void compareOrAppendRaw(ActorMsg msg) {
        AsyncAppendEntriesRequest request = msg.getPayload();
        long startIndex = request.getPrevLogIndex() + 1;
        List<byte[]> entries = request.getEntries();
        if (!entries.isEmpty()) {
            journal.compareOrAppendRaw(request.getEntries(), startIndex);
        }
        actor.send("State", "maybeUpdateNonLeaderConfig", msg);
    }

    @ActorListener(payload = true)
    private void commit(ActorMsg msg) {
        AsyncAppendEntriesRequest request = msg.getPayload();
        try {
            if (request.getLeaderCommit() > journal.commitIndex()) {
                journal.commit(Math.min(request.getLeaderCommit(), journal.maxIndex()));
//            threads.wakeupThread(threadName(STATE_MACHINE_THREAD));
            }
        } catch (Exception e) {
           logger.warn("Commit failed!");
           actor.reply(msg, new AsyncAppendEntriesResponse(e));
           return;
        }
        actor.send("Follower", "onCommit", msg);
        actor.pubMsg("onJournalCommit", this.getRaftJournal());
    }

    public Actor getActor() {
        return actor;
    }
}
