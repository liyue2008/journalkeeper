package io.journalkeeper.core.journal;

import io.journalkeeper.core.api.JournalEntry;
import io.journalkeeper.core.api.JournalEntryParser;
import io.journalkeeper.core.api.RaftJournal;
import io.journalkeeper.exceptions.StateRecoverException;
import io.journalkeeper.persistence.BufferPool;
import io.journalkeeper.persistence.PersistenceFactory;
import io.journalkeeper.utils.actor.Actor;
import io.journalkeeper.utils.actor.annotation.ActorListener;
import io.journalkeeper.utils.actor.annotation.ActorScheduler;
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

    private final Actor actor = Actor.builder("Journal").setHandlerInstance(this).build();

    public JournalActor(JournalEntryParser journalEntryParser, Config config, Properties properties) {
        this.config = config;
        this.properties = properties;
        persistenceFactory = ServiceSupport.load(PersistenceFactory.class);
        bufferPool = ServiceSupport.load(BufferPool.class);
        this.journal = new Journal(persistenceFactory, bufferPool, journalEntryParser);
    }

    public RaftJournal getRaftJournal() {
        return journal;
    }

    @ActorListener
    private void recover(RecoverJournalRequest request) {
        try {
            doRecover(request.getPartitions(), request.getJournalSnapshot(), request.getCommitIndex());
        } catch (IOException e) {
            throw new StateRecoverException(e);
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

    @ActorListener
    private long append(List<JournalEntry> journalEntries) {
        long position;
        if (journalEntries.size() == 1) {
            position = journal.append(journalEntries.get(0));
        } else {
            List<Long> positions = journal.append(journalEntries);
            position = positions.get(positions.size() - 1);
        }
        actor.pub("onJournalAppend", this.getRaftJournal());
        return position;
    }
    @ActorListener
    private void compareOrAppendRaw(List<byte[]> entries, long startIndex) {
        journal.compareOrAppendRaw(entries, startIndex);
    }

    @ActorListener
    private void commit(long commitIndex) throws IOException {

        if (commitIndex > journal.commitIndex()) {
            journal.commit(Math.min(commitIndex, journal.maxIndex()));
            actor.pub("onJournalCommit", this.getRaftJournal());
        }

    }
    @ActorScheduler(interval = 100L)
    private void flush() {
        long flushCount = journal.flushOnce();
        if (flushCount > 0) {
            actor.pub("onJournalFlush", this.journal.maxIndex());
        }
    }

    public Actor getActor() {
        return actor;
    }
}
