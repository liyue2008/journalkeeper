package io.journalkeeper.core.journal;

import io.journalkeeper.core.api.JournalEntryParser;
import io.journalkeeper.core.api.RaftJournal;
import io.journalkeeper.core.raft.ServerContext;
import io.journalkeeper.persistence.BufferPool;
import io.journalkeeper.persistence.PersistenceFactory;
import io.journalkeeper.utils.actor.ActorBase;
import io.journalkeeper.utils.actor.PostOffice;
import io.journalkeeper.utils.config.Config;
import io.journalkeeper.utils.spi.ServiceSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Set;

public class JournalActor extends ActorBase {
private static final Logger logger = LoggerFactory.getLogger( JournalActor.class );
    private final PersistenceFactory persistenceFactory;
    private final Journal journal;

    private final BufferPool bufferPool;

    private final ServerContext serverContext;

    private final Config config;


    public JournalActor(JournalEntryParser journalEntryParser, ServerContext serverContext) {
        super(DEFAULT_CAPACITY, serverContext.getPostOffice());
        this.serverContext = serverContext;
        persistenceFactory = ServiceSupport.load(PersistenceFactory.class);
        bufferPool = ServiceSupport.load(BufferPool.class);
        this.config = serverContext.getConfig();
        this.journal = new Journal(persistenceFactory, bufferPool, journalEntryParser);
    }

    public RaftJournal getRaftJournal() {
        return journal;
    }

    private void recover(RecoverJournalRequest request) {
        boolean success = false;
        try {
            doRecover(request.getPartitions(), request.getJournalSnapshot(), request.getCommitIndex());
            success = true;
        } catch (Exception e) {
            logger.error("Recover Journal failed! Path: {}", config.get("working_dir"), e);
        } finally {
            send("RaftServer", "recoverd", success);
        }
    }
    private void doRecover(Set<Integer> partitions, JournalSnapshot journalSnapshot, long commitIndex) throws IOException {
        journal.recover(config.get("working_dir"), commitIndex, journalSnapshot, serverContext.getProperties());
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

    @Override
    public String addr() {
        return "Journal";
    }
}
