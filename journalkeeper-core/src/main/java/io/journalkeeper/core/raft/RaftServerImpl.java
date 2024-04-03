package io.journalkeeper.core.raft;

import io.journalkeeper.core.api.*;
import io.journalkeeper.core.entry.internal.InternalEntriesSerializeSupport;
import io.journalkeeper.core.entry.internal.InternalEntryType;
import io.journalkeeper.core.entry.internal.UpdateVotersS1Entry;
import io.journalkeeper.core.entry.internal.UpdateVotersS2Entry;
import io.journalkeeper.core.journal.Journal;
import io.journalkeeper.core.journal.JournalActor;
import io.journalkeeper.core.state.ConfigState;
import io.journalkeeper.core.state.StateActor;
import io.journalkeeper.persistence.LockablePersistence;
import io.journalkeeper.persistence.PersistenceFactory;
import io.journalkeeper.utils.actor.ActorBase;
import io.journalkeeper.utils.actor.ActorMsg;
import io.journalkeeper.utils.actor.PostOffice;
import io.journalkeeper.utils.config.Config;
import io.journalkeeper.utils.config.PropertiesConfigProvider;
import io.journalkeeper.utils.spi.ServiceSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static io.journalkeeper.core.api.RaftJournal.INTERNAL_PARTITION;

public class RaftServerImpl extends ActorBase implements RaftServer {
    private static final Logger logger = LoggerFactory.getLogger( RaftServerImpl.class );

    private final Properties properties;
    private final JournalEntryParser journalEntryParser;

    private final ServerContext serverContext;

    private Roll roll;

    private final StateActor stateActor;

    private final JournalActor journalActor;

    private final Config config;

    public RaftServerImpl(Roll roll, StateFactory stateFactory, JournalEntryParser journalEntryParser, Properties properties, PostOffice postOffice) {
        super(DEFAULT_CAPACITY, postOffice);
        this.roll = roll;
        this.journalEntryParser = journalEntryParser;
        this.properties = properties;
        this.serverContext = new ServerContext(properties);
        persistenceFactory = ServiceSupport.load(PersistenceFactory.class);

        config = serverContext.getConfig();
        config.load(new PropertiesConfigProvider(properties));

        this.stateActor = new StateActor(stateFactory, config, properties, postOffice);
        this.journalActor= new JournalActor(journalEntryParser, serverContext);
    }

    @Override
    public String addr() {
        return "RaftServer";
    }

    @Override
    public Roll roll() {
        return this.roll;
    }

    @Override
    public void init(URI uri, List<URI> voters, Set<Integer> partitions, URI preferredLeader) throws IOException {
        send("State", "init", new StateActor.InitRequest(uri, voters, partitions, preferredLeader));
    }

    @Override
    public boolean isInitialized() {
        return this.stateActor.isInitialized();
    }

    @Override
    public void recover() throws IOException {
        acquireFileLock();
        send("State", "recover", null);
    }
    private void recoverd(RaftJournal journal) {
        CompletableFuture.allOf(
            sendF("RaftServer", "recoverVoterConfig", null),
            sendF("Voter", "maybeUpdateTermOnRecovery", journal)
                ).thenRun(this::releaseFileLock);
    }

    /**
     * Check reserved entries to ensure the last UpdateVotersConfig entry is applied to the current voter config.
     */
    private void recoverVoterConfig(ActorMsg msg) {
        boolean isRecoveredFromJournal = false;
        RaftJournal journal = journalActor.getRaftJournal();
        List<CompletableFuture<?>> futures = new ArrayList<>();
        for (long index = journal.maxIndex(INTERNAL_PARTITION) - 1;
             index >= journal.minIndex(INTERNAL_PARTITION);
             index--) {
            JournalEntry entry = journal.readByPartition(INTERNAL_PARTITION, index);
            InternalEntryType type = InternalEntriesSerializeSupport.parseEntryType(entry.getPayload().getBytes());

            if (type == InternalEntryType.TYPE_UPDATE_VOTERS_S1) {
                UpdateVotersS1Entry updateVotersS1Entry = InternalEntriesSerializeSupport.parse(entry.getPayload().getBytes());
                futures.add(sendF("State","setConfigState", new ConfigState(
                        updateVotersS1Entry.getConfigOld(), updateVotersS1Entry.getConfigNew())));
                isRecoveredFromJournal = true;
                break;
            } else if (type == InternalEntryType.TYPE_UPDATE_VOTERS_S2) {
                UpdateVotersS2Entry updateVotersS2Entry = InternalEntriesSerializeSupport.parse(entry.getPayload().getBytes());
                futures.add(sendF("State","setConfigState", new ConfigState(updateVotersS2Entry.getConfigNew())));
                isRecoveredFromJournal = true;
                break;
            }
        }

        if (isRecoveredFromJournal) {
            logger.info("Voters config is recovered from journal.");
        } else {
            logger.info("No voters config entry found in journal, Using config in the metadata.");
        }

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .thenRun(() -> reply(msg, null));
    }

    @Override
    public URI serverUri() {
        return null;
    }

    @Override
    public void start() {

    }

    @Override
    public void stop() {

    }

    @Override
    public ServerState serverState() {
        return null;
    }


    private LockablePersistence lockablePersistence;
    private static final String LOCK_FILE = "lock";

    private Path lockFilePath() {
        return config.<Path>get("working_dir").resolve(LOCK_FILE);
    }
    /**
     * 持久化实现接入点
     */
    protected PersistenceFactory persistenceFactory;
    private void acquireFileLock() {
        if (null == this.lockablePersistence) {
            this.lockablePersistence = persistenceFactory.createLock(lockFilePath());
            try {
                this.lockablePersistence.lock();
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        } else {
            throw new IllegalStateException("File lock should be null!");
        }
    }

    private void releaseFileLock() {
        if (null != this.lockablePersistence) {
            try {
                this.lockablePersistence.unlock();
            } catch (IOException e) {
                logger.warn("Unlock file {} failed, cause: {}.", lockFilePath(), e.getMessage());
            }
            this.lockablePersistence = null;
        }
    }
}
