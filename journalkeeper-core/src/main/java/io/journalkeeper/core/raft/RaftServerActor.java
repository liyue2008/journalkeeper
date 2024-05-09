package io.journalkeeper.core.raft;

import io.journalkeeper.core.api.*;
import io.journalkeeper.core.config.ServerConfigDeclaration;
import io.journalkeeper.core.entry.internal.InternalEntriesSerializeSupport;
import io.journalkeeper.core.entry.internal.InternalEntryType;
import io.journalkeeper.core.entry.internal.UpdateVotersS1Entry;
import io.journalkeeper.core.entry.internal.UpdateVotersS2Entry;
import io.journalkeeper.core.journal.JournalActor;
import io.journalkeeper.core.state.ConfigState;
import io.journalkeeper.core.state.StateActor;
import io.journalkeeper.exceptions.JournalException;
import io.journalkeeper.persistence.LockablePersistence;
import io.journalkeeper.persistence.PersistenceFactory;
import io.journalkeeper.rpc.client.*;
import io.journalkeeper.rpc.server.*;
import io.journalkeeper.utils.actor.*;
import io.journalkeeper.utils.actor.annotation.ActorListener;
import io.journalkeeper.utils.actor.annotation.ActorMessage;
import io.journalkeeper.utils.actor.annotation.ResponseManually;
import io.journalkeeper.utils.config.Config;
import io.journalkeeper.utils.config.PropertiesConfigProvider;
import io.journalkeeper.utils.spi.ServiceSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static io.journalkeeper.core.api.RaftJournal.INTERNAL_PARTITION;

public class RaftServerActor implements  RaftServer {
    private static final Logger logger = LoggerFactory.getLogger( RaftServerActor.class );

    private final ServerContext context;
    private ServerRpc serverRpc;
    private final Actor actor = Actor.builder("RaftServer").setHandlerInstance(this).build();


    public RaftServerActor(Roll roll, StateFactory stateFactory, JournalEntryParser journalEntryParser, Properties properties) {
        this.persistenceFactory = ServiceSupport.load(PersistenceFactory.class);

        Config config = new Config();
        ServerConfigDeclaration serverConfigDeclaration = new ServerConfigDeclaration();
        serverConfigDeclaration.declare(config);
        config.load(new PropertiesConfigProvider(properties));

        this.context = buildServerContext(roll, stateFactory, journalEntryParser, properties, config);

    }

    private ServerContext buildServerContext(Roll roll, StateFactory stateFactory, JournalEntryParser journalEntryParser, Properties properties, Config config) {
        JournalActor journalActor = new JournalActor(journalEntryParser, config, properties);
        StateActor stateActor = new StateActor(roll, stateFactory, journalEntryParser, journalActor.getRaftJournal(),config, properties);
        VoterActor voterActor = new VoterActor(journalActor.getRaftJournal(),stateActor.getState(), config);
        LeaderActor leaderActor = new LeaderActor(journalEntryParser, journalActor.getRaftJournal(), stateActor.getState(), config);
        ServerRpcActor serverRpcActor = new ServerRpcActor();
        this.serverRpc = serverRpcActor;
        RpcActor rpcActor = new RpcActor(properties);
        EventBusActor eventBusActor = new EventBusActor();
        FollowerActor followerActor = new FollowerActor(stateActor.getState(), journalActor.getRaftJournal(), config);

        PostOffice postOffice = PostOffice.builder()
                .addActor(actor)
                .name(config.get("server_name"))
                .addActor(journalActor.getActor())
                .addActor(stateActor.getActor())
                .addActor(voterActor.getActor())
                .addActor(leaderActor.getActor())
                .addActor(serverRpcActor.getActor())
                .addActor(rpcActor.getActor())
                .addActor(eventBusActor.getActor())
                .addActor(followerActor.getActor())
                .build();
        return new ServerContext(properties, config, stateActor.getState(), journalActor.getRaftJournal(), voterActor.getRaftVoter(),eventBusActor.getEventBus(), postOffice);

    }

    @Override
    public Roll roll() {
        return this.context.getState().getRole();
    }

    @Override
    public void init(URI uri, List<URI> voters, Set<Integer> partitions, URI preferredLeader) throws IOException {
        try {
            actor.sendThen("State", "init", uri, voters, partitions, preferredLeader).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new JournalException(e);
        }
    }

    @Override
    public boolean isInitialized() {
        return this.context.getState().isInitialized();
    }

    @Override
    public void recover() throws IOException {
        acquireFileLock();
        actor.<JournalActor.RecoverJournalRequest>sendThen("State", "recover")
                .thenCompose(request -> actor.sendThen("Journal", "recover", request))
                .thenCompose(r -> CompletableFuture.allOf(
                        actor.sendThen("RaftServer", "recoverVoterConfig"),
                        actor.sendThen("Voter", "maybeUpdateTermOnRecovery")
                )).whenComplete((r, e) -> {
                    if (e != null) {
                        logger.warn("Recover failed!", e);
                    } else {
                        logger.info("Recover ends!");
                    }
                    releaseFileLock();
                });
    }

    /**
     * Check reserved entries to ensure the last UpdateVotersConfig entry is applied to the current voter config.
     */
    @ActorListener
    @ResponseManually
    private void recoverVoterConfig(@ActorMessage ActorMsg msg) {
        boolean isRecoveredFromJournal = false;
        List<CompletableFuture<?>> futures = new ArrayList<>();
        for (long index = context.getJournal().maxIndex(INTERNAL_PARTITION) - 1;
             index >= context.getJournal().minIndex(INTERNAL_PARTITION);
             index--) {
            JournalEntry entry = context.getJournal().readByPartition(INTERNAL_PARTITION, index);
            InternalEntryType type = InternalEntriesSerializeSupport.parseEntryType(entry.getPayload().getBytes());

            if (type == InternalEntryType.TYPE_UPDATE_VOTERS_S1) {
                UpdateVotersS1Entry updateVotersS1Entry = InternalEntriesSerializeSupport.parse(entry.getPayload().getBytes());
                futures.add(actor.sendThen("State","setConfigState", new ConfigState(
                        updateVotersS1Entry.getConfigOld(), updateVotersS1Entry.getConfigNew())));
                isRecoveredFromJournal = true;
                break;
            } else if (type == InternalEntryType.TYPE_UPDATE_VOTERS_S2) {
                UpdateVotersS2Entry updateVotersS2Entry = InternalEntriesSerializeSupport.parse(entry.getPayload().getBytes());
                futures.add(actor.sendThen("State","setConfigState", new ConfigState(updateVotersS2Entry.getConfigNew())));
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
                .thenRun(() -> actor.reply(msg, null));
    }

    @Override
    public URI serverUri() {
        return context.getState().getLocalUri();
    }

    @Override
    public void start() {
        actor.pub("onStart", context);
    }

    @Override
    public void stop() {
        actor.pub("onStop");
        context.getPostOffice().stop();
    }

    @Override
    public ServerState serverState() {
        return null;
    }


    private LockablePersistence lockablePersistence;
    private static final String LOCK_FILE = "lock";

    private Path lockFilePath() {
        return context.getConfig().<Path>get("working_dir").resolve(LOCK_FILE);
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


    @ActorListener
    private List<URI> getObservers() {
        // TODO
        return Collections.emptyList();
    }

    @ActorListener
    private GetServersResponse getServers() {
        // TODO
        return null;
    }
    @ActorListener
    private GetServerStatusResponse getServerStatus() {
        // TODO
        return null;
    }

    @ActorListener
    private UpdateVotersResponse updateVoters(UpdateVotersRequest request) {
        // TODO
        return null;
    }


    @ActorListener
    private GetServerEntriesResponse getServerEntries(GetServerEntriesRequest request) {
        // TODO
        return null;
    }

    @ActorListener
    private InstallSnapshotResponse installSnapshot(InstallSnapshotRequest request) {
        // TODO
        return null;
    }

    public ServerRpc getServerRpc() {
        return serverRpc;
    }
}
