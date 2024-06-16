package io.journalkeeper.core.raft;

import io.journalkeeper.core.api.*;
import io.journalkeeper.core.config.ServerConfigDeclaration;
import io.journalkeeper.core.journal.JournalActor;
import io.journalkeeper.core.metric.MetricProvider;
import io.journalkeeper.core.metric.MetricProviderImpl;
import io.journalkeeper.core.state.StateActor;
import io.journalkeeper.exceptions.JournalException;
import io.journalkeeper.exceptions.RecoverException;
import io.journalkeeper.monitor.MonitorCollector;
import io.journalkeeper.persistence.LockablePersistence;
import io.journalkeeper.persistence.PersistenceFactory;
import io.journalkeeper.rpc.client.*;
import io.journalkeeper.rpc.server.*;
import io.journalkeeper.utils.actor.*;
import io.journalkeeper.utils.actor.annotation.ActorListener;
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
    private final Collection<MonitorCollector> monitorCollectors;
    private final RaftServerMonitorInfoProvider monitorInfoProvider;

    public RaftServerActor(Roll roll, StateFactory stateFactory, JournalEntryParser journalEntryParser, Properties properties) {
        this.persistenceFactory = ServiceSupport.load(PersistenceFactory.class);

        Config config = new Config();
        ServerConfigDeclaration serverConfigDeclaration = new ServerConfigDeclaration();
        serverConfigDeclaration.declare(config);
        config.load(new PropertiesConfigProvider(properties));

        this.context = buildServerContext(roll, stateFactory, journalEntryParser, properties, config);
        this.monitorCollectors = ServiceSupport.loadAll(MonitorCollector.class);
        this.monitorInfoProvider = new RaftServerMonitorInfoProvider(context);
    }

    private ServerContext buildServerContext(Roll roll, StateFactory stateFactory, JournalEntryParser journalEntryParser, Properties properties, Config config) {
        MetricProviderImpl metricProvider  = new MetricProviderImpl(config.get("enable_metric"), config.get("print_metric_interval_sec"));
        JournalActor journalActor = new JournalActor(journalEntryParser, config, properties);
        StateActor stateActor = new StateActor(stateFactory, journalEntryParser, journalActor.getRaftJournal(),config, properties);
        VoterActor voterActor = new VoterActor(roll, journalEntryParser, journalActor.getRaftJournal(),stateActor.getState(), metricProvider, config);
        ServerRpcActor serverRpcActor = new ServerRpcActor();

        this.serverRpc = serverRpcActor;
        RpcActor rpcActor = new RpcActor(properties);
        EventBusActor eventBusActor = new EventBusActor();

        PostOffice postOffice = PostOffice.builder()
                .addActor(actor)
                .name(config.get("server_name"))
                .addActor(journalActor.getActor())
                .addActor(stateActor.getActor())
                .addActor(voterActor.getActor())
                .addActor(serverRpcActor.getActor())
                .addActor(rpcActor.getActor())
                .addActor(eventBusActor.getActor())
                .addActor(metricProvider.getActor())
                .build();
        return new ServerContext(properties, config, stateActor.getState(),
                journalActor.getRaftJournal(), voterActor.getMonitoredVoter(),
                journalActor.getMonitoredJournal(), eventBusActor.getEventBus(),
                postOffice, this);

    }

    @Override
    public Roll roll() {
        throw new UnsupportedOperationException("未实现");
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
        try {
            actor.<JournalActor.RecoverJournalRequest>sendThen("State", "recover")
                    .thenCompose(request -> actor.sendThen("Journal", "recover", request))
                    .thenCompose(r -> CompletableFuture.allOf(
                            actor.sendThen("State", "recoverVoterConfig"),
                            actor.sendThen("Voter", "maybeUpdateTermOnRecovery")
                    )).whenComplete((r, e) -> {
                        if (e != null) {
                            logger.warn("Recover failed!", e);
                        } else {
                            logger.info("Recover success!");
                        }
                        releaseFileLock();
                    }).get();
        } catch (Throwable e) {
            throw new RecoverException(e);
        }
    }


    @Override
    public URI serverUri() {
        return context.getState().getLocalUri();
    }


    private void addMonitorProviderToCollectors() {
        if (null != monitorCollectors) {
            for (MonitorCollector monitorCollector : monitorCollectors) {
                monitorCollector.addServer(monitorInfoProvider);
            }
        }
    }

    private void removeMonitorProviderToCollectors() {
        if (null != monitorCollectors) {
            for (MonitorCollector monitorCollector : monitorCollectors) {
                monitorCollector.removeServer(monitorInfoProvider);
            }
        }
    }

    @Override
    public void start() {
        try {
            startAsync().get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public CompletableFuture<Void> startAsync() {
        addMonitorProviderToCollectors();

        return actor.pubThen("onStart", context);
    }

    @Override
    public void stop() {
        try {
            stopAsync().get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public CompletableFuture<Void> stopAsync() {
        return actor.pubThen("onStop").thenRunAsync(() -> {
            context.getPostOffice().stop();
            removeMonitorProviderToCollectors();
        });
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
        // TODO 维护Raft集群和Observer的心跳。
        return Collections.emptyList();
    }

    @ActorListener
    private GetServersResponse getServers() {
        // TODO
        return null;
    }



    public ServerRpc getServerRpc() {
        return serverRpc;
    }
}
