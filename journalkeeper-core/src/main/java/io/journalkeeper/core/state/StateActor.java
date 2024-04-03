package io.journalkeeper.core.state;

import io.journalkeeper.core.api.StateFactory;
import io.journalkeeper.core.entry.internal.ReservedPartition;
import io.journalkeeper.core.config.ServerConfigDeclaration;
import io.journalkeeper.core.journal.JournalActor;
import io.journalkeeper.core.server.PartialSnapshot;
import io.journalkeeper.exceptions.RecoverException;
import io.journalkeeper.persistence.LockablePersistence;
import io.journalkeeper.persistence.MetadataPersistence;
import io.journalkeeper.persistence.PersistenceFactory;
import io.journalkeeper.persistence.ServerMetadata;
import io.journalkeeper.utils.actor.ActorBase;
import io.journalkeeper.utils.actor.PostOffice;
import io.journalkeeper.utils.config.Config;
import io.journalkeeper.utils.files.FileUtils;
import io.journalkeeper.utils.spi.ServiceSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

import static io.journalkeeper.core.api.RaftJournal.DEFAULT_PARTITION;
import static io.journalkeeper.core.api.RaftJournal.INTERNAL_PARTITION;
import static io.journalkeeper.core.transaction.JournalTransactionManager.TRANSACTION_PARTITION_COUNT;
import static io.journalkeeper.core.transaction.JournalTransactionManager.TRANSACTION_PARTITION_START;

/**
 * 管理State、Metadata和Snapshots的Actor
 */
public class StateActor extends ActorBase {
    private static final Logger logger = LoggerFactory.getLogger( StateActor.class );

    private static final String STATE_PATH = "state";
    private static final String SNAPSHOTS_PATH = "snapshots";
    private static final String METADATA_PATH = "metadata";
    private static final String METADATA_FILE = "metadata";
    private static final String PARTIAL_SNAPSHOT_PATH = "partial_snapshot";


    /**
     * 存放节点上所有状态快照的稀疏数组，数组的索引（key）就是快照对应的日志位置的索引
     */
    protected final NavigableMap<Long, Snapshot> snapshots = new ConcurrentSkipListMap<>();
    protected final PartialSnapshot partialSnapshot;

    protected final StateFactory stateFactory;
    /**
     * 持久化实现接入点
     */
    protected PersistenceFactory persistenceFactory;
    /**
     * 元数据持久化服务
     */
    protected MetadataPersistence metadataPersistence;

    private LockablePersistence lockablePersistence;
    private URI uri;

    /**
     * 节点上的最新状态 和 被状态机执行的最大日志条目的索引值（从 0 开始递增）
     */
    protected final JournalKeeperState state;

    private final Config config;

    private final Properties properties;

    public StateActor(StateFactory stateFactory, Config config, Properties properties, PostOffice postOffice) {
        super(DEFAULT_CAPACITY, postOffice);
        this.stateFactory = stateFactory;
        this.config = config;
        this.properties = properties;
        persistenceFactory = ServiceSupport.load(PersistenceFactory.class);

        metadataPersistence = persistenceFactory.createMetadataPersistenceInstance();

        this.state = new JournalKeeperState(stateFactory, metadataPersistence);

        this.partialSnapshot = new PartialSnapshot(partialSnapshotPath());

//        TODO: 设计这部分拦截器的实现
//        state.addInterceptor(InternalEntryType.TYPE_SCALE_PARTITIONS, this::scalePartitions);
//        state.addInterceptor(InternalEntryType.TYPE_LEADER_ANNOUNCEMENT, this::announceLeader);
//        state.addInterceptor(InternalEntryType.TYPE_CREATE_SNAPSHOT, this::createSnapShot);
//        state.addInterceptor(InternalEntryType.TYPE_RECOVER_SNAPSHOT, this::recoverSnapShot);

    }


    private Path workingDir() {
        return config.get("working_dir");
    }

    private Path statePath() {
        return workingDir().resolve(STATE_PATH);
    }

    private Path metadataFile() {
        return workingDir().resolve(METADATA_PATH).resolve(METADATA_FILE);
    }



    protected Path snapshotsPath() {
        return workingDir().resolve(SNAPSHOTS_PATH);
    }
    protected Path partialSnapshotPath() {
        return workingDir().resolve(PARTIAL_SNAPSHOT_PATH);
    }

//    private void createSnapshot() {
//        long lastApplied = state.lastApplied();
//        logger.info("Creating snapshot at index: {}...", lastApplied);
//        Path snapshotPath = snapshotsPath().resolve(String.valueOf(lastApplied));
//        try {
//            FileUtils.deleteFolder(snapshotPath);
//            state.dump(snapshotPath);
//            Snapshot.markComplete(snapshotPath);
//            Snapshot snapshot = new Snapshot(stateFactory, metadataPersistence);
//            snapshot.recover(snapshotPath, properties);
//            snapshot.createSnapshot(journal);
//
//            snapshots.put(snapshot.lastApplied(), snapshot);
//            logger.info("Snapshot at index: {} created, {}.", lastApplied, snapshot);
//
//        } catch (IOException e) {
//            logger.warn("Create snapshot exception! Snapshot: {}.", snapshotPath, e);
//        }
//    }


    private void createFistSnapshot(List<URI> voters, Set<Integer> partitions, URI preferredLeader) throws IOException {
        Snapshot snapshot = new Snapshot(stateFactory, metadataPersistence);
        Path snapshotPath = snapshotsPath().resolve(String.valueOf(0L));
        snapshot.init(snapshotPath, voters, partitions, preferredLeader);
        Snapshot.markComplete(snapshotPath);
        snapshot.recover(snapshotPath, properties);
        snapshot.createFirstSnapshot(partitions);
        snapshot.close();
    }


    /**
     * 上次保存的元数据
     */
    private ServerMetadata lastSavedServerMetadata = null;


    protected ServerMetadata createServerMetadata() {
        ServerMetadata serverMetadata = new ServerMetadata();
        serverMetadata.setInitialized(true);
        serverMetadata.setThisServer(uri);
        serverMetadata.setCommitIndex(0L);
        return serverMetadata;
    }

    public  static  class  InitRequest {
        private final URI uri;
        private final List<URI> voters;
        private final Set<Integer> userPartitions;
        private final URI preferredLeader;

        public InitRequest(URI uri, List<URI> voters, Set<Integer> userPartitions, URI preferredLeader) {
            this.uri = uri;
            this.voters = voters;
            this.userPartitions = userPartitions;
            this.preferredLeader = preferredLeader;
        }

        public URI getUri() {
            return uri;
        }

        public List<URI> getVoters() {
            return voters;
        }

        public Set<Integer> getUserPartitions() {
            return userPartitions;
        }

        public URI getPreferredLeader() {
            return preferredLeader;
        }
    }
    private void init (InitRequest request) {
        try {
            doInit(request.getUri(), request.getVoters(), request.getUserPartitions(), request.getPreferredLeader());
        } catch (IOException e) {
            logger.error("Initialization failed!", e);
        }

    }

    private  void doInit(URI uri, List<URI> voters, Set<Integer> userPartitions, URI preferredLeader) throws IOException {
            if (null == userPartitions) {
                userPartitions = new HashSet<>();
            }

            if (userPartitions.isEmpty()) {
                userPartitions.add(DEFAULT_PARTITION);
            }

            ReservedPartition.validatePartitions(userPartitions);
            this.uri = uri;
            Set<Integer> partitions = new HashSet<>(userPartitions);
            partitions.add(INTERNAL_PARTITION);
            partitions.addAll(IntStream.range(TRANSACTION_PARTITION_START, TRANSACTION_PARTITION_START + TRANSACTION_PARTITION_COUNT).boxed().collect(Collectors.toSet()));
            state.init(statePath(), voters, partitions, preferredLeader);
            createFistSnapshot(voters, partitions, preferredLeader);
            lastSavedServerMetadata = createServerMetadata();
            metadataPersistence.save(metadataFile(), lastSavedServerMetadata);
    }

    public boolean isInitialized() {
        try {
            ServerMetadata metadata = metadataPersistence.load(metadataFile(), ServerMetadata.class);
            return metadata != null && metadata.isInitialized();
        } catch (Exception e) {
            return false;
        }
    }

    private void recover() {

        try {
            doRecover();
            send("Journal", "recover",
                    new JournalActor.RecoverJournalRequest(
                            state.getPartitions(),
                            snapshots.firstEntry().getValue().getJournalSnapshot(),
                            lastSavedServerMetadata.getCommitIndex()
                    )
            );
        } catch (Exception e) {
            logger.error("Recover state failed! Path: {}", statePath(), e);
        }
    }
    private void doRecover() throws IOException {

        lastSavedServerMetadata = metadataPersistence.load(metadataFile(), ServerMetadata.class);
        if (lastSavedServerMetadata == null || !lastSavedServerMetadata.isInitialized()) {
            throw new RecoverException(
                    String.format("Recover failed! Cause: metadata is not initialized. Metadata path: %s.",
                            metadataFile().toString()));
        }
        onMetadataRecovered(lastSavedServerMetadata);
        state.recover(statePath(), properties);
        recoverSnapshots();


    }


    private void recoverSnapshots() throws IOException {
        if (!Files.isDirectory(snapshotsPath())) {
            Files.createDirectories(snapshotsPath());
        }
        StreamSupport.stream(
                        Files.newDirectoryStream(snapshotsPath(),
                                entry -> entry.getFileName().toString().matches("\\d+")
                        ).spliterator(), false)
                .map(path -> {
                    try {
                        Snapshot snapshot = new Snapshot(stateFactory, metadataPersistence);
                        snapshot.recover(path, properties);
                        if (Long.parseLong(path.getFileName().toString()) == snapshot.lastApplied()) {
                            return snapshot;
                        } else {
                            return null;
                        }
                    } catch (Throwable t) {
                        logger.warn("Recover snapshot {} exception: ", path.toString(), t);
                        return null;
                    }
                }).filter(Objects::nonNull)
                .peek(Snapshot::close)
                .forEach(snapshot -> snapshots.put(snapshot.lastApplied(), snapshot));
    }

    protected void onMetadataRecovered(ServerMetadata metadata) {
        this.uri = metadata.getThisServer();
    }



}
