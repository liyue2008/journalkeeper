package io.journalkeeper.core.state;

import io.journalkeeper.core.api.*;
import io.journalkeeper.core.entry.internal.*;
import io.journalkeeper.core.journal.JournalActor;
import io.journalkeeper.core.raft.RaftState;
import io.journalkeeper.core.server.PartialSnapshot;
import io.journalkeeper.exceptions.RecoverException;
import io.journalkeeper.exceptions.StateRecoverException;
import io.journalkeeper.persistence.MetadataPersistence;
import io.journalkeeper.persistence.PersistenceFactory;
import io.journalkeeper.persistence.ServerMetadata;
import io.journalkeeper.rpc.client.*;
import io.journalkeeper.rpc.server.GetServerStateRequest;
import io.journalkeeper.rpc.server.GetServerStateResponse;
import io.journalkeeper.rpc.server.InstallSnapshotRequest;
import io.journalkeeper.rpc.server.InstallSnapshotResponse;
import io.journalkeeper.utils.ThreadSafeFormat;
import io.journalkeeper.utils.actor.*;
import io.journalkeeper.utils.actor.annotation.*;
import io.journalkeeper.utils.config.Config;
import io.journalkeeper.utils.spi.ServiceSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.nio.channels.ClosedByInterruptException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

import static io.journalkeeper.core.api.RaftJournal.DEFAULT_PARTITION;
import static io.journalkeeper.core.api.RaftJournal.INTERNAL_PARTITION;
import static io.journalkeeper.core.entry.internal.InternalEntryType.TYPE_UPDATE_VOTERS_S1;
import static io.journalkeeper.core.entry.internal.InternalEntryType.TYPE_UPDATE_VOTERS_S2;
import static io.journalkeeper.core.transaction.JournalTransactionManager.TRANSACTION_PARTITION_COUNT;
import static io.journalkeeper.core.transaction.JournalTransactionManager.TRANSACTION_PARTITION_START;

/**
 * 管理State、Metadata和Snapshots的Actor
 */
public class StateActor implements RaftState{
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

    private URI localUri;
    private RaftServer.Roll roll;


    private final JournalEntryParser journalEntryParser;
    /**
     * 节点上的最新状态 和 被状态机执行的最大日志条目的索引值（从 0 开始递增）
     */
    private final JournalKeeperState state;

    private final RaftJournal journal;

    private final Config config;

    private final Properties properties;

    private boolean isRecovered = false;

    public StateActor(RaftServer.Roll roll, StateFactory stateFactory, JournalEntryParser journalEntryParser, RaftJournal journal, Config config, Properties properties) {
        this.roll = roll;
        this.stateFactory = stateFactory;
        this.journalEntryParser = journalEntryParser;
        this.journal = journal;
        this.config = config;
        this.properties = properties;
        persistenceFactory = ServiceSupport.load(PersistenceFactory.class);

        metadataPersistence = persistenceFactory.createMetadataPersistenceInstance();

        this.state = new JournalKeeperState(stateFactory, metadataPersistence, actor);

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


    //Receiver implementation:
    //1. Reply immediately if term < currentTerm
    //2. Create new snapshot file if first chunk (offset is 0)
    //3. Write data into snapshot file at given offset
    //4. Reply and wait for more data chunks if done is false
    //5. Save snapshot file, discard any existing or partial snapshot
    //with a smaller index
    //6. If existing log entry has same index and term as snapshot’s
    //last included entry, retain log entries following it and reply
    //7. Discard the entire log
    //8. Reset state machine using snapshot contents (and load
    //snapshot’s cluster configuration)
    @ResponseManually
    @ActorListener
    private void installSnapshot(@ActorMessage ActorMsg msg) {
        InstallSnapshotRequest request = msg.getPayload();
        long offset = request.getOffset();
        long lastIncludedIndex = request.getLastIncludedIndex();
        int lastIncludedTerm = request.getLastIncludedTerm();
        byte[] data = request.getData();
        boolean isDone = request.isDone();
        actor.<Integer>sendThen("Voter", "onAppendEntries", request.getTerm(), request.getLeaderId())
                .thenAccept(term -> {
                    if (term > request.getTerm()) {
                        actor.reply(msg, new InstallSnapshotResponse(term));
                        return ;
                    }
                    logger.info("Install snapshot, offset: {}, lastIncludedIndex: {}, lastIncludedTerm: {}, data length: {}, isDone: {}... " +
                                    "journal minIndex: {}, maxIndex: {}, commitIndex: {}...",
                            ThreadSafeFormat.formatWithComma(offset),
                            ThreadSafeFormat.formatWithComma(lastIncludedIndex),
                            lastIncludedTerm,
                            data.length,
                            isDone,
                            ThreadSafeFormat.formatWithComma(journal.minIndex()),
                            ThreadSafeFormat.formatWithComma(journal.maxIndex()),
                            ThreadSafeFormat.formatWithComma(journal.commitIndex())
                    );
                    long lastApplied = lastIncludedIndex + 1;
                    Path snapshotPath = snapshotsPath().resolve(String.valueOf(lastApplied));
                    try {
                        partialSnapshot.installTrunk(offset, data, snapshotPath);

                        if (isDone) {
                            Snapshot snapshot;
                            logger.info("All snapshot files received, discard any existing snapshot with a same or smaller index...");
                            // discard any existing snapshot with a same or smaller index
                            NavigableMap<Long, Snapshot> headMap = snapshots.headMap(lastApplied, true);
                            while (!headMap.isEmpty()) {
                                snapshot = headMap.remove(headMap.firstKey());
                                logger.info("Discard snapshot: {}.", snapshot.getPath());
                                snapshot.close();
                                snapshot.clear();
                            }
                            partialSnapshot.finish();
                            logger.info("add the installed snapshot to snapshots: {}...", snapshotPath);
                            // add the installed snapshot to snapshots.
                            snapshot = new Snapshot(stateFactory, metadataPersistence);
                            snapshot.recover(snapshotPath, properties);
                            snapshots.put(lastApplied, snapshot);

                            logger.info("New installed snapshot: {}.", snapshot.getJournalSnapshot());

                            actor.send("Journal", "compact", snapshot.getJournalSnapshot(), lastIncludedIndex, lastIncludedTerm);

                            state.close();
                            state.clear();
                            snapshot.dump(statePath());
                            state.recover(statePath(), properties);
                            logger.info("Install snapshot successfully!");
                        }

                    } catch (IOException e) {
                        logger.warn("Install snapshot exception: ", e);
                        actor.reply(msg, new InstallSnapshotResponse(e));
                    }
                    actor.reply(msg, new InstallSnapshotResponse(term));
                });
    }

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
        serverMetadata.setThisServer(localUri);
        serverMetadata.setCommitIndex(0L);
        return serverMetadata;
    }

    private final Actor actor = Actor.builder("State").setHandlerInstance(this).build();

    public Actor getActor() {
        return actor;
    }
    public URI getServerUri() {
        return localUri;
    }

    public RaftState getState() {
        return this;
    }

    @ActorListener
    private void init (URI uri, List<URI> voters, Set<Integer> userPartitions, URI preferredLeader) {
        try {
            doInit(uri, voters,  userPartitions, preferredLeader);
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
            this.localUri = uri;
            Set<Integer> partitions = new HashSet<>(userPartitions);
            partitions.add(INTERNAL_PARTITION);
            partitions.addAll(IntStream.range(TRANSACTION_PARTITION_START, TRANSACTION_PARTITION_START + TRANSACTION_PARTITION_COUNT).boxed().collect(Collectors.toSet()));
            state.init(statePath(), voters, partitions, preferredLeader);
            createFistSnapshot(voters, partitions, preferredLeader);
            lastSavedServerMetadata = createServerMetadata();
            metadataPersistence.save(metadataFile(), lastSavedServerMetadata);
    }

    @Override
    public NavigableMap<Long, Snapshot> getSnapshots() {
        return Collections.unmodifiableNavigableMap(snapshots);
    }

    @Override
    public boolean isInitialized() {
        try {
            ServerMetadata metadata = metadataPersistence.load(metadataFile(), ServerMetadata.class);
            return metadata != null && metadata.isInitialized();
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public URI getLocalUri() {
        return localUri;
    }


    @Override
    public RaftServer.Roll getRole() {
        return roll;
    }

    @Override
    public long commitIndex() {
        return 0;
    }

    @ActorListener
    private JournalActor.RecoverJournalRequest recover() {

        try {
            doRecover();
            isRecovered = true;
            actor.pub("onStateRecovered");
            return new JournalActor.RecoverJournalRequest(
                    state.getPartitions(),
                    snapshots.firstEntry().getValue().getJournalSnapshot(),
                    lastSavedServerMetadata.getCommitIndex()
            );
        } catch (IOException e) {
            throw new StateRecoverException(e);
        }
    }
    private void doRecover() throws IOException {

        lastSavedServerMetadata = metadataPersistence.load(metadataFile(), ServerMetadata.class);
        if (lastSavedServerMetadata == null || !lastSavedServerMetadata.isInitialized()) {
            throw new RecoverException(
                    String.format("Recover failed! Cause: metadata is not initialized. Metadata path: %s.",
                            metadataFile()));
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
        this.localUri = metadata.getThisServer();
    }

    @ActorListener
    private void setConfigState(ActorMsg msg) {
        ConfigState configState = msg.getPayload();
        this.state.setConfigState(configState);
        actor.reply(msg, null);
    }
    @ActorListener
    public QueryStateResponse queryServerState(QueryStateRequest request) {
        StateQueryResult result = state.query(request.getQuery(), journal);
        return new QueryStateResponse(result.getResult(), result.getLastApplied());
    }

    @ActorListener(topic = "lastApplied")
    private LastAppliedResponse lastAppliedListener() {
        return new LastAppliedResponse(state.lastApplied());
    }

    @Override
    public Long lastApplied() {
        return state.lastApplied();
    }

    @Override
    public List<URI> getConfigNew() {
        return state.getConfigState().getConfigNew();
    }

    @Override
    public List<URI> getConfigOld() {
        return state.getConfigState().getConfigOld();
    }

    @Override
    public List<URI> getConfigAll() {
        return state.getConfigState().voters();
    }

    @Override
    public boolean isJointConsensus() {
        return state.getConfigState().isJointConsensus();
    }

    @Override
    public long getConfigEpoch() {
        return state.getConfigState().getEpoch();
    }

    @Override
    public URI getPreferredLeader() {
        return state.getPreferredLeader();
    }

    @Override
    public ConfigState getConfigState() {
        return state.getConfigState();
    }

    @ActorListener
    private List<URI> getVoters() {
        return state.voters();
    }

    @ActorListener
    private QueryStateResponse querySnapshot(QueryStateRequest request) {
        // TODO
        return null;
    }

    @ActorListener(topic = "getSnapshots")
    private GetSnapshotsResponse doGetSnapshots() {
        // TODO
        return  null;
    }
    @ActorListener
    private GetServerStateResponse getServerState(GetServerStateRequest request) {
        // TODO
        return null;
    }

    @ActorListener
    private ConvertRollResponse convertRoll(ConvertRollRequest request) {
        this.roll = request.getRoll();
        return new ConvertRollResponse();
    }
    @ActorListener
    private void maybeRollbackConfig(long startIndex) {
        ConfigState voterStateMachine = state.getConfigState();
        if (startIndex < journal.maxIndex()) {

            long index = journal.maxIndex(INTERNAL_PARTITION);
            long startOffset = journal.readOffset(startIndex);
            while (--index >= journal.minIndex(INTERNAL_PARTITION)) {
                JournalEntry entry = journal.readByPartition(INTERNAL_PARTITION, index);
                if (entry.getOffset() < startOffset) {
                    break;
                }
                InternalEntryType reservedEntryType = InternalEntriesSerializeSupport.parseEntryType(entry.getPayload().getBytes());
                if (reservedEntryType == TYPE_UPDATE_VOTERS_S2) {
                    UpdateVotersS2Entry updateVotersS2Entry = InternalEntriesSerializeSupport.parse(entry.getPayload().getBytes());
                    voterStateMachine.rollbackToJointConsensus(updateVotersS2Entry.getConfigOld());
                } else if (reservedEntryType == TYPE_UPDATE_VOTERS_S1) {
                    voterStateMachine.rollbackToOldConfig();
                }
            }
        }
    }
    @ActorListener
    private void maybeUpdateNonLeaderConfig(List<byte[]> entries) throws Exception {
        ConfigState voterStateMachine = state.getConfigState();

        for (byte[] rawEntry : entries) {
            JournalEntry entryHeader = journalEntryParser.parseHeader(rawEntry);
            if (entryHeader.getPartition() == INTERNAL_PARTITION) {
                int headerLength = journalEntryParser.headerLength();
                InternalEntryType entryType = InternalEntriesSerializeSupport.parseEntryType(rawEntry, headerLength);
                if (entryType == TYPE_UPDATE_VOTERS_S1) {
                    UpdateVotersS1Entry updateVotersS1Entry = InternalEntriesSerializeSupport.parse(rawEntry, headerLength, rawEntry.length - headerLength);

                    voterStateMachine.toJointConsensus(updateVotersS1Entry.getConfigOld(), updateVotersS1Entry.getConfigNew(),
                            () -> null);
                } else if (entryType == TYPE_UPDATE_VOTERS_S2) {
                    voterStateMachine.toNewConfig(() -> null);
                }
            }
        }

    }

    @ActorScheduler(interval = 100L) // TODO: flush interval 需要从配置中读取
    private void flush() {
        if(!isRecovered){
            return;
        }
        try {
            state.flush();
            ServerMetadata metadata = createServerMetadata();
            if (!metadata.equals(lastSavedServerMetadata)) {
                metadataPersistence.save(metadataFile(), metadata);
                lastSavedServerMetadata = metadata;
            }
            actor.pub("onStateFlush");
        } catch (ClosedByInterruptException ignored) {
        } catch (Throwable e) {
            logger.warn("Flush exception, commitIndex: {}, lastApplied: {}, server: {}: ",
                    journal.commitIndex(), state.lastApplied(), localUri, e);
        }
    }

    /**
     * 监听属性commitIndex的变化，
     * 当commitIndex变更时如果commitIndex > lastApplied，
     * 反复执行applyEntries直到lastApplied == commitIndex：
     * 1. 如果需要，复制当前状态为新的快照保存到属性snapshots, 索引值为lastApplied。
     * 2. lastApplied自增，将log[lastApplied]应用到状态机，更新当前状态state；
     *
     */
    @ActorSubscriber(topic = "onJournalCommit")
    private void applyEntries() {
        if(!isRecovered){
            return;
        }
        if (state.lastApplied() >= journal.commitIndex()) {
            return;
        }
        long offset = journal.readOffset(state.lastApplied());
        JournalEntry entryHeader = journal.readEntryHeaderByOffset(offset);
        StateResult stateResult = state.applyEntry(entryHeader, new EntryFutureImpl(journal, offset), journal);
        actor.pub("onStateChange", stateResult);
    }

}
