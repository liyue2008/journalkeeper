package io.journalkeeper.core.state;

import io.journalkeeper.base.ReplicableIterator;
import io.journalkeeper.core.api.*;
import io.journalkeeper.core.entry.internal.*;
import io.journalkeeper.core.journal.JournalActor;
import io.journalkeeper.core.raft.RaftState;
import io.journalkeeper.core.server.PartialSnapshot;
import io.journalkeeper.exceptions.*;
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
import io.journalkeeper.utils.files.FileUtils;
import io.journalkeeper.utils.spi.ServiceSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.nio.channels.ClosedByInterruptException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
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
    // 用于给Observer发送snapshot
    private int nextSnapshotIteratorId = 0;
    private final Map<Integer, ReplicableIterator> snapshotIteratorMap = new HashMap<>();

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

    private final JournalEntryParser journalEntryParser;
    /**
     * 节点上的最新状态 和 被状态机执行的最大日志条目的索引值（从 0 开始递增）
     */
    private final JournalKeeperState state;

    private final RaftJournal journal;

    private final Config config;

    private final Properties properties;

    private boolean isRecovered = false;

    public StateActor(StateFactory stateFactory, JournalEntryParser journalEntryParser, RaftJournal journal, Config config, Properties properties) {
        this.stateFactory = stateFactory;
        this.journalEntryParser = journalEntryParser;
        this.journal = journal;
        this.config = config;
        this.properties = properties;
        persistenceFactory = ServiceSupport.load(PersistenceFactory.class);

        metadataPersistence = persistenceFactory.createMetadataPersistenceInstance();

        this.state = new JournalKeeperState(stateFactory, metadataPersistence, actor);

        this.partialSnapshot = new PartialSnapshot(partialSnapshotPath());

        this.actor.addScheduler(config.<Long>get("flush_interval_ms"), TimeUnit.MILLISECONDS, "flush", this::flush);

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
                    try {
                        doInstallSnapshot(offset, lastIncludedIndex, lastIncludedTerm, data, isDone);

                    } catch (IOException e) {
                        logger.warn("Install snapshot exception: ", e);
                        actor.reply(msg, new InstallSnapshotResponse(e));
                    }
                    actor.reply(msg, new InstallSnapshotResponse(term));
                });
    }

    @ActorListener
    private void doInstallSnapshot(long offset, long lastIncludedIndex, int lastIncludedTerm, byte[] data, boolean isDone) throws IOException {
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
        serverMetadata.setCommitIndex(journal.commitIndex());
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
    public long commitIndex() {
        return journal.commitIndex();
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
    public QueryStateResponse queryServerState(QueryStateRequest request) {
        long lastApplied = state.lastApplied();
        if (request.getIndex() > 0 && state.lastApplied() < request.getIndex()) {
            return new QueryStateResponse(new IllegalStateException());
        } else {
            StateQueryResult queryResult = state.query(request.getQuery(), journal);
            return new QueryStateResponse(queryResult.getResult(), queryResult.getLastApplied());
        }
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

    /**
     * 如果请求位置存在对应的快照，直接从快照中读取状态返回；如果请求位置不存在对应的快照，那么需要找到最近快照日志，以这个最近快照日志对应的快照为输入，从最近快照日志开始（不含）直到请求位置（含）依次在状态机中执行这些日志，执行完毕后得到的快照就是请求位置的对应快照，读取这个快照的状态返回给客户端即可。
     * 实现流程：
     *
     * 对比logIndex与在属性snapshots数组的上下界，检查请求位置是否越界，如果越界返回INDEX_OVERFLOW/INDEX_UNDERFLOW错误。
     * 查询snapshots[logIndex]是否存在，如果存在快照中读取状态返回，否则下一步；
     * 找到snapshots中距离logIndex最近且小于logIndex的快照位置和快照，记为nearestLogIndex和nearestSnapshot；
     * 从log中的索引位置nearestLogIndex + 1开始，读取N条日志，N = logIndex - nearestLogIndex获取待执行的日志数组execLogs[]；
     * 调用以nearestSnapshot为输入，依次在状态机stateMachine中执行execLogs，得到logIndex位置对应的快照，从快照中读取状态返回。
     */
    @ActorListener
    private QueryStateResponse querySnapshot(QueryStateRequest request) {
        try {
            if (request.getIndex() > state.lastApplied()) {
                throw new IndexOverflowException();
            }

            if (request.getIndex() == state.lastApplied()) {
                StateQueryResult queryResult = state.query(request.getQuery(), journal);
                if (queryResult.getLastApplied() == request.getIndex()) {
                    return new QueryStateResponse(queryResult.getResult(), queryResult.getLastApplied());
                }
            }

            Snapshot snapshot;
            Map.Entry<Long, Snapshot> nearestSnapshot = snapshots.floorEntry(request.getIndex());
            if (null == nearestSnapshot) {
                throw new IndexUnderflowException();
            }

            if (request.getIndex() == nearestSnapshot.getKey()) {
                snapshot = nearestSnapshot.getValue();
            } else {
                snapshot = new Snapshot(stateFactory, metadataPersistence);
                Path tempSnapshotPath = snapshotsPath().resolve(String.valueOf(request.getIndex()));
                if (Files.exists(tempSnapshotPath)) {
                    throw new ConcurrentModificationException(String.format("A snapshot of position %d is creating, please retry later.", request.getIndex()));
                }
                nearestSnapshot.getValue().dump(tempSnapshotPath);
                snapshot.recover(tempSnapshotPath, properties);

                while (snapshot.lastApplied() < request.getIndex()) {
                    long offset = journal.readOffset(snapshot.lastApplied());
                    JournalEntry header = journal.readEntryHeaderByOffset(offset);
                    snapshot.applyEntry(header, new EntryFutureImpl(journal, offset), journal);
                }
                snapshot.flush();

                snapshots.putIfAbsent(request.getIndex(), snapshot);
            }
            return new QueryStateResponse(snapshot.query(request.getQuery(), journal).getResult());
        } catch (Throwable throwable) {
            return new QueryStateResponse(throwable);
        }
    }
    private void createSnapshot() {
        long lastApplied = state.lastApplied();
        logger.info("Creating snapshot at index: {}...", lastApplied);
        Path snapshotPath = snapshotsPath().resolve(String.valueOf(lastApplied));
        try {
            FileUtils.deleteFolder(snapshotPath);
            state.dump(snapshotPath);
            Snapshot.markComplete(snapshotPath);
            Snapshot snapshot = new Snapshot(stateFactory, metadataPersistence);
            snapshot.recover(snapshotPath, properties);
            snapshot.createSnapshot(journal);

            snapshots.put(snapshot.lastApplied(), snapshot);
            logger.info("Snapshot at index: {} created, {}.", lastApplied, snapshot);

        } catch (IOException e) {
            logger.warn("Create snapshot exception! Snapshot: {}.", snapshotPath, e);
        }
    }


    @ActorSubscriber
    private void onInternalEntryApply(InternalEntryType type, byte [] entry) {
        if (type == InternalEntryType.TYPE_CREATE_SNAPSHOT) {
            createSnapshot();
        } else if (type == InternalEntryType.TYPE_RECOVER_SNAPSHOT) {
            recoverSnapShot(entry);
        }
    }

    @ActorListener
    private GetServerStateResponse getServerState(GetServerStateRequest request) {
        try {
            int iteratorId;
            if (request.getIteratorId() >= 0) {
                iteratorId = request.getIteratorId();
            } else {
                long snapshotIndex = request.getLastIncludedIndex() + 1;
                Snapshot snapshot = snapshots.get(snapshotIndex);
                if (null != snapshot) {
                    ReplicableIterator iterator = snapshot.iterator();
                    iteratorId = ++ nextSnapshotIteratorId;
                    snapshotIteratorMap.put(iteratorId, iterator);
                    actor.runDelay(1, TimeUnit.MINUTES, () -> snapshotIteratorMap.remove(iteratorId));
                } else {
                    throw new NoSuchSnapshotException();
                }
            }
            ReplicableIterator iterator = snapshotIteratorMap.get(iteratorId);
            if (null != iterator) {
                return new GetServerStateResponse(
                        iterator.lastIncludedIndex(), iterator.lastIncludedTerm(),
                        iterator.offset(), iterator.nextTrunk(), !iterator.hasMoreTrunks(), iteratorId
                );
            } else {
                throw new NoSuchSnapshotException();
            }
        } catch (Throwable t) {
            logger.warn("GetServerState exception!", t);
            return new GetServerStateResponse(t);
        }

    }

    private void recoverSnapShot(byte[] internalEntry) {
        RecoverSnapshotEntry recoverSnapshotEntry = InternalEntriesSerializeSupport.parse(internalEntry);
        Snapshot targetSnapshot = snapshots.get(recoverSnapshotEntry.getIndex());
        if (targetSnapshot == null) {
            logger.warn("recover snapshot failed, snapshot not exist, index: {}", recoverSnapshotEntry.getIndex());
            return;
        }
        try {
            createSnapshot();
            doRecoverSnapshot(targetSnapshot);
        } catch (Exception e) {
            logger.info("recover snapshot exception, target snapshot: {}", targetSnapshot.getPath(), e);
        }
    }

    private void doRecoverSnapshot(Snapshot targetSnapshot) throws IOException {
        logger.info("recover snapshot, target snapshot: {}", targetSnapshot.getPath());
        state.closeUnsafe();
        state.clearUserState();
        targetSnapshot.dumpUserState(statePath());
        state.recoverUserStateUnsafe();
        logger.info("recover snapshot success, target snapshot: {}", targetSnapshot.getPath());
    }

    /**
     * Check reserved entries to ensure the last UpdateVotersConfig entry is applied to the current voter config.
     */
    @ActorListener
    private void recoverVoterConfig() {
        boolean isRecoveredFromJournal = false;
        List<CompletableFuture<?>> futures = new ArrayList<>();
        for (long index = journal.maxIndex(INTERNAL_PARTITION) - 1;
             index >= journal.minIndex(INTERNAL_PARTITION);
             index--) {
            JournalEntry entry = journal.readByPartition(INTERNAL_PARTITION, index);
            InternalEntryType type = InternalEntriesSerializeSupport.parseEntryType(entry.getPayload().getBytes());

            if (type == InternalEntryType.TYPE_UPDATE_VOTERS_S1) {
                UpdateVotersS1Entry updateVotersS1Entry = InternalEntriesSerializeSupport.parse(entry.getPayload().getBytes());
                state.setConfigState(new ConfigState(
                        updateVotersS1Entry.getConfigOld(), updateVotersS1Entry.getConfigNew()));
                isRecoveredFromJournal = true;
                break;
            } else if (type == InternalEntryType.TYPE_UPDATE_VOTERS_S2) {
                UpdateVotersS2Entry updateVotersS2Entry = InternalEntriesSerializeSupport.parse(entry.getPayload().getBytes());
                state.setConfigState(new ConfigState(updateVotersS2Entry.getConfigNew()));
                isRecoveredFromJournal = true;
                break;
            }
        }

        if (isRecoveredFromJournal) {
            logger.debug("Voters config is recovered from journal.");
        } else {
            logger.debug("No voters config entry found in journal, Using config in the metadata.");
        }
    }

    @ActorListener
    private void maybeUpdateLeaderConfig(UpdateClusterStateRequest request) throws Exception {
        UpdateRequest updateRequest;
        if (request.getRequests().size() == 1 && (updateRequest = request.getRequests().get(0)).getPartition() == INTERNAL_PARTITION) {
            InternalEntryType entryType = InternalEntriesSerializeSupport.parseEntryType(updateRequest.getEntry());
            if (entryType == TYPE_UPDATE_VOTERS_S1) {
                UpdateVotersS1Entry updateVotersS1Entry = InternalEntriesSerializeSupport.parse(updateRequest.getEntry());
                state.getConfigState().toJointConsensus(updateVotersS1Entry.getConfigOld(), updateVotersS1Entry.getConfigNew(), null);
                actor.pub("onConfigToJointConsensus", state.getConfigState().voters());

            } else if(entryType == TYPE_UPDATE_VOTERS_S2) {
                state.getConfigState().toNewConfig(null);
                actor.pub("onConfigToNewConfig", state.getConfigState().voters());
            }
        }
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

    @ActorListener
    private void addInterceptor(ApplyReservedEntryInterceptor interceptor) {
        state.addInterceptor(interceptor);
    }

    @ActorListener
    private void removeInterceptor(ApplyReservedEntryInterceptor interceptor) {
        state.removeInterceptor(interceptor);
    }

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
    @ActorSubscriber
    private void onStop() {
        flush();
    }

    /**
     * 监听属性commitIndex的变化，
     * 当commitIndex变更时如果commitIndex > lastApplied，
     * 反复执行applyEntries直到lastApplied == commitIndex：
     * 1. 如果需要，复制当前状态为新的快照保存到属性snapshots, 索引值为lastApplied。
     * 2. lastApplied自增，将log[lastApplied]应用到状态机，更新当前状态state；
     *
     */
    @ActorScheduler(interval = 100L)
    @ActorSubscriber(topic = "onJournalCommit")
    private void applyEntries() {
        if(!isRecovered){
            return;
        }
        while (state.lastApplied() < journal.commitIndex()) {
            long offset = journal.readOffset(state.lastApplied());
            JournalEntry entryHeader = journal.readEntryHeaderByOffset(offset);
            StateResult stateResult = state.applyEntry(entryHeader, new EntryFutureImpl(journal, offset), journal);
            actor.pub("onStateChange", stateResult);
        }
    }

}
