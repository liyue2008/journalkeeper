package io.journalkeeper.core.raft;

import io.journalkeeper.core.api.RaftJournal;
import io.journalkeeper.core.server.PartialSnapshot;
import io.journalkeeper.core.state.Snapshot;
import io.journalkeeper.rpc.server.AsyncAppendEntriesRequest;
import io.journalkeeper.rpc.server.AsyncAppendEntriesResponse;
import io.journalkeeper.rpc.server.InstallSnapshotRequest;
import io.journalkeeper.rpc.server.InstallSnapshotResponse;
import io.journalkeeper.utils.ThreadSafeFormat;
import io.journalkeeper.utils.actor.Actor;
import io.journalkeeper.utils.actor.annotation.ActorListener;
import io.journalkeeper.utils.actor.ActorMsg;
import io.journalkeeper.utils.actor.annotation.ActorMessage;
import io.journalkeeper.utils.actor.annotation.ResponseManually;
import io.journalkeeper.utils.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.NavigableMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;


public class FollowerActor {
    private static final Logger logger = LoggerFactory.getLogger( FollowerActor.class );
    private static final String PARTIAL_SNAPSHOT_PATH = "partial_snapshot";
    private final Actor actor = Actor.builder("Follower").setHandlerInstance(this).build();

    private final RaftState state;

    private final RaftJournal journal;

    private final Config config;

    private int term;

    /**
     * Leader 日志当前的最大位置
     */
    private long leaderMaxIndex = -1L;
    private boolean isActive = true;

    private final PartialSnapshot partialSnapshot;

    FollowerActor(RaftState state, RaftJournal journal, Config config) {
        this.state = state;
        this.journal = journal;
        this.config = config;
        this.partialSnapshot = new PartialSnapshot(snapshotsPath());
    }

    private Path snapshotsPath() {
        return config.<Path>get("working_dir").resolve("snapshots");
    }
    /**
     * 1. 如果 term < currentTerm返回 false
     * 如果 term > currentTerm且节点当前的状态不是FOLLOWER，将节点当前的状态转换为FOLLOWER；
     * 如果在prevLogIndex处的日志的任期号与prevLogTerm不匹配时，返回 false
     * 如果一条已经存在的日志与新的冲突（index 相同但是任期号 term 不同），则删除已经存在的日志和它之后所有的日志
     * 添加任何在已有的日志中不存在的条目
     * 如果leaderCommit > commitIndex，将commitIndex设置为leaderCommit和最新日志条目索引号中较小的一个
     */
    @ActorListener
    @ResponseManually
    public void asyncAppendEntries(@ActorMessage ActorMsg msg) {
        AsyncAppendEntriesRequest request = msg.getPayload();


        // 1. State.maybeRollbackConfig 如果要删除部分未提交的日志，并且待删除的这部分存在配置变更日志，则需要回滚配置
        // 2. Journal.compareOrAppendRaw 从index位置开始：
        //    如果一条已经存在的日志与新的冲突（index 相同但是任期号 term 不同），则删除已经存在的日志和它之后所有的日志
        //    添加任何在已有的日志中不存在的条目。
        // 3. State.maybeUpdateNonLeaderConfig 非Leader（Follower和Observer）复制日志到本地后，如果日志中包含配置变更，则立即变更配置
        // 4. Journal.commit 提交日志：如果leaderCommit > commitIndex，将commitIndex设置为leaderCommit和最新日志条目索引号中较小的一个。
        // 5. Follower.onCommit 更新leaderMaxIndex，返回响应。
        // TODO: 增加preferLeader逻辑。

        if (!isActive || request.getTerm() < term) {
            actor.reply(msg, new AsyncAppendEntriesResponse(false, request.getPrevLogIndex() + 1,
                    term, request.getEntries().size()));
            return;
        }

        actor.send("Voter", "onAppendEntries", request.getTerm(), request.getLeader());

        boolean notHeartBeat = null != request.getEntries() && !request.getEntries().isEmpty();
        // Reply false if log does not contain an entry at prevLogIndex
        // whose term matches prevLogTerm
        final long startIndex = request.getPrevLogIndex() + 1;
        final List<byte[]> entries = request.getEntries();
        if (notHeartBeat &&
                (request.getPrevLogIndex() < journal.minIndex() - 1 ||
                        request.getPrevLogIndex() >= journal.maxIndex() ||
                        journal.getTerm(request.getPrevLogIndex()) != request.getPrevLogTerm())
        ) {
            actor.reply(msg, new AsyncAppendEntriesResponse(false, startIndex,
                    request.getTerm(), request.getEntries().size()));
            return;
        }
        // 如果要删除部分未提交的日志，并且待删除的这部分存在配置变更日志，则需要回滚配置
        actor.sendThen("State", "maybeRollbackConfig", startIndex)
                .thenCompose(ignored -> actor.sendThen("Journal","compareOrAppendRaw", entries, request.getPrevLogIndex() + 1))
                .thenCompose(ignored -> actor.sendThen("State", "maybeUpdateNonLeaderConfig", entries))
                .thenCompose(ignored -> actor.sendThen("Journal", "commit", request.getLeaderCommit()))
                .thenRun(() -> {
                    if (leaderMaxIndex < request.getMaxIndex()) {
                        leaderMaxIndex = request.getMaxIndex();
                    }
                    actor.reply(msg, new AsyncAppendEntriesResponse(true, request.getPrevLogIndex() + 1,
                            request.getTerm(), request.getEntries().size()));
                })
                .exceptionally(t -> {
                    actor.reply(msg, new AsyncAppendEntriesResponse(t));
                    return null;
                });

    }

    public Actor getActor() {
        return actor;
    }

    @ActorListener
    private void setActive(boolean active, int term) {
        this.isActive = active;
        this.term = term;
    }

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
        if (term < request.getTerm()) {
            actor.reply(msg, new InstallSnapshotResponse(term));
            return;
        }
        actor.send("Voter", "onAppendEntries", request.getTerm(), request.getLeaderId());

        long offset = request.getOffset();
        long lastIncludedIndex = request.getLastIncludedIndex();
        int lastIncludedTerm = request.getLastIncludedTerm();
        byte[] data = request.getData();
        boolean isDone = request.isDone();

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

        } catch (IOException e) {
            logger.warn("Install snapshot exception: ", e);
            actor.reply(msg, new InstallSnapshotResponse(e));
            return;
        }
        if (isDone) {
            actor.<Snapshot>sendThen("State", "installSnapshot", snapshotPath, lastApplied)
                    .thenCompose(snapshot ->
                            actor.sendThen("Journal", "compact", snapshot.getJournalSnapshot(), lastIncludedIndex, lastIncludedTerm)
                            .thenApply(ignored -> snapshot)
                    ).thenCompose(snapshot -> actor.sendThen("State", "recoverFromSnapshot", snapshot))
                    .thenRun(() -> logger.info("Install snapshot successfully!"))
                    .thenRun(() -> actor.reply(msg, new InstallSnapshotResponse(term)))
                    .exceptionally(t -> {
                        logger.warn("Install snapshot exception: ", t);
                        actor.reply(msg, new InstallSnapshotResponse(t));
                        return null;
                    });

        } else {
            actor.reply(msg, new InstallSnapshotResponse(term));
        }

    }




}
