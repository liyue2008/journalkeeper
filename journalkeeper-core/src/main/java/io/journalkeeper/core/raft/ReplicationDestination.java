package io.journalkeeper.core.raft;


import io.journalkeeper.base.ReplicableIterator;
import io.journalkeeper.core.api.RaftJournal;
import io.journalkeeper.core.state.Snapshot;
import io.journalkeeper.exceptions.IndexUnderflowException;
import io.journalkeeper.rpc.server.*;
import io.journalkeeper.utils.actor.Actor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Map;


class ReplicationDestination {
    private static final Logger logger = LoggerFactory.getLogger( ReplicationDestination.class );

    private final URI uri;

    private final RaftJournal journal;
    private final RaftState state;
    private final int replicationBatchSize = -1;
    private final Actor actor;
    private boolean installSnapshotInProgress = false;

    /**
     * 需要发给它的下一个日志条目的索引（初始化为领导人上一条日志的索引值 +1）
     */
    private long nextIndex;
    /**
     * 已经复制到该服务器的日志的最高索引值（从 0 开始递增）
     */
    private long matchIndex = 0L;

    /**
     * 上次从FOLLOWER收到心跳（asyncAppendEntries）成功响应的时间戳
     */
    private long lastHeartbeatResponseTime;
    private long lastHeartbeatRequestTime = 0L;

    private boolean committed = true;


    ReplicationDestination(URI uri, long nextIndex, Actor actor, RaftState state, RaftJournal journal) {
        this.uri = uri;
        this.nextIndex = nextIndex;
        this.actor = actor;
        this.state = state;
        this.journal = journal;
        this.lastHeartbeatResponseTime = 0L;
    }


    void replication() {
        long maxIndex = journal.maxIndex();


        // 如果有必要，先安装第一个快照
        Map.Entry<Long, Snapshot> fistSnapShotEntry = state.getSnapshots().firstEntry();
        maybeInstallSnapshotFirst(fistSnapShotEntry);

        // 读取需要复制的Entry
        List<byte[]> entries;
        if (!installSnapshotInProgress && nextIndex < maxIndex) { // 复制
            entries = journal.readRaw(nextIndex, this.replicationBatchSize);
        } else { // 心跳
            entries = Collections.emptyList();
        }

        // 构建请求并发送
        AsyncAppendEntriesRequest request =
                new AsyncAppendEntriesRequest(state.getTerm(), state.getLocalUri(),
                        nextIndex - 1, this.getPreLogTerm(nextIndex),
                        entries, state.commitIndex(), maxIndex);


        actor.sendThen("Rpc", "asyncAppendEntries", request)
                .thenAccept(resp -> {
                    handleAppendEntriesResponse(resp, entries.size(), fistSnapShotEntry.getKey());
                });
        lastHeartbeatRequestTime = System.currentTimeMillis();
    }


    private void handleAppendEntriesResponse(Object resp, int entrySize, long startIndex) {
        if (null == resp) {
            // 没收到响应或者请求失败
            // 等下一个心跳超时之后，再进入这个方法会自动重试
        } else if (resp instanceof Exception) {
            Exception e = (Exception) resp;
            logger.warn("Replication execution exception, from {} to {}, cause: {}.", state.getLocalUri(), uri, null == e.getCause()? e.getMessage() : e.getCause().getMessage());
        } else {


            AsyncAppendEntriesResponse response = (AsyncAppendEntriesResponse) resp;
            if(!response.success()) {
                return;
            }
            // 成功收到响应响应
            lastHeartbeatResponseTime = System.currentTimeMillis();
            if (response.isSuccess()) { // 复制成功
                if (entrySize > 0) {
                    nextIndex += entrySize;
                    matchIndex = nextIndex;
                    committed = false;
                    actor.send("Leader","commit",null);
                }
            } else {
                // 不匹配，回退
                int rollbackSize = (int) Math.min(replicationBatchSize, nextIndex - startIndex);
                nextIndex -= rollbackSize;
            }
        }
    }

    private void maybeInstallSnapshotFirst(Map.Entry<Long, Snapshot> fistSnapShotEntry) {
        if (nextIndex <= fistSnapShotEntry.getKey()) {
            installSnapshot(fistSnapShotEntry.getValue());
            nextIndex = fistSnapShotEntry.getKey();
        }
    }

    private void installSnapshot(Snapshot snapshot) {
        if (installSnapshotInProgress) {
            return;
        }
        installSnapshotInProgress = true;

        try {
            logger.info("Install snapshot to {} ...", this.getUri());

            int offset = 0;
            ReplicableIterator iterator = snapshot.iterator();
            while (iterator.hasMoreTrunks()) {
                byte[] trunk = iterator.nextTrunk();
                InstallSnapshotRequest request = new InstallSnapshotRequest(
                        state.getTerm(), state.getLocalUri(), snapshot.lastIncludedIndex(), snapshot.lastIncludedTerm(),
                        offset, trunk, !iterator.hasMoreTrunks()
                );
                boolean lastRequest = !iterator.hasMoreTrunks();
                actor.sendThen("Rpc", "installSnapshot", request)
                        .thenAccept(resp -> handleInstallSnapshotResponse(resp, lastRequest));
                offset += trunk.length;
            }
        } catch (IOException t) {
            logger.warn("Install snapshot to {} failed!", this.getUri(), t);
            installSnapshotInProgress = false;
        }
    }

    private void handleInstallSnapshotResponse(Object resp, boolean last) {
        if (resp instanceof Exception) {
            Exception e = (Exception) resp;
            logger.warn("Install snapshot execution exception, from {} to {}, cause: {}.", state.getLocalUri(), uri, null == e.getCause()? e.getMessage() : e.getCause().getMessage());
            if (last) {
                installSnapshotInProgress = false;
            }
        } else {
            InstallSnapshotResponse response = (InstallSnapshotResponse) resp;
            if (!response.success()) {
                logger.warn("Install snapshot to {} failed! Cause: {}.", this.getUri(), response.errorString());
                if (last) {
                    installSnapshotInProgress = false;
                }
            }
        }
    }


    private int getPreLogTerm(long currentLogIndex) {
        if (currentLogIndex > journal.minIndex()) {
            return journal.getTerm(currentLogIndex - 1);
        } else if (currentLogIndex == journal.minIndex() && state.getSnapshots().containsKey(currentLogIndex)) {
            return state.getSnapshots().get(currentLogIndex).lastIncludedTerm();
        } else if (currentLogIndex == 0) {
            return -1;
        } else {
            throw new IndexUnderflowException();
        }
    }

    URI getUri() {
        return uri;
    }

    long getNextIndex() {
        return nextIndex;
    }

    long getMatchIndex() {
        return matchIndex;
    }

    long getLastHeartbeatResponseTime() {
        return lastHeartbeatResponseTime;
    }

    long getLastHeartbeatRequestTime() {
        return lastHeartbeatRequestTime;
    }

    public boolean isCommitted() {
        return committed;
    }

    public void setCommitted(boolean committed) {
        this.committed = committed;
    }

    @Override
    public String toString() {
        return "{" +
                "uri=" + uri +
                ", nextIndex=" + nextIndex +
                ", matchIndex=" + matchIndex +
                '}';
    }
}

