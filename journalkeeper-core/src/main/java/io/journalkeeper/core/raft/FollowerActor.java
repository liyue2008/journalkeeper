package io.journalkeeper.core.raft;

import io.journalkeeper.core.api.RaftJournal;
import io.journalkeeper.rpc.server.AsyncAppendEntriesRequest;
import io.journalkeeper.rpc.server.AsyncAppendEntriesResponse;
import io.journalkeeper.utils.actor.Actor;
import io.journalkeeper.utils.actor.ActorListener;
import io.journalkeeper.utils.actor.ActorMsg;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;


public class FollowerActor {
    private static final Logger logger = LoggerFactory.getLogger( FollowerActor.class );
    private final Actor actor = new Actor("Follower");

    private final RaftState state;

    private final RaftJournal journal;

    /**
     * Leader 日志当前的最大位置
     */
    private long leaderMaxIndex = -1L;

    FollowerActor(RaftState state, RaftJournal journal) {
        this.state = state;
        this.journal = journal;
        actor.setHandlerInstance(this);
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
    private void asyncAppendEntries(ActorMsg msg) {
        // 1. State.maybeRollbackConfig 如果要删除部分未提交的日志，并且待删除的这部分存在配置变更日志，则需要回滚配置
        // 2. Journal.compareOrAppendRaw 从index位置开始：
        //    如果一条已经存在的日志与新的冲突（index 相同但是任期号 term 不同），则删除已经存在的日志和它之后所有的日志
        //    添加任何在已有的日志中不存在的条目。
        // 3. State.maybeUpdateNonLeaderConfig 非Leader（Follower和Observer）复制日志到本地后，如果日志中包含配置变更，则立即变更配置
        // 4. Journal.commit 提交日志：如果leaderCommit > commitIndex，将commitIndex设置为leaderCommit和最新日志条目索引号中较小的一个。
        // 5. Follower.onCommit 更新leaderMaxIndex，返回响应。

        AsyncAppendEntriesRequest request = msg.getPayload();
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
        } else {
            // 如果要删除部分未提交的日志，并且待删除的这部分存在配置变更日志，则需要回滚配置
            actor.sendThen("State", "maybeRollbackConfig", startIndex)
                    .thenCompose(ignored -> actor.sendThen("Journal","compareOrAppendRaw", entries, request.getPrevLogIndex() + 1))
                    .thenCompose(ignored -> actor.sendThen("State", "maybeUpdateNonLeaderConfig", msg))
                    .thenCompose(ignored -> actor.sendThen("Journal", "commit", request.getLeaderCommit()))
                    .thenRun(() -> this.onCommit(msg))
                    .exceptionally(t -> {
                        actor.reply(msg, new AsyncAppendEntriesResponse(t));
                        return null;
                    });
        }
    }



    private void onCommit(ActorMsg msg) {
        AsyncAppendEntriesRequest request = msg.getPayload();
        if (leaderMaxIndex < request.getMaxIndex()) {
            leaderMaxIndex = request.getMaxIndex();
        }
        actor.reply(msg, new AsyncAppendEntriesResponse(true, request.getPrevLogIndex() + 1,
                state.getTerm(), request.getEntries().size()));
    }

    public Actor getActor() {
        return actor;
    }
}
