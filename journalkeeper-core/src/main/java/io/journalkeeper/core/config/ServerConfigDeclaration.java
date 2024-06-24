package io.journalkeeper.core.config;

import io.journalkeeper.utils.config.Config;

import java.nio.file.Path;
import java.nio.file.Paths;

public class ServerConfigDeclaration {


    public void declare(Config config) {

        // AbstractServer
        config.declare("snapshot_interval_sec", Integer.class, 0, true, "快照间隔，单位：秒");
        config.declare("rpc_timeout_ms", Long.class, 1000L, true, "RPC超时时间，单位：毫秒");
        config.declare("flush_interval_ms", Long.class, 50L, true, "刷盘间隔，单位：毫秒");
        config.declare("working_dir", Path.class,  Paths.get(System.getProperty("user.dir")).resolve("journalkeeper"), true, "工作目录");
        config.declare("get_state_batch_size", Integer.class, 1024 * 1024, true, "获取状态批量大小");
        config.declare("enable_metric", Boolean.class, false, true, "是否启用Metric，显示详细的性能数据，但处理能力会下降");
        config.declare("disable_logo", Boolean.class, false, true, "是否禁用LOGO");
        config.declare("print_metric_interval_sec", Integer.class, 0, true, "指标打印间隔，单位：秒");
        config.declare("journal_retention_min", Integer.class, 0, true, "日志保留时间，单位：分钟");
        config.declare("enable_events", Boolean.class, true, true, "是否启用事件");
        config.declare("server_name", String.class, "", true, "Server名称，用于在同一进程多Server情况下区分");

        // JournalPersistence
        config.declare("persistence.journal.file_header_size", Integer.class, 128, true, "文件头长度");
        config.declare("persistence.journal.file_data_size", Integer.class, 32 * 1024 * 1024, true, "文件内数据最大长度");
        config.declare("persistence.journal.cached_file_core_count", Integer.class, 3, true, "缓存文件的核心数量。");
        config.declare("persistence.journal.cached_file_max_count", Integer.class, 10, true, "缓存文件的最大数量。");
        config.declare("persistence.journal.max_dirty_size", Long.class, 128 * 1024 * 1024L, true, "脏数据最大长度，超过这个长度append将阻塞");

        // IndexPersistence
        config.declare("persistence.index.file_header_size", Integer.class, 128, true, "文件头长度");
        config.declare("persistence.index.file_data_size", Integer.class, 128 * 1024, true, "文件内数据最大长度");
        config.declare("persistence.index.cached_file_core_count", Integer.class, 12, true, "缓存文件的核心数量。");
        config.declare("persistence.index.cached_file_max_count", Integer.class, 40, true, "缓存文件的最大数量。");
        config.declare("persistence.index.max_dirty_size", Long.class, 0L, true, "脏数据最大长度，超过这个长度append将阻塞");

        // Voter
        config.declare("heartbeat_interval_ms", Long.class, 100L, true, "心跳间隔，单位：毫秒");
        config.declare("election_timeout_ms", Long.class, 300L, true, "选举超时，单位：毫秒");
        config.declare("replication_batch_size", Integer.class, 128, true, "复制批次大小");
        config.declare("cache_requests", Integer.class, 1024, true, "请求缓存大小");
        config.declare("transaction_timeout_ms", Long.class, 10L * 60 * 1000, true, "事务超时，单位：毫秒");
        config.declare("print_state_interval_sec", Integer.class, 0, true, "打印状态间隔，单位：秒");
        config.declare("enable_pre_vote", Boolean.class, true, true, "是否启用预投票");
        config.declare("enable_check_quorum", Boolean.class, true, true, "是否启用检查法定人数");
        config.declare("check_quorum_timeout_ms", Long.class, 300L, true, "检查法定人数超时，单位：毫秒");
        config.declare("observer.pull_batch_size", Integer.class, 4 * 1024 * 1024, true, "Observer拉取日志批量大小");
        config.declare("observer.pull_interval_ms", Long.class, 100L, true, "Observer拉取日志批量大小");
        config.declare("observer.parents", String.class, "",true, "Observer父节点列表");
    }
}
