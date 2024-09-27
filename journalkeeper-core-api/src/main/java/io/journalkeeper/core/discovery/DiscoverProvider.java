package io.journalkeeper.core.discovery;

import java.net.URI;
import java.util.List;
import java.util.Properties;

/**
 * Raft集群初试化时自动发现节点服务。
 */
public interface DiscoverProvider {
    /**
     * 初始化
     * @param properties 配置
     */
    void init(Properties properties);


    /**
     * 注册节点
     * @param uri 节点URI
     */
    void register(URI uri, String token);

    /**
     * 发现节点
     * @return 节点URI集合
     */
    List<URI> discoverNodes(String token);

}
