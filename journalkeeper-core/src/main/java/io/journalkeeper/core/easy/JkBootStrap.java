package io.journalkeeper.core.easy;

import io.journalkeeper.core.api.AdminClient;
import io.journalkeeper.core.api.RaftServer;
import io.journalkeeper.core.serialize.WrappedBootStrap;

import java.net.URI;
import java.util.List;
import java.util.Properties;

public class JkBootStrap {

    private final WrappedBootStrap<JkRequest, JkResponse, JkRequest, JkResponse> bootStrap;
    /**
     * 初始化远程模式的BootStrap，本地没有任何Server，所有操作直接请求远程Server。
     *
     * @param servers    远程Server 列表
     * @param properties 配置属性
     */
    public JkBootStrap(List<URI> servers,
                            Properties properties) {
        this.bootStrap = new WrappedBootStrap<>(servers, properties);
    }

    public JkBootStrap(JkStateFactory stateFactory,
                            Properties properties) {
        this(RaftServer.Roll.VOTER, stateFactory, properties);
    }

    /**
     * 初始化本地Server模式BootStrap，本地包含一个Server，请求本地Server通信。
     *
     * @param roll                本地Server的角色。
     * @param stateFactory 状态机工厂，用户创建状态机实例
     * @param properties          配置属性
     */
    public JkBootStrap(RaftServer.Roll roll, JkStateFactory stateFactory,
                            Properties properties) {
        this.bootStrap = new WrappedBootStrap<>(roll,stateFactory, properties);
    }

    public JkClient getClient() {
        return new JkClient(bootStrap.getClient());
    }

    public RaftServer getServer() {
        return bootStrap.getServer();
    }

    public AdminClient getAdminClient() {
        return bootStrap.getAdminClient();
    }

    public void shutdown() {
        bootStrap.shutdown();
    }

    public Properties getProperties() {
        return bootStrap.getProperties();
    }

    public JkClient getLocalClient() {
        return new JkClient(bootStrap.getLocalClient());
    }

}
