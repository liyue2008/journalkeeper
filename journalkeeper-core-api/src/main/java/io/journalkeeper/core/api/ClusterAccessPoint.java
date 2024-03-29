/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.journalkeeper.core.api;

/**
 * 集群访问接入点
 * @author LiYue
 * Date: 2019-03-14
 */
public interface ClusterAccessPoint {
    /**
     * 获取客户端实例
     * @return RaftClient实例
     */
    RaftClient getRaftClient();


    /**
     * 获取绑定到本地Server节点的RaftClient实例，所有请求不经过RPC，直接调用本地Server节点。
     * 注意：如果本地Server节点不是LEADER，所有LEADER请求将会抛出{@link io.journalkeeper.exceptions.NotLeaderException}
     * @return 本地RaftClient实例
     */
    RaftClient getLocalRaftClient();

    /**
     * 获取Server实例
     * @return RaftServer实例，如果本地存在Server返回Server实例，否则返回null。
     */
    RaftServer getServer();

    /**
     * 获取管理端实例
     * @return AdminClient实例。
     */
    AdminClient getAdminClient();

    /**
     * 获取绑定到本地Server节点的AdminClient实例，所有请求不经过RPC，直接调用本地Server节点。
     * 注意：如果本地Server节点不是LEADER，所有LEADER请求将会抛出{@link io.journalkeeper.exceptions.NotLeaderException}
     * @return 本地AdminClient实例
     */
    AdminClient getLocalAdminClient();
}
