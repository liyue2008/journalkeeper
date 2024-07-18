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
package io.journalkeeper.rpc.client;

/**
 * RPC 方法
 * {@link ClientServerRpc#pullEvents(PullEventsRequest) pullEvents()}
 * 请求参数
 * @author LiYue
 * Date: 2019-04-22
 */
public class PullEventsRequest {
    private final long index;

    public PullEventsRequest(long index) {
        this.index = index;
    }

    /**
     * 获取事件起始index
     * @return 事件起始index
     */
    public long getIndex() {
        return index;
    }

}
