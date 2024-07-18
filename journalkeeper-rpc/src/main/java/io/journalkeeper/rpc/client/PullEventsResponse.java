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

import io.journalkeeper.rpc.BaseResponse;
import io.journalkeeper.rpc.StatusCode;
import io.journalkeeper.utils.event.PullEvent;

import java.util.Collections;
import java.util.List;

/**
 * RPC 方法
 * {@link ClientServerRpc#pullEvents(PullEventsRequest) pullEvents{}}
 * 返回响应。
 * <p>
 * 返回从上次ack 的序号至今的所有事件，保证事件有序。
 * 如果没有事件返回长度为0的List。
 * <p>
 * StatusCode:
 * StatusCode.PULL_WATCH_ID_NOT_EXISTS: 监听ID不存在。
 * @author LiYue
 * Date: 2019-04-22
 */
public class PullEventsResponse extends BaseResponse {
    private final List<PullEvent> pullEvents;
    private final long latestIndex;

    public PullEventsResponse(long latestIndex, List<PullEvent> pullEvents) {
        this.latestIndex = latestIndex;
        if (null != pullEvents) {
            setStatusCode(StatusCode.SUCCESS);
        } else {
            setStatusCode(StatusCode.PULL_WATCH_ID_NOT_EXISTS);
        }
        this.pullEvents = pullEvents;
    }

    public PullEventsResponse(Throwable throwable) {
        super(throwable);
        this.pullEvents = Collections.emptyList();
        this.latestIndex = -1;
    }

    /**
     * 返回的事件列表
     * @return 返回的事件列表
     */
    public List<PullEvent> getPullEvents() {
        return pullEvents;
    }

    /**
     * 最新的事件序号
     * @return 最新的事件序号
     */
    public long getLatestIndex() {
        return latestIndex;
    }
}
