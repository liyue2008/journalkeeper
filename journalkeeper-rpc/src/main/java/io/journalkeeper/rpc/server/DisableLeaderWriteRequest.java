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
package io.journalkeeper.rpc.server;

/**
 * @author LiYue
 * Date: 2019-09-12
 */
public class DisableLeaderWriteRequest {
    // timeout in ms before leader re-enable write service
    private final long timeoutMs;
    private final int term;

    public DisableLeaderWriteRequest(long timeoutMs, int term) {
        this.timeoutMs = timeoutMs;
        this.term = term;
    }

    public long getTimeoutMs() {
        return timeoutMs;
    }

    public int getTerm() {
        return term;
    }
}
