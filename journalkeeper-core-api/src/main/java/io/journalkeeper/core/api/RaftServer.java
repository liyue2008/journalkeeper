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

import io.journalkeeper.utils.state.StateServer;

import java.net.URI;
import java.util.List;
import java.util.Set;

/**
 * Raft节点
 * @author LiYue
 * Date: 2019-03-14
 */

public interface RaftServer extends StateServer {

    Roll roll();

    default void init(URI uri, List<URI> voters) {
        init(uri, voters, null);
    }

    default void init(URI uri, List<URI> voters, Set<Integer> partitions) {
        init(uri, voters, partitions, null);
    }

    void init(URI uri, List<URI> voters, Set<Integer> partitions, URI preferredLeader);

    boolean isInitialized();

    void recover();

    URI serverUri();

    enum Roll {VOTER, OBSERVER}

}
