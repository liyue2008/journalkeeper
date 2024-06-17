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
package io.journalkeeper.core.metric;

import java.net.URI;

/**
 * @author LiYue
 * Date: 2019-09-10
 */
public class MetricNames {
    public final static String METRIC_UPDATE_CLUSTER_STATE = "UPDATE_CLUSTER_STATE";
    public final static String METRIC_APPEND_JOURNAL = "APPEND_JOURNAL";
    public final static String METRIC_APPEND_ENTRIES_RPC = "APPEND_ENTRIES_RPC";
    public final static String METRIC_OBSERVER_REPLICATION = "OBSERVER_REPLICATION";

    public static String compose(String prefix, URI uri) {
        return prefix + "-" + uri.toString();
    }

}
