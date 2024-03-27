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
package io.journalkeeper.core.entry.internal;

import java.io.Serializable;
import java.util.Set;

import static io.journalkeeper.core.entry.internal.InternalEntryType.TYPE_SCALE_PARTITIONS;

/**
 * @author LiYue
 * Date: 2019-05-09
 */

public class ScalePartitionsEntry extends InternalEntry {
    private final Set<Integer> partitions;

    public ScalePartitionsEntry(Set<Integer> partitions) {
        super(TYPE_SCALE_PARTITIONS);
        this.partitions = partitions;
    }

    public ScalePartitionsEntry(Set<Integer> partitions, int version) {
        super(TYPE_SCALE_PARTITIONS, version);
        this.partitions = partitions;
    }

    public Set<Integer> getPartitions() {
        return partitions;
    }
}
