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
package io.journalkeeper.utils.event;


/**
 * @author LiYue
 * Date: 2019-04-12
 */
public class PullEvent extends Event {
    private final long sequence;

    public PullEvent(int eventType, long sequence, byte [] eventData) {
        super(eventType, eventData);
        this.sequence = sequence;
    }

    public long getSequence() {
        return sequence;
    }
}
