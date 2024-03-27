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
package io.journalkeeper.coordinating.client;

import io.journalkeeper.coordinating.state.domain.StateTypes;

import java.io.Serializable;

/**
 * CoordinatingEvent
 * author: gaohaoxiang
 *
 * date: 2019/6/11
 */
public class CoordinatingEvent implements Serializable {

    private static final long serialVersionUID = 7124472968765728227L;
    private StateTypes type;
    private byte[] key;
    private byte[] value;

    public CoordinatingEvent() {}

    public CoordinatingEvent(StateTypes type, byte[] key) {
        this.type = type;
        this.key = key;
    }

    public CoordinatingEvent(StateTypes type, byte[] key, byte[] value) {
        this.type = type;
        this.key = key;
        this.value = value;
    }

    public StateTypes getType() {
        return type;
    }

    public void setType(StateTypes type) {
        this.type = type;
    }

    public byte[] getKey() {
        return key;
    }

    public void setKey(byte[] key) {
        this.key = key;
    }

    public byte[] getValue() {
        return value;
    }

    public void setValue(byte[] value) {
        this.value = value;
    }
}