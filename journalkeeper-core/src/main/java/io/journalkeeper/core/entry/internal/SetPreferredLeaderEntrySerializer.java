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

import java.nio.ByteBuffer;

/**
 * @author LiYue
 * Date: 2019-05-09
 * <p>
 * Type:                                    1 byte
 * preferred leader URI String
 *  Length of the URI String in bytes:    2 bytes
 *  URI String in bytes                   variable length
 * ...
 *
 */
public class SetPreferredLeaderEntrySerializer extends InternalEntrySerializer<SetPreferredLeaderEntry> {
    private int sizeOf(SetPreferredLeaderEntry entry) {
        return
                Short.BYTES +  // Length of the URI String in bytes: 2 bytes
                entry.getPreferredLeader().toASCIIString().length(); // URI String in bytes: variable length
    }

    @Override
    protected byte[] serialize(SetPreferredLeaderEntry entry, byte[] header) {
        byte[] bytes = new byte[header.length + sizeOf(entry)];
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        buffer.put(header);
        UriSerializeSupport.serializerUri(buffer, entry.getPreferredLeader());
        return bytes;
    }

    @Override
    protected SetPreferredLeaderEntry parse(ByteBuffer byteBuffer, int type, int version) {
        return new SetPreferredLeaderEntry(UriSerializeSupport.parseUri(byteBuffer), version);
    }

}
