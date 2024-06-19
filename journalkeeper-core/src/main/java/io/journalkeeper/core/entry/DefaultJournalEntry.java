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
package io.journalkeeper.core.entry;

import io.journalkeeper.core.api.BytesFragment;
import io.journalkeeper.core.api.JournalEntry;
import io.journalkeeper.core.journal.ParseJournalException;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * @author LiYue
 * Date: 2019/10/12
 */
public class DefaultJournalEntry implements JournalEntry {
    public final static short MAGIC_CODE = ByteBuffer.wrap(new byte[]{(byte) 0XF4, (byte) 0X3C}).getShort();

    // 包含Header和Payload
    private final byte[] serializedBytes;
    private final ByteBuffer serializedBuffer;
    private int offset = 0;

    DefaultJournalEntry(byte[] serializedBytes, boolean checkMagic, boolean checkLength) {
        this.serializedBytes = serializedBytes;
        this.serializedBuffer = ByteBuffer.wrap(serializedBytes);
        if (checkMagic) {
            checkMagic();
        }
        if (checkLength) {
            checkLength(serializedBytes);
        }
    }

    private void checkLength(byte[] serializedBytes) {
        if (serializedBytes.length != getLength()) {
            throw new ParseJournalException(
                    String.format("Declared length %d not equals actual length %d！",
                            getLength(), serializedBytes.length));
        }
    }

    private ByteBuffer serializedBuffer() {
        return serializedBuffer;
    }

    private void checkMagic() {
        short magic = JournalEntryParseSupport.getShort(serializedBuffer(), JournalEntryParseSupport.MAGIC);
        if (magicCode() != magic) {
            throw new ParseJournalException(String.format("Check magic failed, magic: %s, current: %s, content: %s",
                    magicCode(), magic, new String(serializedBytes)));
        }
    }

    @Override
    public int getBatchSize() {
        return JournalEntryParseSupport.getShort(serializedBuffer(), JournalEntryParseSupport.BATCH_SIZE);
    }

    public void setBatchSize(int batchSize) {
        JournalEntryParseSupport.setShort(serializedBuffer(), JournalEntryParseSupport.BATCH_SIZE, (short) batchSize);
    }

    @Override
    public int getPartition() {
        return JournalEntryParseSupport.getShort(serializedBuffer(), JournalEntryParseSupport.PARTITION);
    }

    public void setPartition(int partition) {
        JournalEntryParseSupport.setShort(serializedBuffer(), JournalEntryParseSupport.PARTITION, (short) partition);
    }

    @Override
    public int getOffset() {
        return offset;
    }

    public void setOffset(int offset) {
        this.offset = offset;
    }

    @Override
    public int getTerm() {
        return JournalEntryParseSupport.getInt(serializedBuffer(), JournalEntryParseSupport.TERM);
    }

    public void setTerm(int term) {
        JournalEntryParseSupport.setInt(serializedBuffer(), JournalEntryParseSupport.TERM, term);
    }

    @Override
    public BytesFragment getPayload() {
        return new BytesFragment(
                serializedBytes,
                JournalEntryParseSupport.getHeaderLength(),
                serializedBytes.length - JournalEntryParseSupport.getHeaderLength());
    }

    @Override
    public final byte[] getSerializedBytes() {
        return serializedBytes;
    }

    @Override
    public int getLength() {
        return JournalEntryParseSupport.getInt(serializedBuffer(), JournalEntryParseSupport.LENGTH);
    }

    private void setLength(int length) {
        JournalEntryParseSupport.setInt(serializedBuffer(), JournalEntryParseSupport.LENGTH, length);
    }

    @Override
    public long getTimestamp() {
        return JournalEntryParseSupport.getLong(serializedBuffer(), JournalEntryParseSupport.TIMESTAMP);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DefaultJournalEntry that = (DefaultJournalEntry) o;
        return Arrays.equals(serializedBytes, that.serializedBytes);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(serializedBytes);
    }

    @SuppressWarnings("SameReturnValue")
    private short magicCode() {
        return MAGIC_CODE;
    }
}
