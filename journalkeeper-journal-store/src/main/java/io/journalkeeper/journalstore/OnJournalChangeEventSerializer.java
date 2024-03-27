package io.journalkeeper.journalstore;

import io.journalkeeper.base.Serializer;

import java.nio.ByteBuffer;

public class OnJournalChangeEventSerializer implements Serializer<OnJournalChangeEvent> {
    private static final int FIXED_LENGTH =
            Long.BYTES + Long.BYTES + Long.BYTES;
    @Override
    public byte[] serialize(OnJournalChangeEvent entry) {
        ByteBuffer buffer = ByteBuffer.allocate(FIXED_LENGTH);
                buffer.putLong(entry.getPartition());
                buffer.putLong(entry.getMinIndex());
                buffer.putLong(entry.getMaxIndex());
                return buffer.array();
    }

    @Override
    public OnJournalChangeEvent parse(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        long partition = buffer.getLong();
        long minIndex = buffer.getLong();
        long maxIndex = buffer.getLong();
        return new OnJournalChangeEvent(partition, minIndex, maxIndex);
    }
}
