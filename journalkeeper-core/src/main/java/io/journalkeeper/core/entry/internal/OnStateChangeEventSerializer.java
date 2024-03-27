package io.journalkeeper.core.entry.internal;

import java.nio.ByteBuffer;

public class OnStateChangeEventSerializer  extends InternalEntrySerializer<OnStateChangeEvent> {
    @Override
    protected byte[] serialize(OnStateChangeEvent event, byte[] header) {
        byte [] buffer = new byte[header.length + Long.BYTES];
        ByteBuffer byteBuffer = ByteBuffer.wrap(buffer);
        byteBuffer.put(header);
        byteBuffer.putLong(event.getLastApplied());
        return buffer;
    }

    @Override
    protected OnStateChangeEvent parse(ByteBuffer byteBuffer, int type, int version) {
        return new OnStateChangeEvent(version, byteBuffer.getLong());
    }
}
