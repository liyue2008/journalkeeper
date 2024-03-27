package io.journalkeeper.core.entry.internal;

import java.nio.ByteBuffer;

public class OnLeaderChangeEventSerializer extends InternalEntrySerializer<OnLeaderChangeEvent> {
    @Override
    protected byte[] serialize(OnLeaderChangeEvent entry, byte[] header) {
        byte [] buffer = new byte[header.length + Integer.BYTES + UriSerializeSupport.sizeOf(entry.getLeader())];
        ByteBuffer byteBuffer = ByteBuffer.wrap(buffer);
        byteBuffer.put(header);
        UriSerializeSupport.serializerUri(byteBuffer, entry.getLeader());
        byteBuffer.putInt(entry.getTerm());
        return buffer;
    }

    @Override
    protected OnLeaderChangeEvent parse(ByteBuffer byteBuffer, int type, int version) {
        return new OnLeaderChangeEvent(UriSerializeSupport.parseUri(byteBuffer), byteBuffer.getInt(),version);
    }
}
