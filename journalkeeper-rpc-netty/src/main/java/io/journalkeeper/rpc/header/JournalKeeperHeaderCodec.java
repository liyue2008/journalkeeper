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
package io.journalkeeper.rpc.header;


import io.journalkeeper.rpc.remoting.serialize.CodecSupport;
import io.journalkeeper.rpc.remoting.transport.codec.Codec;
import io.journalkeeper.rpc.remoting.transport.command.Direction;
import io.journalkeeper.rpc.remoting.transport.exception.TransportException;
import io.netty.buffer.ByteBuf;

import java.net.URI;

/**
 * JournalKeeper协议头编解码器
 * <p>
 * MAGIC: 4 bytes
 * VERSION: 1 byte
 * IDENTITY: 1 byte, 8 bits (High to Low):
 *      7: Unused
 *      6: Unused
 *      5: Unused
 *      4: Unused
 * <p>
 *      3: Unused
 *      2: Unused
 *      1: ONE_WAY: 1: ONE_WAY 0: REQUEST_RESPONSE
 *      0: DIRECTION: 0: REQUEST, 1: RESPONSE
 * REQUEST_ID: 4 bytes
 * TYPE: 1 byte
 * TIMESTAMP: 8 bytes
 * STATUS(Response only): 2 bytes
 * ERROR(Response only): variable
 * DESTINATION: variable
 * author: gaohaoxiang
 * <p>
 * date: 2018/8/21
 */
public class JournalKeeperHeaderCodec implements Codec {

    private static final int HEADER_LENGTH = 4 + 1 + 1 + 4 + 1 + 8;

    @Override
    public JournalKeeperHeader decode(ByteBuf buffer) throws TransportException.CodecException {
        if (buffer.readableBytes() < HEADER_LENGTH) {
            return null;
        }

        int magic = buffer.readInt();
        if (magic != JournalKeeperHeader.MAGIC) {
            return null;
        }

        byte version = buffer.readByte();
        byte identity = buffer.readByte();
        int requestId = buffer.readInt();
        byte type = buffer.readByte();
        long sendTime = buffer.readLong();
        short status = 0;
        String error = null;
        Direction direction = Direction.valueOf(identity & 0x1);
        boolean oneWay = ((identity >> 1) & 0x1) == 0x1;

        if (direction.equals(Direction.RESPONSE)) {
            // 1个字节的状态码
            status = buffer.readUnsignedByte();
            // 2个字节的异常长度
            // 异常信息
            try {
                error = CodecSupport.decodeString(buffer);
            } catch (Exception e) {
                throw new TransportException.CodecException(e.getMessage());
            }
        }
        URI destination = null;
        if (direction.equals(Direction.REQUEST)) {
            destination = URI.create(CodecSupport.decodeString(buffer));
        }

        return new JournalKeeperHeader(version, oneWay, direction, requestId, type, sendTime, destination, status, error);
    }

    @Override
    public void encode(Object payload, ByteBuf buffer) throws TransportException.CodecException {
        JournalKeeperHeader header = (JournalKeeperHeader) payload;
        // 响应类型
        byte identity = (byte) ((header.getDirection().ordinal() & 0x1) | (header.isOneWay() ? 0x2 : 0x0));

        buffer.writeInt(JournalKeeperHeader.MAGIC);
        buffer.writeByte(header.getVersion());
        buffer.writeByte(identity);
        buffer.writeInt(header.getRequestId());
        buffer.writeByte(header.getType());
        buffer.writeLong(header.getSendTime());
        if (header.getDirection().equals(Direction.RESPONSE)) {
            buffer.writeByte(header.getStatus());
            try {
                CodecSupport.encodeString(buffer, header.getError());
            } catch (Exception e) {
                throw new TransportException.CodecException(e.getMessage());
            }
        }
        if (header.getDirection().equals(Direction.REQUEST)) {
            CodecSupport.encodeString(buffer, header.getDestination().toASCIIString());
        }
    }
}