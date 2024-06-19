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
package io.journalkeeper.rpc.codec;

import io.journalkeeper.rpc.header.JournalKeeperHeader;
import io.journalkeeper.rpc.remoting.serialize.CodecSupport;
import io.journalkeeper.rpc.remoting.transport.command.Type;
import io.journalkeeper.rpc.server.AsyncAppendEntriesResponse;
import io.netty.buffer.ByteBuf;

/**
 * @author LiYue
 * Date: 2019-04-02
 */
public class AsyncAppendEntriesResponseCodec extends ResponseCodec<AsyncAppendEntriesResponse> implements Type {
    @Override
    protected void encodeResponse(JournalKeeperHeader header, AsyncAppendEntriesResponse response, ByteBuf buffer) {
        //boolean success, long journalIndex, int term, int entryCount
        CodecSupport.encodeBoolean(buffer, response.isSuccess());
        CodecSupport.encodeLong(buffer, response.getJournalIndex());
        CodecSupport.encodeInt(buffer, response.getTerm());
        CodecSupport.encodeInt(buffer, response.getEntryCount());
    }

    @Override
    protected AsyncAppendEntriesResponse decodeResponse(JournalKeeperHeader header, ByteBuf buffer) {
        return new AsyncAppendEntriesResponse(
                CodecSupport.decodeBoolean(buffer),
                CodecSupport.decodeLong(buffer),
                CodecSupport.decodeInt(buffer),
                CodecSupport.decodeInt(buffer)
        );
    }

    @Override
    public int type() {
        return RpcTypes.ASYNC_APPEND_ENTRIES_RESPONSE;
    }
}
