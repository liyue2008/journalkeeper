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

import io.journalkeeper.rpc.client.PullEventsRequest;
import io.journalkeeper.rpc.header.JournalKeeperHeader;
import io.journalkeeper.rpc.remoting.serialize.CodecSupport;
import io.journalkeeper.rpc.remoting.transport.command.Type;
import io.netty.buffer.ByteBuf;

/**
 * @author LiYue
 * Date: 2019-04-02
 */
public class PullEventsRequestCodec extends GenericPayloadCodec<PullEventsRequest> implements Type {
    @Override
    protected void encodePayload(JournalKeeperHeader header, PullEventsRequest request, ByteBuf buffer) {
        CodecSupport.encodeLong(buffer, request.getPullWatchId());
        CodecSupport.encodeLong(buffer, request.getAckSequence());

    }

    @Override
    protected PullEventsRequest decodePayload(JournalKeeperHeader header, ByteBuf buffer) {
        return new PullEventsRequest(
                CodecSupport.decodeLong(buffer),
                CodecSupport.decodeLong(buffer)
        );
    }

    @Override
    public int type() {
        return RpcTypes.PULL_EVENTS_REQUEST;
    }
}
