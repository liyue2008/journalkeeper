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

import io.journalkeeper.rpc.client.QueryStateResponse;
import io.journalkeeper.rpc.header.JournalKeeperHeader;
import io.journalkeeper.rpc.remoting.serialize.CodecSupport;
import io.journalkeeper.rpc.remoting.transport.command.Types;
import io.netty.buffer.ByteBuf;

/**
 * @author LiYue
 * Date: 2019-04-02
 */
public class QueryStateResponseCodec extends LeaderResponseCodec<QueryStateResponse> implements Types {
    @Override
    protected void encodeLeaderResponse(JournalKeeperHeader header, QueryStateResponse response, ByteBuf buffer) {
        CodecSupport.encodeLong(buffer, response.getLastApplied());
        CodecSupport.encodeBytes(buffer, response.getResult());
    }

    @Override
    protected QueryStateResponse decodeLeaderResponse(JournalKeeperHeader header, ByteBuf buffer) {
        long lastApplied = CodecSupport.decodeLong(buffer);
        byte[] result = CodecSupport.decodeBytes(buffer);
        return new QueryStateResponse(result, lastApplied);
    }


    @Override
    public int[] types() {
        return new int[]{
                RpcTypes.QUERY_CLUSTER_STATE_RESPONSE,
                RpcTypes.QUERY_SERVER_STATE_RESPONSE,
                RpcTypes.QUERY_SNAPSHOT_RESPONSE
        };
    }
}
