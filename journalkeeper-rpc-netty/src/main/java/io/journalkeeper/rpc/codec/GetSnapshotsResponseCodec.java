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

import io.journalkeeper.core.api.SnapshotEntry;
import io.journalkeeper.core.api.SnapshotsEntry;
import io.journalkeeper.rpc.client.GetSnapshotsResponse;
import io.journalkeeper.rpc.header.JournalKeeperHeader;
import io.journalkeeper.rpc.remoting.serialize.CodecSupport;
import io.journalkeeper.rpc.remoting.transport.command.Type;
import io.netty.buffer.ByteBuf;

import java.util.ArrayList;

/**
 * GetSnapshostsResponseCodec
 * author: gaohaoxiang
 * date: 2019/12/13
 */
public class GetSnapshotsResponseCodec extends LeaderResponseCodec<GetSnapshotsResponse> implements Type {

    @Override
    protected void encodeLeaderResponse(JournalKeeperHeader header, GetSnapshotsResponse leaderResponse, ByteBuf buffer) {
        CodecSupport.encodeCollection(buffer, leaderResponse.getSnapshots().getSnapshots(),
                (obj, entryBuffer) -> {
                    SnapshotEntry entry = (SnapshotEntry) obj;
                    CodecSupport.encodeLong(entryBuffer, entry.getIndex());
                    CodecSupport.encodeLong(entryBuffer, entry.getTimestamp());
                });
    }

    @Override
    protected GetSnapshotsResponse decodeLeaderResponse(JournalKeeperHeader header, ByteBuf buffer) {
        return new GetSnapshotsResponse(
                new SnapshotsEntry( new ArrayList<>(CodecSupport.decodeCollection(buffer, entryBuffer -> new SnapshotEntry(
                        CodecSupport.decodeLong(entryBuffer),
                        CodecSupport.decodeLong(entryBuffer)
                )))));
    }

    @Override
    public int type() {
        return RpcTypes.GET_SNAPSHOTS_RESPONSE;
    }
}