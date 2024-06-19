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

import io.journalkeeper.rpc.header.JournalKeeperHeaderCodec;
import io.journalkeeper.rpc.remoting.transport.codec.Codec;
import io.journalkeeper.rpc.remoting.transport.codec.Decoder;
import io.journalkeeper.rpc.remoting.transport.codec.DefaultDecoder;
import io.journalkeeper.rpc.remoting.transport.codec.DefaultEncoder;
import io.journalkeeper.rpc.remoting.transport.codec.Encoder;
import io.journalkeeper.rpc.remoting.transport.codec.PayloadCodecFactory;
import io.journalkeeper.rpc.remoting.transport.exception.TransportException;
import io.netty.buffer.ByteBuf;

/**
 * @author LiYue
 * Date: 2019-04-01
 */
public class JournalKeeperCodec implements Codec {
    private final PayloadCodecFactory payloadCodecFactory;

    private final Decoder decoder;
    private final Encoder encoder;

    public JournalKeeperCodec() {
        PayloadCodecFactory payloadCodecFactory = new PayloadCodecFactory();
        PayloadCodecRegistry.register(payloadCodecFactory);
        Codec headerCodec = new JournalKeeperHeaderCodec();
        this.payloadCodecFactory = payloadCodecFactory;
        this.decoder = new DefaultDecoder(headerCodec, payloadCodecFactory);
        this.encoder = new DefaultEncoder(headerCodec, payloadCodecFactory);

    }

    @Override
    public Object decode(ByteBuf buffer) throws TransportException.CodecException {
        return decoder.decode(buffer);
    }

    @Override
    public void encode(Object obj, ByteBuf buffer) throws TransportException.CodecException {
        encoder.encode(obj, buffer);
    }

    public PayloadCodecFactory getPayloadCodecFactory() {
        return payloadCodecFactory;
    }
}
