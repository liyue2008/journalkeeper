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
package io.journalkeeper.rpc.remoting.transport.codec;

import io.journalkeeper.rpc.remoting.transport.command.Header;
import io.journalkeeper.rpc.remoting.transport.command.Payload;
import io.netty.buffer.ByteBuf;

/**
 * jmq消息体编码器
 * author: gaohaoxiang
 * <p>
 * date: 2018/8/21
 */
public interface PayloadEncoder<H extends Header, T extends Payload> {

    void encode(T payload, ByteBuf buffer, H header);
}