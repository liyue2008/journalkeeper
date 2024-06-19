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
package io.journalkeeper.rpc.remoting.transport;

import java.util.Set;

/**
 * 通信属性
 * author: gaohaoxiang
 * <p>
 * date: 2018/8/13
 */
public interface TransportAttribute {

    <T> T set(Object key, Object value);

    <T> T putIfAbsent(Object key, Object value);

    <T> T get(Object key);

    <T> T remove(Object key);

    boolean contains(Object key);

    Set<Object> keys();
}