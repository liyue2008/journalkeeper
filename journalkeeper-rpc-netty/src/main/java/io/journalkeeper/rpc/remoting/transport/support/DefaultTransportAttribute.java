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
package io.journalkeeper.rpc.remoting.transport.support;

import io.journalkeeper.rpc.remoting.transport.TransportAttribute;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 默认通信属性
 * author: gaohaoxiang
 * <p>
 * date: 2018/8/14
 */
@SuppressWarnings("unchecked")
public class DefaultTransportAttribute implements TransportAttribute {

    private final AtomicReference<ConcurrentMap<Object, Object>> attributes = new AtomicReference<>();

    @Override
    public <T> T set(Object key, Object value) {
        return (T) getOrNewAttributes().put(key, value);
    }

    @Override
    public <T> T putIfAbsent(Object key, Object value) {
        return (T) getOrNewAttributes().putIfAbsent(key, value);
    }

    @Override
    public <T> T get(Object key) {
        ConcurrentMap<Object, Object> attributes = this.attributes.get();
        if (attributes == null) {
            return null;
        }
        return (T) attributes.get(key);
    }

    @Override
    public <T> T remove(Object key) {
        ConcurrentMap<Object, Object> attributes = this.attributes.get();
        if (attributes == null) {
            return null;
        }
        return (T) attributes.remove(key);
    }

    @Override
    public boolean contains(Object key) {
        ConcurrentMap<Object, Object> attributes = this.attributes.get();
        if (attributes == null) {
            return false;
        }
        return attributes.containsKey(key);
    }

    @Override
    public Set<Object> keys() {
        ConcurrentMap<Object, Object> attributes = this.attributes.get();
        if (attributes == null) {
            return Collections.emptySet();
        }
        return attributes.keySet();
    }

    protected ConcurrentMap<Object, Object> getOrNewAttributes() {
        ConcurrentMap<Object, Object> result = attributes.get();
        if (result == null) {
            attributes.compareAndSet(null, new ConcurrentHashMap<>());
            return attributes.get();
        }
        return result;
    }
}