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
package io.journalkeeper.coordinating.state;

import io.journalkeeper.coordinating.state.config.CoordinatingConfigs;
import io.journalkeeper.coordinating.state.domain.CoordinatingEvent;
import io.journalkeeper.coordinating.state.domain.StateTypes;
import io.journalkeeper.coordinating.state.domain.WriteRequest;
import io.journalkeeper.coordinating.state.store.KVStore;
import io.journalkeeper.coordinating.state.store.KVStoreManager;
import io.journalkeeper.core.easy.JkFireable;
import io.journalkeeper.core.easy.JkState;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.Properties;

/**
 * CoordinatingState
 * author: gaohaoxiang
 *
 * date: 2019/5/30
 */
public class CoordinatingState extends JkState {

    private KVStore kvStore;

    public CoordinatingState() {
        registerQueryCommandHandler(StateTypes.GET.getType(), this::doGet);
        registerQueryCommandHandler(StateTypes.EXIST.getType(), this::doExist);
        registerQueryCommandHandler(StateTypes.LIST.getType(), this::doList);
        registerExecuteCommandHandler(StateTypes.SET.getType(), this::doSet);
        registerExecuteCommandHandler(StateTypes.REMOVE.getType(), this::doRemove);
        registerExecuteCommandHandler(StateTypes.COMPARE_AND_SET.getType(), this::doCompareAndSet);
    }

    @Override
    public void recover(Path path, Properties properties) {
        this.kvStore = KVStoreManager.getFactory(properties.getProperty(CoordinatingConfigs.STATE_STORE)).create(path, properties);
    }



    private void fireEvent(String type, byte [] key, byte [] value, JkFireable fireable) {
        fireable.fireEvent(new CoordinatingEvent(type, new String(key, StandardCharsets.UTF_8), new String(value, StandardCharsets.UTF_8)));
    }

    @Override
    public void close() {
        kvStore.close();
    }

    protected byte [] doGet(byte[] key) {
         return kvStore.get(key);
    }

    protected boolean doExist(byte[] key) {
        return kvStore.exist(key);
    }

    protected List<byte[]> doList(List<byte[]> keys) {
        return kvStore.multiGet(keys);
    }

    protected boolean doSet(WriteRequest writeRequest, JkFireable fireable) {
        byte[] key = writeRequest.getKey();
        byte[] value = writeRequest.getValue();
        boolean result = kvStore.set(key, value);
        fireEvent(StateTypes.SET.getType(), key, value, fireable);
        return result;
    }

    protected boolean doRemove(byte[] key, JkFireable fireable) {
        boolean result =  kvStore.remove(key);
        fireEvent(StateTypes.SET.getType(), key, null, fireable);
        return result;

    }

    protected boolean doCompareAndSet(WriteRequest writeRequest, JkFireable fireable) {
        byte[] key = writeRequest.getKey();
        byte[] expect = writeRequest.getExpect();
        byte[] value = writeRequest.getValue();
        boolean result =  kvStore.compareAndSet(key, expect, value);
        fireEvent(StateTypes.SET.getType(), key, value, fireable);
        return result;

    }
}
