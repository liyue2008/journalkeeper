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
package io.journalkeeper.core.state;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.journalkeeper.core.easy.JkState;
import io.journalkeeper.core.easy.JkStateFactory;
import io.journalkeeper.core.easy.StateContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author LiYue
 * Date: 2019-04-03
 */
public class KvStateFactory extends JkStateFactory {

    private static final Logger logger = LoggerFactory.getLogger(KvStateFactory.class);
    private static final String FILENAME = "map";
    private static final String CMD_GET = "GET";
    private static final String CMD_SET = "SET";
    private static final String CMD_DEL = "DEL";
    private static final String CMD_LIST = "KEYS";
    private static final String CMD_FIRE_EVENT = "FIRE_EVENT";


    @Override
    protected void onStateCreated(JkState state) {
        super.onStateCreated(state);
        State myState = new State();
        state.registerExecuteCommandHandler(CMD_SET, myState::set);
        state.registerExecuteCommandHandler(CMD_DEL, myState::del);
        state.registerExecuteCommandHandler(CMD_FIRE_EVENT, myState::fireEvent);
        state.registerQueryCommandHandler(CMD_GET, myState::get);
        state.registerQueryCommandHandler(CMD_LIST, myState::list);
        state.onRecover(myState::doRecover);
        state.onFlush(myState::flush);
    }

    private static class State {

        private final Gson gson = new Gson();
        private Map<String, String> stateMap = new HashMap<>();
        private Path statePath;

        private final AtomicBoolean isDirty = new AtomicBoolean(false);

        public void doRecover(Path statePath, Properties properties) {
            this.statePath = statePath;
            try {
                stateMap = gson.fromJson(new String(Files.readAllBytes(statePath.resolve(FILENAME)), StandardCharsets.UTF_8),
                        new TypeToken<HashMap<String, String>>() {
                        }.getType());
                int keys = stateMap == null ? -1 : stateMap.size();
                isDirty.set(false);
                logger.info("State map recovered from {}, keys {} ", statePath, keys);
            } catch (NoSuchFileException e) {
                stateMap = new HashMap<>();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        private void checkInput(String[] input, int count) {
            if (input.length < count) {
                throw new IllegalArgumentException("Bad request: " + String.join(" ", input) + "!");
            }
        }

        private void set(String param) {
            String[] splt = param.split("\\s");
            checkInput(splt, 2);
            stateMap.put(splt[0], splt[1]);
            isDirty.compareAndSet(false, true);

        }

        private void del(String key) {
            stateMap.remove(key);
            isDirty.compareAndSet(false, true);

        }

        private String get(String key) {
            return stateMap.get(key);
        }

        private void fireEvent(String msg, StateContext fireable) {
            fireable.fireEvent(msg);
        }


        private String list() {
            return String.join(", ", stateMap.keySet());
        }

        public void flush() throws IOException {
            if (isDirty.compareAndSet(true, false)) {
                Files.write(statePath.resolve(FILENAME), gson.toJson(stateMap).getBytes(StandardCharsets.UTF_8));
            }
        }
    }
}
