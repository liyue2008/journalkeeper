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
package io.journalkeeper.sql.state;

import io.journalkeeper.base.Serializer;
import io.journalkeeper.core.api.RaftJournal;
import io.journalkeeper.core.api.State;
import io.journalkeeper.core.api.StateResult;
import io.journalkeeper.sql.client.SQLEvent;
import io.journalkeeper.sql.client.domain.*;
import io.journalkeeper.sql.exception.SQLException;
import io.journalkeeper.sql.state.config.SQLConfigs;
import io.journalkeeper.sql.state.handler.SQLStateHandler;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.util.Properties;

/**
 * SQLState
 * author: gaohaoxiang
 * date: 2019/8/1
 */
public class SQLState implements State {

    private static final Logger logger = LoggerFactory.getLogger(SQLState.class);
    private final Serializer<WriteRequest> writeRequestSerializer;
    private final Serializer<WriteResponse> writeResponseSerializer;
    private final Serializer<ReadRequest> readRequestSerializer;
    private final Serializer<ReadResponse> readResponseSerializer;

    private final Serializer<SQLEvent> eventSerializer;
    private Path path;
    private Properties properties;
    private SQLExecutor executor;
    private SQLStateHandler handler;

    public SQLState(Serializer<WriteRequest> writeRequestSerializer, Serializer<WriteResponse> writeResponseSerializer, Serializer<ReadRequest> readRequestSerializer, Serializer<ReadResponse> readResponseSerializer, Serializer<SQLEvent> eventSerializer) {
        this.writeRequestSerializer = writeRequestSerializer;
        this.writeResponseSerializer = writeResponseSerializer;
        this.readRequestSerializer = readRequestSerializer;
        this.readResponseSerializer = readResponseSerializer;
        this.eventSerializer = eventSerializer;
    }

    @Override
    public void recover(Path path, Properties properties) throws IOException {
        this.path = path;
        this.executor = SQLExecutorManager.getExecutor(properties.getProperty(SQLConfigs.EXECUTOR_TYPE)).create(path, properties);
        if (this.executor == null) {
            throw new IllegalArgumentException("executor not exist");
        }
        this.properties = properties;
        this.handler = new SQLStateHandler(properties, executor);
        initExecutor(properties);
    }

    protected void initExecutor(Properties properties) {
        String initFile = properties.getProperty(SQLConfigs.INIT_FILE);
        if (StringUtils.isBlank(initFile)) {
            return;
        }

        try {
            InputStream initFileStream = SQLState.class.getResourceAsStream(initFile);
            if (initFileStream == null) {
                logger.warn("init file not exist, file: {}", initFile);
                return;
            }

            String sql = IOUtils.toString(initFileStream, Charset.forName("UTF-8"));
            executor.update(sql, null);
        } catch (Exception e) {
            logger.error("init exception", e);
            throw new SQLException(e);
        }
    }

    @Override
    public StateResult execute(byte[] requestEntry, int partition, long index, int batchSize, RaftJournal raftJournal) {
        WriteRequest request = writeRequestSerializer.parse(requestEntry);
        WriteResponse response = handler.handleWrite(request);
        SQLEvent event = new SQLEvent(OperationTypes.valueOf(request.getType()),request.getSql(), request.getParams().toArray(new String[request.getParams().size()]));

        StateResult result = new StateResult(
                writeResponseSerializer.serialize(response),
                eventSerializer.serialize(event)
        );
        return result;
    }

    @Override
    public byte[] query(byte[] request, RaftJournal raftJournal) {
        return readResponseSerializer.serialize(handler.handleRead(
                readRequestSerializer.parse(request)
        ));
    }

    @Override
    public void close() {
        this.executor.close();
    }
}