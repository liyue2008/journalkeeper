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
package io.journalkeeper.rpc.header;

import io.journalkeeper.rpc.remoting.transport.command.Direction;
import io.journalkeeper.rpc.remoting.transport.command.Header;

import java.net.URI;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author LiYue
 * Date: 2019-03-28
 */
public class JournalKeeperHeader implements Header {

    public final static int MAGIC = 0x3f4e93d7;
    private static final AtomicInteger requestIdGenerator = new AtomicInteger(0);
    public final static int DEFAULT_VERSION = 2;
    private boolean oneWay;
    private int status;
    private String error;
    private int requestId;
    private Direction direction;
    private int version;
    private int type;
    private long sendTime;
    private URI destination;

    public JournalKeeperHeader() {
    }

    public JournalKeeperHeader(int version, Direction direction, int type, URI destination) {
        this(version, false, direction, nextRequestId(), type, System.currentTimeMillis(), destination, 0, null);
    }

    public JournalKeeperHeader(int version, Direction direction, int requestId, int type, URI destination) {
        this(version, false, direction, requestId, type, System.currentTimeMillis(), destination, 0, null);
    }

    public JournalKeeperHeader(int version, boolean oneWay, Direction direction, int requestId, int type, long sendTime, URI destination, int status, String error) {
        this.version = version;
        this.oneWay = oneWay;
        this.direction = direction;
        this.requestId = requestId;
        this.type = type;
        this.sendTime = sendTime;
        this.status = status;
        this.error = error;
        this.destination = destination;
    }

    private static int nextRequestId() {
        return requestIdGenerator.incrementAndGet();
    }

    @Override
    public boolean isOneWay() {
        return oneWay;
    }

    @Override
    public void setOneWay(boolean oneWay) {
        this.oneWay = oneWay;
    }

    @Override
    public int getStatus() {
        return status;
    }

    @Override
    public void setStatus(int status) {
        this.status = status;
    }

    @Override
    public String getError() {
        return error;
    }

    @Override
    public void setError(String error) {
        this.error = error;
    }

    @Override
    public int getRequestId() {
        return requestId;
    }

    @Override
    public void setRequestId(int requestId) {
        this.requestId = requestId;
    }

    @Override
    public Direction getDirection() {
        return direction;
    }

    @Override
    public void setDirection(Direction direction) {
        this.direction = direction;
    }

    @Override
    public int getVersion() {
        return version;
    }

    @Override
    public void setVersion(int version) {
        this.version = version;
    }

    @Override
    public int getType() {
        return type;
    }

    @Override
    public void setType(int type) {
        this.type = type;
    }

    public long getSendTime() {
        return sendTime;
    }

    public void setSendTime(long sendTime) {
        this.sendTime = sendTime;
    }

    @Override
    public URI getDestination() {
        return destination;
    }

    @Override
    public void setDestination(URI destination) {
        this.destination = destination;
    }

    @Override
    public String toString() {
        return "JournalKeeperHeader{" +
                "oneWay=" + oneWay +
                ", status=" + status +
                ", error='" + error + '\'' +
                ", requestId=" + requestId +
                ", direction=" + direction +
                ", version=" + version +
                ", type=" + type +
                ", sendTime=" + sendTime +
                ", destination=" + destination +
                '}';
    }
}
