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
package io.journalkeeper.rpc.utils;

import io.journalkeeper.exceptions.ServerNotFoundException;
import io.journalkeeper.rpc.BaseResponse;
import io.journalkeeper.rpc.RpcException;
import io.journalkeeper.rpc.StatusCode;
import io.journalkeeper.rpc.codec.RpcTypes;
import io.journalkeeper.rpc.header.JournalKeeperHeader;
import io.journalkeeper.rpc.payload.GenericPayload;
import io.journalkeeper.rpc.payload.VoidPayload;
import io.journalkeeper.rpc.remoting.transport.Transport;
import io.journalkeeper.rpc.remoting.transport.command.Command;
import io.journalkeeper.rpc.remoting.transport.command.Direction;
import io.journalkeeper.rpc.remoting.transport.command.Header;

import java.net.URI;
import java.util.concurrent.CompletableFuture;

/**
 * @author LiYue
 * Date: 2019-04-01
 */
public class CommandSupport {
    private static <T> Command newRequestCommand(int type, T request, URI destination, int version) {
        JournalKeeperHeader header = new JournalKeeperHeader(version, Direction.REQUEST, type, destination);
        return new Command(header, new GenericPayload<>(request));
    }

    private static Command newRequestCommand(int type, URI destination, int version) {
        JournalKeeperHeader header = new JournalKeeperHeader(version, Direction.REQUEST, type, destination);
        return new Command(header, new VoidPayload());
    }

    public static <Q, R extends BaseResponse> CompletableFuture<R> sendRequest(Q request, int requestType, Transport transport, URI destination, int version) {
        Command requestCommand = null == request ? newRequestCommand(requestType, destination, version) : newRequestCommand(requestType, request, destination, version);
        return transport.async(requestCommand)
                .thenApply(responseCommand -> {
                    if (responseCommand.getHeader().getType() == RpcTypes.VOID_PAYLOAD) {
                        if (responseCommand.getHeader().getStatus() == StatusCode.SERVER_NOT_FOUND.getCode()) {
                            throw new ServerNotFoundException(responseCommand.getHeader().getError());
                        } else {
                            throw new RpcException(
                                            String.format("StatusCode: (%d)%s, ErrorMessage: %s",
                                                    responseCommand.getHeader().getStatus(),
                                                    StatusCode.valueOf(responseCommand.getHeader().getStatus()).getMessage(),
                                                    responseCommand.getHeader().getError())
                                    );
                        }
                    } else {
                        return GenericPayload.get(responseCommand.getPayload());
                    }
                });
    }

    public static void sendResponse(BaseResponse response, int responseType, Command requestCommand, Transport transport) {
        Command responseCommand = newResponseCommand(response, responseType, requestCommand);
        transport.acknowledge(requestCommand, responseCommand);
    }

    public static Command newResponseCommand(BaseResponse response, int responseType, Command requestCommand) {
        Header requestHeader = requestCommand.getHeader();
        JournalKeeperHeader header = new JournalKeeperHeader(requestHeader.getVersion(), Direction.RESPONSE, requestHeader.getRequestId(), responseType, null);
        header.setStatus(response.getStatusCode().getCode());
        header.setError(response.getError());

        return new Command(header, new GenericPayload<>(response));
    }

    public static Command newVoidPayloadResponse(int status, String error, Command requestCommand) {
        Header requestHeader = requestCommand.getHeader();
        JournalKeeperHeader header = new JournalKeeperHeader(requestHeader.getVersion(), Direction.RESPONSE, requestHeader.getRequestId(), RpcTypes.VOID_PAYLOAD, null);
        header.setStatus(status);
        header.setError(error);

        return new Command(header, new VoidPayload());
    }
}
