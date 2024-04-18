package io.journalkeeper.core.raft;

import java.net.URI;

public class RpcMsg <T>{
    private final URI uri;

    private final T request;

    public RpcMsg(URI uri, T request) {
        this.uri = uri;
        this.request = request;
    }

    public URI getUri() {
        return uri;
    }

    public T getRequest() {
        return request;
    }
}
