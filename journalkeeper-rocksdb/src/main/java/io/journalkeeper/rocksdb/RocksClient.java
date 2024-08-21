package io.journalkeeper.rocksdb;

import io.journalkeeper.core.easy.JkClient;
import java.nio.charset.StandardCharsets;

public class RocksClient {
    private final JkClient jkClient;

    public RocksClient(JkClient jkClient) {
        this.jkClient = jkClient;
    }

    public String get(String key) {
        RocksResponse response = jkClient.<RocksRequest, RocksResponse>query(RocksOperation.GET, new RocksRequest(key.getBytes(StandardCharsets.UTF_8))).join();
        if (response.isSuccess()) {
            if (response.getValue() == null) {
                return null;
            }
            return new String(response.getValue(), StandardCharsets.UTF_8);
        } else {
            throw new RuntimeException(response.getError());
        }
    }

    public void put(String key, String value) {
        jkClient.<RocksRequest, RocksResponse>update(RocksOperation.PUT, new RocksRequest(key.getBytes(StandardCharsets.UTF_8), value.getBytes(StandardCharsets.UTF_8))).join();
    }

    public void del(String key) {
        jkClient.<RocksRequest, RocksResponse>update(RocksOperation.DEL, new RocksRequest(key.getBytes(StandardCharsets.UTF_8))).join();
    }
}
