package io.journalkeeper.rocksdb;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;

public class RocksRequest implements Serializable {
    private final byte [] key;
    private final byte [] value;
    private final static long serialVersionUID = 6793785003979862581L;
    public RocksRequest(byte [] key) {
        this(key, null);
    }
    public RocksRequest(byte [] key, byte [] value) {
        this.key = key;
        this.value = value;
    }

    public byte [] getKey() {
        return key;
    }

    public byte [] getValue() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RocksRequest that = (RocksRequest) o;
        return Objects.deepEquals(key, that.key) && Objects.deepEquals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(Arrays.hashCode(key), Arrays.hashCode(value));
    }

    @Override
    public String toString() {
        return "RocksRequest{" +
                "key=" + Arrays.toString(key) +
                ", value=" + Arrays.toString(value) +
                '}';
    }
}
