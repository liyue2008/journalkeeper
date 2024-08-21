package io.journalkeeper.rocksdb;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;

public class RocksResponse implements Serializable {
    private final byte [] key;
    private final byte [] value;
    private final boolean success;
    private final String error;
    private final static long serialVersionUID = -3616929684752884699L;

    public RocksResponse(byte [] key, byte [] value) {
        this(key, value, true, null);
    }

    public RocksResponse(byte [] key, byte [] value, String error) {
        this(key, value, false, error);
    }

    public RocksResponse( byte [] key, byte [] value, boolean success, String error) {
        this.key = key;
        this.value = value;
        this.success = success;
        this.error = error;
    }

    public byte [] getKey() {
        return key;
    }

    public byte [] getValue() {
        return value;
    }

    public boolean isSuccess() {
        return success;
    }

    public String getError() {
        return error;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RocksResponse that = (RocksResponse) o;
        return success == that.success && Objects.deepEquals(key, that.key) && Objects.deepEquals(value, that.value) && Objects.equals(error, that.error);
    }

    @Override
    public int hashCode() {
        return Objects.hash(Arrays.hashCode(key), Arrays.hashCode(value), success, error);
    }

    @Override
    public String toString() {
        return "RocksResponse{" +
                "key=" + Arrays.toString(key) +
                ", value=" + Arrays.toString(value) +
                ", success=" + success +
                ", error='" + error + '\'' +
                '}';
    }
}
