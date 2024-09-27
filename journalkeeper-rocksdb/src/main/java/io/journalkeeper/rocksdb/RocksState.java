package io.journalkeeper.rocksdb;

import io.journalkeeper.exceptions.RecoverException;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.nio.file.Path;
import java.util.Properties;

class RocksState {

    private Options options = null;
    private RocksDB rocksDB = null;
    private final static String ROCKS_PATH = "rocksdb";

    @SuppressWarnings("resource")
    void recover(Path statePath, Properties properties) {
        try {
            Path rocksPath = statePath.resolve(ROCKS_PATH);
            RocksDB.loadLibrary();
            options = new Options().setCreateIfMissing(true);
            rocksDB = RocksDB.open(options, rocksPath.toString());
            
        } catch (Exception e) {
            throw new RecoverException(e);
        } 

    }

    RocksResponse get(RocksRequest request){
        try {
            return new RocksResponse(request.getKey(), rocksDB.get(request.getKey()));
        } catch (RocksDBException e) {
            return new RocksResponse(request.getKey(), null, e.getMessage());
        }
    }

    RocksResponse put(RocksRequest request){
        try {
            rocksDB.put(request.getKey(), request.getValue());
            return new RocksResponse(request.getKey(), request.getValue());
        } catch (RocksDBException e) {
            return new RocksResponse(request.getKey(), null, e.getMessage());
        }
    }

    RocksResponse del(RocksRequest request) {
        try {
            rocksDB.delete(request.getKey());
            return new RocksResponse(request.getKey(), request.getValue());
        } catch (RocksDBException e) {
            return new RocksResponse(request.getKey(), null, e.getMessage());
        }
    }

    void close() {
        if (null != options) {
            options.close();
        }

        if (null != rocksDB) {
            rocksDB.close();
        }
    }
}
