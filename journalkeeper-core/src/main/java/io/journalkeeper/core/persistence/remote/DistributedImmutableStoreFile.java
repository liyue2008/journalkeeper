package io.journalkeeper.core.persistence.remote;

import io.journalkeeper.core.persistence.StoreFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/**
 * @author LiYue
 * Date: 2020/5/13
 */
public class DistributedImmutableStoreFile implements StoreFile {
    private static final Logger logger = LoggerFactory.getLogger(DistributedImmutableStoreFile.class);
    // 文件全局位置
    private final long filePosition;
    // 文件头长度
    private final int headerSize;
    // 对应的File
    private final Path path;
    // 文件内数据长度
    private final int dataLength;

    private long timestamp = -1L;

    public DistributedImmutableStoreFile(long filePosition, Path path, int headerSize) throws IOException {
        this.filePosition = filePosition;
        this.headerSize = headerSize;
        this.path = path;
        this.dataLength = (int) (Files.size(path) - headerSize);
    }

    @Override
    public File file() {
        return path.toFile();
    }

    @Override
    public long position() {
        return filePosition;
    }

    @Override
    public boolean unload() {
        return false;
    }

    @Override
    public void forceUnload() {}

    @Override
    public boolean hasPage() {
        return false;
    }


    public long timestamp() {
        if (timestamp == -1L) {
            // 文件存在初始化时间戳
            readTimestamp();
        }
        return timestamp;
    }

    private void readTimestamp() {
        ByteBuffer timeBuffer = ByteBuffer.allocate(8);
        try (RandomAccessFile raf = new RandomAccessFile(path.toFile(), "r"); FileChannel fileChannel = raf.getChannel()) {
            fileChannel.position(0);
            fileChannel.read(timeBuffer);
        } catch (Exception e) {
            logger.warn("Exception: ", e);
        } finally {
            timestamp = timeBuffer.getLong(0);
        }
    }

    @Override
    public ByteBuffer read(int position, int length) throws IOException {
        int readLength = Math.min(dataLength - position, length);
        ByteBuffer readBuffer = ByteBuffer.allocate(readLength);
        try (RandomAccessFile raf = new RandomAccessFile(path.toFile(), "r"); FileChannel fileChannel = raf.getChannel()) {
            fileChannel.position(position + headerSize);
            fileChannel.read(readBuffer);
        }
        readBuffer.flip();
        return readBuffer;
    }

    @Override
    public Long readLong(int position) throws IOException{
        return read(position, Long.BYTES).getLong();
    }

    @Override
    public int append(ByteBuffer byteBuffer) {
        throw new UnsupportedOperationException();
    }


    @Override
    public int append(List<ByteBuffer> byteBuffers) {
        throw new UnsupportedOperationException();
    }

    /**
     * 刷盘
     */
    // Not thread safe!
    @Override
    public int flush() {
        throw new UnsupportedOperationException();
    }

    // Not thread safe!
    @Override
    public void rollback(int position) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isClean() {
        return true;
    }

    @Override
    public int writePosition() {
        return dataLength;
    }

    @Override
    public int fileDataSize() {
        return dataLength;
    }

    @Override
    public int flushPosition() {
        return dataLength;
    }

    @Override
    public int size() {
        return dataLength + headerSize;
    }

    @Override
    public void closeWrite() {}

    @Override
    public void force() {
        throw new UnsupportedOperationException();
    }

}
