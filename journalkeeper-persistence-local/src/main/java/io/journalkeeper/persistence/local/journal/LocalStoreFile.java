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
package io.journalkeeper.persistence.local.journal;


import io.journalkeeper.persistence.local.cache.BufferHolder;
import io.journalkeeper.persistence.local.cache.MemoryCacheManager;
import io.journalkeeper.utils.locks.CasLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Cleaner;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.FileChannel;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.StampedLock;

/**
 * 支持并发、带缓存页、顺序写入的文件
 */
@SuppressWarnings("ResultOfMethodCallIgnored")
public class LocalStoreFile implements StoreFile, BufferHolder {
    private static final Logger logger = LoggerFactory.getLogger(LocalStoreFile.class);
    // 缓存页类型
    // 只读：
    // MAPPED_BUFFER：mmap映射内存镜像文件；
    // 读写：
    // DIRECT_BUFFER: 数据先写入DirectBuffer，异步刷盘到文件，性能最好；
    private final static int MAPPED_BUFFER = 0, DIRECT_BUFFER = 1, NO_BUFFER = -1;
    // 文件全局位置
    private final long filePosition;
    // 文件头长度
    private final int headerSize;
    // 对应的File
    private final File file;
    // buffer读写锁：
    // 访问(包括读和写）buffer时加读锁；
    // 加载、释放buffer时加写锁；
    private final StampedLock bufferLock = new StampedLock();

    // 文件锁，读写文件时加锁
    private final CasLock fileLock = new CasLock();
    // 缓存页
    private ByteBuffer pageBuffer = null;
    private int bufferType = NO_BUFFER;

    private final MemoryCacheManager bufferPool;
    private final int capacity;
    private long lastAccessTime = System.currentTimeMillis();

    // 当前刷盘位置
    private int flushPosition;
    // 当前写入位置
    private int writePosition = 0;

    private long timestamp = -1L;
    private final AtomicBoolean forced = new AtomicBoolean(false);
    private volatile boolean writeClosed = true;
    private FileChannel fileChannel;
    private RandomAccessFile raf;

    LocalStoreFile(long filePosition, File base, int headerSize, MemoryCacheManager bufferPool, int maxFileDataLength) {
        this.filePosition = filePosition;
        this.headerSize = headerSize;
        this.bufferPool = bufferPool;
        this.file = new File(base, String.valueOf(filePosition));
        if (file.exists() && file.length() > headerSize) {
            this.writePosition = (int) (file.length() - headerSize);
            this.flushPosition = writePosition;
        }
        this.capacity = Math.max(maxFileDataLength, (int )(file.length() - headerSize));
    }

    @Override
    public File file() {
        return file;
    }

    @Override
    public long position() {
        return filePosition;
    }

    private void loadRoUnsafe() throws IOException {
        if (null != pageBuffer) throw new IOException("Buffer already loaded!");
        bufferPool.allocateMMap(this);
        ByteBuffer loadBuffer;
        try (RandomAccessFile raf = new RandomAccessFile(file, "r"); FileChannel fileChannel = raf.getChannel()) {
            loadBuffer =
                    fileChannel.map(FileChannel.MapMode.READ_ONLY, headerSize, file.length() - headerSize);
            pageBuffer = loadBuffer;
            bufferType = MAPPED_BUFFER;
            pageBuffer.clear();
            forced.set(true);
        } catch (ClosedByInterruptException cie) {
            throw cie;
        } catch (Throwable t) {
            logger.warn("Exception: ", t);
            bufferPool.releaseMMap(this);
            pageBuffer = null;
            throw t;
        }
    }

    private void loadRwUnsafe() throws IOException {
        if (bufferType == DIRECT_BUFFER) {
            return;
        } else if (bufferType == MAPPED_BUFFER) {
            unloadUnsafe();
        }

        ByteBuffer buffer = bufferPool.allocateDirect(capacity, this);
        loadDirectBuffer(buffer);
        writeClosed = false;

    }

    private void loadDirectBuffer(ByteBuffer buffer) throws IOException {
        boolean needLoadFileContent = file.exists() && file.length() > headerSize;
        boolean writeTimestamp = !file.exists();
        // 打开文件描述符
        raf = new RandomAccessFile(file, "rw");
        fileChannel = raf.getChannel();
        if (writeTimestamp) {
            // 第一次创建文件写入头部预留128字节中0位置开始的前8字节长度:文件创建时间戳
            writeTimestamp();
        }
        if (needLoadFileContent) {
            logger.info("Reload file for write! size: {}, file: {}.", file.length(), file);
            fileChannel.position(headerSize);
            int length;
            do {
                length = fileChannel.read(buffer);
            } while (length > 0);
            buffer.clear();
        }
        this.pageBuffer = buffer;
        bufferType = DIRECT_BUFFER;
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
        try (RandomAccessFile raf = new RandomAccessFile(file, "r"); FileChannel fileChannel = raf.getChannel()) {
            fileChannel.position(0);
            fileChannel.read(timeBuffer);
        } catch (Exception e) {
            logger.warn("Exception: ", e);
        } finally {
            timestamp = timeBuffer.getLong(0);
        }
    }

    private void writeTimestamp() {
        ByteBuffer timeBuffer = ByteBuffer.allocate(8);
        long creationTime = System.currentTimeMillis();
        timeBuffer.putLong(0, creationTime);
        try (RandomAccessFile raf = new RandomAccessFile(file, "rw"); FileChannel fileChannel = raf.getChannel()) {
            fileChannel.position(0);
            fileChannel.write(timeBuffer);
        } catch (Exception e) {
            logger.warn("Exception:", e);
        } finally {
            timestamp = creationTime;
        }
    }

    @Override
    public boolean unload() {
        long stamp = bufferLock.writeLock();
        try {
            if (isClean()) {
                unloadUnsafe();
                return true;
            } else {
                return false;
            }
        } finally {
            bufferLock.unlockWrite(stamp);
        }
    }

    @Override
    public void forceUnload() {
        long stamp = bufferLock.writeLock();
        try {
            unloadUnsafe();
        } finally {
            bufferLock.unlockWrite(stamp);
        }
    }

    @Override
    public boolean hasPage() {
        return this.bufferType != NO_BUFFER;
    }

    @Override
    public ByteBuffer read(int position, int length) throws IOException {
        touch();
        long stamp = bufferLock.readLock();
        try {
            while (!hasPage()) {
                long ws = bufferLock.tryConvertToWriteLock(stamp);
                if (ws != 0L) {
                    // 升级成写锁成功
                    stamp = ws;
                    loadRoUnsafe();
                } else {
                    bufferLock.unlockRead(stamp);
                    stamp = bufferLock.writeLock();
                }
            }
            long rs = bufferLock.tryConvertToReadLock(stamp);
            if (rs != 0L) {
                stamp = rs;
            }
            ByteBuffer byteBuffer = pageBuffer.asReadOnlyBuffer();
            byteBuffer.position(position);
            byteBuffer.limit(writePosition);

            ByteBuffer dest = ByteBuffer.allocate(Math.min(length, byteBuffer.remaining()));
            if (length < byteBuffer.remaining()) {
                byteBuffer.limit(byteBuffer.position() + length);
            }
            dest.put(byteBuffer);
            dest.flip();
            return dest;

        } finally {
            bufferLock.unlock(stamp);
        }
    }

    @Override
    public Long readLong(int position) throws IOException{
        touch();
        long stamp = bufferLock.readLock();
        try {
            while (!hasPage()) {
                long ws = bufferLock.tryConvertToWriteLock(stamp);
                if (ws != 0L) {
                    // 升级成写锁成功
                    stamp = ws;
                    loadRoUnsafe();
                } else {
                    bufferLock.unlockRead(stamp);
                    stamp = bufferLock.writeLock();
                }
            }
            long rs = bufferLock.tryConvertToReadLock(stamp);
            if (rs != 0L) {
                stamp = rs;
            }

            return pageBuffer.getLong(position);
        } finally {
            bufferLock.unlock(stamp);
        }
    }

    // Not thread safe!
    private int appendToPageBuffer(ByteBuffer byteBuffer) {
        pageBuffer.position(writePosition);
        int writeLength = byteBuffer.remaining();
        pageBuffer.put(byteBuffer);

        writePosition += writeLength;
        return writeLength;
    }

    // Not thread safe!
    private int appendToPageBuffer(List<ByteBuffer> byteBuffers) {
        pageBuffer.position(writePosition);
        int writeLength = 0;
        for (ByteBuffer byteBuffer : byteBuffers) {
            writeLength += byteBuffer.remaining();
            pageBuffer.put(byteBuffer);
        }
        writePosition += writeLength;
        return writeLength;
    }

    @Override
    public int append(ByteBuffer byteBuffer) throws IOException {
        touch();
        long stamp = bufferLock.readLock();
        try {
            while (bufferType != DIRECT_BUFFER) {
                long ws = bufferLock.tryConvertToWriteLock(stamp);
                if (ws != 0L) {
                    // 升级成写锁成功
                    stamp = ws;
                    loadRwUnsafe();
                } else {
                    bufferLock.unlockRead(stamp);
                    stamp = bufferLock.writeLock();
                }
            }
            long rs = bufferLock.tryConvertToReadLock(stamp);
            if (rs != 0L) {
                stamp = rs;
            }
            return appendToPageBuffer(byteBuffer);
        } finally {
            bufferLock.unlock(stamp);
        }
    }


    @Override
    public int append(List<ByteBuffer> byteBuffers) throws IOException {
        touch();
        long stamp = bufferLock.readLock();
        try {
            while (bufferType != DIRECT_BUFFER) {
                long ws = bufferLock.tryConvertToWriteLock(stamp);
                if (ws != 0L) {
                    // 升级成写锁成功
                    stamp = ws;
                    loadRwUnsafe();
                } else {
                    bufferLock.unlockRead(stamp);
                    stamp = bufferLock.writeLock();
                }
            }
            long rs = bufferLock.tryConvertToReadLock(stamp);
            if (rs != 0L) {
                stamp = rs;
            }
            return appendToPageBuffer(byteBuffers);
        } finally {
            bufferLock.unlock(stamp);
        }
    }

    private void touch() {
        lastAccessTime = System.currentTimeMillis();
    }

    /**
     * 刷盘
     */
    // Not thread safe!
    @Override
    public int flush() throws IOException {
        long stamp = bufferLock.readLock();
        try {
            if (writePosition > flushPosition && fileLock.tryLock()) {

                try {
                    if (!file.exists()) {
                        // 第一次创建文件写入头部预留128字节中0位置开始的前8字节长度:文件创建时间戳
                        writeTimestamp();
                    }
                    return flushPageBuffer();

                } finally {
                    fileLock.unlock();
                }

            }
            return 0;
        } finally {
            bufferLock.unlockRead(stamp);
        }
    }
    private void ensureOpen() throws FileNotFoundException {
        if (fileChannel == null || !fileChannel.isOpen()) {
            // 打开文件描述符
            raf = new RandomAccessFile(file, "rw");
            fileChannel = raf.getChannel();
        }
    }
    private int flushPageBuffer() throws IOException {
        ensureOpen();
        int flushEnd = writePosition;
        ByteBuffer flushBuffer = pageBuffer.asReadOnlyBuffer();
        flushBuffer.position(flushPosition);
        flushBuffer.limit(flushEnd);
        fileChannel.position(headerSize + flushPosition);
        int flushSize = flushEnd - flushPosition;

        while (flushBuffer.hasRemaining()) {
            fileChannel.write(flushBuffer);
        }
        flushPosition = flushEnd;
        return flushSize;
    }

    // Not thread safe!
    @Override
    public void rollback(int position) throws IOException {
        long stamp = bufferLock.writeLock();
        try {
            if (position < writePosition) {
                writePosition = position;
            }
            if (position < flushPosition) {
                fileLock.waitAndLock();
                try {
                    flushPosition = position;
                    if (fileChannel != null && fileChannel.isOpen()) {
                        fileChannel.truncate(position + headerSize);
                    } else {
                        try (RandomAccessFile raf = new RandomAccessFile(file, "rw"); FileChannel fileChannel = raf.getChannel()) {
                            fileChannel.truncate(position + headerSize);
                        }
                    }
                } finally {
                    fileLock.unlock();
                }
            }
        } finally {
            bufferLock.unlockWrite(stamp);
        }
    }

    @Override
    public boolean isClean() {
        return flushPosition >= writePosition;
    }

    @Override
    public int writePosition() {
        return writePosition;
    }

    @Override
    public int fileDataSize() {
        return Math.max((int) file.length() - headerSize, 0);
    }

    @Override
    public int flushPosition() {
        return flushPosition;
    }

    @Override
    public long lastAccessTime() {
        return lastAccessTime;
    }


    private void unloadUnsafe() {
        if (MAPPED_BUFFER == this.bufferType) {
            unloadMappedBuffer();
        } else if (DIRECT_BUFFER == this.bufferType) {
            unloadDirectBuffer();
        }
        try {
            closeFileChannel();
        } catch (IOException e) {
            logger.warn("Close file {} exception: ", file.getAbsolutePath(), e);
        }
        writeClosed = true;
    }

    private void unloadDirectBuffer() {
        final ByteBuffer direct = pageBuffer;
        pageBuffer = null;
        this.bufferType = NO_BUFFER;
        if (null != direct) bufferPool.releaseDirect(direct, this);

    }

    private void unloadMappedBuffer() {
        try {
            final Buffer mapped = pageBuffer;
            pageBuffer = null;
            this.bufferType = NO_BUFFER;
            if (null != mapped) {
                Method getCleanerMethod;
                getCleanerMethod = mapped.getClass().getMethod("cleaner");
                getCleanerMethod.setAccessible(true);
                Cleaner cleaner = (Cleaner) getCleanerMethod.invoke(mapped, new Object[0]);
                cleaner.clean();
            }
            bufferPool.releaseMMap(this);
        } catch (Exception e) {
            logger.warn("Release direct buffer exception: ", e);
        }
    }

    @Override
    public int size() {
        return capacity;
    }

    @Override
    public boolean isFree() {
        return isClean();
    }

    @Override
    public boolean evict() {
        return unload();
    }

    @Override
    public void closeWrite() {
        writeClosed = true;
    }
    @Override
    public boolean writable() {
        return bufferType == DIRECT_BUFFER && !writeClosed;
    }

    private void closeFileChannel() throws IOException {
        force();
        if (null != fileChannel) {
            fileChannel.close();
        }
        if (null != raf) {
            raf.close();
        }
    }

    @Override
    public void force() throws IOException {
        if(forced.compareAndSet(false, true)) {
            fileLock.waitAndLock();
            ensureOpen();
            try {
                fileChannel.force(true);
            } catch (Throwable t) {
                forced.set(false);
                throw t;
            } finally {
                fileLock.unlock();
            }
        }
    }

}
