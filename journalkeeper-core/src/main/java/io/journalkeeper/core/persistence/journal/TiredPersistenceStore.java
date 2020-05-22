package io.journalkeeper.core.persistence.journal;

import io.journalkeeper.core.persistence.JournalPersistence;
import io.journalkeeper.core.persistence.StoreFile;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * 分层的JournalPersistence。包含一个冷数据存储和热数据存储。
 * @author LiYue
 * Date: 2020/5/13
 */
public class TiredPersistenceStore  implements JournalPersistence, Closeable {
    private final JournalPersistence hotJournalPersistence;
    private final JournalPersistence coldJournalPersistence;

    public TiredPersistenceStore(JournalPersistence hotJournalPersistence, JournalPersistence coldJournalPersistence) {
        this.hotJournalPersistence = hotJournalPersistence;
        this.coldJournalPersistence = coldJournalPersistence;
    }


    @Override
    public long min() {
        return coldJournalPersistence != null ? coldJournalPersistence.min(): hotJournalPersistence.min() ;
    }

    @Override
    public long physicalMin() {
       return coldJournalPersistence != null ? coldJournalPersistence.physicalMin(): hotJournalPersistence.physicalMin();
    }

    @Override
    public long max() {
        return hotJournalPersistence.max();
    }

    @Override
    public long flushed() {
        return hotJournalPersistence.flushed();
    }

    @Override
    public void flush() throws IOException {
        hotJournalPersistence.flush();
    }

    @Override
    public void truncate(long givenMax) throws IOException {
        hotJournalPersistence.truncate(givenMax);
    }

    @Override
    public long compact(long givenMin) throws IOException {
        long size = 0L;
        if(null != coldJournalPersistence) {
            size += coldJournalPersistence.compact(givenMin);
        }
        size += hotJournalPersistence.compact(givenMin);
        return size;
    }

    @Override
    public void appendFile(Path srcPath) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long append(byte[] entry) throws IOException {
        return hotJournalPersistence.append(entry);
    }

    @Override
    public long append(List<byte[]> entries) throws IOException {
        return hotJournalPersistence.append(entries);
    }

    private void checkReadPosition(long position) {
        long p;
        if ((p = min()) > position) {
            throw new PositionUnderflowException(position, p);
        } else if (position >= (p = max())) {
            throw new PositionOverflowException(position, p);
        }

    }

    @Override
    public byte[] read(long position, int length) throws IOException {
        checkReadPosition(position);
        return position >= hotJournalPersistence.min() ?
                hotJournalPersistence.read(position, length):
                coldJournalPersistence.read(position, length);
    }

    @Override
    public Long readLong(long position) throws IOException {
        checkReadPosition(position);
        return position >= hotJournalPersistence.min() ?
                hotJournalPersistence.readLong(position):
                coldJournalPersistence.readLong(position);
    }

    @Override
    public void recover(Path path, long min, Properties properties) throws IOException {
        if(null == coldJournalPersistence) {
            hotJournalPersistence.recover(path, min, properties);
        } else {
            throw new UnsupportedOperationException();
        }
    }

    public void recover(Path codePath, Path hotPath, long min, Properties properties) throws IOException {
        hotJournalPersistence.recover(hotPath, min, properties);
        if(null != coldJournalPersistence) {
            coldJournalPersistence.recover(codePath, min, properties);
            if(hotJournalPersistence.getStoreFiles().isEmpty()) {
                hotJournalPersistence.compact(coldJournalPersistence.max());
            }
        }


    }

    @Override
    public void delete() throws IOException {
        if(null != coldJournalPersistence) {
            coldJournalPersistence.delete();
        }
        hotJournalPersistence.delete();
    }

    @Override
    public Path getBasePath() {
        return hotJournalPersistence.getBasePath();
    }

    @Override
    public List<StoreFile> getStoreFiles() {
        if (null == coldJournalPersistence) {
            return hotJournalPersistence.getStoreFiles();
        } else {

            List<StoreFile> hotStoreFiles = hotJournalPersistence.getStoreFiles();
            long hotMin;
            if(!hotStoreFiles.isEmpty()) {
                hotMin = hotStoreFiles.get(0).position();
            } else {
                hotMin = Long.MAX_VALUE;
            }
            return Stream.concat(
                coldJournalPersistence.getStoreFiles()
                        .stream()
                        .filter(storeFile -> storeFile.position() < hotMin),
                hotStoreFiles.stream()
            ).collect(Collectors.toList());

        }

    }

    @Override
    public void close() throws IOException {
        hotJournalPersistence.close();
        if(null != coldJournalPersistence) {
            coldJournalPersistence.close();
        }
    }
}
