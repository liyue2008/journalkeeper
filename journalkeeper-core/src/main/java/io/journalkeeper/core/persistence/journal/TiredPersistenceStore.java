package io.journalkeeper.core.persistence.journal;

import io.journalkeeper.core.persistence.JournalPersistence;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Properties;

/**
 * @author LiYue
 * Date: 2020/5/13
 */
public class TiredPersistenceStore  implements JournalPersistence, Closeable {
    private JournalPersistence hotJournalPersistence;
    private JournalPersistence coldJournalPersistence;

    @Override
    public long min() {
        return 0;
    }

    @Override
    public long physicalMin() {
        return 0;
    }

    @Override
    public long max() {
        return 0;
    }

    @Override
    public long flushed() {
        return 0;
    }

    @Override
    public void flush() throws IOException {

    }

    @Override
    public void truncate(long givenMax) throws IOException {

    }

    @Override
    public long compact(long givenMin) throws IOException {
        return 0;
    }

    @Override
    public long append(byte[] entry) throws IOException {
        return 0;
    }

    @Override
    public long append(List<byte[]> entries) throws IOException {
        return 0;
    }

    @Override
    public byte[] read(long position, int length) throws IOException {
        return new byte[0];
    }

    @Override
    public Long readLong(long position) throws IOException {
        return null;
    }

    @Override
    public void recover(Path path, long min, Properties properties) throws IOException {

    }

    @Override
    public void recover(Path path, Properties properties) throws IOException {

    }

    @Override
    public void delete() throws IOException {

    }

    @Override
    public Path getBasePath() {
        return null;
    }

    @Override
    public List<File> getFileList() {
        return null;
    }

    @Override
    public void close() throws IOException {

    }
}
