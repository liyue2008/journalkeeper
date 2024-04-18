package io.journalkeeper.core.raft;

import java.util.List;

public interface JournalRo {
    long maxIndex();

    List<byte[]> readRaw(long index, int size);

    long commitIndex();

    long minIndex();

    int getTerm(long index);
}
