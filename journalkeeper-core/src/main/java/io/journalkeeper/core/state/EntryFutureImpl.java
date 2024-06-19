package io.journalkeeper.core.state;

import io.journalkeeper.core.api.EntryFuture;
import io.journalkeeper.core.api.RaftJournal;

/**
 * @author LiYue
 * Date: 2020/3/2
 */
public class EntryFutureImpl implements EntryFuture {
    private final RaftJournal journal;
    private final long offset;

    public EntryFutureImpl(RaftJournal journal, long offset) {
        this.journal = journal;
        this.offset = offset;
    }

    @Override
    public byte[] get() {
        return journal.readByOffset(offset).getPayload().getBytes();
    }
}
