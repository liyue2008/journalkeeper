package io.journalkeeper.journalstore;

public class OnJournalChangeEvent {
    private final long partition;
    private final long minIndex;
    private final long maxIndex;

    public OnJournalChangeEvent(long partition, long minIndex, long maxIndex) {
        this.partition = partition;
        this.minIndex = minIndex;
        this.maxIndex = maxIndex;
    }

    public long getPartition() {
        return partition;
    }

    public long getMinIndex() {
        return minIndex;
    }

    public long getMaxIndex() {
        return maxIndex;
    }
}
