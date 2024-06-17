package io.journalkeeper.core.journal;

import io.journalkeeper.core.api.JournalEntry;
import io.journalkeeper.core.api.JournalEntryParser;
import io.journalkeeper.core.api.RaftJournal;
import io.journalkeeper.core.entry.internal.InternalEntriesSerializeSupport;
import io.journalkeeper.core.entry.internal.InternalEntryType;
import io.journalkeeper.core.entry.internal.ScalePartitionsEntry;
import io.journalkeeper.core.metric.MetricNames;
import io.journalkeeper.core.metric.MetricProvider;
import io.journalkeeper.core.monitor.MonitoredJournal;
import io.journalkeeper.exceptions.JournalException;
import io.journalkeeper.exceptions.StateRecoverException;
import io.journalkeeper.metric.JMetric;
import io.journalkeeper.monitor.DiskMonitorInfo;
import io.journalkeeper.monitor.JournalMonitorInfo;
import io.journalkeeper.monitor.JournalPartitionMonitorInfo;
import io.journalkeeper.persistence.BufferPool;
import io.journalkeeper.persistence.JournalPersistence;
import io.journalkeeper.persistence.MonitoredPersistence;
import io.journalkeeper.persistence.PersistenceFactory;
import io.journalkeeper.utils.actor.Actor;
import io.journalkeeper.utils.actor.annotation.ActorListener;
import io.journalkeeper.utils.actor.annotation.ActorScheduler;
import io.journalkeeper.utils.actor.annotation.ActorSubscriber;
import io.journalkeeper.utils.config.Config;
import io.journalkeeper.utils.spi.ServiceSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

import static io.journalkeeper.core.api.RaftJournal.RESERVED_PARTITIONS_START;
import static io.journalkeeper.core.journal.Journal.INDEX_STORAGE_SIZE;


public class JournalActor {
private static final Logger logger = LoggerFactory.getLogger( JournalActor.class );
    private final PersistenceFactory persistenceFactory;
    private final Journal journal;

    private final BufferPool bufferPool;

    private final Properties properties;

    private final Config config;


    private final Actor actor;

    private final MonitoredJournal monitoredJournal;

    private final MetricProvider metricProvider;
    private final JMetric appendJournalMetric;


    public JournalActor(JournalEntryParser journalEntryParser, Config config, MetricProvider metricProvider, Properties properties) {
        this.config = config;
        this.properties = properties;
        persistenceFactory = ServiceSupport.load(PersistenceFactory.class);
        bufferPool = ServiceSupport.load(BufferPool.class);
        this.journal = new Journal(persistenceFactory, bufferPool, journalEntryParser);
        this.monitoredJournal = new MonitoredJournalImpl();
        this.actor = Actor.builder().addr("Journal").setHandlerInstance(this).privatePostman(config.get("performance_mode")).build();
        this.metricProvider = metricProvider;
        this.appendJournalMetric = metricProvider.getMetric(MetricNames.METRIC_APPEND_JOURNAL);
    }

    public RaftJournal getRaftJournal() {
        return journal;
    }

    @ActorListener
    private void recover(RecoverJournalRequest request) {
        try {
            doRecover(request.getPartitions(), request.getJournalSnapshot(), request.getCommitIndex());
        } catch (IOException e) {
            throw new StateRecoverException(e);
        }
    }
    private void doRecover(Set<Integer> partitions, JournalSnapshot journalSnapshot, long commitIndex) throws IOException {
        journal.recover(config.get("working_dir"), commitIndex, journalSnapshot, properties);
        journal.rePartition(partitions);
    }

    public static class RecoverJournalRequest{
        private final Set<Integer> partitions;
        private final JournalSnapshot journalSnapshot;
        private final long commitIndex;

        public RecoverJournalRequest(Set<Integer> partitions, JournalSnapshot journalSnapshot, long commitIndex) {
            this.partitions = partitions;
            this.journalSnapshot = journalSnapshot;
            this.commitIndex = commitIndex;
        }

        public Set<Integer> getPartitions() {
            return partitions;
        }

        public JournalSnapshot getJournalSnapshot() {
            return journalSnapshot;
        }

        public long getCommitIndex() {
            return commitIndex;
        }
    }

    @ActorListener
    private long append(List<JournalEntry> journalEntries) {
        this.appendJournalMetric.start();
        long start = System.nanoTime();
        long position;
        if (journalEntries.size() == 1) {
            position = journal.append(journalEntries.get(0));
        } else {
            List<Long> positions = journal.append(journalEntries);
            position = positions.get(positions.size() - 1);
        }
        this.appendJournalMetric.end(journalEntries.stream().mapToLong(JournalEntry::getLength).sum());
        return position;
    }

    @ActorListener
    private long appendBatchRaw(List<byte []> rawEntries) {
        return journal.appendBatchRaw(rawEntries);
    }
    @ActorListener
    private void compareOrAppendRaw(List<byte[]> entries, long startIndex) {
        journal.compareOrAppendRaw(entries, startIndex);
    }

    @ActorListener
    private void commit(long commitIndex) throws IOException {

        if (commitIndex > journal.commitIndex()) {
            journal.commit(Math.min(commitIndex, journal.maxIndex()));
            actor.pub("onJournalCommit");
        }

    }
    @ActorScheduler(interval = 100L)
    private void flush() {
        long flushCount = journal.flushOnce();
        if (flushCount > 0) {
            actor.pub("onJournalFlush", this.journal.maxIndex());
        }
    }

    @ActorListener
    private void compact(JournalSnapshot journalSnapshot, long lastIncludedIndex, int lastIncludedTerm) throws IOException {
        // If existing log entry has same index and term as snapshot’s
        // last included entry, retain log entries following it.
        // Discard the entire log

        logger.info("Compact journal entries, journal: {}...", journal);
        if (journal.minIndex() >= lastIncludedIndex &&
                lastIncludedIndex < journal.maxIndex() &&
                journal.getTerm(lastIncludedIndex) == lastIncludedTerm) {
            journal.compact(journalSnapshot);

        } else {
            journal.clear(journalSnapshot);
        }
        logger.info("Compact journal finished, journal: {}.", journal);
    }
    @ActorSubscriber
    private void onStop() {
        flush();
        this.metricProvider.removeMetric(MetricNames.METRIC_APPEND_JOURNAL);
    }

    public Actor getActor() {
        return actor;
    }

    public MonitoredJournal getMonitoredJournal() {
        return monitoredJournal;
    }

    private class MonitoredJournalImpl implements MonitoredJournal {

        @Override
        public JournalMonitorInfo collectJournalMonitorInfo() {
            JournalMonitorInfo journalMonitorInfo = new JournalMonitorInfo();
                journalMonitorInfo.setMinIndex(journal.minIndex());
                journalMonitorInfo.setMaxIndex(journal.maxIndex());
                journalMonitorInfo.setFlushIndex(journal.flushedIndex());
                journalMonitorInfo.setCommitIndex(journal.commitIndex());
                JournalPersistence journalPersistence = journal.getJournalPersistence();
                journalMonitorInfo.setMinOffset(journalPersistence.min());
                journalMonitorInfo.setMaxOffset(journalPersistence.max());
                journalMonitorInfo.setFlushOffset(journalPersistence.flushed());
                JournalPersistence indexPersistence = journal.getIndexPersistence();
                journalMonitorInfo.setIndexMinOffset(indexPersistence.min());
                journalMonitorInfo.setIndexMaxOffset(indexPersistence.max());
                journalMonitorInfo.setIndexFlushOffset(indexPersistence.flushed());

                Map<Integer, JournalPersistence> partitionMap = journal.getPartitionMap();
                if (null != partitionMap) {
                    List<JournalPartitionMonitorInfo> partitionMonitorInfoList = new ArrayList<>(partitionMap.size());
                    partitionMap
                            .entrySet().stream()
                            .filter(entry -> entry.getKey() < RESERVED_PARTITIONS_START)
                            .forEach(entry -> {
                                int partition = entry.getKey();
                                JournalPersistence persistence = entry.getValue();
                                JournalPartitionMonitorInfo partitionMonitorInfo = new JournalPartitionMonitorInfo();
                                partitionMonitorInfo.setPartition(partition);
                                partitionMonitorInfo.setMinIndex(persistence.min() / INDEX_STORAGE_SIZE);
                                partitionMonitorInfo.setMaxIndex(persistence.max() / INDEX_STORAGE_SIZE);
                                partitionMonitorInfo.setMinOffset(persistence.min());
                                partitionMonitorInfo.setMaxOffset(persistence.max());
                                partitionMonitorInfo.setFlushOffset(persistence.flushed());
                                partitionMonitorInfoList.add(partitionMonitorInfo);
                            });
                    journalMonitorInfo.setPartitions(partitionMonitorInfoList);
                }
            return journalMonitorInfo;
        }

        @Override
        public DiskMonitorInfo collectDistMonitorInfo() {
            JournalPersistence journalPersistence = journal.getJournalPersistence();
            DiskMonitorInfo diskMonitorInfo = new DiskMonitorInfo();
            if (journalPersistence instanceof MonitoredPersistence) {
                MonitoredPersistence monitoredPersistence = (MonitoredPersistence) journalPersistence;

                diskMonitorInfo.setPath(monitoredPersistence.getPath());
                diskMonitorInfo.setFree(monitoredPersistence.getFreeSpace());
                diskMonitorInfo.setTotal(monitoredPersistence.getTotalSpace());
            }
            return diskMonitorInfo;
        }

    }
    @ActorSubscriber
    private void onInternalEntryApply(InternalEntryType type, byte [] entry) {
        if (type == InternalEntryType.TYPE_SCALE_PARTITIONS) {
            scalePartitions(entry);
        }
    }


    private void scalePartitions( byte[] internalEntry) {
        ScalePartitionsEntry scalePartitionsEntry = InternalEntriesSerializeSupport.parse(internalEntry);
        Set<Integer> partitions = scalePartitionsEntry.getPartitions();
        try {
            Set<Integer> currentPartitions = journal.getPartitions();
            currentPartitions.removeIf(p -> p >= RESERVED_PARTITIONS_START);

            for (int partition : partitions) {
                if (!currentPartitions.contains(partition)) {
                    journal.addPartition(partition);
                }
            }

            List<Integer> toBeRemoved = new ArrayList<>();
            for (Integer partition : currentPartitions) {
                if (!partitions.contains(partition)) {
                    toBeRemoved.add(partition);
                }
            }
            for (Integer partition : toBeRemoved) {
                journal.removePartition(partition);
            }

            logger.info("Journal repartitioned, partitions: {}, path: {}.",
                    journal.getPartitions(), config.get("working_dir"));
        } catch (IOException e) {
            throw new JournalException(e);
        }
    }
}
