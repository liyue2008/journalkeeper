package io.journalkeeper.core.monitor;

import io.journalkeeper.monitor.DiskMonitorInfo;
import io.journalkeeper.monitor.JournalMonitorInfo;

public interface MonitoredJournal {
    JournalMonitorInfo collectJournalMonitorInfo();
    DiskMonitorInfo collectDistMonitorInfo();
}
