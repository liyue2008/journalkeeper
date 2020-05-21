package io.journalkeeper.core.persistence.cache;

/**
 * @author LiYue
 * Date: 2020/3/5
 */
public interface PreloadCacheMetric {
    int getBufferSize();

    int getCoreCount();

    int getMaxCount();

    int getUsedCount();

    int getCachedCount();

}
