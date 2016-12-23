package io.indexr.segment.pack;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import io.indexr.segment.RSIndex;

public class IndexExpiredMemCache extends ExpiredMemCache implements IndexMemCache {
    private static final Logger logger = LoggerFactory.getLogger(IndexExpiredMemCache.class);

    public IndexExpiredMemCache(long expireMS, long maxCap) {
        this(expireMS, maxCap, 500, TimeUnit.SECONDS.toMillis(20), 300, TimeUnit.MINUTES.toMillis(5));
    }

    public IndexExpiredMemCache(long expireMS,
                                long maxCap,
                                int fullCountThreshold,
                                long frequencyThresholdMS,
                                int dirtyThreshold,
                                long minGCPeriodMS) {
        super("index", expireMS, maxCap, fullCountThreshold, frequencyThresholdMS, dirtyThreshold, minGCPeriodMS);
        logger.info("Expire = {} minutes", expireMS / 1000 / 60);
        logger.info("Capacity = {} MB", maxCap / 1024 / 1024);
    }

    @Override
    public int indexCount() {
        return (int) this.cache.size();
    }

    @Override
    public void putIndex(long segmentId, int columnId, RSIndex index) {
        cache.put(MemCache.genKey(segmentId, columnId, 0), new ItemWrapper(index));
    }

    @Override
    public void removeIndex(long segmentId, int columnId) {
        cache.invalidate(MemCache.genKey(segmentId, columnId, 0));
    }

    @Override
    public RSIndex getIndex(long segmentId, int columnId) {
        ItemWrapper iw;
        return (iw = cache.getIfPresent(MemCache.genKey(segmentId, columnId, 0))) == null ? null : iw.get();
    }

    @Override
    public RSIndex getIndex(long segmentId, int columnId, Callable<? extends RSIndex> indexLoader) {
        try {
            ItemWrapper iw = cache.get(MemCache.genKey(segmentId, columnId, 0), () -> new ItemWrapper(indexLoader.call()));
            return iw == null ? null : iw.get();
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }
}
