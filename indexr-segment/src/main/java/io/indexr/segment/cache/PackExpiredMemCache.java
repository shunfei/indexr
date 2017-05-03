package io.indexr.segment.cache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class PackExpiredMemCache extends ExpiredMemCache implements PackMemCache {
    private static final Logger logger = LoggerFactory.getLogger(PackExpiredMemCache.class);

    public PackExpiredMemCache(long expireMS, long maxCap) {
        this(expireMS, maxCap, 2000, TimeUnit.SECONDS.toMillis(20), 1000, TimeUnit.MINUTES.toMillis(15));
    }

    public PackExpiredMemCache(long expireMS, long maxCap, int fullCountThreshold, long frequencyThresholdMS, int dirtyThreshold, long minGCPeriodMS) {
        super("pack", expireMS, maxCap, fullCountThreshold, frequencyThresholdMS, dirtyThreshold, minGCPeriodMS);
        logger.info("Expire = {} minutes", expireMS / 1000 / 60);
        logger.info("Capacity = {} MB", maxCap / 1024 / 1024);
    }

    @Override
    public int packCount() {
        return (int) cache.size();
    }

    @Override
    public void putPack(long segmentId, int columnId, int packId, CachedByteSlice pack) {
        cache.put(MemCache.genKey(segmentId, columnId, packId), new ItemWrapper(pack));
    }

    @Override
    public void removePack(long segmentId, int columnId, int packId) {
        cache.invalidate(MemCache.genKey(segmentId, columnId, packId));
    }

    @Override
    public CachedByteSlice getPack(long segmentId, int columnId, int packId) {
        ItemWrapper iw;
        return (iw = cache.getIfPresent(MemCache.genKey(segmentId, columnId, packId))) == null ? null : iw.get();
    }

    @Override
    public CachedByteSlice getPack(long segmentId, int columnId, int packId, Callable<CachedByteSlice> loader) {
        try {
            ItemWrapper iw = cache.get(MemCache.genKey(segmentId, columnId, packId), () -> new ItemWrapper(loader.call()));
            return iw == null ? null : iw.get();
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }
}
