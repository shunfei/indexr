package io.indexr.segment.pack;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalCause;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.cache.Weigher;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

import io.indexr.data.Freeable;
import io.indexr.data.Sizable;

/**
 * We assume this cache is the only long standing reference in the system.
 * We will manually clean up the underlying memory after an item is expired, so an item taken from
 * this cache must have done with its processing before expired in this cache.
 * 
 * This assumption should not be a big problem since an item from this cache should only be used by
 * query process and scanning a pack should be fast enough. But in extremely case when a released item
 * is still accessed, it could crash the process right away!
 */
public abstract class ExpiredMemCache implements MemCache {
    private static final Logger logger = LoggerFactory.getLogger(ExpiredMemCache.class);
    private static final long minExpireMS = TimeUnit.MINUTES.toMillis(5);

    protected final Cache<Long, ItemWrapper> cache;
    protected final long expire;
    protected final long capacity;

    private int fullCount = 0;
    private long totalFullCount = 0;
    private long lastFullTime = System.currentTimeMillis();

    private int dirtyCount = 0;
    private long lastGCTime = 0;

    public ExpiredMemCache(String name, long expireMS, long maxCap) {
        this(name, expireMS, maxCap, 500, TimeUnit.SECONDS.toMillis(30), 300, TimeUnit.MINUTES.toMillis(5));
    }

    public ExpiredMemCache(String name, long expireMS, long maxCap, int fullCountThreshold, long frequencyThresholdMS, int dirtyThreshold, long minGCPeriodMS) {
        if (expireMS < minExpireMS) {
            expireMS = minExpireMS;
        }
        this.expire = expireMS;
        this.capacity = maxCap;

        long finalExpireMS = expireMS;
        cache = CacheBuilder.newBuilder()
                .initialCapacity(2048)
                .expireAfterAccess(finalExpireMS, TimeUnit.MILLISECONDS)
                .weigher(new Weigher<Long, ItemWrapper>() {
                    @Override
                    public int weigh(Long key, ItemWrapper value) {
                        return (int) ((Sizable) value.item).size();
                    }
                })
                .maximumWeight(maxCap)
                .removalListener(new RemovalListener<Long, ItemWrapper>() {
                    @Override
                    public void onRemoval(RemovalNotification<Long, ItemWrapper> notification) {
                        ItemWrapper iw = notification.getValue();
                        if (iw == null) {
                            return;
                        }

                        RemovalCause cause = notification.getCause();
                        assert cause == RemovalCause.SIZE || cause == RemovalCause.EXPIRED || cause == RemovalCause.EXPLICIT
                                : String.format("Cache remove cause is %s", cause.name());

                        if (cause == RemovalCause.SIZE) {
                            if ((totalFullCount & 0xFFL) == 0) {
                                System.out.printf("%s cache got full the %d times.\n", name, totalFullCount);
                            }
                            totalFullCount++;
                            if (iw.lastGetMS + finalExpireMS <= System.currentTimeMillis()) {
                                // Already long enough, free it.
                                ((Freeable) iw.item).free();
                            } else {
                                dirtyCount++;
                            }

                            fullCount++;
                            if (fullCount >= fullCountThreshold) {
                                if (lastFullTime + frequencyThresholdMS >= System.currentTimeMillis()) {
                                    logger.warn("Mem cache get full frequently, over {} times in {} seconds", fullCountThreshold, frequencyThresholdMS / 1000);

                                    if (dirtyCount >= dirtyThreshold && lastGCTime + minGCPeriodMS <= System.currentTimeMillis()) {
                                        // Try to free those dead ByteBuffers.
                                        System.gc();
                                        lastGCTime = System.currentTimeMillis();
                                    }
                                }

                                fullCount = 0;
                                dirtyCount = 0;
                                lastFullTime = System.currentTimeMillis();
                            }
                        } else {
                            ((Freeable) iw.item).free();
                        }
                    }
                })
                .build();
    }

    @Override
    public long expire() {
        return expire;
    }

    @Override
    public long capacity() {
        return capacity;
    }

    @Override
    public void close() {
        cache.invalidateAll();
    }

    protected static class ItemWrapper {
        Object item;
        long lastGetMS;

        public ItemWrapper(Object item) {
            this.item = item;
            lastGetMS = System.currentTimeMillis();
        }

        <T> T get() {
            lastGetMS = System.currentTimeMillis();
            return (T) item;
        }
    }
}
