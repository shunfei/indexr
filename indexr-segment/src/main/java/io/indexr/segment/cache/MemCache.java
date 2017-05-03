package io.indexr.segment.cache;

import java.util.concurrent.atomic.AtomicLong;

import io.indexr.segment.storage.StorageSegment;

/**
 * A pack pool to reduce pressure on loading IO and decompress CPU.
 */
public interface MemCache {
    // Max integral value 32 bit number can hold.
    public static long MAX_SEGMENT_ID = 0xFFFF_FFFFL;
    public static long SEMGNET_ID_MASK = MAX_SEGMENT_ID;
    public static AtomicLong nextSegmentId = new AtomicLong(0);

    /**
     * The id for IntegratedSegment, unique in local JVM.
     * Only used for cache.
     */
    public static long nextSegmentId() {
        long mine = nextSegmentId.incrementAndGet();
        if (mine > MAX_SEGMENT_ID) {
            throw new Error("Run out of segment id, knock the guy who wrote this code and reboot process.");
        }
        return mine;
    }

    /**
     * The expire time of this cache, in milliseconds.
     */
    long expire();

    /**
     * The capacity of this cache, in bytes.
     */
    long capacity();

    void close();

    public static long genKey(long segmentId, int columnId, int packId) {
        assert segmentId <= MAX_SEGMENT_ID;
        assert columnId < StorageSegment.MAX_COLUMN_COUNT;
        assert packId < StorageSegment.MAX_PACK_COUNT;

        return (segmentId << 32) | ((columnId & StorageSegment.COLUMN_ID_MASK) << 16) | (packId & StorageSegment.PACK_ID_MASK);
    }
}
