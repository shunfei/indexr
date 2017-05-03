package io.indexr.segment.cache;

import java.util.concurrent.Callable;

public interface PackMemCache extends MemCache {
    int packCount();

    void putPack(long segmentId, int columnId, int packId, CachedByteSlice pack);

    void removePack(long segmentId, int columnId, int packId);

    CachedByteSlice getPack(long segmentId, int columnId, int packId);

    CachedByteSlice getPack(long segmentId, int columnId, int packId, Callable<CachedByteSlice> packLoader);

}
