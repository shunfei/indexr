package io.indexr.segment.pack;

import java.util.concurrent.Callable;

import io.indexr.segment.PackExtIndex;

public interface ExtIndexMemCache extends MemCache {
    int packCount();

    void putPack(long segmentId, int columnId, int packId, PackExtIndex extIndex);

    void removePack(long segmentId, int columnId, int packId);

    PackExtIndex getPack(long segmentId, int columnId, int packId);

    PackExtIndex getPack(long segmentId, int columnId, int packId, Callable<PackExtIndex> packLoader);
}
