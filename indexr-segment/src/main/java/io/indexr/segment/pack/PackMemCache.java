package io.indexr.segment.pack;

import java.util.concurrent.Callable;

public interface PackMemCache extends MemCache {
    int packCount();

    void putPack(long segmentId, int columnId, int packId, DataPack pack);

    void removePack(long segmentId, int columnId, int packId);

    DataPack getPack(long segmentId, int columnId, int packId);

    DataPack getPack(long segmentId, int columnId, int packId, Callable<DataPack> packLoader);

}
