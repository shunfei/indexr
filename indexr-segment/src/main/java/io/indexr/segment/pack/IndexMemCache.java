package io.indexr.segment.pack;

import java.util.concurrent.Callable;

import io.indexr.segment.RSIndex;

public interface IndexMemCache extends MemCache {
    int indexCount();

    void putIndex(long segmentId, int columnId, RSIndex index);

    void removeIndex(long segmentId, int columnId);

    RSIndex getIndex(long segmentId, int columnId);

    RSIndex getIndex(long segmentId, int columnId, Callable<? extends RSIndex> valueLoader);
}
