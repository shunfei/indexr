package io.indexr.segment.cache;

import io.indexr.segment.pack.DataPackNode;

public interface DpnCache {

    DataPackNode[] get(int columnId);

    void put(int columnId, DataPackNode[] dpns);
}
