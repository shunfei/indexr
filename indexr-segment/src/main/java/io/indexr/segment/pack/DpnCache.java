package io.indexr.segment.pack;

public interface DpnCache {

    DataPackNode[] get(int columnId);

    void put(int columnId, DataPackNode[] dpns);
}
