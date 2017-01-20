package io.indexr.segment.pack;

import com.google.common.collect.Lists;

import java.io.IOException;

import io.indexr.data.Freeable;
import io.indexr.io.ByteBufferReader;
import io.indexr.io.ByteSlice;
import io.indexr.segment.Column;
import io.indexr.segment.ColumnType;
import io.indexr.segment.RSIndex;

public abstract class StorageColumn implements Column, Freeable {
    final int version;
    final int columnId;
    final String name;
    final byte dataType;

    int packCount;
    // Local cache while outer cache not work.
    //WeakReference<RSIndex> indexWeakRef;
    //WeakReference<DataPack>[] packWeakRef;

    // Cache for dpns.
    DataPackNode[] _dpns;

    StorageColumn(int version,
                  int columnId,
                  String name,
                  byte dataType,
                  long rowCount) {
        this.version = version;
        this.columnId = columnId;
        this.name = name;
        this.dataType = dataType;

        this.packCount = DataPack.rowCountToPackCount(rowCount);
    }


    DataPackNode[] loadDPNs() throws IOException {
        long time = System.currentTimeMillis();
        DataPackNode[] dpns = new DataPackNode[packCount];
        if (packCount == 0) {
            return dpns;
        }
        try (ByteBufferReader reader = openDPNReader()) {
            ByteSlice buffer = ByteSlice.allocateDirect(DataPackNode.SERIALIZED_SIZE);
            for (int id = 0; id < packCount; id++) {
                reader.read(id * DataPackNode.SERIALIZED_SIZE, buffer.byteBuffer(), DataPackNode.SERIALIZED_SIZE);
                buffer.byteBuffer().clear();
                DataPackNode dpn = DataPackNode.from(version, buffer);
                dpns[id] = dpn;
            }
            buffer.free();
        }
        PackDurationStat.INSTANCE.add_loadDPN(System.currentTimeMillis() - time);
        return dpns;
    }

    // The return pack is in compressed status.
    DataPack loadPack(DataPackNode dpn) throws IOException {
        long time = System.currentTimeMillis();
        DataPack pack;
        try (ByteBufferReader reader = openPackReader(dpn)) {
            ByteSlice buffer = ByteSlice.allocateDirect(dpn.packSize());
            reader.read(dpn.packAddr(), buffer.byteBuffer(), dpn.packSize());
            // call clear() to get rid of ByteBuffer's pos, limit stuff.
            buffer.byteBuffer().clear();
            pack = DataPack.from(buffer, dpn);
        }
        PackDurationStat.INSTANCE.add_loadPack(System.currentTimeMillis() - time);
        return pack;
    }

    RSIndex loadIndex() throws IOException {
        long time = System.currentTimeMillis();
        RSIndex index;
        try (ByteBufferReader reader = openIndexReader()) {
            DataPackNode[] dpns = getDPNs();
            if (dpns.length == 0) {
                return null;
            }
            DataPackNode lastDPN = dpns[dpns.length - 1];
            int bufferSize = (int) (lastDPN.indexAddr() + lastDPN.indexSize());
            assert bufferSize == Lists.newArrayList(dpns).stream().map(DataPackNode::indexSize).reduce(0, Integer::sum);

            ByteSlice buffer = ByteSlice.allocateDirect(bufferSize);
            reader.read(0, buffer.byteBuffer(), bufferSize);
            buffer.byteBuffer().clear();
            switch (dataType) {
                case ColumnType.INT:
                case ColumnType.LONG:
                    index = new RSIndex_Histogram(buffer, packCount, false);
                    break;
                case ColumnType.FLOAT:
                case ColumnType.DOUBLE:
                    index = new RSIndex_Histogram(buffer, packCount, true);
                    break;
                case ColumnType.STRING:
                    switch (version) {
                        case Version.VERSION_0_ID:
                            index = new EmptyRSIndexStr();
                            break;
                        case Version.VERSION_1_ID:
                        case Version.VERSION_2_ID:
                            index = new RSIndex_CMap(buffer, packCount);
                            break;
                        default:
                            index = new RSIndex_CMap_V2(buffer, packCount);
                            break;
                    }
                    break;
                default:
                    throw new IllegalArgumentException(String.format("Not support data type of %s", dataType));
            }
        }
        PackDurationStat.INSTANCE.add_loadIndex(System.currentTimeMillis() - time);
        return index;
    }

    DPRowSpliterator.DPLoader getDPLoader() {
        return this::pack;
    }

    // ========================================
    // Column methods implementaion.
    // ========================================

    @Override
    public String name() {
        return name;
    }

    @Override
    public byte dataType() {
        return dataType;
    }

    @Override
    public int packCount() {
        return packCount;
    }

    @Override
    public long rowCount() throws IOException {
        if (packCount == 0) {
            return 0;
        } else {
            DataPackNode[] dpns = getDPNs();
            return dpns[dpns.length - 1].objCount() + ((packCount - 1) << DataPack.SHIFT);
        }
    }

    @Override
    public DataPackNode dpn(int packId) throws IOException {
        return getDPNs()[packId];
    }

    DataPackNode[] getDPNs() throws IOException {
        if (_dpns != null) {
            return _dpns;
        }
        return _dpns = loadDPNs();
    }

    @Override
    public RSIndex rsIndex() throws IOException {
        return loadIndex();
    }

    @Override
    public DataPack pack(int packId) throws IOException {
        DataPack rtItem;
        DataPackNode dpn = dpn(packId);
        rtItem = loadPack(dpn);
        rtItem.decompress(dpn);
        return rtItem;
    }

    //@Override
    //public RSIndex rsIndex() throws IOException {
    //    RSIndex rtItem;
    //    if (indexWeakRef != null && (rtItem = indexWeakRef.get()) != null) {
    //        return rtItem;
    //    }
    //
    //    rtItem = loadIndex();
    //
    //    // Those code risk multi-thread issue, an item put in cache may be ignored by another thread.
    //    // But it is not a very big deal, as it can finally freed by GC.
    //    RSIndex cacheItem;
    //    if (indexWeakRef != null && (cacheItem = indexWeakRef.get()) != null) {
    //        if (cacheItem == rtItem) {
    //            return rtItem;
    //        } else {
    //            rtItem.free();
    //            return cacheItem;
    //        }
    //    } else {
    //        indexWeakRef = new WeakReference<RSIndex>(rtItem);
    //        return rtItem;
    //    }
    //}
    //
    //@Override
    //public DataPack pack(int packId) throws IOException {
    //    DataPack rtItem;
    //    if (packWeakRef == null) {
    //        packWeakRef = new WeakReference[packCount];
    //    }
    //    if (packWeakRef[packId] != null && (rtItem = packWeakRef[packId].get()) != null) {
    //        return rtItem;
    //    }
    //
    //    DataPackNode dpn = dpn(packId);
    //    rtItem = loadPack(dpn);
    //    rtItem.decompress(dpn);
    //
    //    // Those code risk multi-thread issue, an item put in cache may be ignored by another thread.
    //    // But it is not a very big deal, as it can finally freed by GC.
    //    DataPack cacheItem;
    //    if (packWeakRef[packId] != null && (cacheItem = packWeakRef[packId].get()) != null) {
    //        if (cacheItem == rtItem) {
    //            return rtItem;
    //        } else {
    //            rtItem.free();
    //            return cacheItem;
    //        }
    //    } else {
    //        packWeakRef[packId] = new WeakReference<DataPack>(rtItem);
    //        return rtItem;
    //    }
    //}

    @Override
    public void free() {
        freeLocalCache();
        _dpns = null;
    }

    void freeLocalCache() {
        //RSIndex index;
        //if (indexWeakRef != null && (index = indexWeakRef.get()) != null) {
        //    index.free();
        //}
        //indexWeakRef = null;
        //
        //if (packWeakRef != null) {
        //    WeakReference<DataPack>[] _packWeakRef = packWeakRef;
        //    packWeakRef = null;
        //    DataPack pack;
        //    for (WeakReference<DataPack> wr : _packWeakRef) {
        //        if (wr != null && (pack = wr.get()) != null) {
        //            pack.free();
        //        }
        //    }
        //}
    }

    // ========================================
    // abstract methods
    // ========================================

    abstract ByteBufferReader openDPNReader() throws IOException;

    abstract ByteBufferReader openIndexReader() throws IOException;

    abstract ByteBufferReader openPackReader(DataPackNode dpn) throws IOException;

    abstract StorageColumn copy(long segmentId, int columnId, String name) throws IOException;
}
