package io.indexr.segment.pack;

import java.io.IOException;

import io.indexr.data.Freeable;
import io.indexr.io.ByteBufferReader;
import io.indexr.io.ByteSlice;
import io.indexr.segment.Column;
import io.indexr.segment.ColumnType;
import io.indexr.segment.PackExtIndex;
import io.indexr.segment.RSIndex;
import io.indexr.segment.SQLType;

public abstract class StorageColumn implements Column, Freeable {
    final int version;
    final int columnId;
    final String name;
    final SQLType sqlType;

    int packCount;

    // Cache for dpns.
    DataPackNode[] _dpns;

    StorageColumn(int version,
                  int columnId,
                  String name,
                  SQLType sqlType,
                  long rowCount) {
        this.version = version;
        this.columnId = columnId;
        this.name = name;
        this.sqlType = sqlType;

        this.packCount = DataPack.rowCountToPackCount(rowCount);
    }


    DataPackNode[] loadDPNs() throws IOException {
        long time = System.currentTimeMillis();
        DataPackNode[] dpns = new DataPackNode[packCount];
        if (packCount == 0) {
            return dpns;
        }
        try (ByteBufferReader reader = openDPNReader()) {
            ByteSlice buffer = ByteSlice.allocateDirect(DataPackNode.serializedSize(version));
            for (int id = 0; id < packCount; id++) {
                reader.read(id * DataPackNode.serializedSize(version),
                        buffer.byteBuffer(),
                        DataPackNode.serializedSize(version));
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
            int bufferSize = 0;
            for (DataPackNode dpn : dpns) {
                bufferSize += dpn.indexSize();
            }

            ByteSlice buffer = ByteSlice.allocateDirect(bufferSize);
            reader.read(0, buffer.byteBuffer(), bufferSize);
            buffer.byteBuffer().clear();

            if (sqlType.dataType == ColumnType.STRING) {
                switch (version) {
                    case Version.VERSION_0_ID:
                        index = new RSIndex_Str_Invalid();
                        break;
                    case Version.VERSION_1_ID:
                    case Version.VERSION_2_ID:
                        index = new RSIndex_CMap(buffer, packCount);
                        break;
                    default:
                        index = new RSIndex_CMap_V2(buffer, packCount);
                        break;
                }
            } else {
                if (version < Version.VERSION_6_ID) {
                    index = new RSIndex_Histogram(buffer, packCount, ColumnType.isFloatPoint(sqlType.dataType));
                } else {
                    index = new RSIndex_Histogram_V2(buffer, packCount, ColumnType.isFloatPoint(sqlType.dataType));
                }
            }

        }
        PackDurationStat.INSTANCE.add_loadIndex(System.currentTimeMillis() - time);
        return index;
    }

    // The return pack is in compressed status.
    PackExtIndex loadExtIndex(DataPackNode dpn) throws IOException {
        if (dpn.extIndexSize() == 0) {
            return new PackExtIndex_Unused();
        }
        long time = System.currentTimeMillis();
        PackExtIndex extIndex;
        try (ByteBufferReader reader = openExtIndexReader()) {
            ByteSlice buffer = ByteSlice.allocateDirect(dpn.extIndexSize());
            reader.read(dpn.extIndexAddr(), buffer.byteBuffer(), dpn.extIndexSize());
            buffer.byteBuffer().clear();
            switch (sqlType.dataType) {
                case ColumnType.INT:
                case ColumnType.LONG:
                case ColumnType.FLOAT:
                case ColumnType.DOUBLE:
                    extIndex = new PackExtIndex_Unused();
                    break;
                case ColumnType.STRING:
                    if (dpn.version() < Version.VERSION_6_ID) {
                        extIndex = new PackExtIndex_Unused();
                    } else {
                        extIndex = new PackExtIndex_Str_Hash(null, buffer, dpn.objCount());
                    }
                    break;
                default:
                    throw new IllegalArgumentException(String.format("Unsupported data type %s", sqlType));
            }
        }
        PackDurationStat.INSTANCE.add_loadIndex(System.currentTimeMillis() - time);
        return extIndex;
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
    public SQLType sqlType() {
        return sqlType;
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
    public PackExtIndex extIndex(int packId) throws IOException {
        PackExtIndex extIndex;
        DataPackNode dpn = dpn(packId);
        extIndex = loadExtIndex(dpn);
        extIndex.decompress(dpn);
        return extIndex;
    }

    @Override
    public DataPack pack(int packId) throws IOException {
        DataPack rtItem;
        DataPackNode dpn = dpn(packId);
        rtItem = loadPack(dpn);
        rtItem.decompress(dpn);
        return rtItem;
    }

    @Override
    public void free() {
        freeLocalCache();
        _dpns = null;
    }

    void freeLocalCache() {}

    // ========================================
    // abstract methods
    // ========================================

    abstract ByteBufferReader openDPNReader() throws IOException;

    abstract ByteBufferReader openIndexReader() throws IOException;

    abstract ByteBufferReader openExtIndexReader() throws IOException;

    abstract ByteBufferReader openPackReader(DataPackNode dpn) throws IOException;

    abstract StorageColumn copy(long segmentId, int columnId, String name) throws IOException;
}
