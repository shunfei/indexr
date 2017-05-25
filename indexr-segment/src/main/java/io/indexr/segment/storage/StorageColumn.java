package io.indexr.segment.storage;

import java.io.IOException;

import io.indexr.data.Freeable;
import io.indexr.io.ByteBufferReader;
import io.indexr.io.ByteSlice;
import io.indexr.segment.Column;
import io.indexr.segment.PackDurationStat;
import io.indexr.segment.PackExtIndex;
import io.indexr.segment.RSIndex;
import io.indexr.segment.SQLType;
import io.indexr.segment.SegmentMode;
import io.indexr.segment.pack.DataPack;
import io.indexr.segment.pack.DataPackNode;

public abstract class StorageColumn implements Column, Freeable {
    protected final int version;
    protected final SegmentMode mode;
    protected final int columnId;
    protected final String name;
    protected final SQLType sqlType;
    protected final boolean isIndexed; // Only meaningful in vlt mode.

    protected int packCount;

    // Cache for dpns.
    protected DataPackNode[] _dpns;

    protected StorageColumn(int version,
                            SegmentMode mode,
                            int columnId,
                            String name,
                            SQLType sqlType,
                            boolean isIndexed,
                            long rowCount) {
        this.version = version;
        this.mode = mode;
        this.columnId = columnId;
        this.name = name;
        this.sqlType = sqlType;
        this.isIndexed = isIndexed;

        this.packCount = DataPack.rowCountToPackCount(rowCount);
    }

    protected DataPackNode[] loadDPNs() throws IOException {
        long time = System.currentTimeMillis();
        DataPackNode[] dpns = new DataPackNode[packCount];
        if (packCount == 0) {
            return dpns;
        }
        int dpnSize = mode.versionAdapter.dpnSize(version, mode);
        try (ByteBufferReader reader = openDPNReader()) {
            ByteSlice buffer = ByteSlice.allocateDirect(dpnSize);
            for (int id = 0; id < packCount; id++) {
                reader.read(id * dpnSize, buffer.byteBuffer());
                buffer.byteBuffer().clear();
                DataPackNode dpn = mode.versionAdapter.createDPN(version, mode, buffer);
                dpns[id] = dpn;
            }
            buffer.free();
        }
        PackDurationStat.INSTANCE.add_loadDPN(System.currentTimeMillis() - time);
        return dpns;
    }

    // The return pack is in compressed status.
    public ByteSlice loadPack(DataPackNode dpn) throws IOException {
        long time = System.currentTimeMillis();
        ByteSlice buffer;
        try (ByteBufferReader reader = openPackReader(dpn)) {
            buffer = ByteSlice.allocateDirect(dpn.packSize());
            reader.read(dpn.packAddr(), buffer.byteBuffer());
            // call clear() to get rid of ByteBuffer's pos, limit stuff.
            buffer.byteBuffer().clear();
        }
        PackDurationStat.INSTANCE.add_loadPack(System.currentTimeMillis() - time);
        return buffer;
    }

    public RSIndex loadIndex() throws IOException {
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
            reader.read(0, buffer.byteBuffer());
            buffer.byteBuffer().clear();

            index = mode.versionAdapter.createRSIndex(version, mode, sqlType.dataType, buffer, packCount);
        }
        PackDurationStat.INSTANCE.add_loadIndex(System.currentTimeMillis() - time);
        return index;
    }

    public PackExtIndex loadExtIndex(DataPackNode dpn) throws IOException {
        return mode.versionAdapter.createExtIndex(version, mode, dataType(), isIndexed, dpn, () -> loadExtIndexData(dpn));
    }

    public ByteSlice loadExtIndexData(DataPackNode dpn) throws IOException {
        if (dpn.extIndexSize() == 0) {
            return ByteSlice.empty();
        }
        long time = System.currentTimeMillis();
        ByteSlice buffer;
        try (ByteBufferReader reader = openExtIndexReader()) {
            buffer = ByteSlice.allocateDirect(dpn.extIndexSize());
            reader.read(dpn.extIndexAddr(), buffer.byteBuffer());
            buffer.byteBuffer().clear();
        }
        PackDurationStat.INSTANCE.add_loadIndex(System.currentTimeMillis() - time);
        return buffer;
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
    public boolean isIndexed() {
        return isIndexed;
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

    public DataPackNode[] getDPNs() throws IOException {
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
        return loadExtIndex(dpn(packId));
    }

    @Override
    public DataPack pack(int packId) throws IOException {
        DataPackNode dpn = dpn(packId);
        DataPack pack = new DataPack(null, loadPack(dpn), dpn);
        pack.decompress(dataType(), dpn);
        return pack;
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

    protected abstract ByteBufferReader openDPNReader() throws IOException;

    protected abstract ByteBufferReader openIndexReader() throws IOException;

    protected abstract ByteBufferReader openExtIndexReader() throws IOException;

    protected abstract ByteBufferReader openPackReader(DataPackNode dpn) throws IOException;

    protected abstract StorageColumn copy(long segmentId, int columnId, String name) throws IOException;
}
