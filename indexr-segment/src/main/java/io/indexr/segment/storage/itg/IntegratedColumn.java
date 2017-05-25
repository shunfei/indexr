package io.indexr.segment.storage.itg;

import java.io.IOException;

import io.indexr.io.ByteBufferReader;
import io.indexr.io.ByteSlice;
import io.indexr.segment.OuterIndex;
import io.indexr.segment.PackExtIndex;
import io.indexr.segment.RSIndex;
import io.indexr.segment.SQLType;
import io.indexr.segment.SegmentMode;
import io.indexr.segment.cache.CachedByteSlice;
import io.indexr.segment.cache.DpnCache;
import io.indexr.segment.cache.ExtIndexMemCache;
import io.indexr.segment.cache.IndexMemCache;
import io.indexr.segment.cache.PackMemCache;
import io.indexr.segment.pack.DataPack;
import io.indexr.segment.pack.DataPackNode;
import io.indexr.segment.storage.StorageColumn;

public class IntegratedColumn extends StorageColumn {
    private ByteBufferReader.Opener dataSource;
    private final long dpnBase;
    private final long indexBase;
    private final long extIndexBase;
    private final long outerIndexBase;
    private final long outerIndexSize;
    private final long packBase;

    private final long segmentId;
    private DpnCache dpnCache;
    private IndexMemCache indexMemCache;
    private ExtIndexMemCache extIndexMemCache;
    private PackMemCache packMemCache;

    IntegratedColumn(int version,
                     SegmentMode mode,
                     long segmentId,
                     int columnId,
                     String name,
                     SQLType sqlType,
                     boolean isIndexed,
                     long rowCount,
                     ByteBufferReader.Opener dataSource,
                     long dpnBase,
                     long indexBase,
                     long extIndexBase,
                     long outerIndexBase,
                     long outerIndexSize,
                     long packBase,
                     DpnCache dpnCache,
                     IndexMemCache indexMemCache,
                     ExtIndexMemCache extIndexMemCache,
                     PackMemCache packMemCache) {
        super(version, mode, columnId, name, sqlType, isIndexed, rowCount);
        this.dataSource = dataSource;
        this.dpnBase = dpnBase;
        this.indexBase = indexBase;
        this.extIndexBase = extIndexBase;
        this.outerIndexBase = outerIndexBase;
        this.outerIndexSize = outerIndexSize;
        this.packBase = packBase;
        this.segmentId = segmentId;
        this.dpnCache = dpnCache;
        this.indexMemCache = indexMemCache;
        this.extIndexMemCache = extIndexMemCache;
        this.packMemCache = packMemCache;
    }

    @Override
    protected StorageColumn copy(long segmentId, int columnId, String name) throws IOException {
        IntegratedColumn column = new IntegratedColumn(
                this.version,
                this.mode,
                segmentId,
                columnId,
                name,
                this.sqlType,
                this.isIndexed,
                this.rowCount(),
                this.dataSource,
                this.dpnBase,
                this.indexBase,
                this.extIndexBase,
                this.outerIndexBase,
                this.outerIndexSize,
                this.packBase,
                null, // dpnCache is passed by old segment instance, we should not use it.
                this.indexMemCache,
                this.extIndexMemCache,
                this.packMemCache);
        // dpn will not change.
        column._dpns = this._dpns;

        return column;
    }

    public long outerIndexSize() {
        return outerIndexSize;
    }

    @Override
    protected ByteBufferReader openDPNReader() throws IOException {
        return dataSource.open(dpnBase);
    }

    @Override
    protected ByteBufferReader openIndexReader() throws IOException {
        return dataSource.open(indexBase);
    }

    @Override
    protected ByteBufferReader openExtIndexReader() throws IOException {
        return dataSource.open(extIndexBase);
    }

    @Override
    protected ByteBufferReader openPackReader(DataPackNode dpn) throws IOException {
        return dataSource.open(packBase);
    }

    @Override
    public DataPackNode[] getDPNs() throws IOException {
        if (dpnCache == null) {
            return super.getDPNs();
        } else {
            DataPackNode[] dpns;
            if ((dpns = dpnCache.get(columnId)) == null) {
                dpns = loadDPNs();
                dpnCache.put(columnId, dpns);
            }
            return dpns;
        }
    }

    @Override
    public RSIndex rsIndex() throws IOException {
        if (indexMemCache == null) {
            return super.rsIndex();
        } else {
            return indexMemCache.getIndex(segmentId, columnId, this::loadIndex);
        }
    }

    @Override
    public PackExtIndex extIndex(int packId) throws IOException {
        DataPackNode dpn = dpn(packId);
        if (extIndexMemCache == null || dpn.extIndexSize() == 0) {
            return super.extIndex(packId);
        } else {
            ByteSlice.Supplier data = () -> {
                CachedByteSlice cached = extIndexMemCache.getPack(segmentId, columnId, packId, () -> new CachedByteSlice(loadExtIndexData(dpn)));
                return cached.data().unfreeable();
            };
            return mode.versionAdapter.createExtIndex(version, mode, dataType(), isIndexed, dpn, data);
        }
    }

    @Override
    public OuterIndex outerIndex() throws IOException {
        return mode.versionAdapter.loadOuterIndex(version, mode, dataType(), dataSource.open(outerIndexBase), outerIndexSize);
    }

    @Override
    public DataPack pack(int packId) throws IOException {
        if (packMemCache == null) {
            return super.pack(packId);
        } else {
            DataPackNode dpn = dpn(packId);
            CachedByteSlice cached = packMemCache.getPack(segmentId, columnId, packId, () -> new CachedByteSlice(loadPack(dpn)));
            ByteSlice cmpData = cached.data().unfreeable();
            DataPack dataPack = new DataPack(null, cmpData, dpn);
            dataPack.decompress(dataType(), dpn);
            return dataPack;
        }
    }

    @Override
    public void free() {
        super.free();
        if (dataSource == null) {
            return;
        }
        dataSource = null;
        dpnCache = null;
        indexMemCache = null;
        extIndexMemCache = null;
        packMemCache = null;
    }
}
