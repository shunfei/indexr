package io.indexr.segment.pack;

import java.io.IOException;

import io.indexr.io.ByteBufferReader;
import io.indexr.segment.PackExtIndex;
import io.indexr.segment.RSIndex;
import io.indexr.segment.SQLType;

public class IntegratedColumn extends StorageColumn {
    private ByteBufferReader.Opener dataSource;
    private final long dpnBase;
    private final long indexBase;
    private final long extIndexBase;
    private final long packBase;

    private final long segmentId;
    private DpnCache dpnCache;
    private IndexMemCache indexMemCache;
    private ExtIndexMemCache extIndexMemCache;
    private PackMemCache packMemCache;

    IntegratedColumn(int version,
                     long segmentId,
                     int columnId,
                     String name,
                     SQLType sqlType,
                     long rowCount,
                     ByteBufferReader.Opener dataSource,
                     long dpnBase,
                     long indexBase,
                     long extIndexBase,
                     long packBase,
                     DpnCache dpnCache,
                     IndexMemCache indexMemCache,
                     ExtIndexMemCache extIndexMemCache,
                     PackMemCache packMemCache) {
        super(version, columnId, name, sqlType, rowCount);
        this.dataSource = dataSource;
        this.dpnBase = dpnBase;
        this.indexBase = indexBase;
        this.extIndexBase = extIndexBase;
        this.packBase = packBase;
        this.segmentId = segmentId;
        this.dpnCache = dpnCache;
        this.indexMemCache = indexMemCache;
        this.extIndexMemCache = extIndexMemCache;
        this.packMemCache = packMemCache;
    }

    @Override
    StorageColumn copy(long segmentId, int columnId, String name) throws IOException {
        IntegratedColumn column = new IntegratedColumn(
                this.version,
                segmentId,
                columnId,
                name,
                this.sqlType,
                this.rowCount(),
                this.dataSource,
                this.dpnBase,
                this.indexBase,
                this.extIndexBase,
                this.packBase,
                null, // dpnCache is passed by old segment instance, we should not use it.
                this.indexMemCache,
                this.extIndexMemCache,
                this.packMemCache);
        // dpn will not change.
        column._dpns = this._dpns;

        return column;
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
    ByteBufferReader openExtIndexReader() throws IOException {
        return dataSource.open(extIndexBase);
    }

    @Override
    protected ByteBufferReader openPackReader(DataPackNode dpn) throws IOException {
        return dataSource.open(packBase);
    }

    @Override
    DataPackNode[] getDPNs() throws IOException {
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
        if (extIndexMemCache == null) {
            return super.extIndex(packId);
        } else {
            return extIndexMemCache.getPack(segmentId, columnId, packId, () -> {
                DataPackNode dpn = dpn(packId);
                PackExtIndex extIndex = loadExtIndex(dpn);
                extIndex.decompress(dpn);
                return extIndex;
            });
        }
    }

    @Override
    public DataPack pack(int packId) throws IOException {
        if (packMemCache == null) {
            return super.pack(packId);
        } else {
            return packMemCache.getPack(segmentId, columnId, packId, () -> {
                DataPackNode dpn = dpn(packId);
                DataPack pack = loadPack(dpn);
                pack.decompress(dpn);
                return pack;
            });
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
