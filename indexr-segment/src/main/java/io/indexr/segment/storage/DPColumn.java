package io.indexr.segment.storage;

import org.apache.commons.io.IOUtils;
import org.apache.spark.unsafe.types.UTF8String;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

import io.indexr.io.ByteBufferReader;
import io.indexr.io.ByteBufferWriter;
import io.indexr.io.ByteSlice;
import io.indexr.segment.ColumnType;
import io.indexr.segment.PackExtIndex;
import io.indexr.segment.PackRSIndex;
import io.indexr.segment.RSIndex;
import io.indexr.segment.SQLType;
import io.indexr.segment.SegmentMode;
import io.indexr.segment.pack.DataPack;
import io.indexr.segment.pack.DataPackNode;
import io.indexr.segment.pack.VirtualDataPack;

public class DPColumn extends StorageColumn {
    private static final Logger logger = LoggerFactory.getLogger(DPColumn.class);

    private final Path dpnFilePath;
    private final Path indexFilePath;
    private final Path extIndexFilePath;
    private final Path packFilePath;


    DPColumn(int version,
             SegmentMode mode,
             int columnId,
             String name,
             SQLType sqlType,
             boolean isIndexed,
             long rowCount,
             Path segmentPath) {
        this(version,
                mode,
                columnId,
                name,
                sqlType,
                isIndexed,
                rowCount,
                segmentPath.resolve(name + ".dpn"),
                segmentPath.resolve(name + ".index"),
                segmentPath.resolve(name + ".extindex"),
                segmentPath.resolve(name + ".pack"));
    }

    DPColumn(int version,
             SegmentMode mode,
             int columnId,
             String name,
             SQLType sqlType,
             boolean isIndexed,
             long rowCount,
             Path dpnFilePath,
             Path indexFilePath,
             Path extIndexFilePath,
             Path packFilePath) {
        super(version, mode, columnId, name, sqlType, isIndexed, rowCount);
        this.dpnFilePath = dpnFilePath;
        this.indexFilePath = indexFilePath;
        this.extIndexFilePath = extIndexFilePath;
        this.packFilePath = packFilePath;
    }

    @Override
    protected StorageColumn copy(long segmentId, int columnId, String name) throws IOException {
        DPColumn column = new DPColumn(
                version,
                mode,
                columnId,
                name,
                this.sqlType,
                this.isIndexed,
                this.rowCount(),
                this.dpnFilePath,
                this.indexFilePath,
                this.extIndexFilePath,
                this.packFilePath
        );
        column._dpns = this._dpns;
        return column;
    }

    @Override
    protected ByteBufferReader openDPNReader() throws IOException {
        FileChannel dpnFile = FileChannel.open(dpnFilePath, StandardOpenOption.READ);
        return ByteBufferReader.of(dpnFile, 0, dpnFile::close);
    }

    @Override
    protected ByteBufferReader openIndexReader() throws IOException {
        FileChannel indexFile = FileChannel.open(indexFilePath, StandardOpenOption.READ);
        return ByteBufferReader.of(indexFile, 0, indexFile::close);
    }

    @Override
    protected ByteBufferReader openExtIndexReader() throws IOException {
        FileChannel extIndexFile = FileChannel.open(extIndexFilePath, StandardOpenOption.READ);
        return ByteBufferReader.of(extIndexFile, 0, extIndexFile::close);
    }

    @Override
    protected ByteBufferReader openPackReader(DataPackNode dpn) throws IOException {
        FileChannel packFile = FileChannel.open(packFilePath, StandardOpenOption.READ);
        return ByteBufferReader.of(packFile, 0, packFile::close);
    }

    @Override
    public RSIndex rsIndex() throws IOException {
        if (update) {
            return null;
        }
        return super.rsIndex();
    }

    @Override
    public PackExtIndex extIndex(int packId) throws IOException {
        if (update) {
            return null;
        }
        return super.extIndex(packId);
    }

    @Override
    public DataPack pack(int packId) throws IOException {
        if (update) {
            return null;
        }
        return super.pack(packId);
    }

    @Override
    public void free() {
        super.free();
        freeUpdateResources();
    }

    // ------------------------------------------------------------------
    // Update stuff
    // ------------------------------------------------------------------

    private boolean update;

    private VirtualDataPack cache;

    private FileChannel packFileWrite;
    private FileChannel dpnFileWrite;
    private FileChannel indexFileWrite;
    private FileChannel extIndexFileWrite;

    private long packFileOffset;
    private long dpnFileOffset;
    private long indexFileOffset;
    private long extIndexFileOffset;

    void initUpdate() throws IOException {
        update = true;

        super.freeLocalCache();
        openWriteFiles();
        setUpCache();
    }

    private void openWriteFiles() {
        try {
            OpenOption[] writeOptions = new StandardOpenOption[]{
                    StandardOpenOption.WRITE,
                    StandardOpenOption.CREATE};

            packFileWrite = FileChannel.open(packFilePath, writeOptions);
            dpnFileWrite = FileChannel.open(dpnFilePath, writeOptions);
            indexFileWrite = FileChannel.open(indexFilePath, writeOptions);
            extIndexFileWrite = FileChannel.open(extIndexFilePath, writeOptions);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void setUpCache() throws IOException {
        if (packCount == 0) {
            // No pack at all, happy!
            packFileOffset = 0;
            dpnFileOffset = 0;
            indexFileOffset = 0;
            extIndexFileOffset = 0;
            cache = new VirtualDataPack(sqlType.dataType, null);
        } else {
            DataPackNode[] dpns = getDPNs();
            DataPackNode lastDPN = dpns[dpns.length - 1];
            int objCount = lastDPN.objCount();
            if (lastDPN.objCount() == DataPack.MAX_COUNT) {
                // Last pack is full, leave it alone.
                packFileOffset = lastDPN.packAddr() + lastDPN.packSize();
                dpnFileOffset = mode.versionAdapter.dpnSize(version, mode) * packCount;
                indexFileOffset = lastDPN.indexAddr() + lastDPN.indexSize();
                extIndexFileOffset = lastDPN.extIndexAddr() + lastDPN.extIndexSize();
                cache = new VirtualDataPack(sqlType.dataType, null);
            } else {
                // Last pack is not full, we need to append new data into it.

                packFileOffset = lastDPN.packAddr();
                dpnFileOffset = mode.versionAdapter.dpnSize(version, mode) * (packCount - 1);
                indexFileOffset = lastDPN.indexAddr();
                extIndexFileOffset = lastDPN.extIndexAddr();

                // Remove the data from last pack into cache.
                ByteSlice lastPackData = loadPack(lastDPN);
                DataPack lastPack = new DataPack(null, lastPackData, lastDPN);
                lastPack.decompress(dataType(), lastDPN);
                packCount--;

                cache = new VirtualDataPack(sqlType.dataType, lastPack);
                lastPack.free();
            }
        }
    }

    private void saveCache() throws IOException {
        PackBundle p = mode.versionAdapter.createPackBundle(version, mode, dataType(), isIndexed, cache);
        DataPack pack = p.dataPack;
        DataPackNode dpn = p.dpn;
        PackRSIndex index = p.rsIndex;
        PackExtIndex extIndex = p.extIndex;

        savePack(dpn, pack, index, extIndex);

        cache.clear();
        // Those instances will never used again, reclaim memory.
        index.free();
        extIndex.free();
        pack.free();
    }

    private void savePack(DataPackNode dpn, DataPack pack, PackRSIndex index, PackExtIndex extIndex) throws IOException {
        dpn.setPackSize(pack.serializedSize());
        dpn.setPackAddr(packFileOffset);

        dpn.setIndexSize(index.serializedSize());
        dpn.setIndexAddr(indexFileOffset);

        dpn.setExtIndexSize(extIndex.serializedSize());
        dpn.setExtIndexAddr(extIndexFileOffset);

        pack.write(ByteBufferWriter.of(packFileWrite, packFileOffset, null));
        packFileWrite.force(false);
        packFileOffset += dpn.packSize();

        index.write(ByteBufferWriter.of(indexFileWrite, indexFileOffset, null));
        indexFileWrite.force(false);
        indexFileOffset += dpn.indexSize();

        extIndex.write(ByteBufferWriter.of(extIndexFileWrite, extIndexFileOffset, null));
        extIndexFileWrite.force(false);
        extIndexFileOffset += dpn.extIndexSize();

        dpn.write(ByteBufferWriter.of(dpnFileWrite, dpnFileOffset, null));
        dpnFileWrite.force(false);
        dpnFileOffset += mode.versionAdapter.dpnSize(version, mode);

        packCount++;

        // Now disable the cache.
        _dpns = null;
    }

    public void add(int val) throws IOException {
        if (cache.add(val)) {
            saveCache();
        }
    }

    public void add(long val) throws IOException {
        if (cache.add(val)) {
            saveCache();
        }
    }

    public void add(float val) throws IOException {
        if (cache.add(val)) {
            saveCache();
        }
    }

    public void add(double val) throws IOException {
        if (cache.add(val)) {
            saveCache();
        }
    }

    public void add(UTF8String val) throws IOException {
        if (cache.add(val)) {
            saveCache();
        }
    }

    // ------------------------------------------------------------------
    // Close & clean
    // ------------------------------------------------------------------

    private void freeUpdateResources() {
        cache = null;

        IOUtils.closeQuietly(packFileWrite);
        packFileWrite = null;
        IOUtils.closeQuietly(dpnFileWrite);
        dpnFileWrite = null;
        IOUtils.closeQuietly(indexFileWrite);
        indexFileWrite = null;
        IOUtils.closeQuietly(extIndexFileWrite);
        extIndexFileWrite = null;
    }

    public void seal() throws IOException {
        if (!update) {
            return;
        }
        // save the remaining cache.
        if (cache != null && cache.valueCount() > 0) {
            saveCache();
        }
        freeUpdateResources();

        update = false;
    }

    // ------------------------------------------------------------------
    // Merge stuff
    // ------------------------------------------------------------------

    public void merge(List<StorageColumn> columns) throws IOException {
        if (!update) {
            initUpdate();
        }

        DataPackNode[] dpns = loadDPNs();
        assert dpns.length == packCount;
        VirtualDataPack insertPack = this.cache;

        List<DataPack> appendByRows = new ArrayList<>(columns.size() + 1);

        for (StorageColumn column : columns) {
            if (column instanceof DPColumn) {
                ((DPColumn) column).seal();
            }
            int colPackCount = column.packCount;
            if (colPackCount == 0) {
                continue;
            }
            DataPackNode[] colDPNs = column.getDPNs();

            // Here we driectly load packs and indexes from disk, not use cache.
            // As we are going to free them later.

            RSIndex rsIndex = column.loadIndex();
            for (int packId = 0; packId < colPackCount; packId++) {
                DataPackNode dpn = colDPNs[packId];
                PackRSIndex packIndex = rsIndex.packIndex(packId);
                PackExtIndex extIndex = column.loadExtIndex(dpn);
                DataPack pack = new DataPack(null, column.loadPack(dpn), dpn);
                if (dpn.version() == version
                        && dpn.objCount() != 0
                        && (dpn.objCount() & DataPack.MASK) == 0) {
                    // Copy full packs directly.
                    this.savePack(dpn.clone(), pack, packIndex, extIndex);
                    pack.free();
                } else {
                    // If pack is not full or the versions not match, then append values by rows.
                    pack.decompress(dataType(), dpn);
                    appendByRows.add(pack);
                }
            }
            rsIndex.free();
        }

        // Now append fragment packs' value.
        switch (sqlType.dataType) {
            case ColumnType.INT:
                for (DataPack pack : appendByRows) {
                    for (int i = 0; i < pack.valueCount(); i++) {
                        add(pack.intValueAt(i));
                    }
                    pack.free();
                }
                break;
            case ColumnType.LONG:
                for (DataPack pack : appendByRows) {
                    for (int i = 0; i < pack.valueCount(); i++) {
                        add(pack.longValueAt(i));
                    }
                    pack.free();
                }
                break;
            case ColumnType.FLOAT:
                for (DataPack pack : appendByRows) {
                    for (int i = 0; i < pack.valueCount(); i++) {
                        add(pack.floatValueAt(i));
                    }
                    pack.free();
                }
                break;
            case ColumnType.DOUBLE:
                for (DataPack pack : appendByRows) {
                    for (int i = 0; i < pack.valueCount(); i++) {
                        add(pack.doubleValueAt(i));
                    }
                    pack.free();
                }
                break;
            case ColumnType.STRING:
                for (DataPack pack : appendByRows) {
                    for (int i = 0; i < pack.valueCount(); i++) {
                        add(pack.stringValueAt(i).clone());
                    }
                    pack.free();
                }
                break;
            default:
                throw new IllegalStateException(String.format("Not support data type of %s", sqlType.dataType));
        }

        seal();
    }
}
