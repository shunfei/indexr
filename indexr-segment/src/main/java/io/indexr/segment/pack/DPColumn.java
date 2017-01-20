package io.indexr.segment.pack;

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
import io.indexr.segment.ColumnType;
import io.indexr.segment.PackRSIndex;
import io.indexr.segment.RSIndex;
import io.indexr.util.Pair;

public class DPColumn extends StorageColumn {
    private static final Logger logger = LoggerFactory.getLogger(DPColumn.class);

    private final Path dpnFilePath;
    private final Path indexFilePath;
    private final Path packFilePath;

    DPColumn(int version,
             int columnId,
             String name,
             byte dataType,
             long rowCount,
             Path segmentPath) {
        this(version,
                columnId,
                name,
                dataType,
                rowCount,
                segmentPath.resolve(name + ".dpn"),
                segmentPath.resolve(name + ".index"),
                segmentPath.resolve(name + ".pack"));
    }

    DPColumn(int version,
             int columnId,
             String name,
             byte dataType,
             long rowCount,
             Path dpnFilePath,
             Path indexFilePath,
             Path packFilePath) {
        super(version, columnId, name, dataType, rowCount);
        this.dpnFilePath = dpnFilePath;
        this.indexFilePath = indexFilePath;
        this.packFilePath = packFilePath;
    }

    @Override
    StorageColumn copy(long segmentId, int columnId, String name) throws IOException {
        DPColumn column = new DPColumn(
                version,
                columnId,
                name,
                this.dataType,
                this.rowCount(),
                this.dpnFilePath,
                this.indexFilePath,
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
    private boolean compress = true;

    private VirtualDataPack cache;
    private PackRSIndex packIndex;

    private FileChannel packFileWrite;
    private FileChannel dpnFileWrite;
    private FileChannel indexFileWrite;

    private long packFileOffset;
    private long dpnFileOffset;
    private long indexFileOffset;

    void setCompress(boolean compress) {
        this.compress = compress;
    }

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
            cache = new VirtualDataPack(dataType, null);
        } else {
            DataPackNode[] dpns = getDPNs();
            DataPackNode lastDPN = dpns[dpns.length - 1];
            int objCount = lastDPN.objCount();
            if (lastDPN.objCount() == DataPack.MAX_COUNT) {
                // Last pack is full, leave it alone.
                packFileOffset = lastDPN.packAddr() + lastDPN.packSize();
                dpnFileOffset = DataPackNode.SERIALIZED_SIZE * packCount;
                indexFileOffset = lastDPN.indexAddr() + lastDPN.indexSize();
                cache = new VirtualDataPack(dataType, null);
            } else {
                // Last pack is not full, we need to append new data into it.

                packFileOffset = lastDPN.packAddr();
                dpnFileOffset = DataPackNode.SERIALIZED_SIZE * (packCount - 1);
                indexFileOffset = lastDPN.indexAddr();

                // Remove the data from last pack into cache.
                DataPack lastPack = loadPack(lastDPN);
                lastPack.decompress(lastDPN);
                packCount--;

                cache = new VirtualDataPack(dataType, lastPack);
                lastPack.free();
            }
        }

        // Set up index.
        switch (dataType) {
            case ColumnType.INT:
            case ColumnType.LONG:
                packIndex = new RSIndex_Histogram.HistPackIndex(false);
                break;
            case ColumnType.FLOAT:
            case ColumnType.DOUBLE:
                packIndex = new RSIndex_Histogram.HistPackIndex(true);
                break;
            case ColumnType.STRING:
                switch (version) {
                    case Version.VERSION_0_ID:
                        packIndex = new EmptyRSIndexStr.EmptyPackIndex();
                        break;
                    case Version.VERSION_1_ID:
                    case Version.VERSION_2_ID:
                        packIndex = new RSIndex_CMap.CMapPackIndex();
                        break;
                    default:
                        packIndex = new RSIndex_CMap_V2.CMapPackIndex();
                        break;
                }
                break;
            default:
                throw new IllegalArgumentException(String.format("Not support data type of %s", dataType));
        }
    }

    private void saveCache() throws IOException {
        Pair<DataPack, DataPackNode> p = cache.asPack(version, packIndex);
        DataPack pack = p.first;
        DataPackNode dpn = p.second;

        dpn.setCompress(compress);
        pack.compress(dpn);

        savePack(dpn, pack, packIndex);

        cache.clear();
        packIndex.clear();

        // This pack instance will never used again, reclaim its memory.
        // And packIndex is reclaimed when seal() is called.
        pack.free();
    }

    private void savePack(DataPackNode dpn, DataPack pack, PackRSIndex index) throws IOException {
        dpn.setPackSize(pack.serializedSize());
        dpn.setPackAddr(packFileOffset);

        dpn.setIndexSize(index.serializedSize());
        dpn.setIndexAddr(indexFileOffset);

        pack.write(ByteBufferWriter.of(packFileWrite, packFileOffset, null));
        packFileWrite.force(false);
        packFileOffset += dpn.packSize();

        index.write(ByteBufferWriter.of(indexFileWrite, indexFileOffset, null));
        indexFileWrite.force(false);
        indexFileOffset += dpn.indexSize();

        dpn.write(ByteBufferWriter.of(dpnFileWrite, dpnFileOffset, null));
        dpnFileWrite.force(false);
        dpnFileOffset += DataPackNode.SERIALIZED_SIZE;

        int packId = packCount;
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
        if (packIndex != null) {
            packIndex.free();
            packIndex = null;
        }

        IOUtils.closeQuietly(packFileWrite);
        packFileWrite = null;
        IOUtils.closeQuietly(dpnFileWrite);
        dpnFileWrite = null;
        IOUtils.closeQuietly(indexFileWrite);
        indexFileWrite = null;
    }

    public void seal() throws IOException {
        if (!update) {
            return;
        }
        // save the remaining cache.
        if (cache != null && cache.count() > 0) {
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
                PackRSIndex index = rsIndex.packIndex(packId);
                DataPack pack = column.loadPack(dpn);
                if (dpn.version() == version
                        && dpn.objCount() != 0
                        && (dpn.objCount() & DataPack.MASK) == 0) {
                    // Copy full packs directly.
                    this.savePack(dpn.clone(), pack, index);
                    pack.free();
                } else {
                    // If pack is not full or the versions not match, then append values by rows.
                    pack.decompress(dpn);
                    appendByRows.add(pack);
                }
            }
            rsIndex.free();
        }

        // Now append fragment packs' value.
        switch (dataType) {
            case ColumnType.INT:
                for (DataPack pack : appendByRows) {
                    for (int i = 0; i < pack.count(); i++) {
                        add(pack.intValueAt(i));
                    }
                    pack.free();
                }
                break;
            case ColumnType.LONG:
                for (DataPack pack : appendByRows) {
                    for (int i = 0; i < pack.count(); i++) {
                        add(pack.longValueAt(i));
                    }
                    pack.free();
                }
                break;
            case ColumnType.FLOAT:
                for (DataPack pack : appendByRows) {
                    for (int i = 0; i < pack.count(); i++) {
                        add(pack.floatValueAt(i));
                    }
                    pack.free();
                }
                break;
            case ColumnType.DOUBLE:
                for (DataPack pack : appendByRows) {
                    for (int i = 0; i < pack.count(); i++) {
                        add(pack.doubleValueAt(i));
                    }
                    pack.free();
                }
                break;
            case ColumnType.STRING:
                for (DataPack pack : appendByRows) {
                    for (int i = 0; i < pack.count(); i++) {
                        add(pack.stringValueAt(i).clone());
                    }
                    pack.free();
                }
                break;
            default:
                throw new IllegalStateException(String.format("Not support data type of %s", dataType));
        }

        seal();
    }
}
