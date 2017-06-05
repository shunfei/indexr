package io.indexr.vlt.segment.index;

import org.apache.commons.io.FileUtils;
import org.apache.spark.unsafe.Platform;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import io.indexr.data.BytePiece;
import io.indexr.data.DictStruct;
import io.indexr.data.StringsStructOnByteBufferReader;
import io.indexr.io.ByteBufferReader;
import io.indexr.plugin.Plugins;
import io.indexr.plugin.VLTPlugin;
import io.indexr.segment.Column;
import io.indexr.segment.ColumnSchema;
import io.indexr.segment.ColumnType;
import io.indexr.segment.SQLType;
import io.indexr.segment.SegmentMode;
import io.indexr.segment.SegmentSchema;
import io.indexr.segment.pack.DataPack;
import io.indexr.segment.pack.DataPackNode;
import io.indexr.segment.storage.DPSegment;
import io.indexr.segment.storage.DPSegmentTest;
import io.indexr.segment.storage.OpenOption;
import io.indexr.segment.storage.TestRows;
import io.indexr.segment.storage.Version;
import io.indexr.util.BinarySearch;
import io.indexr.util.DirectBitMap;
import io.indexr.util.IOUtil;

public class DictMergeTest {
    private static final Logger log = LoggerFactory.getLogger(DPSegmentTest.class);
    private static Path workDir;

    private static List<ColumnSchema> columnSchemas = Arrays.asList(
            new ColumnSchema("idx_c0", SQLType.INT, true),
            new ColumnSchema("idx_c1", SQLType.BIGINT, true),
            new ColumnSchema("idx_c2", SQLType.FLOAT, true),
            new ColumnSchema("idx_c3", SQLType.DOUBLE, true),
            new ColumnSchema("idx_c4", SQLType.VARCHAR, true)
    );
    private static SegmentSchema segmentSchema = new SegmentSchema(columnSchemas);

    private static final int rowCount = DataPack.MAX_COUNT * 3 + 99;

    static {
        Plugins.loadPluginsNEX();
    }

    @BeforeClass
    public static void init() throws IOException {
        workDir = Files.createTempDirectory("segment_test_");
        log.debug("workDir: {}", workDir.toString());
    }

    @AfterClass
    public static void cleanUp() throws IOException {
        FileUtils.deleteDirectory(workDir.toFile());
    }

    @Test
    public void test() throws IOException {
        for (Version version : Version.values()) {
            if (version.id >= Version.VERSION_7_ID) {
                _test(version.id, VLTPlugin.VLT, "DictMerge", workDir.resolve("DictMerge"));
            }
        }
    }

    private void _test(int version, SegmentMode mode, String name, Path path) throws IOException {
        DPSegment segment = DPSegment.open(
                version,
                mode,
                path,
                name,
                segmentSchema,
                OpenOption.Overwrite).update();
        DPSegmentTest.addRows(segment, TestRows.genRandomRows(rowCount, columnSchemas));
        segment.seal();


        for (int colId = 0; colId < segment.schema().getColumns().size(); colId++) {
            check(segment.column(colId));
        }
    }

    private void check(Column column) throws IOException {
        ArrayList<DataPack> dps = new ArrayList<>();
        ArrayList<DictStruct> structs = new ArrayList<>();
        for (int packId = 0; packId < column.packCount(); packId++) {
            DataPackNode dpn = column.dpn(packId);
            if (dpn.isDictEncoded()) {
                DataPack pack = column.pack(packId);
                dps.add(pack);
                structs.add(pack.dictStruct(column.dataType(), dpn));
            }
        }

        DictStruct[] dictStructs = structs.toArray(new DictStruct[0]);
        DataPack[] dataPacks = dps.toArray(new DataPack[0]);

        DictMerge dictMerge = new DictMerge();
        dictMerge.merge(column.dataType(), dictStructs);

        Path entryFilePath = column.dataType() == ColumnType.STRING
                ? dictMerge.combineStringOffsetAndEntryFile() : dictMerge.entryFilePath();
        FileChannel entryFile = FileChannel.open(
                entryFilePath,
                StandardOpenOption.READ,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING);
        FileChannel bitmapFile = FileChannel.open(
                dictMerge.bitmapFilePath(),
                StandardOpenOption.READ,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING);

        ByteBuffer entryFileBuffer = entryFile.map(FileChannel.MapMode.READ_ONLY, 0, entryFile.size());
        ByteBuffer bitmapFileBuffer = bitmapFile.map(FileChannel.MapMode.READ_ONLY, 0, bitmapFile.size());

        entryFileBuffer.order(ByteOrder.nativeOrder());
        bitmapFileBuffer.order(ByteOrder.nativeOrder());

        ByteBufferReader entryFileReader = ByteBufferReader.of(entryFileBuffer, 0, null);

        switch (column.dataType()) {
            case ColumnType.INT:
                checkInts(dataPacks, dictMerge.entryCount(), entryFileReader, bitmapFileBuffer);
                break;
            case ColumnType.LONG:
                checkLongs(dataPacks, dictMerge.entryCount(), entryFileReader, bitmapFileBuffer);
                break;
            case ColumnType.FLOAT:
                checkFloats(dataPacks, dictMerge.entryCount(), entryFileReader, bitmapFileBuffer);
                break;
            case ColumnType.DOUBLE:
                checkDoubles(dataPacks, dictMerge.entryCount(), entryFileReader, bitmapFileBuffer);
                break;
            case ColumnType.STRING:
                checkStrings(dataPacks, dictMerge.entryCount(), entryFileReader, bitmapFileBuffer);
                break;
            default:
                throw new RuntimeException("Illegal data type");
        }

        dictMerge.free();
        dictMerge.clean();
        IOUtil.closeQuietly(entryFile);
        IOUtil.closeQuietly(bitmapFile);
        for (DataPack dataPack : dataPacks) {
            dataPack.free();
        }
    }

    private void checkInts(DataPack[] dataPacks, int entryCount, ByteBufferReader entryFileReader, ByteBuffer bitmapFileBuffer) throws IOException {
        int packCount = dataPacks.length;
        int bitmapSize = DirectBitMap.bits2words(packCount) << 3;
        byte[] bitmapBuffer = new byte[bitmapSize];
        DirectBitMap bitMap = new DirectBitMap(bitmapBuffer, Platform.BYTE_ARRAY_OFFSET, bitmapSize >>> 3, null);

        for (int packId = 0; packId < packCount; packId++) {
            DataPack dataPack = dataPacks[packId];
            for (int rowId = 0; rowId < dataPack.valueCount(); rowId++) {
                int value = dataPack.intValueAt(rowId);
                int pos = BinarySearch.binarySearchInts(entryFileReader, entryCount, value);
                Assert.assertTrue(pos >= 0);
                bitmapFileBuffer.position(pos * bitmapSize);
                bitmapFileBuffer.get(bitmapBuffer);
                Assert.assertTrue(bitMap.get(packId));
            }
        }
    }

    private void checkLongs(DataPack[] dataPacks, int entryCount, ByteBufferReader entryFileReader, ByteBuffer bitmapFileBuffer) throws IOException {
        int packCount = dataPacks.length;
        int bitmapSize = DirectBitMap.bits2words(packCount) << 3;
        byte[] bitmapBuffer = new byte[bitmapSize];
        DirectBitMap bitMap = new DirectBitMap(bitmapBuffer, Platform.BYTE_ARRAY_OFFSET, bitmapSize >>> 3, null);

        for (int packId = 0; packId < packCount; packId++) {
            DataPack dataPack = dataPacks[packId];
            for (int rowId = 0; rowId < dataPack.valueCount(); rowId++) {
                long value = dataPack.longValueAt(rowId);
                int pos = BinarySearch.binarySearchLongs(entryFileReader, entryCount, value);
                Assert.assertTrue(pos >= 0);
                bitmapFileBuffer.position(pos * bitmapSize);
                bitmapFileBuffer.get(bitmapBuffer);
                Assert.assertTrue(bitMap.get(packId));
            }
        }
    }


    private void checkFloats(DataPack[] dataPacks, int entryCount, ByteBufferReader entryFileReader, ByteBuffer bitmapFileBuffer) throws IOException {
        int packCount = dataPacks.length;
        int bitmapSize = DirectBitMap.bits2words(packCount) << 3;
        byte[] bitmapBuffer = new byte[bitmapSize];
        DirectBitMap bitMap = new DirectBitMap(bitmapBuffer, Platform.BYTE_ARRAY_OFFSET, bitmapSize >>> 3, null);

        for (int packId = 0; packId < packCount; packId++) {
            DataPack dataPack = dataPacks[packId];
            for (int rowId = 0; rowId < dataPack.valueCount(); rowId++) {
                float value = dataPack.floatValueAt(rowId);
                int pos = BinarySearch.binarySearchFloats(entryFileReader, entryCount, value);
                Assert.assertTrue(pos >= 0);
                bitmapFileBuffer.position(pos * bitmapSize);
                bitmapFileBuffer.get(bitmapBuffer);
                Assert.assertTrue(bitMap.get(packId));
            }
        }
    }


    private void checkDoubles(DataPack[] dataPacks, int entryCount, ByteBufferReader entryFileReader, ByteBuffer bitmapFileBuffer) throws IOException {
        int packCount = dataPacks.length;
        int bitmapSize = DirectBitMap.bits2words(packCount) << 3;
        byte[] bitmapBuffer = new byte[bitmapSize];
        DirectBitMap bitMap = new DirectBitMap(bitmapBuffer, Platform.BYTE_ARRAY_OFFSET, bitmapSize >>> 3, null);

        for (int packId = 0; packId < packCount; packId++) {
            DataPack dataPack = dataPacks[packId];
            for (int rowId = 0; rowId < dataPack.valueCount(); rowId++) {
                double value = dataPack.doubleValueAt(rowId);
                int pos = BinarySearch.binarySearchDoubles(entryFileReader, entryCount, value);
                Assert.assertTrue(pos >= 0);
                bitmapFileBuffer.position(pos * bitmapSize);
                bitmapFileBuffer.get(bitmapBuffer);
                Assert.assertTrue(bitMap.get(packId));
            }
        }
    }


    private void checkStrings(DataPack[] dataPacks, int entryCount, ByteBufferReader entryFileReader, ByteBuffer bitmapFileBuffer) throws IOException {
        int packCount = dataPacks.length;
        int bitmapSize = DirectBitMap.bits2words(packCount) << 3;
        byte[] bitmapBuffer = new byte[bitmapSize];
        DirectBitMap bitMap = new DirectBitMap(bitmapBuffer, Platform.BYTE_ARRAY_OFFSET, bitmapSize >>> 3, null);

        StringsStructOnByteBufferReader stringsStruct = new StringsStructOnByteBufferReader(entryCount, entryFileReader);
        byte[] keyBuffer = new byte[ColumnType.MAX_STRING_UTF8_SIZE];
        BytePiece value = new BytePiece();
        for (int packId = 0; packId < packCount; packId++) {
            DataPack dataPack = dataPacks[packId];
            for (int rowId = 0; rowId < dataPack.valueCount(); rowId++) {
                dataPack.rawValueAt(rowId, value);
                int pos = BinarySearch.binarySearchStrings(stringsStruct, value.base, value.addr, value.len, keyBuffer);
                Assert.assertTrue(pos >= 0);
                bitmapFileBuffer.position(pos * bitmapSize);
                bitmapFileBuffer.get(bitmapBuffer);
                Assert.assertTrue(bitMap.get(packId));
            }
        }
    }

}
