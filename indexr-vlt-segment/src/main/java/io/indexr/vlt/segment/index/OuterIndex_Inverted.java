package io.indexr.vlt.segment.index;

import com.google.common.base.Preconditions;

import org.apache.spark.unsafe.types.UTF8String;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import io.indexr.data.Cleanable;
import io.indexr.data.DataType;
import io.indexr.data.DictStruct;
import io.indexr.data.StringsStructOnByteBufferReader;
import io.indexr.io.ByteBufferReader;
import io.indexr.io.ByteBufferWriter;
import io.indexr.segment.Column;
import io.indexr.segment.ColumnType;
import io.indexr.segment.OuterIndex;
import io.indexr.segment.SegmentMode;
import io.indexr.segment.pack.DataPack;
import io.indexr.segment.pack.DataPackNode;
import io.indexr.segment.storage.StorageColumn;
import io.indexr.util.BinarySearch;
import io.indexr.util.BitMap;
import io.indexr.util.ByteBufferUtil;
import io.indexr.util.DirectBitMap;
import io.indexr.util.IOUtil;
import io.indexr.util.Trick;

/**
 * |  metadata | dict entries | bitmap | bitmap merge |
 */
public class OuterIndex_Inverted implements OuterIndex {
    public static final int MARK = 1;
    public static final int METADATA_SIZE = 4 + 4 + 4 + 4 + 8 + 8;

    private byte dataType;
    private int dictEntryCount;
    private int packCount;
    private int bitmapMergeSlotCount;
    private ByteBufferReader dictReader;
    private ByteBufferReader bitmapReader;
    private ByteBufferReader bitmapMergeReader;

    public OuterIndex_Inverted(byte dataType, ByteBufferReader reader) throws IOException {
        this.dataType = dataType;

        ByteBuffer buffer = ByteBufferUtil.allocateHeap(METADATA_SIZE);
        reader.read(0, buffer);
        buffer.flip();

        int mark = buffer.getInt();
        this.dictEntryCount = buffer.getInt();
        this.packCount = buffer.getInt();
        this.bitmapMergeSlotCount = buffer.getInt();
        long dictSize = buffer.getLong();
        long bitmapSize = buffer.getLong();

        Preconditions.checkState(mark == MARK);

        long totalBitmapSize = dictEntryCount * MergeBitMapUtil.bitmapSize(packCount);

        this.dictReader = ByteBufferReader.of(reader, METADATA_SIZE, reader);
        this.bitmapReader = ByteBufferReader.of(reader, METADATA_SIZE + dictSize, null);
        this.bitmapMergeReader = ByteBufferReader.of(reader, METADATA_SIZE + dictSize + totalBitmapSize, null);

        if (dataType == ColumnType.STRING) {
            // See StringsStruct
            assert dictSize == ((dictEntryCount + 1) << 2) + reader.readInt(METADATA_SIZE + (dictEntryCount << 2));
        } else {
            assert dictSize == dictEntryCount << DataType.numTypeShift(dataType);
        }
    }

    private int idToMergeSlotId(int id) {
        return MergeBitMapUtil.idToMergeSlotId(bitmapMergeSlotCount, dictEntryCount, id);
    }

    private int mergeSlotIdToId(int slotId) {
        return MergeBitMapUtil.mergeSlotIdToId(bitmapMergeSlotCount, dictEntryCount, slotId);
    }

    /**
     * [startEntryId, endEntryId)
     */
    private BitMap readMergedBitMap(int startEntryId, int endEntryId) throws IOException {
        assert startEntryId < endEntryId;

        DirectBitMap bitmap = new DirectBitMap(packCount);

        if (bitmapMergeSlotCount == 0) {
            MergeBitMapUtil.readAndMergeBitmaps(
                    bitmapReader,
                    bitmap,
                    startEntryId,
                    endEntryId);
            return new BitMap(bitmap, packCount);
        }

        int startSlot = idToMergeSlotId(startEntryId);
        int endSlot = idToMergeSlotId(endEntryId);

        if (startSlot == endSlot) {
            MergeBitMapUtil.readAndMergeBitmaps(
                    bitmapReader,
                    bitmap,
                    startEntryId,
                    endEntryId);
        } else {
            assert startSlot < endSlot;

            //  | ........ | ........ | ~ | ........ | ........ |
            //       ^     ^                         ^       ^
            //     start1  end1                    start2   end2

            int start1 = startEntryId;
            int end1 = mergeSlotIdToId(startSlot + 1);

            int start2 = mergeSlotIdToId(endSlot);
            int end2 = endEntryId;

            if (start1 >= end1) {
                System.out.println();
            }
            assert start1 < end1;
            assert start2 <= end2;
            assert end1 <= start2;

            if (end1 == start2) {
                assert startSlot + 1 == endSlot;

                MergeBitMapUtil.readAndMergeBitmaps(
                        bitmapReader,
                        bitmap,
                        start1,
                        end2);
            } else {
                MergeBitMapUtil.readAndMergeBitmaps(
                        bitmapReader,
                        bitmap,
                        start1,
                        end1);
                if (start2 < end2) {
                    MergeBitMapUtil.readAndMergeBitmaps(
                            bitmapReader,
                            bitmap,
                            start2,
                            end2);
                }

                int mergeStart = startSlot + 1;
                int mergeEnd = endSlot;
                if (mergeStart < mergeEnd) {
                    MergeBitMapUtil.readAndMergeBitmaps(
                            bitmapMergeReader,
                            bitmap,
                            mergeStart,
                            mergeEnd);
                }
            }
        }
        return new BitMap(bitmap, packCount);
    }

    private int searchEntry(long numValue, UTF8String strValue) throws IOException {
        int entryId;
        switch (dataType) {
            case ColumnType.INT:
                entryId = BinarySearch.binarySearchInts(dictReader, dictEntryCount, (int) numValue);
                break;
            case ColumnType.LONG:
                entryId = BinarySearch.binarySearchLongs(dictReader, dictEntryCount, numValue);
                break;
            case ColumnType.FLOAT:
                entryId = BinarySearch.binarySearchFloats(dictReader, dictEntryCount, (float) Double.longBitsToDouble(numValue));
                break;
            case ColumnType.DOUBLE:
                entryId = BinarySearch.binarySearchDoubles(dictReader, dictEntryCount, Double.longBitsToDouble(numValue));
                break;
            case ColumnType.STRING:
                StringsStructOnByteBufferReader strings = new StringsStructOnByteBufferReader(dictEntryCount, dictReader);
                byte[] valBuffer = new byte[ColumnType.MAX_STRING_UTF8_SIZE];
                entryId = BinarySearch.binarySearchStrings(
                        strings,
                        strValue.getBaseObject(), strValue.getBaseOffset(), strValue.numBytes(),
                        valBuffer);
                break;
            default:
                throw new RuntimeException("Illegal dataType: " + dataType);
        }
        return entryId;
    }

    @Override
    public BitMap equal(Column column, long numValue, UTF8String strValue, boolean isNot) throws IOException {
        assert packCount == column.packCount();

        int entryId = searchEntry(numValue, strValue);
        if (entryId < 0) {
            return isNot ? BitMap.ALL : BitMap.NONE;
        } else if (dictEntryCount == 1) {
            return isNot ? BitMap.NONE : BitMap.ALL;
        }
        if (isNot) {
            // "not equal" can not be handled by this index.
            return BitMap.ALL;
        }

        return readMergedBitMap(entryId, entryId + 1);
    }


    @Override
    public BitMap in(Column column, long[] numValues, UTF8String[] strValues, boolean isNot) throws IOException {
        int inCount = ColumnType.isNumber(dataType) ? numValues.length : strValues.length;
        int[] entryIds = new int[inCount];
        int entryIdCount = 0;
        for (int i = 0; i < inCount; i++) {
            long numValue = numValues == null ? 0 : numValues[i];
            UTF8String strValue = strValues == null ? null : strValues[i];
            int entryId = searchEntry(numValue, strValue);
            if (entryId >= 0) {
                entryIds[entryIdCount++] = entryId;
            }
        }

        if (entryIdCount == 0) {
            return isNot ? BitMap.ALL : BitMap.NONE;
        } else if (dictEntryCount == 1) {
            return isNot ? BitMap.NONE : BitMap.ALL;
        }

        if (isNot) {
            // "not in" can not be handled by this index.
            return BitMap.ALL;
        }

        DirectBitMap bitmap = new DirectBitMap(packCount);
        MergeBitMapUtil.readAndMergeBitmaps(bitmapReader, bitmap, Trick.subArray(entryIds, 0, entryIdCount));
        return new BitMap(bitmap, packCount);
    }

    private BitMap greater(int entryId, boolean acceptEqual, boolean isNot) throws IOException {
        // TODO optimize for acceptEqual
        int split;
        if (entryId >= 0) {
            split = entryId;
        } else {
            split = -entryId - 1;
        }
        Preconditions.checkState(split >= 0 && split <= dictEntryCount);

        int start = 0, end = 0;
        if (!isNot) {
            // greater
            if (split == 0) {
                return BitMap.ALL;
            } else if (split == dictEntryCount) {
                return BitMap.NONE;
            } else {
                start = split;
                end = dictEntryCount;
            }
        } else {
            // less
            if (split == dictEntryCount) {
                return BitMap.ALL;
            } else {
                start = 0;
                end = entryId >= 0 ? entryId + 1 : -entryId - 1;
            }
        }

        return readMergedBitMap(start, end);
    }

    @Override
    public BitMap greater(Column column, long numValue, UTF8String strValue, boolean acceptEqual, boolean isNot) throws IOException {
        int entryId = searchEntry(numValue, strValue);
        return greater(entryId, acceptEqual, isNot);
    }

    @Override
    public BitMap between(Column column, long numValue1, long numValue2, UTF8String strValue1, UTF8String strValue2, boolean isNot) throws IOException {
        int entryId1 = searchEntry(numValue1, strValue1);
        int entryId2 = searchEntry(numValue2, strValue2);

        if (!isNot) {
            // between
            BitMap bitmap1 = greater(entryId1, true, false);
            BitMap bitmap2 = greater(entryId2, true, true);
            return BitMap.and_free(bitmap1, bitmap2);
        } else {
            // not between
            BitMap bitmap1 = greater(entryId1, false, true);
            BitMap bitmap2 = greater(entryId2, false, false);
            return BitMap.or_free(bitmap1, bitmap2);
        }
    }

    @Override
    public BitMap like(Column column, long numValue, UTF8String strValue, boolean isNot) throws IOException {
        // TODO optimize me!
        return BitMap.ALL;
    }

    @Override
    public void close() throws IOException {
        if (dictReader != null) {
            dictReader.close();
            dictReader = null;
            bitmapReader.close();
            bitmapReader = null;
        }
    }

    public static class Cache implements OuterIndex.Cache {
        private final Cleanable cleanable;

        private final int entryCount;
        private final int packCount;
        private final int bitmapMergeSlotCount;

        private final long entryFileSize;
        private final long entryOffsetFileSize;
        private final long bitmapFileSize;

        private final Path entryFilePath;
        private final Path entryOffsetFilePath; // For strings
        private final Path bitmapFilePath;

        public Cache(int entryCount,
                     int packCount,
                     int bitmapMergeSlotCount,
                     long entryFileSize,
                     long entryOffsetFileSize,
                     long bitmapFileSize,
                     Path entryFilePath,
                     Path entryOffsetFilePath,
                     Path bitmapFilePath,
                     Cleanable cleanable) {
            this.cleanable = cleanable;

            this.entryCount = entryCount;
            this.packCount = packCount;
            this.bitmapMergeSlotCount = bitmapMergeSlotCount;
            this.entryFileSize = entryFileSize;
            this.entryOffsetFileSize = entryOffsetFileSize;
            this.bitmapFileSize = bitmapFileSize;
            this.entryFilePath = entryFilePath;
            this.entryOffsetFilePath = entryOffsetFilePath;
            this.bitmapFilePath = bitmapFilePath;
        }

        @Override
        public void close() throws IOException {
            if (cleanable != null) {
                cleanable.clean();
            }
        }

        @Override
        public long size() throws IOException {
            return METADATA_SIZE + entryFileSize + entryOffsetFileSize + bitmapFileSize;
        }

        @Override
        public int write(ByteBufferWriter writer) throws IOException {
            FileChannel entryOffsetFile = null;
            FileChannel entryFile = null;
            FileChannel bitmapFile = null;
            try {
                long totalSize = 0;

                // First mark who we are.
                ByteBuffer buffer = ByteBufferUtil.allocateHeap(METADATA_SIZE);
                buffer.putInt(MARK);
                buffer.putInt(entryCount);
                buffer.putInt(packCount);
                buffer.putInt(bitmapMergeSlotCount);
                buffer.putLong(entryOffsetFileSize + entryFileSize);
                buffer.putLong(bitmapFileSize);
                buffer.flip();
                writer.write(buffer);
                totalSize += METADATA_SIZE;

                // Move in data.
                if (entryOffsetFilePath != null) {
                    entryOffsetFile = FileChannel.open(entryOffsetFilePath, StandardOpenOption.READ);
                    writer.write(entryOffsetFile.map(FileChannel.MapMode.READ_ONLY, 0, entryOffsetFile.size()));
                    totalSize += entryOffsetFile.size();
                }
                entryFile = FileChannel.open(entryFilePath, StandardOpenOption.READ);
                writer.write(entryFile.map(FileChannel.MapMode.READ_ONLY, 0, entryFile.size()));
                totalSize += entryFile.size();

                bitmapFile = FileChannel.open(bitmapFilePath, StandardOpenOption.READ);
                writer.write(bitmapFile.map(FileChannel.MapMode.READ_ONLY, 0, bitmapFile.size()));
                totalSize += bitmapFile.size();

                assert totalSize == this.size();
            } finally {
                if (entryFile != null) {
                    IOUtil.closeQuietly(entryFile);
                    entryFile = null;
                }
                if (entryOffsetFile != null) {
                    IOUtil.closeQuietly(entryOffsetFile);
                    entryOffsetFile = null;
                }
                if (bitmapFile != null) {
                    IOUtil.closeQuietly(bitmapFile);
                    bitmapFile = null;
                }
            }
            return 0;
        }
    }

    public static Cache create(int version, SegmentMode mode, StorageColumn column) throws IOException {
        DataPack[] dataPacks = new DataPack[column.packCount()];
        DictStruct[] structs = new DictStruct[column.packCount()];
        DictMerge dictMerge = new DictMerge();
        try {
            for (int packId = 0; packId < column.packCount(); packId++) {
                DataPackNode dpn = column.dpn(packId);
                if (!dpn.isDictEncoded()) {
                    return null;
                }
                DataPack pack = new DataPack(null, column.loadPack(dpn), dpn);
                dataPacks[packId] = pack;
                structs[packId] = pack.dictStruct(column.dataType(), dpn);
            }

            dictMerge.merge(column.dataType(), structs);

            return new Cache(
                    dictMerge.entryCount(),
                    column.packCount(),
                    dictMerge.bitmapMergeSlotCount(),
                    dictMerge.entryFileSize(),
                    dictMerge.entryOffsetFileSize(),
                    dictMerge.bitmapFileSize(),
                    dictMerge.entryFilePath(),
                    dictMerge.entryOffsetFilePath(),
                    dictMerge.bitmapFilePath(),
                    dictMerge);
        } finally {
            for (DataPack dataPack : dataPacks) {
                if (dataPack != null) {
                    dataPack.free();
                }
            }
            dictMerge.free();
        }
    }

}
