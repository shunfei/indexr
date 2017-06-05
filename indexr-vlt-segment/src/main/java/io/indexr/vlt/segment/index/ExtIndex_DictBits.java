package io.indexr.vlt.segment.index;

import com.google.common.base.Preconditions;

import io.indexr.vlt.codec.dict.DictCompressCodec;

import org.apache.spark.unsafe.types.UTF8String;

import java.io.IOException;

import io.indexr.data.DictStruct;
import io.indexr.data.StringsStruct;
import io.indexr.segment.Column;
import io.indexr.segment.ColumnType;
import io.indexr.segment.PackExtIndex;
import io.indexr.segment.pack.DataPack;
import io.indexr.segment.pack.DataPackNode;
import io.indexr.util.BinarySearch;
import io.indexr.util.BitMap;
import io.indexr.util.MemoryUtil;
import io.indexr.util.SQLLike;

/**
 * Work closely with {@link DictCompressCodec}.
 */
public class ExtIndex_DictBits implements PackExtIndex {
    private byte dataType;

    public ExtIndex_DictBits(byte dataType) {
        this.dataType = dataType;
    }

    private int searchEntry(DictStruct struct, long numValue, UTF8String strValue) throws IOException {
        int dictEntryCount = struct.dictEntryCount();
        int entryId;
        switch (dataType) {
            case ColumnType.INT:
                entryId = BinarySearch.binarySearchInts(struct.dictEntriesAddr(), dictEntryCount, (int) numValue);
                break;
            case ColumnType.LONG:
                entryId = BinarySearch.binarySearchLongs(struct.dictEntriesAddr(), dictEntryCount, numValue);
                break;
            case ColumnType.FLOAT:
                entryId = BinarySearch.binarySearchFloats(struct.dictEntriesAddr(), dictEntryCount, (float) Double.longBitsToDouble(numValue));
                break;
            case ColumnType.DOUBLE:
                entryId = BinarySearch.binarySearchDoubles(struct.dictEntriesAddr(), dictEntryCount, Double.longBitsToDouble(numValue));
                break;
            case ColumnType.STRING:
                StringsStruct strings = struct.stringDictEntries();
                entryId = BinarySearch.binarySearchStrings(strings, strValue.getBaseObject(), strValue.getBaseOffset(), strValue.numBytes());
                break;
            default:
                throw new RuntimeException("Illegal dataType: " + dataType);
        }
        return entryId;
    }

    @Override
    public BitMap equal(Column column, int packId, long numValue, UTF8String strValue) throws IOException {
        DataPack pack = column.pack(packId);
        DataPackNode dpn = column.dpn(packId);
        DictStruct struct = pack.dictStruct(dataType, dpn);

        int entryId = searchEntry(struct, numValue, strValue);
        if (entryId < 0) {
            return BitMap.NONE;
        } else if (struct.dictEntryCount() == 1) {
            return BitMap.ALL;
        }

        long dataAddr = struct.dataAddr();
        BitMap bitSet = new BitMap();
        for (int i = 0; i < dpn.objCount(); i++) {
            if (MemoryUtil.getInt(dataAddr + (i << 2)) == entryId) {
                bitSet.set(i);
            }
        }
        return bitSet;
    }

    @Override
    public BitMap in(Column column, int packId, long[] numValues, UTF8String[] strValues) throws IOException {
        DataPack pack = column.pack(packId);
        DataPackNode dpn = column.dpn(packId);
        DictStruct struct = pack.dictStruct(dataType, dpn);
        int inCount = ColumnType.isNumber(dataType) ? numValues.length : strValues.length;
        int[] entryIds = new int[inCount];
        int entryIdCount = 0;
        for (int i = 0; i < inCount; i++) {
            long numValue = numValues == null ? 0 : numValues[i];
            UTF8String strValue = strValues == null ? null : strValues[i];
            int entryId = searchEntry(struct, numValue, strValue);
            if (entryId >= 0) {
                entryIds[entryIdCount++] = entryId;
            }
        }

        if (entryIdCount == 0) {
            return BitMap.NONE;
        }

        long dataAddr = struct.dataAddr();
        BitMap bitSet = new BitMap();
        for (int i = 0; i < dpn.objCount(); i++) {
            int vid = MemoryUtil.getInt(dataAddr + (i << 2));
            for (int eId = 0; eId < entryIdCount; eId++) {
                if (vid == entryIds[eId]) {
                    bitSet.set(i);
                }
            }
        }
        return bitSet;
    }

    @Override
    public BitMap greater(Column column, int packId, long numValue, UTF8String strValue, boolean acceptEqual) throws IOException {
        DataPack pack = column.pack(packId);
        DataPackNode dpn = column.dpn(packId);
        DictStruct struct = pack.dictStruct(dataType, dpn);

        int entryId = searchEntry(struct, numValue, strValue);

        int start;
        if (entryId >= 0) {
            start = acceptEqual ? entryId : entryId + 1;
        } else {
            start = -entryId - 1;
        }
        Preconditions.checkState(start >= 0);

        if (start >= struct.dictEntryCount()) {
            return BitMap.NONE;
        } else if (start == 0) {
            return BitMap.ALL;
        }

        long dataAddr = struct.dataAddr();
        BitMap bitSet = new BitMap();
        for (int i = 0; i < dpn.objCount(); i++) {
            if (MemoryUtil.getInt(dataAddr + (i << 2)) >= start) {
                bitSet.set(i);
            }
        }
        return bitSet;
    }

    @Override
    public BitMap between(Column column, int packId, long numValue1, long numValue2, UTF8String strValue1, UTF8String strValue2) throws IOException {
        DataPack pack = column.pack(packId);
        DataPackNode dpn = column.dpn(packId);
        DictStruct struct = pack.dictStruct(dataType, dpn);
        int entryId1 = searchEntry(struct, numValue1, strValue1);
        int entryId2 = searchEntry(struct, numValue2, strValue2);

        int dictEntryCount = struct.dictEntryCount();

        // [from, to)
        int from = entryId1 >= 0 ? entryId1 : (-entryId1 - 1);
        int to = entryId2 >= 0 ? entryId2 + 1 : (-entryId2 - 1);

        Preconditions.checkState(from >= 0 && from < dictEntryCount);
        Preconditions.checkState(to >= 0 && to <= dictEntryCount);

        if (to - from <= 0) {
            return BitMap.NONE;
        } else if (to - from >= dictEntryCount) {
            return BitMap.ALL;
        }

        long dataAddr = struct.dataAddr();
        BitMap bitSet = new BitMap();
        for (int i = 0; i < dpn.objCount(); i++) {
            int id = MemoryUtil.getInt(dataAddr + (i << 2));
            if (id >= from && id < to) {
                bitSet.set(i);
            }
        }
        return bitSet;
    }

    @Override
    public BitMap like(Column column, int packId, long numValue, UTF8String strValue) throws IOException {
        DataPack pack = column.pack(packId);
        int rowCount = pack.valueCount();
        BitMap res = new BitMap();
        // TODO optimize for like xxx%
        switch (dataType) {
            case ColumnType.STRING: {
                for (int rowId = 0; rowId < rowCount; rowId++) {
                    if (SQLLike.match(pack.stringValueAt(rowId), strValue)) {
                        res.set(rowId);
                    }
                }
                break;
            }
            default:
                throw new IllegalStateException("column type " + dataType + " is illegal in LIKE");
        }
        return fixBitmapInPack(res, rowCount);
    }
}
