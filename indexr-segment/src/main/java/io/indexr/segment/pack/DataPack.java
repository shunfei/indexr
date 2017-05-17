package io.indexr.segment.pack;

import com.google.common.base.Preconditions;

import com.carrotsearch.hppc.BitSetIterator;

import org.apache.spark.unsafe.types.UTF8String;

import java.io.IOException;
import java.nio.ByteBuffer;

import io.indexr.data.BytePiece;
import io.indexr.data.BytePieceSetter;
import io.indexr.data.DictStruct;
import io.indexr.data.DoubleSetter;
import io.indexr.data.FloatSetter;
import io.indexr.data.Freeable;
import io.indexr.data.IntSetter;
import io.indexr.data.LongSetter;
import io.indexr.io.ByteBufferWriter;
import io.indexr.io.ByteSlice;
import io.indexr.segment.ColumnType;
import io.indexr.segment.DPValues;
import io.indexr.segment.PackDurationStat;
import io.indexr.segment.SegmentMode;
import io.indexr.segment.storage.PackBundle;
import io.indexr.segment.storage.Version;
import io.indexr.util.BitMap;
import io.indexr.util.MemoryUtil;
import io.indexr.util.OffheapBitMapIterator;
import io.indexr.util.Wrapper;

public final class DataPack implements DPValues, Freeable {
    public static final int SHIFT = 16;
    public static final int MASK = 0XFFFF;
    public static final int MAX_COUNT = 1 << SHIFT;

    private final int version;
    private final int objCount;

    private ByteSlice data; // Hold a reference, prevent memory from gc.
    private long dataAddr;
    private ByteSlice cmpData;

    // For those dictionary encoded data.
    private ByteSlice dictData;
    private DictStruct dictStruct;

    public DataPack(ByteSlice data, ByteSlice cmpData, DataPackNode dpn) {
        this.data = data;
        if (data != null && data.size() > 0) {
            this.dataAddr = data.address();
        }
        this.cmpData = cmpData;

        this.version = dpn.version();
        this.objCount = dpn.objCount();

        // Hack code.
        if (dpn instanceof DataPackNode_Basic) {
            DataPackNode_Basic v0 = (DataPackNode_Basic) dpn;
            this.numType = v0.numType();
            this.minVal = v0.uniformMin();
            this.maxObjLen = v0.maxObjLen();
        } else {
            this.numType = 0;
            this.minVal = 0;
            this.maxObjLen = 0;
        }
    }

    public final int version() {
        return version;
    }

    public final long dataAddr() {
        return dataAddr;
    }

    public final ByteSlice data() {
        return data;
    }

    public final ByteSlice cmpData() {
        return cmpData;
    }

    public final DictStruct dictStruct(byte dataType, DataPackNode dpn) {
        if (dictStruct == null) {
            long time = System.currentTimeMillis();

            dictData = dpn.mode.versionAdapter.getDictStruct(version, dpn.mode(), dataType, dpn, cmpData);
            dictStruct = new DictStruct(dataType, dictData.address());

            PackDurationStat.INSTANCE.add_compressPack(System.currentTimeMillis() - time);
        }
        return dictStruct;
    }

    @Override
    public final int valueCount() {
        return objCount;
    }

    /**
     * Bytes on disk.
     */
    public final int serializedSize() {
        return cmpData.size();
    }

    @Override
    public final void free() {
        if (data != null && data == cmpData) {
            // It could happen in uncompressed mode.
            data.free();
            data = null;
            cmpData = null;
        } else {
            if (data != null) {
                data.free();
                data = null;
            }
            if (cmpData != null) {
                cmpData.free();
                cmpData = null;
            }
        }
        if (dictData != null) {
            dictData.free();
            dictData = null;
        }
        dictStruct = null;
    }

    public final void write(ByteBufferWriter writer) throws IOException {
        Preconditions.checkState(cmpData != null);
        writer.write(cmpData.toByteBuffer());
    }

    /**
     * Note that this method could update the content of dpn.
     * Make sure to call this method before you actually save the dpn instance to storage.
     */
    public final Object compress(byte dataType, boolean isIndexed, DataPackNode dpn) {
        assert version == dpn.version();
        if (cmpData != null) {
            return null;
        }
        long time = System.currentTimeMillis();

        Wrapper extraInfo = new Wrapper();
        if (dpn.compress()) {
            cmpData = dpn.mode.versionAdapter.compressPack(version, dpn.mode(), dataType, isIndexed, dpn, data, extraInfo);
        } else {
            cmpData = data;
        }

        PackDurationStat.INSTANCE.add_compressPack(System.currentTimeMillis() - time);

        return extraInfo.value;
    }

    public final void decompress(byte dataType, DataPackNode dpn) {
        assert version == dpn.version();
        if (data != null) {
            return;
        }
        long time = System.currentTimeMillis();

        if (dpn.compress()) {
            if (dpn.isDictEncoded()) {
                // Special case for dictionary encoded data.
                data = dpn.mode.versionAdapter.getDataFromDictStruct(version, dpn.mode, dataType, dpn, dictStruct(dataType, dpn));
            } else {
                data = dpn.mode.versionAdapter.decompressPack(version, dpn.mode(), dataType, dpn, cmpData);
            }
        } else {
            data = cmpData;
        }
        dataAddr = data.address();

        PackDurationStat.INSTANCE.add_decompressPack(System.currentTimeMillis() - time);
    }

    // --------------------------------------------------------
    // Number data pack.

    // Only used by Version.VERSION_0_ID
    private final byte numType;
    private final long minVal;

    @Override
    public long uniformValAt(int index, byte type) {
        switch (version) {
            case Version.VERSION_0_ID:
                return NumOp.getVal(numType, dataAddr, index, minVal);
            default:
                switch (type) {
                    case ColumnType.INT:
                        return NumOp.getInt(dataAddr, index);
                    case ColumnType.LONG:
                        return NumOp.getLong(dataAddr, index);
                    case ColumnType.FLOAT:
                        return Double.doubleToRawLongBits(NumOp.getFloat(dataAddr, index));
                    case ColumnType.DOUBLE:
                        return Double.doubleToRawLongBits(NumOp.getDouble(dataAddr, index));
                    default:
                        throw new IllegalStateException("unsupported type " + type);
                }
        }
    }

    @Override
    public int intValueAt(int index) {
        switch (version) {
            case Version.VERSION_0_ID:
                return (int) NumOp.getVal(numType, dataAddr, index, minVal);
            default:
                return NumOp.getInt(dataAddr, index);
        }
    }

    @Override
    public long longValueAt(int index) {
        switch (version) {
            case Version.VERSION_0_ID:
                return NumOp.getVal(numType, dataAddr, index, minVal);
            default:
                return NumOp.getLong(dataAddr, index);
        }
    }

    @Override
    public float floatValueAt(int index) {
        switch (version) {
            case Version.VERSION_0_ID:
                return (float) Double.longBitsToDouble(NumOp.getVal(numType, dataAddr, index, minVal));
            default:
                return NumOp.getFloat(dataAddr, index);
        }
    }

    @Override
    public double doubleValueAt(int index) {
        switch (version) {
            case Version.VERSION_0_ID:
                return Double.longBitsToDouble(NumOp.getVal(numType, dataAddr, index, minVal));
            default:
                return NumOp.getDouble(dataAddr, index);
        }
    }

    @Override
    public void foreach(int start, int count, IntSetter setter) {
        int end = start + count;
        switch (version) {
            case Version.VERSION_0_ID:
                for (int index = start; index < end; index++) {
                    setter.set(index, (int) NumOp.getVal(numType, dataAddr, index, minVal));
                }
                break;
            default:
                for (int index = start; index < end; index++) {
                    setter.set(index, NumOp.getInt(dataAddr, index));
                }
                break;
        }
    }

    public int foreach(BitMap position, IntSetter setter) {
        if (position == BitMap.ALL || position == BitMap.SOME) {
            foreach(0, objCount, setter);
            return objCount;
        }
        OffheapBitMapIterator posIterator = position.iterator();
        int count = 0;
        switch (version) {
            case Version.VERSION_0_ID: {
                int index;
                while ((index = posIterator.nextSetBit()) != BitSetIterator.NO_MORE) {
                    if (index >= objCount) {
                        break;
                    }
                    setter.set(index, (int) NumOp.getVal(numType, dataAddr, index, minVal));
                    count++;
                }
                break;
            }
            default: {
                int index;
                while ((index = posIterator.nextSetBit()) != BitSetIterator.NO_MORE) {
                    if (index >= objCount) {
                        break;
                    }
                    setter.set(index, NumOp.getInt(dataAddr, index));
                    count++;
                }
                break;
            }
        }
        return count;
    }

    @Override
    public void foreach(int start, int count, LongSetter setter) {
        int end = start + count;
        switch (version) {
            case Version.VERSION_0_ID:
                for (int index = start; index < end; index++) {
                    setter.set(index, NumOp.getVal(numType, dataAddr, index, minVal));
                }
                break;
            default:
                for (int index = start; index < end; index++) {
                    setter.set(index, NumOp.getLong(dataAddr, index));
                }
                break;
        }
    }

    public int foreach(BitMap position, LongSetter setter) {
        if (position == BitMap.ALL || position == BitMap.SOME) {
            foreach(0, objCount, setter);
            return objCount;
        }
        OffheapBitMapIterator posIterator = position.iterator();
        int count = 0;
        switch (version) {
            case Version.VERSION_0_ID: {
                int index;
                while ((index = posIterator.nextSetBit()) != BitSetIterator.NO_MORE) {
                    if (index >= objCount) {
                        break;
                    }
                    setter.set(index, NumOp.getVal(numType, dataAddr, index, minVal));
                    count++;
                }
                break;
            }
            default: {
                int index;
                while ((index = posIterator.nextSetBit()) != BitSetIterator.NO_MORE) {
                    if (index >= objCount) {
                        break;
                    }
                    setter.set(index, NumOp.getLong(dataAddr, index));
                    count++;
                }
                break;
            }
        }
        return count;
    }

    @Override
    public void foreach(int start, int count, FloatSetter setter) {
        int end = start + count;
        switch (version) {
            case Version.VERSION_0_ID:
                for (int index = start; index < end; index++) {
                    setter.set(index, (float) Double.longBitsToDouble(NumOp.getVal(numType, dataAddr, index, minVal)));
                }
                break;
            default:
                for (int index = start; index < end; index++) {
                    setter.set(index, NumOp.getFloat(dataAddr, index));
                }
                break;
        }
    }

    public int foreach(BitMap position, FloatSetter setter) {
        if (position == BitMap.ALL || position == BitMap.SOME) {
            foreach(0, objCount, setter);
            return objCount;
        }
        OffheapBitMapIterator posIterator = position.iterator();
        int count = 0;
        switch (version) {
            case Version.VERSION_0_ID: {
                int index;
                while ((index = posIterator.nextSetBit()) != BitSetIterator.NO_MORE) {
                    if (index >= objCount) {
                        break;
                    }
                    setter.set(index, (float) Double.longBitsToDouble(NumOp.getVal(numType, dataAddr, index, minVal)));
                    count++;
                }
                break;
            }
            default: {
                int index;
                while ((index = posIterator.nextSetBit()) != BitSetIterator.NO_MORE) {
                    if (index >= objCount) {
                        break;
                    }
                    setter.set(index, NumOp.getFloat(dataAddr, index));
                    count++;
                }
                break;
            }
        }
        return count;
    }

    @Override
    public void foreach(int start, int count, DoubleSetter setter) {
        int end = start + count;
        switch (version) {
            case Version.VERSION_0_ID:
                for (int index = start; index < end; index++) {
                    setter.set(index, Double.longBitsToDouble(NumOp.getVal(numType, dataAddr, index, minVal)));
                }
                break;
            default:
                for (int index = start; index < end; index++) {
                    setter.set(index, NumOp.getDouble(dataAddr, index));
                }
                break;
        }
    }

    public int foreach(BitMap position, DoubleSetter setter) {
        if (position == BitMap.ALL || position == BitMap.SOME) {
            foreach(0, objCount, setter);
            return objCount;
        }
        OffheapBitMapIterator posIterator = position.iterator();
        int count = 0;
        switch (version) {
            case Version.VERSION_0_ID: {
                int index;
                while ((index = posIterator.nextSetBit()) != BitSetIterator.NO_MORE) {
                    if (index >= objCount) {
                        break;
                    }
                    setter.set(index, Double.longBitsToDouble(NumOp.getVal(numType, dataAddr, index, minVal)));
                    count++;
                }
                break;
            }
            default: {
                int index;
                while ((index = posIterator.nextSetBit()) != BitSetIterator.NO_MORE) {
                    if (index >= objCount) {
                        break;
                    }
                    setter.set(index, NumOp.getDouble(dataAddr, index));
                    count++;
                }
                break;
            }
        }
        return count;
    }

    // --------------------------------------------------------
    // Raw data pack.

    // v0:
    // | str_total_len | start0 | end0 | start1 | end1 | start2 | end2 | s0 | s1 | s2 |
    //         4       | <-                index(int)               -> |<- str_data ->|

    // v1 later:
    // | offset0 | offset1 | offset2 | offset3(str_total_len) | s0 | s1 | s2 |
    // | <-                   index(int)                   -> |<- str_data ->|


    private final int maxObjLen; // Only for v0.
    private static final long[] EMPTY_START_END = new long[]{0, 0};

    // Get the start and end pos of the raw type values.
    private long[] getRawValueStartEnd(int index) {
        long startAddr, endAddr;
        switch (version) {
            case Version.VERSION_0_ID: {
                if (maxObjLen == 0) {
                    return EMPTY_START_END;
                }
                long indexAddr = dataAddr + (index << 3) + 4;
                int str_offset = (objCount << 3) + 4;
                startAddr = MemoryUtil.getInt(indexAddr) + dataAddr + str_offset;
                endAddr = MemoryUtil.getInt(indexAddr + 4) + dataAddr + str_offset;
                break;
            }
            default: {
                long indexAddr = dataAddr + (index << 2);
                int str_offset = (objCount + 1) << 2;
                startAddr = MemoryUtil.getInt(indexAddr) + dataAddr + str_offset;
                endAddr = MemoryUtil.getInt(indexAddr + 4) + dataAddr + str_offset;
                break;
            }
        }
        return new long[]{startAddr, endAddr};
    }

    public final UTF8String stringValueAt(int index) {
        long startAddr, endAddr;
        switch (version) {
            case Version.VERSION_0_ID: {
                if (maxObjLen == 0) {
                    startAddr = 0;
                    endAddr = 0;
                } else {
                    long indexAddr = dataAddr + (index << 3) + 4;
                    int str_offset = (objCount << 3) + 4;
                    startAddr = MemoryUtil.getInt(indexAddr) + dataAddr + str_offset;
                    endAddr = MemoryUtil.getInt(indexAddr + 4) + dataAddr + str_offset;
                }
                break;
            }
            default: {
                long indexAddr = dataAddr + (index << 2);
                int str_offset = (objCount + 1) << 2;
                startAddr = MemoryUtil.getInt(indexAddr) + dataAddr + str_offset;
                endAddr = MemoryUtil.getInt(indexAddr + 4) + dataAddr + str_offset;
                break;
            }
        }

        if (startAddr == endAddr) {
            return UTF8String.EMPTY_UTF8;
        } else {
            return UTF8String.fromAddress(null, startAddr, (int) (endAddr - startAddr));
        }
    }

    public final void rawValueAt(int index, BytePiece bytes) {
        long startAddr, endAddr;
        switch (version) {
            case Version.VERSION_0_ID: {
                if (maxObjLen == 0) {
                    startAddr = 0;
                    endAddr = 0;
                } else {
                    long indexAddr = dataAddr + (index << 3) + 4;
                    int str_offset = (objCount << 3) + 4;
                    startAddr = MemoryUtil.getInt(indexAddr) + dataAddr + str_offset;
                    endAddr = MemoryUtil.getInt(indexAddr + 4) + dataAddr + str_offset;
                }
                break;
            }
            default: {
                long indexAddr = dataAddr + (index << 2);
                int str_offset = (objCount + 1) << 2;
                startAddr = MemoryUtil.getInt(indexAddr) + dataAddr + str_offset;
                endAddr = MemoryUtil.getInt(indexAddr + 4) + dataAddr + str_offset;
                break;
            }
        }

        bytes.base = null;
        bytes.addr = startAddr;
        bytes.len = (int) (endAddr - startAddr);
    }

    public final byte[] rawValueAt(int index) {
        long startAddr, endAddr;
        switch (version) {
            case Version.VERSION_0_ID: {
                if (maxObjLen == 0) {
                    startAddr = 0;
                    endAddr = 0;
                } else {
                    long indexAddr = dataAddr + (index << 3) + 4;
                    int str_offset = (objCount << 3) + 4;
                    startAddr = MemoryUtil.getInt(indexAddr) + dataAddr + str_offset;
                    endAddr = MemoryUtil.getInt(indexAddr + 4) + dataAddr + str_offset;
                }
                break;
            }
            default: {
                long indexAddr = dataAddr + (index << 2);
                int str_offset = (objCount + 1) << 2;
                startAddr = MemoryUtil.getInt(indexAddr) + dataAddr + str_offset;
                endAddr = MemoryUtil.getInt(indexAddr + 4) + dataAddr + str_offset;
                break;
            }
        }

        if (startAddr == endAddr) {
            return new byte[0];
        } else {
            int len = (int) (endAddr - startAddr);
            byte[] bytes = new byte[len];
            MemoryUtil.getBytes(startAddr, bytes, 0, len);
            return bytes;
        }
    }

    public final ByteBuffer valueAt(int index) {
        long startAddr, endAddr;
        switch (version) {
            case Version.VERSION_0_ID: {
                if (maxObjLen == 0) {
                    startAddr = 0;
                    endAddr = 0;
                } else {
                    long indexAddr = dataAddr + (index << 3) + 4;
                    int str_offset = (objCount << 3) + 4;
                    startAddr = MemoryUtil.getInt(indexAddr) + dataAddr + str_offset;
                    endAddr = MemoryUtil.getInt(indexAddr + 4) + dataAddr + str_offset;
                }
                break;
            }
            default: {
                long indexAddr = dataAddr + (index << 2);
                int str_offset = (objCount + 1) << 2;
                startAddr = MemoryUtil.getInt(indexAddr) + dataAddr + str_offset;
                endAddr = MemoryUtil.getInt(indexAddr + 4) + dataAddr + str_offset;
                break;
            }
        }

        if (startAddr == endAddr) {
            return ByteSlice.EmptyByteBuffer;
        } else {
            return MemoryUtil.getByteBuffer(startAddr, (int) (endAddr - startAddr), false);
        }
    }

    @Override
    public final void foreach(int start, int count, BytePieceSetter setter) {
        BytePiece bytes = new BytePiece();
        switch (version) {
            case Version.VERSION_0_ID:
                for (int index = start; index < start + count; index++) {
                    long startAddr, endAddr;

                    if (maxObjLen == 0) {
                        startAddr = 0;
                        endAddr = 0;
                    } else {
                        long indexAddr = dataAddr + (index << 3) + 4;
                        int str_offset = (objCount << 3) + 4;
                        startAddr = MemoryUtil.getInt(indexAddr) + dataAddr + str_offset;
                        endAddr = MemoryUtil.getInt(indexAddr + 4) + dataAddr + str_offset;
                    }

                    bytes.addr = startAddr;
                    bytes.len = (int) (endAddr - startAddr);
                    setter.set(index, bytes);
                }
                break;
            default:
                for (int index = start; index < start + count; index++) {
                    long startAddr, endAddr;

                    long indexAddr = dataAddr + (index << 2);
                    int str_offset = (objCount + 1) << 2;
                    startAddr = MemoryUtil.getInt(indexAddr) + dataAddr + str_offset;
                    endAddr = MemoryUtil.getInt(indexAddr + 4) + dataAddr + str_offset;

                    bytes.addr = startAddr;
                    bytes.len = (int) (endAddr - startAddr);
                    setter.set(index, bytes);
                }
        }

    }

    public int foreach(BitMap position, BytePieceSetter setter) {
        if (position == BitMap.ALL || position == BitMap.SOME) {
            foreach(0, objCount, setter);
            return objCount;
        }
        OffheapBitMapIterator posIterator = position.iterator();
        int count = 0;
        BytePiece bytes = new BytePiece();
        int index;

        switch (version) {
            case Version.VERSION_0_ID:
                while ((index = posIterator.nextSetBit()) != BitSetIterator.NO_MORE) {
                    if (index >= objCount) {
                        break;
                    }

                    long startAddr, endAddr;
                    if (maxObjLen == 0) {
                        startAddr = 0;
                        endAddr = 0;
                    } else {
                        long indexAddr = dataAddr + (index << 3) + 4;
                        int str_offset = (objCount << 3) + 4;
                        startAddr = MemoryUtil.getInt(indexAddr) + dataAddr + str_offset;
                        endAddr = MemoryUtil.getInt(indexAddr + 4) + dataAddr + str_offset;
                    }

                    bytes.addr = startAddr;
                    bytes.len = (int) (endAddr - startAddr);
                    setter.set(index, bytes);

                    count++;
                }
                break;
            default:
                while ((index = posIterator.nextSetBit()) != BitSetIterator.NO_MORE) {
                    if (index >= objCount) {
                        break;
                    }

                    long startAddr, endAddr;

                    long indexAddr = dataAddr + (index << 2);
                    int str_offset = (objCount + 1) << 2;
                    startAddr = MemoryUtil.getInt(indexAddr) + dataAddr + str_offset;
                    endAddr = MemoryUtil.getInt(indexAddr + 4) + dataAddr + str_offset;

                    bytes.addr = startAddr;
                    bytes.len = (int) (endAddr - startAddr);
                    setter.set(index, bytes);

                    count++;
                }
        }

        return count;
    }

    // --------------------------------------------------------
    // Utils.

    /**
     * Get pack valueCount from row valueCount.
     */
    public static int rowCountToPackCount(long count) {
        return (int) ((count + MASK) >>> SHIFT);
    }

    /**
     * Get the packId this row lays in by rowId in column
     */
    public static int packIdByRowId(long rowId) {
        return (int) (rowId >>> SHIFT);
    }

    /**
     * Get the rowId in its pack by rowId in column.
     */
    public static int rowInPackByRowId(long rowId) {
        return (int) (rowId & MASK);
    }

    /**
     * Get the row valueCount of the pack by packId.
     */
    public static int packRowCount(long rowCount, int packId) {
        int packCount = rowCountToPackCount(rowCount);
        assert packId >= 0 && packId < packCount;
        if (packId < packCount - 1) {
            return DataPack.MAX_COUNT;
        } else {
            int packRowCount = (int) (rowCount & MASK);
            return packRowCount == 0 ? DataPack.MAX_COUNT : packRowCount;
        }
    }


    // --------------------------------------------------------
    // Factory.

    public static interface Factory {
        PackBundle createPackBundle(int version, SegmentMode mode, byte dataType, boolean isIndexed, VirtualDataPack cache);
    }
}
