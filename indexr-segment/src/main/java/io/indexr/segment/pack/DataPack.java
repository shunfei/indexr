package io.indexr.segment.pack;

import com.google.common.base.Preconditions;

import org.apache.spark.unsafe.types.UTF8String;

import java.io.IOException;
import java.nio.ByteBuffer;

import io.indexr.data.BytePiece;
import io.indexr.data.BytePieceSetter;
import io.indexr.data.DoubleSetter;
import io.indexr.data.FloatSetter;
import io.indexr.data.Freeable;
import io.indexr.data.IntSetter;
import io.indexr.data.LongSetter;
import io.indexr.data.Sizable;
import io.indexr.io.ByteBufferWriter;
import io.indexr.io.ByteSlice;
import io.indexr.segment.ColumnType;
import io.indexr.segment.DPValues;
import io.indexr.util.MemoryUtil;

public final class DataPack implements DPValues, Freeable, Sizable {
    public static final int SHIFT = 16;
    public static final int MASK = 0XFFFF;
    public static final int MAX_COUNT = 1 << SHIFT;

    final int version;

    ByteSlice data; // Hold a reference, prevent memory from gc.
    long dataAddr;
    ByteSlice cmpData;
    final int objCount;
    final byte type;

    public DataPack(ByteSlice data, ByteSlice cmpData, DataPackNode dpn) {
        this.data = data;
        if (data != null && data.size() > 0) {
            this.dataAddr = data.address();
        }
        this.cmpData = cmpData;
        this.type = dpn.packType();

        this.version = dpn.version();
        this.objCount = dpn.objCount();
        this.numType = dpn.numType();
        this.minVal = dpn.uniformMin();
        this.maxObjLen = dpn.maxObjLen();
    }

    public int version() {
        return version;
    }

    long dataAddr() {
        return dataAddr;
    }

    public ByteSlice data() {
        return data;
    }

    public ByteSlice cmpData() {
        return cmpData;
    }

    public int objCount() {
        return objCount;
    }

    public boolean isFull() {
        return objCount() >= MAX_COUNT;
    }

    /**
     * Bytes on disk.
     */
    public int serializedSize() {
        return cmpData.size();
    }

    /**
     * Bytes in uncompressed.
     */
    @Override
    public long size() {
        return data.size();
    }

    public static DataPack from(ByteSlice buffer, DataPackNode dpn) {
        return new DataPack(null, buffer, dpn);
    }

    @Override
    public void free() {
        if (data != null) {
            data.free();
            data = null;
        }
        if (cmpData != null) {
            cmpData.free();
            cmpData = null;
        }
    }

    @Override
    public int count() {
        return objCount();
    }

    public final void compress(DataPackNode dpn) {
        long time = System.currentTimeMillis();

        Preconditions.checkState(data != null && cmpData == null);
        if (dpn.compress()) {
            cmpData = doCompress(dpn, data);
            data.free();
            data = null;
        } else {
            cmpData = data;
        }

        data = null;
        dataAddr = 0;

        PackDurationStat.INSTANCE.add_compressPack(System.currentTimeMillis() - time);
    }

    public final void decompress(DataPackNode dpn) {
        long time = System.currentTimeMillis();

        Preconditions.checkState(data == null && cmpData != null);
        if (dpn.compress()) {
            data = doDecompress(dpn, cmpData);
            cmpData.free();
        } else {
            data = cmpData;
        }
        dataAddr = data.address();

        cmpData = null;

        PackDurationStat.INSTANCE.add_decompressPack(System.currentTimeMillis() - time);
    }

    ByteSlice doCompress(DataPackNode dpn, ByteSlice data) {
        if (type == DataPackType.Number) {
            return DataPack_N.doCompress(this, dpn, data);
        } else {
            return DataPack_R.doCompress(this, dpn, data);
        }
    }

    ByteSlice doDecompress(DataPackNode dpn, ByteSlice cmpData) {
        if (type == DataPackType.Number) {
            return DataPack_N.doDecompress(this, dpn, cmpData);
        } else {
            return DataPack_R.doDecompress(this, dpn, cmpData);
        }
    }

    public void write(ByteBufferWriter writer) throws IOException {
        Preconditions.checkState(cmpData != null);
        writer.write(cmpData.toByteBuffer(), cmpData.size());
    }

    // --------------------------------------------------------
    // Number data pack.

    byte numType;
    long minVal;

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

    // --------------------------------------------------------
    // Raw data pack.

    // v0:
    // | str_total_len | start0 | end0 | start1 | end1 | start2 | end2 | s0 | s1 | s2 |
    //         4       | <-                index(int)               -> |<- str_data ->|

    // after v1:
    // | offset0 | offset1 | offset2 | offset3(str_total_len) | s0 | s1 | s2 |
    // | <-                   index(int)                   -> |<- str_data ->|

    int maxObjLen; // Only for v0.

    @Override
    public UTF8String stringValueAt(int index) {
        long startAddr, endAddr;
        switch (version) {
            case Version.VERSION_0_ID: {
                if (maxObjLen == 0) {
                    return UTF8String.EMPTY_UTF8;
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
        if (startAddr == endAddr) {
            return UTF8String.EMPTY_UTF8;
        } else {
            return UTF8String.fromAddress(null, startAddr, (int) (endAddr - startAddr));
        }
    }

    @Override
    public void rawValueAt(int index, BytePiece bytes) {
        long startAddr, endAddr;
        switch (version) {
            case Version.VERSION_0_ID: {
                if (maxObjLen == 0) {
                    bytes.addr = 0;
                    bytes.len = 0;
                    return;
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
        bytes.base = null;
        bytes.addr = startAddr;
        bytes.len = (int) (endAddr - startAddr);
    }

    @Override
    public byte[] rawValueAt(int index) {
        long startAddr, endAddr;
        switch (version) {
            case Version.VERSION_0_ID: {
                if (maxObjLen == 0) {
                    return new byte[0];
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
        if (startAddr == endAddr) {
            return new byte[0];
        } else {
            int len = (int) (endAddr - startAddr);
            byte[] bytes = new byte[len];
            MemoryUtil.getBytes(startAddr, bytes, 0, len);
            return bytes;
        }
    }

    public ByteBuffer valueAt(int index) {
        long startAddr, endAddr;
        switch (version) {
            case Version.VERSION_0_ID: {
                if (maxObjLen == 0) {
                    return ByteSlice.EmptyByteBuffer;
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
        if (startAddr == endAddr) {
            return ByteSlice.EmptyByteBuffer;
        } else {
            return MemoryUtil.getByteBuffer(startAddr, (int) (endAddr - startAddr), false);
        }
    }

    @Override
    public void foreach(int start, int count, BytePieceSetter setter) {
        BytePiece bytes = new BytePiece();
        int end = start + count;
        switch (version) {
            case Version.VERSION_0_ID: {
                int str_offset = (objCount << 3) + 4;
                for (int index = start; index < end; index++) {
                    if (maxObjLen == 0) {
                        bytes.addr = 0;
                        bytes.len = 0;
                        continue;
                    }
                    long indexAddr = dataAddr + (index << 3) + 4;
                    long startAddr = MemoryUtil.getInt(indexAddr) + dataAddr + str_offset;
                    long endAddr = MemoryUtil.getInt(indexAddr + 4) + dataAddr + str_offset;

                    bytes.addr = startAddr;
                    bytes.len = (int) (endAddr - startAddr);
                    setter.set(index, bytes);
                }
                break;
            }
            default: {
                int str_offset = (objCount + 1) << 2;
                for (int index = start; index < end; index++) {
                    long indexAddr = dataAddr + (index << 2);
                    long startAddr = MemoryUtil.getInt(indexAddr) + dataAddr + str_offset;
                    long endAddr = MemoryUtil.getInt(indexAddr + 4) + dataAddr + str_offset;

                    bytes.addr = startAddr;
                    bytes.len = (int) (endAddr - startAddr);
                    setter.set(index, bytes);
                }
                break;
            }
        }
    }


    // --------------------------------------------------------
    // Utils.

    /**
     * Get pack count from row count.
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
     * Get the row count of the pack by packId.
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
}
