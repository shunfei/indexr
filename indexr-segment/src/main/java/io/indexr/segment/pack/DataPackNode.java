package io.indexr.segment.pack;

import com.google.common.base.Preconditions;

import java.io.IOException;

import io.indexr.io.ByteBufferWriter;
import io.indexr.io.ByteSlice;

public class DataPackNode {
    // If update any fields, remember to keep in sync with SERIALIZED_SIZE.

    private byte packType;
    private int objCount;

    private long packAddr;  // Pack offset in file.
    private int packSize;   // Size of pack.

    // For number type.
    private byte numType;
    // Min/max value in uniform value format. Only used in compression.
    private long uniformMin;
    private long uniformMax;

    // For raw type. Only used in v0.
    private int maxObjLen;

    private boolean compress;

    private long indexAddr; // Index offset in file.
    private int indexSize;  // Size of index.

    // Min/max value in original value format
    private long minValue;
    private long maxValue;

    // ------------------------------

    private final int version;

    public static final int SERIALIZED_SIZE = 1 + 4 + 8 + 4 + 1 + 8 + 8 + 4 + 1 + 8 + 4 + 8 + 8;

    public DataPackNode(int version) {
        this.version = version;
        this.compress = true; // compress by default.
    }

    public static DataPackNode from(int version, ByteSlice buffer) {
        Preconditions.checkArgument(buffer.size() == SERIALIZED_SIZE);

        DataPackNode dpn = new DataPackNode(version);
        int offset = 0;
        dpn.packType = buffer.get(offset += 0);
        dpn.objCount = buffer.getInt(offset += 1);
        dpn.packAddr = buffer.getLong(offset += 4);
        dpn.packSize = buffer.getInt(offset += 8);
        dpn.numType = buffer.get(offset += 4);
        dpn.uniformMin = buffer.getLong(offset += 1);
        dpn.uniformMax = buffer.getLong(offset += 8);

        if (version <= Version.VERSION_1_ID) {
            // It is a very stupid bug exists only before version_1.
            // The offset after uniformMax should plus 8 not 4.
            // But lots of data already generated so we have to keep this code.
            dpn.maxObjLen = buffer.getInt(offset += 4);
        } else {
            dpn.maxObjLen = buffer.getInt(offset += 8);
        }

        dpn.compress = (buffer.get(offset += 4) & 0x01) == 1;
        dpn.indexAddr = buffer.getLong(offset += 1);
        dpn.indexSize = buffer.getInt(offset += 8);
        dpn.minValue = buffer.getLong(offset += 4);
        dpn.maxValue = buffer.getLong(offset += 8);
        offset += 8;

        if (version <= Version.VERSION_1_ID) {
            assert offset == SERIALIZED_SIZE - 4;
            // Try to do some fix.
            if (dpn.maxValue != 0 && dpn.uniformMax == 0) {
                dpn.uniformMax = dpn.maxValue;
            }
        } else {
            assert offset == SERIALIZED_SIZE;
        }
        return dpn;
    }

    public void write(ByteBufferWriter writer) throws IOException {
        ByteSlice tmp = ByteSlice.allocateDirect(SERIALIZED_SIZE);
        int offset = 0;
        tmp.put(offset += 0, packType);
        tmp.putInt(offset += 1, objCount);
        tmp.putLong(offset += 4, packAddr);
        tmp.putInt(offset += 8, packSize);
        tmp.put(offset += 4, numType);
        tmp.putLong(offset += 1, uniformMin);
        tmp.putLong(offset += 8, uniformMax);
        if (version <= Version.VERSION_1_ID) {
            tmp.putInt(offset += 4, maxObjLen);
        } else {
            tmp.putInt(offset += 8, maxObjLen);
        }
        tmp.put(offset += 4, (byte) (compress ? 1 : 0));
        tmp.putLong(offset += 1, indexAddr);
        tmp.putInt(offset += 8, indexSize);
        tmp.putLong(offset += 4, minValue);
        tmp.putLong(offset += 8, maxValue);
        offset += 8;

        if (version <= Version.VERSION_1_ID) {
            assert offset == SERIALIZED_SIZE - 4;
        } else {
            assert offset == SERIALIZED_SIZE;
        }

        writer.write(tmp.byteBuffer(), SERIALIZED_SIZE);
    }

    public DataPackNode clone() {
        DataPackNode newDPN = new DataPackNode(this.version);
        newDPN.packType = this.packType;
        newDPN.objCount = this.objCount;
        newDPN.packAddr = this.packAddr;
        newDPN.packSize = this.packSize;
        newDPN.numType = this.numType;
        newDPN.uniformMin = this.uniformMin;
        newDPN.uniformMax = this.uniformMax;
        newDPN.maxObjLen = this.maxObjLen;
        newDPN.compress = this.compress;
        newDPN.indexAddr = this.indexAddr;
        newDPN.indexSize = this.indexSize;
        newDPN.minValue = this.minValue;
        newDPN.maxValue = this.maxValue;
        return newDPN;
    }

    // @formatter:off

    public boolean isFull() { return objCount() >= DataPack.MAX_COUNT; }

    public int objCount() { return objCount; }
    void setObjCount(int objCount) { this.objCount = objCount; }

    public byte packType() { return packType; }
    void setPackType(byte packType) { this.packType = packType; }

    public long packAddr() { return packAddr; }
    void setPackAddr(long packAddr) { this.packAddr = packAddr; }

    public int packSize() { return packSize; }
    void setPackSize(int packSize) { this.packSize = packSize; }

    public byte numType() { return numType; }
    void setNumType(byte numType) { this.numType = numType; }

    public long uniformMin() { return uniformMin; }
    void setUniformMin(long val) {  this.uniformMin = val; }

    public long uniformMax() { return uniformMax; }
    void setUniformMax(long val) { this.uniformMax = val; }

    public int maxObjLen() { return maxObjLen; }
    void setMaxObjLen(int val) { maxObjLen = val; }

    public boolean compress() { return compress; }
    void setCompress(boolean v){ compress = v; }

    public int indexSize() { return indexSize; }
    public void setIndexSize(int indexSize) { this.indexSize = indexSize; }

    public long indexAddr() { return indexAddr; }
    public void setIndexAddr(long indexAddr) { this.indexAddr = indexAddr; }

    public long minValue() { return minValue; }
    public void setMinValue(long minValue) { this.minValue = minValue; }

    public long maxValue() { return maxValue; }
    public void setMaxValue(long maxValue) { this.maxValue = maxValue; }

    public int version() { return version; }

    // @formatter:on


    @Override
    public String toString() {
        return "DataPackNode{" +
                "packType=" + packType +
                ", objCount=" + objCount +
                ", packAddr=" + packAddr +
                ", packSize=" + packSize +
                ", numType=" + numType +
                ", uniformMin=" + uniformMin +
                ", uniformMax=" + uniformMax +
                ", maxObjLen=" + maxObjLen +
                ", compress=" + compress +
                ", indexAddr=" + indexAddr +
                ", indexSize=" + indexSize +
                ", minValue=" + minValue +
                ", maxValue=" + maxValue +
                '}';
    }
}
