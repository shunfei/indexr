package io.indexr.segment.pack;

import java.io.IOException;

import io.indexr.io.ByteBufferWriter;
import io.indexr.io.ByteSlice;
import io.indexr.segment.SegmentMode;
import io.indexr.segment.storage.Version;

public class DataPackNode_Basic extends DataPackNode {
    // If update any fields, remember to keep in sync with SERIALIZED_SIZE.

    /**
     * means {@link io.indexr.segment.pack.DataPackType},
     */
    private byte packType;

    // For number type.
    /** {@link NumType} */
    private byte numType;
    // Min/max value in uniform value format. Only used in compression.
    private long uniformMin;
    private long uniformMax;

    // For raw type. Only used in v0.
    private int maxObjLen;

    // ------------------------------

    /** The size less than V6 */
    public static final int SIZE_LE_V6 = 1 + 4 + 8 + 4 + 1 + 8 + 8 + 4 + 1 + 8 + 4 + 8 + 8;
    /** The size greater or equal to V6 */
    public static final int SIZE_GE_V6 = 1 + 4 + 8 + 4 + 1 + 8 + 8 + 4 + 1 + 8 + 4 + 8 + 8 + 8 + 4;

    private DataPackNode_Basic(int version, SegmentMode mode) {
        this(version, mode, mode.versionAdapter.isCompress(version, mode));
    }

    private DataPackNode_Basic(int version, SegmentMode mode, boolean compress) {
        super(version, mode);
        // Here VersionAdapter.isCompress(version, mode) could be differed from compress.
        // Because in the old days we do not have the version and the mode, and the compress is set manually.
        // It doesn't really matter as we always use the compress value passed in.
        this.compress = compress;
    }

    public static DataPackNode_Basic from(int version, SegmentMode mode, ByteSlice buffer) {
        DataPackNode_Basic dpn = new DataPackNode_Basic(version, mode);
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
        if (version >= Version.VERSION_6_ID) {
            dpn.extIndexAddr = buffer.getLong(offset += 8);
            dpn.extIndexSize = buffer.getInt(offset += 8);
        }
        offset += 4;

        if (version <= Version.VERSION_1_ID) {
            // Try to do some fix.
            if (dpn.maxValue != 0 && dpn.uniformMax == 0) {
                dpn.uniformMax = dpn.maxValue;
            }
        }

        return dpn;
    }

    public void write(ByteBufferWriter writer) throws IOException {
        ByteSlice tmp = ByteSlice.allocateDirect(mode.versionAdapter.dpnSize(version, mode));
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
        if (version >= Version.VERSION_6_ID) {
            tmp.putLong(offset += 8, extIndexAddr);
            tmp.putInt(offset += 8, extIndexSize);
        }
        offset += 4;

        writer.write(tmp.byteBuffer());
    }

    public DataPackNode_Basic clone() {
        DataPackNode_Basic newDPN = new DataPackNode_Basic(this.version, this.mode, this.compress);
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
        newDPN.extIndexAddr = this.extIndexAddr;
        newDPN.extIndexSize = this.extIndexSize;
        return newDPN;
    }

    // @formatter:off

    public byte packType() { return packType; }
   public void setPackType(byte packType) { this.packType = packType; }

    public byte numType() { return numType; }
    public void setNumType(byte numType) { this.numType = numType; }

    public long uniformMin() { return uniformMin; }
    public void setUniformMin(long val) {  this.uniformMin = val; }

    public long uniformMax() { return uniformMax; }
    public void setUniformMax(long val) { this.uniformMax = val; }

    public int maxObjLen() { return maxObjLen; }
    public void setMaxObjLen(int val) { maxObjLen = val; }

    // @formatter:on

    @Override
    public String toString() {
        return "DataPackNode{" +
                "packType=" + packType +
                ", valueCount=" + objCount +
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
                ", extIndexAddr=" + extIndexAddr +
                ", extIndexSize=" + extIndexSize +
                ", version=" + version +
                '}';
    }

    public static final Factory factory = new Factory() {
        @Override
        public DataPackNode create(int version, SegmentMode mode) {
            return new DataPackNode_Basic(version, mode);
        }

        @Override
        public DataPackNode create(int version, SegmentMode mode, ByteSlice buffer) {
            return DataPackNode_Basic.from(version, mode, buffer);
        }
    };
}
