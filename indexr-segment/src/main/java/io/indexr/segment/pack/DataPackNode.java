package io.indexr.segment.pack;

import java.io.IOException;

import io.indexr.io.ByteBufferWriter;
import io.indexr.io.ByteSlice;
import io.indexr.segment.SegmentMode;

public abstract class DataPackNode {
    protected int objCount;

    protected boolean compress;

    protected long packAddr;  // Pack offset in file.
    protected int packSize;   // Size of pack in compressed status.

    protected long indexAddr; // Index offset in file.
    protected int indexSize;  // Size of index.

    protected long extIndexAddr;
    protected int extIndexSize;

    // Min/max value in original value format
    protected long minValue;
    protected long maxValue;

    // ======================

    // Those not stored.
    protected final int version;
    protected final SegmentMode mode;

    public DataPackNode(int version, SegmentMode mode) {
        this.version = version;
        this.mode = mode;
    }

    //public static int serializedSize(int version) {
    //    if (version < Version.VERSION_6_ID) {
    //        return 1 + 4 + 8 + 4 + 1 + 8 + 8 + 4 + 1 + 8 + 4 + 8 + 8;
    //    } else {
    //        return 1 + 4 + 8 + 4 + 1 + 8 + 8 + 4 + 1 + 8 + 4 + 8 + 8 + 8 + 4;
    //    }
    //}

    public int version() { return version; }

    public SegmentMode mode() {return mode;}

    public boolean isDictEncoded() {return false;}

    // @formatter:off
    public boolean isFull() { return objCount() >= DataPack.MAX_COUNT; }

    public int objCount() { return objCount; }
    public void setObjCount(int objCount) { this.objCount = objCount; }

    public long packAddr() { return packAddr; }
    public void setPackAddr(long packAddr) { this.packAddr = packAddr; }

    public int packSize() { return packSize; }
    public void setPackSize(int packSize) { this.packSize = packSize; }

    public int indexSize() { return indexSize; }
    public void setIndexSize(int indexSize) { this.indexSize = indexSize; }

    public long indexAddr() { return indexAddr; }
    public void setIndexAddr(long indexAddr) { this.indexAddr = indexAddr; }

    public int extIndexSize() { return extIndexSize; }
    public void setExtIndexSize(int size) { this.extIndexSize = size; }

    public long extIndexAddr() { return extIndexAddr; }
    public void setExtIndexAddr(long addr) { this.extIndexAddr = addr; }

    public long minValue() { return minValue; }
    public void setMinValue(long minValue) { this.minValue = minValue; }

    public long maxValue() { return maxValue; }
    public void setMaxValue(long maxValue) { this.maxValue = maxValue; }

    public boolean compress() { return compress; }

    // @formatter:on

    public abstract DataPackNode clone();

    public abstract void write(ByteBufferWriter writer) throws IOException;

    public static interface Factory {
        DataPackNode create(int version, SegmentMode mode);

        DataPackNode create(int version, SegmentMode mode, ByteSlice buffer);
    }
}
