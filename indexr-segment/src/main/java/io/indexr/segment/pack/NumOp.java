package io.indexr.segment.pack;

import java.nio.ByteBuffer;

import io.indexr.compress.bh.BHCompressor;
import io.indexr.io.ByteSlice;
import io.indexr.util.MemoryUtil;

public class NumOp {

    public static int getInt(long addr, int index) {
        return MemoryUtil.getInt(addr + (index << 2));
    }

    public static long getLong(long addr, int index) {
        return MemoryUtil.getLong(addr + (index << 3));
    }

    public static float getFloat(long addr, int index) {
        return MemoryUtil.getFloat(addr + (index << 2));
    }

    public static double getDouble(long addr, int index) {
        return MemoryUtil.getDouble(addr + (index << 3));
    }

    public static void putInt(long addr, int index, int v) {
        MemoryUtil.setInt(addr + (index << 2), v);
    }

    public static void putLong(long addr, int index, long v) {
        MemoryUtil.setLong(addr + (index << 3), v);
    }

    public static void putFloat(long addr, int index, float v) {
        MemoryUtil.setFloat(addr + (index << 2), v);
    }

    public static void putDouble(long addr, int index, double v) {
        MemoryUtil.setDouble(addr + (index << 3), v);
    }

    public static ByteSlice allocatByteSlice(byte type, int size) {
        switch (type) {
            case NumType.NZero:
                return ByteSlice.empty();
            case NumType.NByte:
                return ByteSlice.allocateDirect(size);
            case NumType.NShort:
                return ByteSlice.allocateDirect(size << 1);
            case NumType.NInt:
                return ByteSlice.allocateDirect(size << 2);
            case NumType.NLong:
                return ByteSlice.allocateDirect(size << 3);
            default:
                throw new UnsupportedOperationException();
        }
    }

    public static ByteSlice bhcompress(byte type, ByteSlice data, int itemSize, long minVal, long maxVal) {
        switch (type) {
            case NumType.NZero:
                return ByteSlice.empty();
            case NumType.NByte:
                return BHCompressor.compressByte(data, itemSize, maxVal - minVal);
            case NumType.NShort:
                return BHCompressor.compressShort(data, itemSize, maxVal - minVal);
            case NumType.NInt:
                return BHCompressor.compressInt(data, itemSize, maxVal - minVal);
            case NumType.NLong:
                return BHCompressor.compressLong(data, itemSize, maxVal - minVal);
            default:
                throw new UnsupportedOperationException();
        }
    }

    public static ByteSlice bhdecompress(byte type, ByteSlice cmpData, int itemSize, long minVal, long maxVal) {
        switch (type) {
            case NumType.NZero:
                return ByteSlice.empty();
            case NumType.NByte:
                return BHCompressor.decompressByte(cmpData, itemSize, maxVal - minVal);
            case NumType.NShort:
                return BHCompressor.decompressShort(cmpData, itemSize, maxVal - minVal);
            case NumType.NInt:
                return BHCompressor.decompressInt(cmpData, itemSize, maxVal - minVal);
            case NumType.NLong:
                return BHCompressor.decompressLong(cmpData, itemSize, maxVal - minVal);
            default:
                throw new UnsupportedOperationException();
        }
    }

    //=======================================================================
    // VERSION_0 deprecated

    public static long getVal(byte type, long addr, int index, long minVal) {
        switch (type) {
            case NumType.NZero:
                return minVal;
            case NumType.NByte:
                return minVal + MemoryUtil.getByte(addr + index);
            case NumType.NShort:
                return minVal + MemoryUtil.getShort(addr + (index << 1));
            case NumType.NInt:
                return minVal + MemoryUtil.getInt(addr + (index << 2));
            case NumType.NLong:
                return minVal + MemoryUtil.getLong(addr + (index << 3));
            default:
                throw new UnsupportedOperationException();
        }
    }

    public static long getVal(byte type, ByteSlice data, int index, long minVal) {
        switch (type) {
            case NumType.NZero:
                return minVal;
            case NumType.NByte:
                return minVal + data.get(index);
            case NumType.NShort:
                return minVal + data.getShort(index << 1);
            case NumType.NInt:
                return minVal + data.getInt(index << 2);
            case NumType.NLong:
                return minVal + data.getLong(index << 3);
            default:
                throw new UnsupportedOperationException();
        }
    }

    public static void putVal(byte type, ByteSlice data, int index, long val, long minVal) {
        switch (type) {
            case NumType.NZero:
                break;
            case NumType.NByte:
                data.put(index, (byte) (val - minVal));
                break;
            case NumType.NShort:
                data.putShort(index << 1, (short) (val - minVal));
                break;
            case NumType.NInt:
                data.putInt(index << 2, (int) (val - minVal));
                break;
            case NumType.NLong:
                data.putLong(index << 3, val - minVal);
                break;
            default:
                throw new UnsupportedOperationException();
        }
    }

    public static void main(String[] args) {
        ByteBuffer bb = ByteBuffer.allocateDirect(1000);
        bb.putFloat(0, 1.1f);
        int a = bb.getInt(0);
        float f = (float) Double.longBitsToDouble((long) a);
        System.out.println(f);
    }
}
