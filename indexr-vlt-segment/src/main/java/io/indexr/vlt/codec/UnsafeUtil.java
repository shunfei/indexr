package io.indexr.vlt.codec;

import com.google.common.base.Preconditions;

import com.sun.jna.Native;

import org.apache.spark.unsafe.Platform;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import io.indexr.util.MemoryUtil;

/**
 * We put some memory utils here.
 */
public class UnsafeUtil {
    public static final int BYTE_ARRAY_OFFSET = Platform.BYTE_ARRAY_OFFSET;

    public static final int INT_ARRAY_OFFSET = Platform.INT_ARRAY_OFFSET;

    public static final int LONG_ARRAY_OFFSET = Platform.LONG_ARRAY_OFFSET;

    public static final int FLOAT_ARRAY_OFFSET = MemoryUtil.unsafe.arrayBaseOffset(float[].class);

    public static final int DOUBLE_ARRAY_OFFSET = Platform.DOUBLE_ARRAY_OFFSET;

    static {
        // Dealing with both big and little endian is either too annoying or hurt performance too much.
        // Since big endian machines are going to disappear (really?), we choose to fool ourself here.
        Preconditions.checkState(ByteOrder.nativeOrder().equals(ByteOrder.LITTLE_ENDIAN), "We only support little endian system, for now!");
    }

    public static byte getByte(Object base, long addr) {
        return Platform.getByte(base, addr);
    }

    public static byte getByte(long addr) {
        return MemoryUtil.getByte(addr);
    }

    public static void setByte(Object base, long addr, byte v) {
        Platform.putByte(base, addr, v);
    }

    public static void setByte(long addr, byte v) {
        MemoryUtil.setByte(addr, v);
    }

    public static void setBytes(long addr, byte[] bytes, int offset, int count) {
        MemoryUtil.setBytes(addr, bytes, offset, count);
    }

    public static int getInt(Object base, long addr) {
        return Platform.getInt(base, addr);
    }

    public static int getInt(long addr) {
        return MemoryUtil.getInt(addr);
    }

    public static void setInt(Object base, long addr, int v) {
        Platform.putInt(base, addr, v);
    }

    public static void setInt(long addr, int v) {
        MemoryUtil.setInt(addr, v);
    }

    public static long getLong(Object base, long addr) {
        return Platform.getLong(base, addr);
    }

    public static long getLong(long addr) {
        return MemoryUtil.getLong(addr);
    }

    public static void setLong(Object base, long addr, long v) {
        Platform.putLong(base, addr, v);
    }

    public static void setLong(long addr, long v) {
        MemoryUtil.setLong(addr, v);
    }

    public static float getFloat(Object base, long addr) {
        return Platform.getFloat(base, addr);
    }

    public static float getFloat(long addr) {
        return MemoryUtil.getFloat(addr);
    }

    public static void setFloat(Object base, long addr, float v) {
        Platform.putFloat(base, addr, v);
    }

    public static void setFloat(long addr, float v) {
        MemoryUtil.setFloat(addr, v);
    }

    public static double getDouble(Object base, long addr) {
        return Platform.getDouble(base, addr);
    }

    public static double getDouble(long addr) {
        return MemoryUtil.getDouble(addr);
    }

    public static void setDouble(Object base, long addr, double v) {
        Platform.putDouble(base, addr, v);
    }

    public static void setDouble(long addr, double v) {
        MemoryUtil.setDouble(addr, v);
    }

    public static long getByteBufferAddr(ByteBuffer buffer) {
        return MemoryUtil.getAddress(buffer);
    }

    public static void copyMemory(long fromAddr, long toAddr, int count) {
        MemoryUtil.copyMemory(fromAddr, toAddr, count);
    }

    public static void copyMemory(
            Object src, long srcOffset, Object dst, long dstOffset, long length) {
        MemoryUtil.copyMemory(src, srcOffset, dst, dstOffset, length);
    }

    public static long allocate(long size) {
        return Native.malloc(size);
    }

    public static void free(long addr) {
        Native.free(addr);
    }
}
