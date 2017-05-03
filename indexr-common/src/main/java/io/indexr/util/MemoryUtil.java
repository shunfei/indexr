package io.indexr.util;

import com.sun.jna.Native;
import com.sun.management.OperatingSystemMXBean;

import sun.misc.Cleaner;
import sun.misc.Unsafe;
import sun.nio.ch.DirectBuffer;

import java.lang.management.ManagementFactory;
import java.lang.reflect.Field;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

// Copies from org.apache.cassandra.io.util.MemoryUtil.java
public class MemoryUtil {
    private static final long UNSAFE_COPY_THRESHOLD = 1024 * 1024L; // copied from java.nio.Bits

    public static final Unsafe unsafe;
    private static final Class<?> DIRECT_BYTE_BUFFER_CLASS;
    private static final long DIRECT_BYTE_BUFFER_ADDRESS_OFFSET;
    private static final long DIRECT_BYTE_BUFFER_CAPACITY_OFFSET;
    private static final long DIRECT_BYTE_BUFFER_LIMIT_OFFSET;
    private static final long DIRECT_BYTE_BUFFER_POSITION_OFFSET;
    private static final long DIRECT_BYTE_BUFFER_ATTACHMENT_OFFSET;
    private static final long DIRECT_BYTE_BUFFER_CLEANER;
    private static final Class<?> BYTE_BUFFER_CLASS;
    private static final long BYTE_BUFFER_OFFSET_OFFSET;
    private static final long BYTE_BUFFER_HB_OFFSET;
    private static final long BYTE_ARRAY_BASE_OFFSET;

    private static final long STRING_VALUE_OFFSET;

    private static final boolean BIG_ENDIAN = ByteOrder.nativeOrder().equals(ByteOrder.BIG_ENDIAN);

    static {
        if (BIG_ENDIAN) {
            throw new RuntimeException("We only suppot littel endian platform!");
        }
        String arch = System.getProperty("os.arch");
        try {
            Field field = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            unsafe = (sun.misc.Unsafe) field.get(null);
            Class<?> clazz = ByteBuffer.allocateDirect(0).getClass();
            DIRECT_BYTE_BUFFER_ADDRESS_OFFSET = unsafe.objectFieldOffset(Buffer.class.getDeclaredField("address"));
            DIRECT_BYTE_BUFFER_CAPACITY_OFFSET = unsafe.objectFieldOffset(Buffer.class.getDeclaredField("capacity"));
            DIRECT_BYTE_BUFFER_LIMIT_OFFSET = unsafe.objectFieldOffset(Buffer.class.getDeclaredField("limit"));
            DIRECT_BYTE_BUFFER_POSITION_OFFSET = unsafe.objectFieldOffset(Buffer.class.getDeclaredField("position"));
            DIRECT_BYTE_BUFFER_ATTACHMENT_OFFSET = unsafe.objectFieldOffset(clazz.getDeclaredField("att"));
            DIRECT_BYTE_BUFFER_CLEANER = unsafe.objectFieldOffset(clazz.getDeclaredField("cleaner"));
            DIRECT_BYTE_BUFFER_CLASS = clazz;

            clazz = ByteBuffer.allocate(0).getClass();
            BYTE_BUFFER_OFFSET_OFFSET = unsafe.objectFieldOffset(ByteBuffer.class.getDeclaredField("offset"));
            BYTE_BUFFER_HB_OFFSET = unsafe.objectFieldOffset(ByteBuffer.class.getDeclaredField("hb"));
            BYTE_BUFFER_CLASS = clazz;

            BYTE_ARRAY_BASE_OFFSET = unsafe.arrayBaseOffset(byte[].class);

            STRING_VALUE_OFFSET = MemoryUtil.unsafe.objectFieldOffset(String.class.getDeclaredField("value"));
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    public static int pageSize() {
        return unsafe.pageSize();
    }

    public static long getAddress(ByteBuffer buffer) {
        assert buffer.getClass() == DIRECT_BYTE_BUFFER_CLASS;
        return unsafe.getLong(buffer, DIRECT_BYTE_BUFFER_ADDRESS_OFFSET);
    }

    public static int getCap(ByteBuffer buffer) {
        assert buffer.getClass() == DIRECT_BYTE_BUFFER_CLASS;
        return unsafe.getInt(buffer, DIRECT_BYTE_BUFFER_CAPACITY_OFFSET);
    }

    public static long allocate(long size) {
        return Native.malloc(size);
    }

    public static void free(long addr) {
        Native.free(addr);
    }

    public static void setByte(long address, byte b) {
        unsafe.putByte(address, b);
    }

    public static void setShort(long address, short s) {
        unsafe.putShort(address, s);
    }

    public static void setInt(long address, int l) {
        unsafe.putInt(address, l);
    }

    public static void setLong(long address, long l) {
        unsafe.putLong(address, l);
    }

    public static void setFloat(long address, float v) {
        unsafe.putFloat(address, v);
    }

    public static void setDouble(long address, double v) {
        unsafe.putDouble(address, v);
    }

    public static byte getByte(long address) {
        return unsafe.getByte(address);
    }

    public static int getShort(long address) {
        return unsafe.getShort(address) & 0xffff;
    }

    public static int getInt(long address) {
        return unsafe.getInt(address);
    }

    public static long getLong(long address) {
        return unsafe.getLong(address);
    }

    public static float getFloat(long address) {
        return unsafe.getFloat(address);
    }

    public static double getDouble(long address) {
        return unsafe.getDouble(address);
    }

    private static class Deallocator implements Runnable {
        private long address;

        private Deallocator(long address) {
            assert (address != 0);
            this.address = address;
        }

        @Override
        public void run() {
            if (address == 0) {
                return;
            }
            free(address);
            address = 0;
        }
    }

    public static ByteBuffer getByteBuffer(long address, int length, boolean autoFree) {
        ByteBuffer instance = getHollowDirectByteBuffer();
        if (autoFree) {
            Cleaner cleaner = Cleaner.create(instance, new Deallocator(address));
            setByteBuffer(instance, address, length, cleaner);
        } else {
            setByteBuffer(instance, address, length, null);
        }
        instance.order(ByteOrder.nativeOrder());
        return instance;
    }

    public static ByteBuffer getHollowDirectByteBuffer() {
        ByteBuffer instance;
        try {
            instance = (ByteBuffer) unsafe.allocateInstance(DIRECT_BYTE_BUFFER_CLASS);
        } catch (InstantiationException e) {
            throw new AssertionError(e);
        }
        instance.order(ByteOrder.nativeOrder());
        return instance;
    }

    public static void setByteBuffer(ByteBuffer instance, long address, int length, Cleaner cleaner) {
        unsafe.putLong(instance, DIRECT_BYTE_BUFFER_ADDRESS_OFFSET, address);
        unsafe.putInt(instance, DIRECT_BYTE_BUFFER_CAPACITY_OFFSET, length);
        unsafe.putInt(instance, DIRECT_BYTE_BUFFER_LIMIT_OFFSET, length);
        if (cleaner != null) {
            unsafe.putObject(instance, DIRECT_BYTE_BUFFER_CLEANER, cleaner);
        }
    }

    public static Object getAttachment(ByteBuffer instance) {
        assert instance.getClass() == DIRECT_BYTE_BUFFER_CLASS;
        return unsafe.getObject(instance, DIRECT_BYTE_BUFFER_ATTACHMENT_OFFSET);
    }

    public static void setAttachment(ByteBuffer instance, Object next) {
        assert instance.getClass() == DIRECT_BYTE_BUFFER_CLASS;
        unsafe.putObject(instance, DIRECT_BYTE_BUFFER_ATTACHMENT_OFFSET, next);
    }

    public static ByteBuffer duplicateDirectByteBuffer(ByteBuffer source, ByteBuffer hollowBuffer) {
        assert source.getClass() == DIRECT_BYTE_BUFFER_CLASS;
        unsafe.putLong(hollowBuffer, DIRECT_BYTE_BUFFER_ADDRESS_OFFSET, unsafe.getLong(source, DIRECT_BYTE_BUFFER_ADDRESS_OFFSET));
        unsafe.putInt(hollowBuffer, DIRECT_BYTE_BUFFER_POSITION_OFFSET, unsafe.getInt(source, DIRECT_BYTE_BUFFER_POSITION_OFFSET));
        unsafe.putInt(hollowBuffer, DIRECT_BYTE_BUFFER_LIMIT_OFFSET, unsafe.getInt(source, DIRECT_BYTE_BUFFER_LIMIT_OFFSET));
        unsafe.putInt(hollowBuffer, DIRECT_BYTE_BUFFER_CAPACITY_OFFSET, unsafe.getInt(source, DIRECT_BYTE_BUFFER_CAPACITY_OFFSET));
        return hollowBuffer;
    }

    public static long getLongByByte(long address) {
        if (BIG_ENDIAN) {
            return (((long) unsafe.getByte(address)) << 56) |
                    (((long) unsafe.getByte(address + 1) & 0xff) << 48) |
                    (((long) unsafe.getByte(address + 2) & 0xff) << 40) |
                    (((long) unsafe.getByte(address + 3) & 0xff) << 32) |
                    (((long) unsafe.getByte(address + 4) & 0xff) << 24) |
                    (((long) unsafe.getByte(address + 5) & 0xff) << 16) |
                    (((long) unsafe.getByte(address + 6) & 0xff) << 8) |
                    (((long) unsafe.getByte(address + 7) & 0xff));
        } else {
            return (((long) unsafe.getByte(address + 7)) << 56) |
                    (((long) unsafe.getByte(address + 6) & 0xff) << 48) |
                    (((long) unsafe.getByte(address + 5) & 0xff) << 40) |
                    (((long) unsafe.getByte(address + 4) & 0xff) << 32) |
                    (((long) unsafe.getByte(address + 3) & 0xff) << 24) |
                    (((long) unsafe.getByte(address + 2) & 0xff) << 16) |
                    (((long) unsafe.getByte(address + 1) & 0xff) << 8) |
                    (((long) unsafe.getByte(address) & 0xff));
        }
    }

    public static int getIntByByte(long address) {
        if (BIG_ENDIAN) {
            return (((int) unsafe.getByte(address)) << 24) |
                    (((int) unsafe.getByte(address + 1) & 0xff) << 16) |
                    (((int) unsafe.getByte(address + 2) & 0xff) << 8) |
                    (((int) unsafe.getByte(address + 3) & 0xff));
        } else {
            return (((int) unsafe.getByte(address + 3)) << 24) |
                    (((int) unsafe.getByte(address + 2) & 0xff) << 16) |
                    (((int) unsafe.getByte(address + 1) & 0xff) << 8) |
                    (((int) unsafe.getByte(address) & 0xff));
        }
    }


    public static int getShortByByte(long address) {
        if (BIG_ENDIAN) {
            return (((int) unsafe.getByte(address)) << 8) |
                    (((int) unsafe.getByte(address + 1) & 0xff));
        } else {
            return (((int) unsafe.getByte(address + 1)) << 8) |
                    (((int) unsafe.getByte(address) & 0xff));
        }
    }

    public static void putLongByByte(long address, long value) {
        if (BIG_ENDIAN) {
            unsafe.putByte(address, (byte) (value >> 56));
            unsafe.putByte(address + 1, (byte) (value >> 48));
            unsafe.putByte(address + 2, (byte) (value >> 40));
            unsafe.putByte(address + 3, (byte) (value >> 32));
            unsafe.putByte(address + 4, (byte) (value >> 24));
            unsafe.putByte(address + 5, (byte) (value >> 16));
            unsafe.putByte(address + 6, (byte) (value >> 8));
            unsafe.putByte(address + 7, (byte) (value));
        } else {
            unsafe.putByte(address + 7, (byte) (value >> 56));
            unsafe.putByte(address + 6, (byte) (value >> 48));
            unsafe.putByte(address + 5, (byte) (value >> 40));
            unsafe.putByte(address + 4, (byte) (value >> 32));
            unsafe.putByte(address + 3, (byte) (value >> 24));
            unsafe.putByte(address + 2, (byte) (value >> 16));
            unsafe.putByte(address + 1, (byte) (value >> 8));
            unsafe.putByte(address, (byte) (value));
        }
    }

    public static void putIntByByte(long address, int value) {
        if (BIG_ENDIAN) {
            unsafe.putByte(address, (byte) (value >> 24));
            unsafe.putByte(address + 1, (byte) (value >> 16));
            unsafe.putByte(address + 2, (byte) (value >> 8));
            unsafe.putByte(address + 3, (byte) (value));
        } else {
            unsafe.putByte(address + 3, (byte) (value >> 24));
            unsafe.putByte(address + 2, (byte) (value >> 16));
            unsafe.putByte(address + 1, (byte) (value >> 8));
            unsafe.putByte(address, (byte) (value));
        }
    }

    public static void setBytes(long address, ByteBuffer buffer) {
        int start = buffer.position();
        int count = buffer.limit() - start;
        if (count == 0)
            return;

        if (buffer.isDirect())
            setBytes(((DirectBuffer) buffer).address() + start, address, count);
        else
            setBytes(address, buffer.array(), buffer.arrayOffset() + start, count);
    }

    /**
     * Transfers objCount bytes from buffer to Memory
     *
     * @param address      start offset in the memory
     * @param buffer       the data buffer
     * @param bufferOffset start offset of the buffer
     * @param count        number of bytes to transfer
     */
    public static void setBytes(long address, byte[] buffer, int bufferOffset, int count) {
        assert buffer != null;
        assert !(bufferOffset < 0 || count < 0 || bufferOffset + count > buffer.length);
        setBytes(buffer, bufferOffset, address, count);
    }

    public static void setBytes(long src, long trg, long count) {
        while (count > 0) {
            long size = (count > UNSAFE_COPY_THRESHOLD) ? UNSAFE_COPY_THRESHOLD : count;
            unsafe.copyMemory(src, trg, size);
            count -= size;
            src += size;
            trg += size;
        }
    }

    public static void setBytes(byte[] src, int offset, long trg, long count) {
        while (count > 0) {
            long size = (count > UNSAFE_COPY_THRESHOLD) ? UNSAFE_COPY_THRESHOLD : count;
            unsafe.copyMemory(src, BYTE_ARRAY_BASE_OFFSET + offset, null, trg, size);
            count -= size;
            offset += size;
            trg += size;
        }
    }

    /**
     * Transfers objCount bytes from Memory starting at memoryOffset to buffer starting at bufferOffset
     *
     * @param address      start offset in the memory
     * @param buffer       the data buffer
     * @param bufferOffset start offset of the buffer
     * @param count        number of bytes to transfer
     */
    public static void getBytes(long address, byte[] buffer, int bufferOffset, int count) {
        if (buffer == null)
            throw new NullPointerException();
        else if (bufferOffset < 0 || count < 0 || count > buffer.length - bufferOffset)
            throw new IndexOutOfBoundsException();
        else if (count == 0)
            return;

        unsafe.copyMemory(null, address, buffer, BYTE_ARRAY_BASE_OFFSET + bufferOffset, count);
    }

    public static char[] getStringValue(String str) {
        return (char[]) MemoryUtil.unsafe.getObject(str, STRING_VALUE_OFFSET);
    }

    public static void copyMemory(long fromAddr, long toAddr, int count) {
        copyMemory(null, fromAddr, null, toAddr, count);
    }

    public static void copyMemory(
            Object src, long srcOffset, Object dst, long dstOffset, long length) {
        // Check if dstOffset is before or after srcOffset to determine if we should copy
        // forward or backwards. This is necessary in case src and dst overlap.
        if (dstOffset < srcOffset) {
            while (length > 0) {
                long size = Math.min(length, UNSAFE_COPY_THRESHOLD);
                unsafe.copyMemory(src, srcOffset, dst, dstOffset, size);
                length -= size;
                srcOffset += size;
                dstOffset += size;
            }
        } else {
            srcOffset += length;
            dstOffset += length;
            while (length > 0) {
                long size = Math.min(length, UNSAFE_COPY_THRESHOLD);
                srcOffset -= size;
                dstOffset -= size;
                unsafe.copyMemory(src, srcOffset, dst, dstOffset, size);
                length -= size;
            }

        }
    }

    public static long getTotalPhysicalMemorySize() {
        return ((OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean()).getTotalPhysicalMemorySize();
    }
}