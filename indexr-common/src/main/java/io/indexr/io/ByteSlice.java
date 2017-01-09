package io.indexr.io;

import com.sun.jna.Pointer;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import io.indexr.data.Freeable;
import io.indexr.util.ByteBufferUtil;
import io.indexr.util.MemoryUtil;

/**
 * A wrapper of {@link ByteBuffer}.
 * 
 * The main reason of this class is to remove the position, mark and limit features of {@link ByteBuffer}.
 * i.e. the position is always <b>0</b>, the limit is always <b>equals</b> to capacity, and the mark is always <b>unused(-1)</b>.
 * You can think it is a pure byte array with slice ability!
 */
public class ByteSlice implements Freeable {
    private ByteBuffer buffer;

    public static final ByteBuffer EmptyByteBuffer = ByteBufferUtil.allocateDirect(0);
    //public static final ByteBuffer EmptyByteBuffer = ByteBuffer.allocate(0);

    public ByteSlice(ByteBuffer buffer) {
        // Forget position, limit!
        assert buffer.position() == 0;
        assert buffer.limit() == buffer.capacity();

        // Important to keep byte order constant!
        buffer.order(ByteOrder.nativeOrder());

        this.buffer = buffer;
    }

    public static ByteSlice empty() {
        return new ByteSlice(EmptyByteBuffer);
    }

    public int size() {
        return buffer.capacity();
    }

    public byte get(int i) {
        return buffer.get(i);
    }

    public void get(int index, byte[] dst, int offset, int length) {
        int end = offset + length;
        for (int i = offset; i < end; i++, index++) {
            dst[i] = get(index);
        }
    }

    public void get(int index, byte[] dst) {
        get(index, dst, 0, dst.length);
    }

    public void put(int i, byte v) {
        buffer.put(i, v);
    }

    public void put(int index, byte[] src, int offset, int length) {
        int end = offset + length;
        for (int i = offset; i < end; i++, index++) {
            buffer.put(index, src[i]);
        }
    }

    public void put(int index, byte[] src) {
        put(index, src, 0, src.length);
    }

    public char getChar(int i) {
        return buffer.getChar(i);
    }

    public void putChar(int i, char v) {
        buffer.putChar(i, v);
    }

    public short getShort(int i) {
        return buffer.getShort(i);
    }

    public void putShort(int i, short v) {
        buffer.putShort(i, v);
    }

    public int getInt(int i) {
        return buffer.getInt(i);
    }

    public void putInt(int i, int v) {
        buffer.putInt(i, v);
    }

    public long getLong(int i) {
        return buffer.getLong(i);
    }

    public void putLong(int i, long v) {
        buffer.putLong(i, v);
    }

    public float getFloat(int i) {
        return buffer.getFloat();
    }

    public void putFloat(int i, float v) {
        buffer.putFloat(i, v);
    }

    public double getDouble(int i) {
        return buffer.getDouble(i + buffer.position());
    }

    public void putDouble(int i, double v) {
        buffer.putDouble(i + buffer.position(), v);
    }

    public ByteSlice duplicate() {
        return new ByteSlice(buffer.duplicate());
    }

    // TODO slice should be faster, without duplicate

    /**
     * Slice a subsequence, i.e. [from, from + size)
     */
    public ByteSlice slice(int from, int size) {
        ByteBuffer tmpBufer = buffer.duplicate();
        tmpBufer.position(from);
        tmpBufer.limit(from + size);
        return new ByteSlice(tmpBufer.slice());
    }

    /**
     * [from, endOfSlice)
     */
    public ByteSlice sliceFrom(int from) {
        ByteBuffer tmpBufer = buffer.duplicate();
        tmpBufer.position(from);
        return new ByteSlice(tmpBufer.slice());
    }

    /**
     * [0, to)
     */
    public ByteSlice sliceTo(int to) {
        ByteBuffer tmpBufer = buffer.duplicate();
        tmpBufer.limit(to);
        return new ByteSlice(tmpBufer.slice());
    }

    public ByteSlice asReadOnlySlice() {
        return new ByteSlice(buffer.asReadOnlyBuffer());
    }

    /**
     * Transform {@link ByteSlice} into {@link ByteBuffer}.
     * They will share the same underlying content. Modify anyone will affect to the other.
     * 
     * The position is always <b>0</b>, the limit is always <b>equals</b> to capacity, and the mark is always <b>unused(-1)</b>.
     */
    public ByteBuffer toByteBuffer() {
        buffer.clear();
        return buffer.duplicate();
    }

    /**
     * Return the underly buffer.
     */
    public ByteBuffer byteBuffer() {
        buffer.clear();
        return buffer;
    }

    public byte[] toByteArray() {
        byte[] arr = buffer.array();
        byte[] theArr = new byte[size()];
        buffer.arrayOffset();
        System.arraycopy(arr, buffer.arrayOffset(), theArr, 0, theArr.length);
        return theArr;
    }

    public long address() {
        assert buffer.isDirect();
        return MemoryUtil.getAddress(buffer);
    }

    public Pointer asPointer() {
        return new Pointer(address());
    }

    /**
     * This method is mean to free the allocated memory this instance held, especially the DirectByteBuffer.
     * 
     * Altough the GC will automatically free the memory before the instance is reclaimed,
     * call free() before a ByteSlice dropped is a good manner which can really help save memory.
     */
    @Override
    public void free() {
        ByteBufferUtil.free(buffer);
        buffer = null;
    }

    /**
     * Set all bytes to 0.
     */
    public void clear() {
        MemoryUtil.unsafe.setMemory(address(), size(), (byte) 0);
    }

    /**
     * Create ByteSlice from memory address and size.
     * 
     * After calling this method, this ByteSlice will take full control of this memory. e.g. You should not
     * reclaim it, update it from outside. Otherwise errors will throw.
     */
    public static ByteSlice fromAddress(long p, int size) {
        return ByteSlice.wrap(MemoryUtil.getByteBuffer(p, size, true));
    }

    /**
     * Create ByteSlice from memory address and size.
     * 
     * After calling this method, this ByteSlice will take full control of this memory. e.g. You should not
     * reclaim it, update it from outside. Otherwise errors will throw.
     */
    public static ByteSlice fromPointer(Pointer p, int size) {
        long native_p = Pointer.nativeValue(p);
        return fromAddress(native_p, size);
    }

    public static ByteSlice wrap(ByteBuffer buffer) {
        return new ByteSlice(buffer);
    }

    public static ByteSlice allocateHeap(int cap) {
        return new ByteSlice(ByteBufferUtil.allocateHeap(cap));
    }

    public static ByteSlice allocateDirect(int cap) {
        return ByteSlice.wrap(ByteBufferUtil.allocateDirect(cap));
    }

    /**
     * Contents equals or not. Slow function, used in test case.
     */
    public static boolean checkEquals(ByteSlice s1, ByteSlice s2) {
        if (s1.size() != s2.size()) {
            return false;
        }
        for (int i = 0; i < s1.size(); i++) {
            if (s1.get(i) != s2.get(i)) {
                return false;
            }
        }
        return true;
    }
}
