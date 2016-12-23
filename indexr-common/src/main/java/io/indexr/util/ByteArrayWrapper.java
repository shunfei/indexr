package io.indexr.util;

import org.apache.spark.unsafe.array.ByteArrayMethods;

import static org.apache.spark.unsafe.Platform.BYTE_ARRAY_OFFSET;

/**
 * A bytes wrapper mainly used to correctly compare contents of two byte lists.
 */
public class ByteArrayWrapper {
    private static final Hasher32 HASHER = new Hasher32(0);

    private Object base;
    private long offset;
    private int len;

    private int hashCode;

    public ByteArrayWrapper() {}

    public ByteArrayWrapper(Object base, long offset, int len) {
        set(base, offset, len);
    }

    public ByteArrayWrapper(byte[] bytes) {
        set(bytes, 0, bytes.length);
    }

    public ByteArrayWrapper(byte[] bytes, int offset, int len) {
        set(bytes, offset, len);
    }

    public void set(Object base, long offset, int len) {
        this.base = base;
        this.offset = offset;
        this.len = len;

        this.hashCode = HASHER.hashUnsafeWords(base, offset, len);
    }

    public void set(byte[] bytes) {
        set(bytes, 0, bytes.length);
    }

    public void set(byte[] bytes, int offset, int len) {
        set((Object) bytes, BYTE_ARRAY_OFFSET + offset, len);
    }

    public Object base() {
        return base;
    }

    public long offset() {
        return offset;
    }

    public int len() {
        return len;
    }

    @Override
    public boolean equals(Object o) {
        ByteArrayWrapper that = (ByteArrayWrapper) o;
        return hashCode == that.hashCode
                && len == that.len
                && ByteArrayMethods.arrayEquals(base, offset, that.base, that.offset, len);
    }

    @Override
    public int hashCode() {
        return hashCode;
    }
}
