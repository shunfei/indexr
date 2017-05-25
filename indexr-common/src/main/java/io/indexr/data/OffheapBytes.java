package io.indexr.data;

import org.apache.spark.unsafe.array.ByteArrayMethods;
import org.apache.spark.unsafe.types.UTF8String;

import io.indexr.util.BytesUtil;
import io.indexr.util.Hasher32;

public class OffheapBytes implements Comparable<OffheapBytes> {
    private static final Hasher32 HASHER = new Hasher32(0);

    private long addr = 0;
    private int len = 0;

    public OffheapBytes(long addr, int len) {
        this.addr = addr;
        this.len = len;
    }

    public OffheapBytes() {}

    public long addr() {
        return addr;
    }

    public int len() {
        return len;
    }

    public void set(long addr, int len) {
        this.addr = addr;
        this.len = len;
    }

    public void set(OffheapBytes v) {
        set(v.addr, v.len);
    }

    @Override
    public int hashCode() {
        return HASHER.hashUnsafeWords(null, addr, len);
    }

    @Override
    public boolean equals(Object o) {
        OffheapBytes that = (OffheapBytes) o;
        return len == that.len
                && ByteArrayMethods.arrayEquals(null, addr, null, that.addr, len);
    }

    @Override
    public int compareTo(OffheapBytes o) {
        return BytesUtil.compareBytes(addr, len, o.addr, o.len);
    }

    public String toString() {
        return UTF8String.fromAddress(null, addr, len).toString();
    }
}
