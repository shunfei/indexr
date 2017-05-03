package io.indexr.data;

import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.types.UTF8String;

import java.util.List;

import io.indexr.io.ByteSlice;
import io.indexr.util.MemoryUtil;
import io.indexr.util.Wrapper;

/**
 * A class help explain the string values' strcuture in an offheap byte array.
 *
 * | offset0 | offset1 | offset2 | offset3(str_total_len) | s0 | s1 | s2 |
 * | <-                   index(int)                   -> |<- str_data ->|
 */
public class StringsStruct {
    private final int count;
    private final long dataAddr;

    public StringsStruct(int count, long dataAddr) {
        this.count = count;
        this.dataAddr = dataAddr;
    }

    public long dataAddr() {
        return dataAddr;
    }

    public int valCount() {
        return count;
    }

    public int totalLen() {
        return totalIndexLen() + totalStringLen();
    }

    public int totalIndexLen() {
        return (count + 1) << 2;
    }

    public int totalStringLen() {
        return MemoryUtil.getInt(dataAddr + (count << 2));
    }

    public void getString(int index, OffheapBytes container) {
        int totalIndexLen = (count + 1) << 2;
        int strOffset = MemoryUtil.getInt(dataAddr + (index << 2));
        int nextOffset = MemoryUtil.getInt(dataAddr + ((index + 1) << 2));
        container.set(dataAddr + totalIndexLen + strOffset, nextOffset - strOffset);
    }

    public OffheapBytes getString(int index) {
        int totalIndexLen = (count + 1) << 2;
        int strOffset = MemoryUtil.getInt(dataAddr + (index << 2));
        int nextOffset = MemoryUtil.getInt(dataAddr + ((index + 1) << 2));
        return new OffheapBytes(dataAddr + totalIndexLen + strOffset, nextOffset - strOffset);
    }

    public UTF8String getUTF8String(int index) {
        int totalIndexLen = (count + 1) << 2;
        int strOffset = MemoryUtil.getInt(dataAddr + (index << 2));
        int nextOffset = MemoryUtil.getInt(dataAddr + ((index + 1) << 2));
        return UTF8String.fromAddress(null, dataAddr + totalIndexLen + strOffset, nextOffset - strOffset);
    }

    public static StringsStruct from(List<byte[]> strings, Wrapper<ByteSlice> memContainer) {
        int totalIndexLen = (strings.size() + 1) << 2;
        int totalStringLen = 0;
        for (byte[] b : strings) {
            totalStringLen += b.length;
        }

        if (memContainer.value == null) {
            memContainer.value = ByteSlice.allocateDirect(totalIndexLen + totalStringLen);
        }
        return from(strings, ((ByteSlice) memContainer.value).address());
    }

    public static StringsStruct from(List<byte[]> strings, long dataAddr) {
        Builder builder = new Builder(strings.size(), dataAddr);
        for (byte[] str : strings) {
            builder.addString(str);
        }
        return builder.build();
    }

    public static StringsStruct from(OffheapBytes[] strings, long dataAddr) {
        Builder builder = new Builder(strings.length, dataAddr);
        for (OffheapBytes str : strings) {
            builder.addString(null, str.addr(), str.len());
        }
        return builder.build();
    }

    public static int needMemory(int count, int totalStringLen) {
        return ((count + 1) << 2) + totalStringLen;
    }

    public static class Builder {
        private int count;
        private long dataAddr;

        private int totalIndexLen;

        private int nextId = 0;
        private int lastEnd = 0;

        public Builder(int count, long dataAddr) {
            this.count = count;
            this.dataAddr = dataAddr;
            this.totalIndexLen = (count + 1) << 2;
        }

        public void addString(byte[] str) {
            addString(str, Platform.BYTE_ARRAY_OFFSET, str.length);
        }

        public void addString(Object base, long offset, int len) {
            assert nextId < count;

            int start = lastEnd;
            int end = start + len;
            MemoryUtil.setInt(dataAddr + (nextId << 2), start);
            //MemoryUtil.setInt(dataAddr + ((nextId + 1) << 2), end);
            MemoryUtil.copyMemory(base, offset, null, dataAddr + totalIndexLen + start, len);

            nextId++;
            lastEnd = end;
        }

        // Add a string seperated by prefix and subfix parts.
        // And indicates the just added string with pointer.
        public void addString(Object preBase, long preOffset, int preLen,
                              Object subBase, long subOffset, int subLen,
                              OffheapBytes pointer) {
            assert nextId < count;

            int start = lastEnd;
            int end = start + preLen + subLen;
            MemoryUtil.setInt(dataAddr + (nextId << 2), start);
            //MemoryUtil.setInt(dataAddr + ((nextId + 1) << 2), end);

            MemoryUtil.copyMemory(preBase, preOffset, null, dataAddr + totalIndexLen + start, preLen);
            MemoryUtil.copyMemory(subBase, subOffset, null, dataAddr + totalIndexLen + start + preLen, subLen);

            nextId++;
            lastEnd = end;

            pointer.set(dataAddr + totalIndexLen + start, preLen + subLen);
        }

        public StringsStruct build() {
            assert nextId == count;
            MemoryUtil.setInt(dataAddr + (count << 2), lastEnd);
            return new StringsStruct(count, dataAddr);
        }
    }
}
