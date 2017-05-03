package io.indexr.data;

import io.indexr.util.MemoryUtil;

import static io.indexr.util.MemoryUtil.getDouble;
import static io.indexr.util.MemoryUtil.getFloat;
import static io.indexr.util.MemoryUtil.getInt;
import static io.indexr.util.MemoryUtil.getLong;
import static io.indexr.util.MemoryUtil.setDouble;
import static io.indexr.util.MemoryUtil.setFloat;
import static io.indexr.util.MemoryUtil.setInt;
import static io.indexr.util.MemoryUtil.setLong;

/**
 * A class help explain the structure of dict encoding.
 * <pre>
 * | entryCount |   dict   |  dataSize |  data(encoded ids) |
 *      4         dictSize       4           dataSize
 * </pre>
 */
public final class DictStruct {
    private final byte dataType;
    private final long addr;

    private final int dictEntryCount;
    private final int dictSize;
    private final int dataSize;

    private final StringsStruct stringDictEntries;

    public DictStruct(byte dataType, long addr) {
        this.dataType = dataType;
        this.addr = addr;
        this.dictEntryCount = MemoryUtil.getInt(addr);
        if (DataType.isNumber(dataType)) {
            this.stringDictEntries = null;
            this.dictSize = dictEntryCount << DataType.numTypeShift(dataType);
        } else {
            this.stringDictEntries = new StringsStruct(dictEntryCount, addr + 4);
            this.dictSize = stringDictEntries.totalLen();
        }
        this.dataSize = MemoryUtil.getInt(addr + 4 + dictSize);
    }

    // @formatter:off
    public byte dataType(){return dataType;}
    public long addr() {return addr;}
    public int size(){return 4 + dictSize + 4 + dataSize;}
    public int dictEntryCount() {return dictEntryCount;}
    public int dictSize() {return dictSize;}
    public long dictEntriesAddr() {return addr + 4;}
    public StringsStruct stringDictEntries() {return stringDictEntries;}
    public int dataSize() {return dataSize;}
    public long dataAddr() {return addr + 4 + dictSize + 4;}
    // @formatter:on

    public void decodeInts(int valCount, long outputAddr) {
        long dataAddr = dataAddr();
        long dictEntriesAddr = dictEntriesAddr();
        for (int i = 0; i < valCount; i++) {
            int vid = getInt(dataAddr + (i << 2));
            int v = getInt(dictEntriesAddr + (vid << 2));
            setInt(outputAddr + (i << 2), v);
        }
    }

    public void decodeLongs(int valCount, long outputAddr) {
        long dataAddr = dataAddr();
        long dictEntriesAddr = dictEntriesAddr();
        for (int i = 0; i < valCount; i++) {
            int vid = getInt(dataAddr + (i << 2));
            long v = getLong(dictEntriesAddr + (vid << 3));
            setLong(outputAddr + (i << 3), v);
        }
    }

    public void decodeFloats(int valCount, long outputAddr) {
        long dataAddr = dataAddr();
        long dictEntriesAddr = dictEntriesAddr();
        for (int i = 0; i < valCount; i++) {
            int vid = getInt(dataAddr + (i << 2));
            float v = getFloat(dictEntriesAddr + (vid << 2));
            setFloat(outputAddr + (i << 2), v);
        }
    }

    public void decodeDoubles(int valCount, long outputAddr) {
        long dataAddr = dataAddr();
        long dictEntriesAddr = dictEntriesAddr();
        for (int i = 0; i < valCount; i++) {
            int vid = getInt(dataAddr + (i << 2));
            double v = getDouble(dictEntriesAddr + (vid << 3));
            setDouble(outputAddr + (i << 3), v);
        }
    }

    public void decodeStrings(int valCount, long outputAddr) {
        long dataAddr = dataAddr();
        long dictEntriesAddr = dictEntriesAddr();
        StringsStruct stringDictEntries = stringDictEntries();

        StringsStruct.Builder stringDataBuilder = new StringsStruct.Builder(valCount, outputAddr);
        OffheapBytes container = new OffheapBytes();
        for (int i = 0; i < valCount; i++) {
            int vid = getInt(dataAddr + (i << 2));
            stringDictEntries.getString(vid, container);
            stringDataBuilder.addString(null, container.addr(), container.len());
        }
        stringDataBuilder.build();
    }
}
