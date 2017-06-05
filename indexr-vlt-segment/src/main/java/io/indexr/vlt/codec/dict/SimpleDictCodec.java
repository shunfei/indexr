package io.indexr.vlt.codec.dict;

import com.google.common.base.Preconditions;

import io.indexr.vlt.codec.Codec;
import io.indexr.vlt.codec.CodecType;
import io.indexr.vlt.codec.ErrorCode;
import io.indexr.vlt.codec.UnsafeUtil;
import io.indexr.vlt.codec.VarInteger;
import io.indexr.vlt.codec.rle.RLEPackingHybridCodec;

import it.unimi.dsi.fastutil.doubles.DoubleArrays;
import it.unimi.dsi.fastutil.doubles.DoubleIterator;
import it.unimi.dsi.fastutil.doubles.DoubleRBTreeSet;
import it.unimi.dsi.fastutil.floats.FloatArrays;
import it.unimi.dsi.fastutil.floats.FloatIterator;
import it.unimi.dsi.fastutil.floats.FloatRBTreeSet;
import it.unimi.dsi.fastutil.ints.IntArrays;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntRBTreeSet;
import it.unimi.dsi.fastutil.longs.LongArrays;
import it.unimi.dsi.fastutil.longs.LongIterator;
import it.unimi.dsi.fastutil.longs.LongRBTreeSet;
import it.unimi.dsi.fastutil.objects.Object2IntLinkedOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

import io.indexr.data.OffheapBytes;
import io.indexr.data.StringsStruct;
import io.indexr.io.ByteSlice;
import io.indexr.util.Wrapper;

import static io.indexr.vlt.codec.UnsafeUtil.getDouble;
import static io.indexr.vlt.codec.UnsafeUtil.getFloat;
import static io.indexr.vlt.codec.UnsafeUtil.getInt;
import static io.indexr.vlt.codec.UnsafeUtil.getLong;
import static io.indexr.vlt.codec.UnsafeUtil.setDouble;
import static io.indexr.vlt.codec.UnsafeUtil.setFloat;
import static io.indexr.vlt.codec.UnsafeUtil.setInt;
import static io.indexr.vlt.codec.UnsafeUtil.setLong;

/**
 * <pre>
 * | dictSize |   dict   |  dataSize |  data(encoded ids) |
 *      4       dictSize       4           dataSize
 * </pre>
 */
public class SimpleDictCodec implements Codec {
    public static final CodecType TYPE = CodecType.SIMPLE_DICT;

    public static final float MAX_DICT_ENTRY_RATIO = 1.0f;
    private final float dictEntryRatioThreshold;

    public SimpleDictCodec() {
        this(1);
    }

    public SimpleDictCodec(float dictEntryRatioThreshold) {
        Preconditions.checkState(dictEntryRatioThreshold <= MAX_DICT_ENTRY_RATIO);
        this.dictEntryRatioThreshold = dictEntryRatioThreshold;
    }

    @Override
    public CodecType type() {
        return TYPE;
    }

    @Override
    public int encodeInts(int valCount, long inputAddr, long outputAddr, int outputLimit, Wrapper extraInfo) {
        if (valCount == 0) return 0;
        IntRBTreeSet dict = new IntRBTreeSet();

        int maxDictEntryCount = (int) Math.ceil(valCount * dictEntryRatioThreshold);
        for (int i = 0; i < valCount; i++) {
            if (dict.add(getInt(inputAddr + (i << 2)))) {
                if (dict.size() > maxDictEntryCount) {
                    dict.clear();
                    return ErrorCode.DATA_IMPROPER;
                }
            }
        }

        IntIterator it = dict.iterator();
        int[] entries = new int[dict.size()];
        int id = 0;
        while (it.hasNext()) {
            entries[id] = it.nextInt();
            id++;
        }
        assert id == dict.size();
        dict.clear();
        int dictEntryCount = entries.length;

        ByteSlice dataEntryIds = ByteSlice.allocateDirect(valCount << 2);
        for (int i = 0; i < valCount; i++) {
            int v = getInt(inputAddr + (i << 2));
            int vid = IntArrays.binarySearch(entries, v);
            assert vid >= 0;
            // data index to entry id
            dataEntryIds.putInt(i << 2, vid);
        }

        // Write dictionary entries.
        int dictEntriesCount = entries.length;
        int dictSize = dictEntriesCount << 2;
        UnsafeUtil.setInt(outputAddr, dictEntriesCount);
        UnsafeUtil.copyMemory(
                entries, UnsafeUtil.INT_ARRAY_OFFSET,
                null, outputAddr + 4,
                dictSize);
        // Write data entry ids.
        RLEPackingHybridCodec dataCodec = new RLEPackingHybridCodec(VarInteger.bitWidth(dictEntriesCount - 1));
        int dataSize = dataCodec.encodeInts(
                valCount,
                dataEntryIds.address(),
                outputAddr + 4 + dictSize + 4,
                outputLimit - (4 + dictSize + 4));
        if (dataSize < 0) {
            dataEntryIds.free();
            return dataSize;
        }
        UnsafeUtil.setInt(outputAddr + 4 + dictSize, dataSize);

        int encodedSize = 4 + dictSize + 4 + dataSize;
        if (encodedSize > outputLimit) {
            return ErrorCode.BUFFER_OVERFLOW;
        }
        return encodedSize;
    }

    @Override
    public int decodeInts(int valCount, long inputAddr, long outputAddr, int outputLimit) {
        if (valCount == 0) return 0;

        int dictEntriesCount = UnsafeUtil.getInt(inputAddr);
        int dictSize = dictEntriesCount << 2;
        long dictAddr = inputAddr + 4;

        ByteSlice dataEntryIds = ByteSlice.allocateDirect(valCount << 2);
        long encodeddataEntryIdsAddr = inputAddr + 4 + dictSize + 4;
        RLEPackingHybridCodec dataCodec = new RLEPackingHybridCodec(VarInteger.bitWidth(dictEntriesCount - 1));
        int consumedSize = dataCodec.decodeInts(
                valCount,
                encodeddataEntryIdsAddr,
                dataEntryIds.address(), dataEntryIds.size());
        if (consumedSize < 0) {
            dataEntryIds.free();
            return consumedSize;
        }

        for (int i = 0; i < valCount; i++) {
            int vid = dataEntryIds.getInt(i << 2);
            int v = getInt(dictAddr + (vid << 2));
            setInt(outputAddr + (i << 2), v);
        }

        dataEntryIds.free();

        return 4 + dictSize + 4 + consumedSize;
    }

    @Override
    public int encodeLongs(int valCount, long inputAddr, long outputAddr, int outputLimit, Wrapper extraInfo) {
        if (valCount == 0) return 0;
        LongRBTreeSet dict = new LongRBTreeSet();

        int maxDictEntryCount = (int) Math.ceil(valCount * dictEntryRatioThreshold);
        for (int i = 0; i < valCount; i++) {
            if (dict.add(getLong(inputAddr + (i << 3)))) {
                if (dict.size() > maxDictEntryCount) {
                    dict.clear();
                    return ErrorCode.DATA_IMPROPER;
                }
            }
        }

        LongIterator it = dict.iterator();
        long[] entries = new long[dict.size()];
        int id = 0;
        while (it.hasNext()) {
            entries[id] = it.nextLong();
            id++;
        }
        assert id == dict.size();
        dict.clear();
        int dictEntryCount = entries.length;

        ByteSlice dataEntryIds = ByteSlice.allocateDirect(valCount << 2);
        for (int i = 0; i < valCount; i++) {
            long v = getLong(inputAddr + (i << 3));
            int vid = LongArrays.binarySearch(entries, v);
            assert vid >= 0;
            // data index to entry id
            dataEntryIds.putInt(i << 2, vid);
        }

        // | dictSize |   dict   |  dataSize |  data(encoded ids) |
        //      4       dictSize       4           dataSize

        // Write dictionary entries.
        int dictEntriesCount = entries.length;
        int dictSize = dictEntriesCount << 3;
        UnsafeUtil.setInt(outputAddr, dictEntriesCount);
        UnsafeUtil.copyMemory(
                entries, UnsafeUtil.LONG_ARRAY_OFFSET,
                null, outputAddr + 4,
                dictSize);
        // Write data entry ids.
        RLEPackingHybridCodec dataCodec = new RLEPackingHybridCodec(VarInteger.bitWidth(dictEntriesCount - 1));
        int dataSize = dataCodec.encodeInts(
                valCount,
                dataEntryIds.address(),
                outputAddr + 4 + dictSize + 4,
                outputLimit - (4 + dictSize + 4));
        if (dataSize < 0) {
            dataEntryIds.free();
            return dataSize;
        }
        UnsafeUtil.setInt(outputAddr + 4 + dictSize, dataSize);

        int encodedSize = 4 + dictSize + 4 + dataSize;
        if (encodedSize > outputLimit) {
            return ErrorCode.BUFFER_OVERFLOW;
        }
        return encodedSize;
    }

    @Override
    public int decodeLongs(int valCount, long inputAddr, long outputAddr, int outputLimit) {
        if (valCount == 0) return 0;
        int dictEntriesCount = UnsafeUtil.getInt(inputAddr);
        int dictSize = dictEntriesCount << 3;
        long dictAddr = inputAddr + 4;

        ByteSlice dataEntryIds = ByteSlice.allocateDirect(valCount << 2);
        long encodeddataEntryIdsAddr = inputAddr + 4 + dictSize + 4;
        RLEPackingHybridCodec dataCodec = new RLEPackingHybridCodec(VarInteger.bitWidth(dictEntriesCount - 1));
        int consumedSize = dataCodec.decodeInts(
                valCount,
                encodeddataEntryIdsAddr,
                dataEntryIds.address(),
                dataEntryIds.size());
        if (consumedSize < 0) {
            dataEntryIds.free();
            return consumedSize;
        }

        for (int i = 0; i < valCount; i++) {
            int vid = dataEntryIds.getInt(i << 2);
            long v = getLong(dictAddr + (vid << 3));
            setLong(outputAddr + (i << 3), v);
        }

        dataEntryIds.free();

        return 4 + dictSize + 4 + consumedSize;
    }

    @Override
    public int encodeFloats(int valCount, long inputAddr, long outputAddr, int outputLimit, Wrapper extraInfo) {
        if (valCount == 0) return 0;
        FloatRBTreeSet dict = new FloatRBTreeSet();

        int maxDictEntryCount = (int) Math.ceil(valCount * dictEntryRatioThreshold);
        for (int i = 0; i < valCount; i++) {
            if (dict.add(getFloat(inputAddr + (i << 2)))) {
                if (dict.size() > maxDictEntryCount) {
                    dict.clear();
                    return ErrorCode.DATA_IMPROPER;
                }
            }
        }

        FloatIterator it = dict.iterator();
        float[] entries = new float[dict.size()];
        int id = 0;
        while (it.hasNext()) {
            entries[id] = it.nextFloat();
            id++;
        }
        assert id == dict.size();
        dict.clear();
        int dictEntryCount = entries.length;

        ByteSlice dataEntryIds = ByteSlice.allocateDirect(valCount << 2);
        for (int i = 0; i < valCount; i++) {
            float v = getFloat(inputAddr + (i << 2));
            int vid = FloatArrays.binarySearch(entries, v);
            assert vid >= 0;
            // data index to entry id
            dataEntryIds.putInt(i << 2, vid);
        }

        // Write dictionary entries.
        int dictEntriesCount = entries.length;
        int dictSize = dictEntriesCount << 2;
        UnsafeUtil.setInt(outputAddr, dictEntriesCount);
        UnsafeUtil.copyMemory(
                entries, UnsafeUtil.FLOAT_ARRAY_OFFSET,
                null, outputAddr + 4,
                dictSize);
        // Write data entry ids.
        RLEPackingHybridCodec dataCodec = new RLEPackingHybridCodec(VarInteger.bitWidth(dictEntriesCount - 1));
        int dataSize = dataCodec.encodeInts(
                valCount,
                dataEntryIds.address(),
                outputAddr + 4 + dictSize + 4,
                outputLimit - (4 + dictSize + 4));
        if (dataSize < 0) {
            dataEntryIds.free();
            return dataSize;
        }
        UnsafeUtil.setInt(outputAddr + 4 + dictSize, dataSize);

        int encodedSize = 4 + dictSize + 4 + dataSize;
        if (encodedSize > outputLimit) {
            return ErrorCode.BUFFER_OVERFLOW;
        }
        return encodedSize;
    }

    @Override
    public int decodeFloats(int valCount, long inputAddr, long outputAddr, int outputLimit) {
        if (valCount == 0) return 0;
        int dictEntriesCount = UnsafeUtil.getInt(inputAddr);
        int dictSize = dictEntriesCount << 2;
        long dictAddr = inputAddr + 4;

        ByteSlice dataEntryIds = ByteSlice.allocateDirect(valCount << 2);
        long encodeddataEntryIdsAddr = inputAddr + 4 + dictSize + 4;
        RLEPackingHybridCodec dataCodec = new RLEPackingHybridCodec(VarInteger.bitWidth(dictEntriesCount - 1));
        int consumedSize = dataCodec.decodeInts(
                valCount,
                encodeddataEntryIdsAddr,
                dataEntryIds.address(),
                dataEntryIds.size());
        if (consumedSize < 0) {
            dataEntryIds.free();
            return consumedSize;
        }

        for (int i = 0; i < valCount; i++) {
            int vid = dataEntryIds.getInt(i << 2);
            float v = getFloat(dictAddr + (vid << 2));
            setFloat(outputAddr + (i << 2), v);
        }

        dataEntryIds.free();

        return 4 + dictSize + 4 + consumedSize;
    }

    @Override
    public int encodeDoubles(int valCount, long inputAddr, long outputAddr, int outputLimit, Wrapper extraInfo) {
        if (valCount == 0) return 0;
        DoubleRBTreeSet dict = new DoubleRBTreeSet();

        int maxDictEntryCount = (int) Math.ceil(valCount * dictEntryRatioThreshold);
        for (int i = 0; i < valCount; i++) {
            if (dict.add(getDouble(inputAddr + (i << 3)))) {
                if (dict.size() > maxDictEntryCount) {
                    dict.clear();
                    return ErrorCode.DATA_IMPROPER;
                }
            }
        }

        DoubleIterator it = dict.iterator();
        double[] entries = new double[dict.size()];
        int id = 0;
        while (it.hasNext()) {
            entries[id] = it.nextDouble();
            id++;
        }
        assert id == dict.size();
        dict.clear();
        int dictEntryCount = entries.length;

        ByteSlice dataEntryIds = ByteSlice.allocateDirect(valCount << 2);
        for (int i = 0; i < valCount; i++) {
            double v = getDouble(inputAddr + (i << 3));
            int vid = DoubleArrays.binarySearch(entries, v);
            assert vid >= 0;
            // data index to entry id
            dataEntryIds.putInt(i << 2, vid);
        }

        // | dictSize |   dict   |  dataSize |  data(encoded ids) |
        //      4       dictSize       4           dataSize

        // Write dictionary entries.
        int dictEntriesCount = entries.length;
        int dictSize = dictEntriesCount << 3;
        UnsafeUtil.setInt(outputAddr, dictEntriesCount);
        UnsafeUtil.copyMemory(
                entries, UnsafeUtil.DOUBLE_ARRAY_OFFSET,
                null, outputAddr + 4,
                dictSize);
        // Write data entry ids.
        RLEPackingHybridCodec dataCodec = new RLEPackingHybridCodec(VarInteger.bitWidth(dictEntriesCount - 1));
        int dataSize = dataCodec.encodeInts(
                valCount,
                dataEntryIds.address(),
                outputAddr + 4 + dictSize + 4,
                outputLimit - (4 + dictSize + 4));
        if (dataSize < 0) {
            dataEntryIds.free();
            return dataSize;
        }
        UnsafeUtil.setInt(outputAddr + 4 + dictSize, dataSize);

        int encodedSize = 4 + dictSize + 4 + dataSize;
        if (encodedSize > outputLimit) {
            return ErrorCode.BUFFER_OVERFLOW;
        }
        return encodedSize;
    }

    @Override
    public int decodeDoubles(int valCount, long inputAddr, long outputAddr, int outputLimit) {
        if (valCount == 0) return 0;
        int dictEntriesCount = UnsafeUtil.getInt(inputAddr);
        int dictSize = dictEntriesCount << 3;
        long dictAddr = inputAddr + 4;

        ByteSlice dataEntryIds = ByteSlice.allocateDirect(valCount << 2);
        long encodeddataEntryIdsAddr = inputAddr + 4 + dictSize + 4;
        RLEPackingHybridCodec dataCodec = new RLEPackingHybridCodec(VarInteger.bitWidth(dictEntriesCount - 1));
        int consumedSize = dataCodec.decodeInts(
                valCount,
                encodeddataEntryIdsAddr,
                dataEntryIds.address(),
                dataEntryIds.size());
        if (consumedSize < 0) {
            dataEntryIds.free();
            return consumedSize;
        }

        for (int i = 0; i < valCount; i++) {
            int vid = dataEntryIds.getInt(i << 2);
            double v = getDouble(dictAddr + (vid << 3));
            setDouble(outputAddr + (i << 3), v);
        }

        dataEntryIds.free();

        return 4 + dictSize + 4 + consumedSize;
    }

    @Override
    public int encodeStrings(int valCount, long inputAddr, long outputAddr, int outputLimit, Wrapper extraInfo) {
        if (valCount == 0) return 0;

        Object2IntLinkedOpenHashMap<OffheapBytes> dict = new Object2IntLinkedOpenHashMap<>();
        dict.defaultReturnValue(-1);

        ByteSlice dataEntryIds = ByteSlice.allocateDirect(valCount << 2);

        StringsStruct stringDatas = new StringsStruct(valCount, inputAddr);
        int maxDictEntryCount = (int) Math.ceil(valCount * dictEntryRatioThreshold);
        int totalDictStringLen = 0;
        for (int i = 0; i < valCount; i++) {
            OffheapBytes v = stringDatas.getString(i);
            int vid = dict.getInt(v);
            if (vid == -1) {
                if (dict.size() >= maxDictEntryCount) {
                    dict.clear();
                    return ErrorCode.DATA_IMPROPER;
                }
                vid = dict.size();
                dict.put(v, vid);
            }
            dataEntryIds.putInt(i << 2, vid);
        }

        ObjectIterator<OffheapBytes> it = dict.keySet().iterator();
        OffheapBytes[] entries = new OffheapBytes[dict.size()];
        int id = 0;
        while (it.hasNext()) {
            entries[id] = it.next();
            id++;
        }
        assert id == dict.size();
        dict.clear();

        // | dict entry count |   dict   |  dataSize |  data(encoded ids) |
        //         4            dictSize       4           dataSize

        // Write dictionary entries.
        int dictEntriesCount = entries.length;
        UnsafeUtil.setInt(outputAddr, dictEntriesCount);
        StringsStruct dictEntriesData = StringsStruct.from(entries, outputAddr + 4);
        int dictSize = dictEntriesData.totalLen();

        // Write data entry ids.
        RLEPackingHybridCodec dataCodec = new RLEPackingHybridCodec(VarInteger.bitWidth(dictEntriesCount - 1));
        int dataSize = dataCodec.encodeInts(
                valCount,
                dataEntryIds.address(),
                outputAddr + 4 + dictSize + 4,
                outputLimit - (4 + dictSize + 4));
        if (dataSize < 0) {
            dataEntryIds.free();
            return dataSize;
        }
        UnsafeUtil.setInt(outputAddr + 4 + dictSize, dataSize);

        int encodedSize = 4 + dictSize + 4 + dataSize;
        if (encodedSize > outputLimit) {
            return ErrorCode.BUFFER_OVERFLOW;
        }
        return encodedSize;
    }

    @Override
    public int decodeStrings(int valCount, long inputAddr, long outputAddr, int outputLimit) {
        if (valCount == 0) return 0;
        int dictEntriesCount = UnsafeUtil.getInt(inputAddr);
        StringsStruct dictEntriesData = new StringsStruct(dictEntriesCount, inputAddr + 4);

        ByteSlice dataEntryIds = ByteSlice.allocateDirect(valCount << 2);
        long encodeddataEntryIdsAddr = inputAddr + 4 + dictEntriesData.totalLen() + 4;
        RLEPackingHybridCodec dataCodec = new RLEPackingHybridCodec(VarInteger.bitWidth(dictEntriesCount - 1));
        int consumedSize = dataCodec.decodeInts(
                valCount,
                encodeddataEntryIdsAddr,
                dataEntryIds.address(),
                dataEntryIds.size());

        StringsStruct.Builder stringDataBuilder = new StringsStruct.Builder(valCount, outputAddr);
        OffheapBytes container = new OffheapBytes();
        for (int i = 0; i < valCount; i++) {
            int vid = dataEntryIds.getInt(i << 2);
            dictEntriesData.getString(vid, container);
            stringDataBuilder.addString(null, container.addr(), container.len());
        }
        stringDataBuilder.build();

        dataEntryIds.free();

        return 4 + dictEntriesData.totalLen() + 4 + consumedSize;
    }
}
