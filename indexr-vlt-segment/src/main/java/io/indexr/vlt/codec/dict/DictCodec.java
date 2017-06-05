package io.indexr.vlt.codec.dict;

import com.google.common.base.Preconditions;

import io.indexr.vlt.codec.Codec;
import io.indexr.vlt.codec.CodecType;
import io.indexr.vlt.codec.ErrorCode;
import io.indexr.vlt.codec.UnsafeUtil;

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
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import it.unimi.dsi.fastutil.objects.ObjectRBTreeSet;

import java.util.Arrays;

import io.indexr.data.DictStruct;
import io.indexr.data.OffheapBytes;
import io.indexr.data.StringsStruct;
import io.indexr.io.ByteSlice;
import io.indexr.util.Wrapper;

import static io.indexr.vlt.codec.UnsafeUtil.copyMemory;
import static io.indexr.vlt.codec.UnsafeUtil.getDouble;
import static io.indexr.vlt.codec.UnsafeUtil.getFloat;
import static io.indexr.vlt.codec.UnsafeUtil.getInt;
import static io.indexr.vlt.codec.UnsafeUtil.getLong;
import static io.indexr.vlt.codec.UnsafeUtil.setDouble;
import static io.indexr.vlt.codec.UnsafeUtil.setFloat;
import static io.indexr.vlt.codec.UnsafeUtil.setInt;
import static io.indexr.vlt.codec.UnsafeUtil.setLong;


/**
 * The encoded data is in {@link DictStruct} format. And you should pass the same {@link DictStruct} to decode methods.
 *
 * Node that the extraInfo is allocated on the inputAddr. So make sure you store the info before release inputAddr.
 *
 * <pre>
 * | entryCount |   dict   |  dataSize |  data(encoded ids) |
 *      4         dictSize       4           dataSize
 * |<-----                encodedSize                 ----->|
 * </pre>
 */
public class DictCodec implements Codec {
    public static final CodecType TYPE = CodecType.DICT;

    private final float dictEntryRatioThreshold;

    /**
     * For decoder.
     */
    public DictCodec() {
        this(1);
    }

    public DictCodec(float dictEntryRatioThreshold) {
        Preconditions.checkState(dictEntryRatioThreshold <= 1);

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
        int dictSize = dictEntryCount << 2;
        if (4 + dictSize > outputLimit) {return ErrorCode.BUFFER_OVERFLOW;}
        UnsafeUtil.setInt(outputAddr, dictEntryCount);
        UnsafeUtil.copyMemory(
                entries, UnsafeUtil.INT_ARRAY_OFFSET,
                null, outputAddr + 4,
                dictSize);
        // Write data entry ids.
        int dataSize = valCount << 2;
        if (4 + dictSize + 4 + dataSize > outputLimit) {return ErrorCode.BUFFER_OVERFLOW;}
        UnsafeUtil.setInt(outputAddr + 4 + dictSize, dataSize);
        copyMemory(dataEntryIds.address(), outputAddr + 4 + dictSize + 4, dataSize);

        if (4 + dictSize + 4 + dataSize > outputLimit) {
            return ErrorCode.BUFFER_OVERFLOW;
        }
        return 4 + dictSize + 4 + dataSize;
    }

    @Override
    public int decodeInts(int valCount, long inputAddr, long outputAddr, int outputLimit) {
        if (valCount == 0) return 0;
        int dictEntryCount = UnsafeUtil.getInt(inputAddr);
        int dictSize = dictEntryCount << 2;
        long dictAddr = inputAddr + 4;

        int dataSize = getInt(inputAddr + 4 + dictSize);
        assert dataSize == valCount << 2;
        long dataEntryIdsAddr = inputAddr + 4 + dictSize + 4;

        for (int i = 0; i < valCount; i++) {
            int vid = getInt(dataEntryIdsAddr + (i << 2));
            int v = getInt(dictAddr + (vid << 2));
            setInt(outputAddr + (i << 2), v);
        }

        return 4 + dictSize + 4 + dataSize;
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

        // Write dictionary entries.
        int dictSize = dictEntryCount << 3;
        if (4 + dictSize > outputLimit) {return ErrorCode.BUFFER_OVERFLOW;}
        UnsafeUtil.setInt(outputAddr, dictEntryCount);
        UnsafeUtil.copyMemory(
                entries, UnsafeUtil.LONG_ARRAY_OFFSET,
                null, outputAddr + 4,
                dictSize);
        // Write data entry ids.
        int dataSize = valCount << 2;
        if (4 + dictSize + 4 + dataSize > outputLimit) {return ErrorCode.BUFFER_OVERFLOW;}
        UnsafeUtil.setInt(outputAddr + 4 + dictSize, dataSize);
        copyMemory(dataEntryIds.address(), outputAddr + 4 + dictSize + 4, dataSize);

        if (4 + dictSize + 4 + dataSize > outputLimit) {
            return ErrorCode.BUFFER_OVERFLOW;
        }
        return 4 + dictSize + 4 + dataSize;
    }

    @Override
    public int decodeLongs(int valCount, long inputAddr, long outputAddr, int outputLimit) {
        if (valCount == 0) return 0;
        int dictEntryCount = UnsafeUtil.getInt(inputAddr);
        int dictSize = dictEntryCount << 3;
        long dictAddr = inputAddr + 4;

        int dataSize = getInt(inputAddr + 4 + dictSize);
        assert dataSize == valCount << 2;
        long dataEntryIdsAddr = inputAddr + 4 + dictSize + 4;

        for (int i = 0; i < valCount; i++) {
            int vid = getInt(dataEntryIdsAddr + (i << 2));
            long v = getLong(dictAddr + (vid << 3));
            setLong(outputAddr + (i << 3), v);
        }

        return 4 + dictSize + 4 + dataSize;
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
        int dictSize = dictEntryCount << 2;
        if (4 + dictSize > outputLimit) {return ErrorCode.BUFFER_OVERFLOW;}
        UnsafeUtil.setInt(outputAddr, dictEntryCount);
        UnsafeUtil.copyMemory(
                entries, UnsafeUtil.FLOAT_ARRAY_OFFSET,
                null, outputAddr + 4,
                dictSize);
        // Write data entry ids.
        int dataSize = valCount << 2;
        if (4 + dictSize + 4 + dataSize > outputLimit) {return ErrorCode.BUFFER_OVERFLOW;}
        UnsafeUtil.setInt(outputAddr + 4 + dictSize, dataSize);
        copyMemory(dataEntryIds.address(), outputAddr + 4 + dictSize + 4, dataSize);

        if (4 + dictSize + 4 + dataSize > outputLimit) {
            return ErrorCode.BUFFER_OVERFLOW;
        }
        return 4 + dictSize + 4 + dataSize;
    }

    @Override
    public int decodeFloats(int valCount, long inputAddr, long outputAddr, int outputLimit) {
        if (valCount == 0) return 0;
        int dictEntryCount = UnsafeUtil.getInt(inputAddr);
        int dictSize = dictEntryCount << 2;
        long dictAddr = inputAddr + 4;

        int dataSize = getInt(inputAddr + 4 + dictSize);
        assert dataSize == valCount << 2;
        long dataEntryIdsAddr = inputAddr + 4 + dictSize + 4;

        for (int i = 0; i < valCount; i++) {
            int vid = getInt(dataEntryIdsAddr + (i << 2));
            float v = getFloat(dictAddr + (vid << 2));
            setFloat(outputAddr + (i << 2), v);
        }

        return 4 + dictSize + 4 + dataSize;
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

        // Write dictionary entries.
        int dictSize = dictEntryCount << 3;
        if (4 + dictSize > outputLimit) {return ErrorCode.BUFFER_OVERFLOW;}
        UnsafeUtil.setInt(outputAddr, dictEntryCount);
        UnsafeUtil.copyMemory(
                entries, UnsafeUtil.DOUBLE_ARRAY_OFFSET,
                null, outputAddr + 4,
                dictSize);
        // Write data entry ids.
        int dataSize = valCount << 2;
        if (4 + dictSize + 4 + dataSize > outputLimit) {return ErrorCode.BUFFER_OVERFLOW;}
        UnsafeUtil.setInt(outputAddr + 4 + dictSize, dataSize);
        copyMemory(dataEntryIds.address(), outputAddr + 4 + dictSize + 4, dataSize);

        if (4 + dictSize + 4 + dataSize > outputLimit) {
            return ErrorCode.BUFFER_OVERFLOW;
        }
        return 4 + dictSize + 4 + dataSize;
    }

    @Override
    public int decodeDoubles(int valCount, long inputAddr, long outputAddr, int outputLimit) {
        if (valCount == 0) return 0;
        int dictEntryCount = UnsafeUtil.getInt(inputAddr);
        int dictSize = dictEntryCount << 3;
        long dictAddr = inputAddr + 4;

        int dataSize = getInt(inputAddr + 4 + dictSize);
        assert dataSize == valCount << 2;
        long dataEntryIdsAddr = inputAddr + 4 + dictSize + 4;

        for (int i = 0; i < valCount; i++) {
            int vid = getInt(dataEntryIdsAddr + (i << 2));
            double v = getDouble(dictAddr + (vid << 3));
            setDouble(outputAddr + (i << 3), v);
        }

        return 4 + dictSize + 4 + dataSize;
    }

    @Override
    public int encodeStrings(int valCount, long inputAddr, long outputAddr, int outputLimit, Wrapper extraInfo) {
        if (valCount == 0) return 0;
        ObjectRBTreeSet<OffheapBytes> dict = new ObjectRBTreeSet<>();

        StringsStruct stringDatas = new StringsStruct(valCount, inputAddr);
        int maxDictEntryCount = (int) Math.ceil(valCount * dictEntryRatioThreshold);
        int totalDictStringLen = 0;
        for (int i = 0; i < valCount; i++) {
            OffheapBytes v = stringDatas.getString(i);
            if (dict.add(v)) {
                totalDictStringLen += v.len();
                if (dict.size() > maxDictEntryCount) {
                    dict.clear();
                    return ErrorCode.DATA_IMPROPER;
                }
            }
        }

        ObjectIterator<OffheapBytes> it = dict.iterator();
        OffheapBytes[] entries = new OffheapBytes[dict.size()];
        int id = 0;
        while (it.hasNext()) {
            entries[id] = it.next();
            id++;
        }
        assert id == dict.size();
        dict.clear();
        int dictEntryCount = entries.length;

        ByteSlice dataEntryIds = ByteSlice.allocateDirect(valCount << 2);
        OffheapBytes strContainer = new OffheapBytes();
        for (int i = 0; i < valCount; i++) {
            stringDatas.getString(i, strContainer);
            int vid = Arrays.binarySearch(entries, strContainer);
            assert vid >= 0;
            // data index to entry id
            dataEntryIds.putInt(i << 2, vid);
        }

        // Write dictionary entries.
        UnsafeUtil.setInt(outputAddr, dictEntryCount);
        StringsStruct dictEntriesData = StringsStruct.from(entries, outputAddr + 4);
        int dictSize = dictEntriesData.totalLen();
        if (4 + dictSize > outputLimit) {return ErrorCode.BUFFER_OVERFLOW;}
        // Write data entry ids.
        int dataSize = valCount << 2;
        if (4 + dictSize + 4 + dataSize > outputLimit) {return ErrorCode.BUFFER_OVERFLOW;}
        UnsafeUtil.setInt(outputAddr + 4 + dictSize, dataSize);
        copyMemory(dataEntryIds.address(), outputAddr + 4 + dictSize + 4, dataSize);

        if (4 + dictSize + 4 + dataSize > outputLimit) {
            return ErrorCode.BUFFER_OVERFLOW;
        }
        return 4 + dictSize + 4 + dataSize;
    }

    @Override
    public int decodeStrings(int valCount, long inputAddr, long outputAddr, int outputLimit) {
        if (valCount == 0) return 0;
        int dictEntryCount = UnsafeUtil.getInt(inputAddr);
        StringsStruct dictEntriesData = new StringsStruct(dictEntryCount, inputAddr + 4);
        int dictSize = dictEntriesData.totalLen();

        int dataSize = getInt(inputAddr + 4 + dictSize);
        assert dataSize == valCount << 2;
        long dataEntryIdsAddr = inputAddr + 4 + dictSize + 4;

        StringsStruct.Builder stringDataBuilder = new StringsStruct.Builder(valCount, outputAddr);
        OffheapBytes container = new OffheapBytes();
        for (int i = 0; i < valCount; i++) {
            int vid = getInt(dataEntryIdsAddr + (i << 2));
            dictEntriesData.getString(vid, container);
            stringDataBuilder.addString(null, container.addr(), container.len());
        }
        stringDataBuilder.build();

        return 4 + dictSize + 4 + dataSize;
    }
}
