package io.indexr.segment.pack;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import org.apache.spark.unsafe.types.UTF8String;

import java.util.List;

import io.indexr.io.ByteSlice;
import io.indexr.segment.ColumnType;
import io.indexr.segment.PackExtIndex;
import io.indexr.segment.PackRSIndexNum;
import io.indexr.segment.PackRSIndexStr;
import io.indexr.segment.SegmentMode;
import io.indexr.segment.index.RSIndex_Histogram;
import io.indexr.segment.index.RSIndex_Histogram_V2;
import io.indexr.segment.storage.PackBundle;
import io.indexr.segment.storage.Version;

public class DataPackCreator_Basic {

    public static PackBundle from(int version, SegmentMode mode, int[] values, int offset, int size) {
        int min, max;
        min = max = values[offset];
        for (int i = offset; i < offset + size; i++) {
            int v = values[i];
            min = Math.min(v, min);
            max = Math.max(v, max);
        }
        switch (version) {
            case Version.VERSION_0_ID:
                return _from_v0(version, mode, values, offset, size, min, max);
            default:
                return _from_v1(version, mode, values, offset, size, min, max);
        }
    }

    public static PackBundle from(int version, SegmentMode mode, long[] values, int offset, int size) {
        long min, max;
        min = max = values[offset];
        for (int i = 0; i < size; i++) {
            long v = values[offset + i];
            min = Math.min(v, min);
            max = Math.max(v, max);
        }
        switch (version) {
            case Version.VERSION_0_ID:
                return _from_v0(version, mode, values, offset, size, NumType.NLong, min, max);
            default:
                return _from_v1(version, mode, values, offset, size, NumType.NLong, min, max);
        }
    }

    public static PackBundle from(int version, SegmentMode mode, float[] values, int offset, int size) {
        float min, max;
        min = max = values[offset];
        for (int i = offset; i < offset + size; i++) {
            float v = values[i];
            min = Math.min(v, min);
            max = Math.max(v, max);
        }
        switch (version) {
            case Version.VERSION_0_ID:
                return _from_v0(version, mode, values, offset, size, min, max);
            default:
                return _from_v1(version, mode, values, offset, size, min, max);
        }
    }

    public static PackBundle from(int version, SegmentMode mode, double[] values, int offset, int size) {
        double min, max;
        min = max = values[offset];
        for (int i = offset; i < offset + size; i++) {
            double v = values[i];
            min = Math.min(v, min);
            max = Math.max(v, max);
        }
        switch (version) {
            case Version.VERSION_0_ID:
                return _from_v0(version, mode, values, offset, size, min, max);
            default:
                return _from_v1(version, mode, values, offset, size, min, max);
        }
    }

    private static PackBundle _from_v1(int version,
                                       SegmentMode mode,
                                       int[] values,
                                       int offset,
                                       int size,
                                       int origMin,
                                       int origMax) {
        assert values.length > 0;
        assert size > 0 && size <= DataPack.MAX_COUNT;

        byte dataType = ColumnType.INT;
        PackRSIndexNum index = (PackRSIndexNum) mode.versionAdapter.createPackRSIndex(version, mode, dataType);
        int min = 0; // Useless here.
        int max = values[offset];
        for (int i = offset; i < offset + size; i++) {
            int val = values[i];
            if (Integer.compareUnsigned(max, val) < 0) {
                max = val;
            }
            index.putValue(val, origMin, origMax);
        }

        byte type = NumType.NInt;
        DataPackNode_Basic dpn = (DataPackNode_Basic) mode.versionAdapter.createDPN(version, mode);
        dpn.setPackType(DataPackType.Number);
        dpn.setObjCount(size);
        dpn.setNumType(type);
        dpn.setUniformMin(min);
        dpn.setUniformMax(max);
        dpn.setMinValue(origMin);
        dpn.setMaxValue(origMax);

        ByteSlice data = NumOp.allocatByteSlice(type, size);
        long addr = data.address();
        for (int i = offset; i < offset + size; i++) {
            NumOp.putInt(addr, i, values[i]);
        }

        DataPack dataPack = new DataPack(data, null, dpn);
        dataPack.compress(dataType, false, dpn);

        PackExtIndex extIndex = mode.versionAdapter.createExtIndex(version, mode, dataType, false, dpn, dataPack, null);

        return new PackBundle(dataPack, dpn, index, extIndex);
    }

    private static PackBundle _from_v1(int version,
                                       SegmentMode mode,
                                       long[] values,
                                       int offset,
                                       int size,
                                       byte suggestType,
                                       long origMin,
                                       long origMax) {
        assert values.length > 0;
        assert size > 0 && size <= DataPack.MAX_COUNT;

        byte dataType = ColumnType.LONG;
        PackRSIndexNum index = (PackRSIndexNum) mode.versionAdapter.createPackRSIndex(version, mode, dataType);
        long min = 0;
        long max = values[offset];
        for (int i = offset; i < offset + size; i++) {
            long val = values[i];
            if (Long.compareUnsigned(max, val) < 0) {
                max = val;
            }
            index.putValue(val, origMin, origMax);
        }

        byte type = NumType.NLong;
        DataPackNode_Basic dpn = (DataPackNode_Basic) mode.versionAdapter.createDPN(version, mode);
        dpn.setPackType(DataPackType.Number);
        dpn.setObjCount(size);
        dpn.setNumType(type);
        dpn.setUniformMin(min);
        dpn.setUniformMax(max);
        dpn.setMinValue(origMin);
        dpn.setMaxValue(origMax);

        ByteSlice data = NumOp.allocatByteSlice(type, size);
        long addr = data.address();
        for (int i = offset; i < offset + size; i++) {
            NumOp.putLong(addr, i, values[i]);
        }

        DataPack dataPack = new DataPack(data, null, dpn);
        dataPack.compress(dataType, false, dpn);

        PackExtIndex extIndex = mode.versionAdapter.createExtIndex(version, mode, dataType, false, dpn, dataPack, null);

        return new PackBundle(dataPack, dpn, index, extIndex);
    }

    private static PackBundle _from_v1(int version,
                                       SegmentMode mode,
                                       float[] values,
                                       int offset,
                                       int size,
                                       float origMin,
                                       float origMax) {
        assert values.length > 0;
        assert size > 0 && size <= DataPack.MAX_COUNT;

        byte dataType = ColumnType.FLOAT;
        PackRSIndexNum index = (PackRSIndexNum) mode.versionAdapter.createPackRSIndex(version, mode, dataType);
        long uniformOrignMin = NumType.floatToLong(origMin);
        long uniformOrignMax = NumType.floatToLong(origMax);
        long min = 0;

        // Float is a little special.
        // We store their 4 bytes raw values directly.
        // But also need the uniform val to build the index,
        // which first convert to double then to long.

        int max = Float.floatToRawIntBits(values[offset]);
        for (int i = offset; i < offset + size; i++) {
            int val = Float.floatToRawIntBits(values[i]);
            if (Integer.compareUnsigned(max, val) < 0) {
                max = val;
            }
            long uniforVal = NumType.floatToLong(values[i]);
            index.putValue(uniforVal, uniformOrignMin, uniformOrignMax);
        }

        byte type = NumType.NInt;
        DataPackNode_Basic dpn = (DataPackNode_Basic) mode.versionAdapter.createDPN(version, mode);
        dpn.setPackType(DataPackType.Number);
        dpn.setObjCount(size);
        dpn.setNumType(type);
        dpn.setUniformMin(min);
        dpn.setUniformMax(max);
        dpn.setMinValue(uniformOrignMin);
        dpn.setMaxValue(uniformOrignMax);

        ByteSlice data = NumOp.allocatByteSlice(type, size);
        long addr = data.address();
        for (int i = offset; i < offset + size; i++) {
            NumOp.putFloat(addr, i, values[i]);
        }

        DataPack dataPack = new DataPack(data, null, dpn);
        dataPack.compress(dataType, false, dpn);

        PackExtIndex extIndex = mode.versionAdapter.createExtIndex(version, mode, dataType, false, dpn, dataPack, null);

        return new PackBundle(dataPack, dpn, index, extIndex);
    }

    private static PackBundle _from_v1(int version,
                                       SegmentMode mode,
                                       double[] values,
                                       int offset,
                                       int size,
                                       double origMin,
                                       double origMax) {
        assert values.length > 0;
        assert size > 0 && size <= DataPack.MAX_COUNT;

        byte dataType = ColumnType.DOUBLE;
        PackRSIndexNum index = (PackRSIndexNum) mode.versionAdapter.createPackRSIndex(version, mode, dataType);
        long uniformOrignMin = NumType.doubleToLong(origMin);
        long uniformOrignMax = NumType.doubleToLong(origMax);
        long min = 0;
        long max = NumType.doubleToLong(values[offset]);
        for (int i = offset; i < offset + size; i++) {
            long val = NumType.doubleToLong(values[i]);
            if (Long.compareUnsigned(max, val) < 0) {
                max = val;
            }
            index.putValue(val, uniformOrignMin, uniformOrignMax);
        }

        byte type = NumType.NLong;
        DataPackNode_Basic dpn = (DataPackNode_Basic) mode.versionAdapter.createDPN(version, mode);
        dpn.setPackType(DataPackType.Number);
        dpn.setObjCount(size);
        dpn.setNumType(type);
        dpn.setUniformMin(min);
        dpn.setUniformMax(max);
        dpn.setMinValue(uniformOrignMin);
        dpn.setMaxValue(uniformOrignMax);

        ByteSlice data = NumOp.allocatByteSlice(type, size);
        long addr = data.address();
        for (int i = offset; i < offset + size; i++) {
            NumOp.putDouble(addr, i, values[i]);
        }

        DataPack dataPack = new DataPack(data, null, dpn);
        dataPack.compress(dataType, false, dpn);

        PackExtIndex extIndex = mode.versionAdapter.createExtIndex(version, mode, dataType, false, dpn, dataPack, null);

        return new PackBundle(dataPack, dpn, index, extIndex);
    }

    //=======================================================================
    // VERSION_0 deprecated

    private static PackRSIndexNum createPackRSIndex(int version, boolean isFloat) {
        if (version < Version.VERSION_6_ID) {
            return new RSIndex_Histogram.PackIndex(isFloat);
        } else {
            return new RSIndex_Histogram_V2.PackIndex(isFloat);
        }
    }

    private static PackBundle _from_v0(int version,
                                       SegmentMode mode,
                                       int[] values,
                                       int offset,
                                       int size,
                                       int origMin,
                                       int origMax) {
        assert values.length > 0;
        assert size > 0 && size <= DataPack.MAX_COUNT;

        byte dataType = ColumnType.INT;
        PackRSIndexNum index = (PackRSIndexNum) mode.versionAdapter.createPackRSIndex(version, mode, dataType);
        long min, max;
        min = max = values[offset];
        for (int i = offset; i < offset + size; i++) {
            int val = values[i];
            min = Math.min(min, val);
            max = Math.max(max, val);
            index.putValue(val, origMin, origMax);
        }

        byte type = NumType.NInt;
        // Simple check for overflow.
        if (max - min < 0) {
            type = NumType.NLong;
        } else {
            type = NumType.fromMaxValue(max - min);
        }
        DataPackNode_Basic dpn = (DataPackNode_Basic) mode.versionAdapter.createDPN(version, mode);
        dpn.setPackType(DataPackType.Number);
        dpn.setObjCount(size);
        dpn.setNumType(type);
        dpn.setUniformMin(min);
        dpn.setUniformMax(max);
        dpn.setMinValue(origMin);
        dpn.setMaxValue(origMax);

        ByteSlice data = NumOp.allocatByteSlice(type, size);
        if (type != NumType.NZero) {
            for (int i = offset; i < offset + size; i++) {
                NumOp.putVal(type, data, i, values[i], min);
            }
        }

        DataPack dataPack = new DataPack(data, null, dpn);
        dataPack.compress(dataType, false, dpn);

        PackExtIndex extIndex = mode.versionAdapter.createExtIndex(version, mode, dataType, false, dpn, dataPack, null);

        return new PackBundle(dataPack, dpn, index, extIndex);
    }

    private static PackBundle _from_v0(int version,
                                       SegmentMode mode,
                                       long[] values,
                                       int offset,
                                       int size,
                                       byte suggestType,
                                       long origMin,
                                       long origMax) {
        assert values.length > 0;
        assert size > 0 && size <= DataPack.MAX_COUNT;

        byte dataType = ColumnType.LONG;
        PackRSIndexNum index = (PackRSIndexNum) mode.versionAdapter.createPackRSIndex(version, mode, dataType);
        long min, max;
        min = max = values[offset];
        for (int i = offset; i < offset + size; i++) {
            long val = values[i];
            min = Math.min(min, val);
            max = Math.max(max, val);
            index.putValue(val, origMin, origMax);
        }

        byte type = NumType.NLong;
        // Simple check for overflow.
        if (max - min < 0) {
            type = NumType.NLong;
        } else {
            type = NumType.fromMaxValue(max - min);
        }
        DataPackNode_Basic dpn = (DataPackNode_Basic) mode.versionAdapter.createDPN(version, mode);
        dpn.setPackType(DataPackType.Number);
        dpn.setObjCount(size);
        dpn.setNumType(type);
        dpn.setUniformMin(min);
        dpn.setUniformMax(max);
        dpn.setMinValue(origMin);
        dpn.setMaxValue(origMax);

        ByteSlice data = NumOp.allocatByteSlice(type, size);
        if (type != NumType.NZero) {
            for (int i = offset; i < offset + size; i++) {
                NumOp.putVal(type, data, i, values[i], min);
            }
        }

        DataPack dataPack = new DataPack(data, null, dpn);
        dataPack.compress(dataType, false, dpn);

        PackExtIndex extIndex = mode.versionAdapter.createExtIndex(version, mode, dataType, false, dpn, dataPack, null);

        return new PackBundle(dataPack, dpn, index, extIndex);
    }

    private static PackBundle _from_v0(int version,
                                       SegmentMode mode,
                                       float[] values,
                                       int offset,
                                       int size,
                                       float origMin,
                                       float origMax) {
        assert values.length > 0;
        assert size > 0 && size <= DataPack.MAX_COUNT;

        byte dataType = ColumnType.FLOAT;
        PackRSIndexNum index = (PackRSIndexNum) mode.versionAdapter.createPackRSIndex(version, mode, dataType);
        long uniformOrignMin = NumType.floatToLong(origMin);
        long uniformOrignMax = NumType.floatToLong(origMax);
        long min, max;
        min = max = NumType.floatToLong(values[offset]);
        for (int i = offset; i < offset + size; i++) {
            long val = NumType.floatToLong(values[i]);
            min = Math.min(min, val);
            max = Math.max(max, val);
            index.putValue(val, uniformOrignMin, uniformOrignMax);
        }

        byte type = NumType.NInt;
        // Simple check for overflow.
        if (max - min < 0) {
            type = NumType.NLong;
        } else {
            type = NumType.fromMaxValue(max - min);
        }
        DataPackNode_Basic dpn = (DataPackNode_Basic) mode.versionAdapter.createDPN(version, mode);
        dpn.setPackType(DataPackType.Number);
        dpn.setObjCount(size);
        dpn.setNumType(type);
        dpn.setUniformMin(min);
        dpn.setUniformMax(max);
        dpn.setMinValue(uniformOrignMin);
        dpn.setMaxValue(uniformOrignMax);

        ByteSlice data = NumOp.allocatByteSlice(type, size);
        if (type != NumType.NZero) {
            for (int i = offset; i < offset + size; i++) {
                NumOp.putVal(type, data, i, NumType.floatToLong(values[i]), min);
            }
        }

        DataPack dataPack = new DataPack(data, null, dpn);
        dataPack.compress(dataType, false, dpn);

        PackExtIndex extIndex = mode.versionAdapter.createExtIndex(version, mode, dataType, false, dpn, dataPack, null);

        return new PackBundle(dataPack, dpn, index, extIndex);
    }

    private static PackBundle _from_v0(int version,
                                       SegmentMode mode,
                                       double[] values,
                                       int offset,
                                       int size,
                                       double origMin,
                                       double origMax) {
        assert values.length > 0;
        assert size > 0 && size <= DataPack.MAX_COUNT;

        byte dataType = ColumnType.DOUBLE;
        PackRSIndexNum index = (PackRSIndexNum) mode.versionAdapter.createPackRSIndex(version, mode, dataType);
        long uniformOrignMin = NumType.doubleToLong(origMin);
        long uniformOrignMax = NumType.doubleToLong(origMax);
        long min, max;
        min = max = NumType.doubleToLong(values[offset]);
        for (int i = offset; i < offset + size; i++) {
            long val = NumType.doubleToLong(values[i]);
            min = Math.min(min, val);
            max = Math.max(max, val);
            index.putValue(val, uniformOrignMin, uniformOrignMax);
        }

        byte type = NumType.NLong;
        // Simple check for overflow.
        if (max - min < 0) {
            type = NumType.NLong;
        } else {
            type = NumType.fromMaxValue(max - min);
        }
        DataPackNode_Basic dpn = (DataPackNode_Basic) mode.versionAdapter.createDPN(version, mode);
        dpn.setPackType(DataPackType.Number);
        dpn.setObjCount(size);
        dpn.setNumType(type);
        dpn.setUniformMin(min);
        dpn.setUniformMax(max);
        dpn.setMinValue(uniformOrignMin);
        dpn.setMaxValue(uniformOrignMax);

        ByteSlice data = NumOp.allocatByteSlice(type, size);
        if (type != NumType.NZero) {
            for (int i = offset; i < offset + size; i++) {
                NumOp.putVal(type, data, i, NumType.doubleToLong(values[i]), min);
            }
        }

        DataPack dataPack = new DataPack(data, null, dpn);
        dataPack.compress(dataType, false, dpn);

        PackExtIndex extIndex = mode.versionAdapter.createExtIndex(version, mode, dataType, false, dpn, dataPack, null);

        return new PackBundle(dataPack, dpn, index, extIndex);
    }

    private static final int MAX_STR_LEN_RAW = ColumnType.MAX_STRING_UTF8_SIZE;

    public static PackBundle fromJavaString(int version, SegmentMode mode, List<? extends CharSequence> strings) {
        return from(version, mode, Lists.transform(strings, s -> UTF8String.fromString(s.toString())));
    }

    public static PackBundle from(int version, SegmentMode mode, List<UTF8String> strings) {
        PackRSIndexStr index = (PackRSIndexStr) mode.versionAdapter.createPackRSIndex(version, mode, ColumnType.STRING);
        switch (version) {
            case Version.VERSION_0_ID:
                return _from_v0(
                        version,
                        mode,
                        strings,
                        index);
            default:
                return _from_v1(
                        version,
                        mode,
                        strings,
                        index);
        }
    }

    /**
     * We encode the string as UTF-8.
     */
    private static PackBundle _from_v1(int version,
                                       SegmentMode mode,
                                       List<UTF8String> strings,
                                       PackRSIndexStr index) {
        int size = strings.size();
        Preconditions.checkArgument(size > 0 && size <= DataPack.MAX_COUNT);

        int strTotalLen = 0;
        for (int i = 0; i < size; i++) {
            UTF8String s = strings.get(i);
            int byteCount = s.numBytes();
            Preconditions.checkState(byteCount <= MAX_STR_LEN_RAW, "string in utf-8 should be smaller than %s bytes", MAX_STR_LEN_RAW);
            strTotalLen += byteCount;

            index.putValue(s);
        }
        int indexLen = (size + 1) << 2;
        ByteSlice data = ByteSlice.allocateDirect(indexLen + strTotalLen);

        int offset = 0;
        for (int i = 0; i < size; i++) {
            UTF8String s = strings.get(i);
            byte[] bytes = s.getBytes();
            int byteLen = bytes.length;
            int start = offset;
            int end = start + byteLen;
            data.putInt(i << 2, start);
            data.putInt((i + 1) << 2, end);
            data.put(indexLen + start, bytes);

            offset = end;
        }

        DataPackNode_Basic dpn = (DataPackNode_Basic) mode.versionAdapter.createDPN(version, mode);
        dpn.setPackType(DataPackType.Raw);
        dpn.setObjCount(size);
        dpn.setMaxObjLen(0); // Always 0

        DataPack dataPack = new DataPack(data, null, dpn);
        dataPack.compress(ColumnType.STRING, false, dpn);

        PackExtIndex extIndex = mode.versionAdapter.createExtIndex(version, mode, ColumnType.STRING, false, dpn, dataPack, null);

        return new PackBundle(dataPack, dpn, index, extIndex);
    }

    /**
     * We encode the string as UTF-8.
     * VERSION_0 deprecated
     */
    private static PackBundle _from_v0(int version,
                                       SegmentMode mode,
                                       List<UTF8String> strings,
                                       PackRSIndexStr index) {
        int size = strings.size();
        Preconditions.checkArgument(size > 0 && size <= DataPack.MAX_COUNT);

        int strTotalLen = 0;
        int strMaxLen = 0;
        for (int i = 0; i < size; i++) {
            UTF8String s = strings.get(i);
            int byteCount = s.numBytes();
            Preconditions.checkState(byteCount <= MAX_STR_LEN_RAW, "string in utf-8 should be smaller than %s bytes", MAX_STR_LEN_RAW);
            strTotalLen += byteCount;
            strMaxLen = Math.max(strMaxLen, byteCount);

            index.putValue(s);
        }
        int indexLen = size << 3;

        ByteSlice data = ByteSlice.allocateDirect(4 + indexLen + strTotalLen);
        data.putInt(0, strTotalLen);


        int lastEnd = 0;
        for (int i = 0; i < size; i++) {
            UTF8String s = strings.get(i);
            byte[] bytes = s.getBytes();
            int byteLen = bytes.length;
            int start = lastEnd;
            int end = start + byteLen;
            data.putInt((i << 3) + 4, start);
            data.putInt((i << 3) + 4 + 4, end);
            data.put(4 + indexLen + start, bytes);

            strMaxLen = Math.max(strMaxLen, byteLen);
            lastEnd = end;
        }

        DataPackNode_Basic dpn = (DataPackNode_Basic) mode.versionAdapter.createDPN(version, mode);
        dpn.setPackType(DataPackType.Raw);
        dpn.setObjCount(size);
        dpn.setMaxObjLen(strMaxLen);

        DataPack dataPack = new DataPack(data, null, dpn);
        dataPack.compress(ColumnType.STRING, false, dpn);

        PackExtIndex extIndex = mode.versionAdapter.createExtIndex(version, mode, ColumnType.STRING, false, dpn, dataPack, null);

        return new PackBundle(dataPack, dpn, index, extIndex);
    }
}