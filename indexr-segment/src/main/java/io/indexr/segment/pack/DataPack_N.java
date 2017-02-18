package io.indexr.segment.pack;

import com.google.common.base.Preconditions;

import java.io.IOException;
import java.nio.channels.WritableByteChannel;

import io.indexr.io.ByteSlice;
import io.indexr.segment.PackExtIndexNum;
import io.indexr.segment.PackRSIndexNum;

class DataPack_N {

    public static void writeToChannel(DataPack pack, WritableByteChannel channel, DataPackNode dpn) throws IOException {
        Preconditions.checkState(pack.cmpData != null);
        channel.write(pack.cmpData.toByteBuffer());
    }

    public static ByteSlice doCompress(DataPack pack, DataPackNode dpn, ByteSlice data) {
        return NumOp.compress(dpn.numType(), data, pack.objCount, dpn.uniformMin(), dpn.uniformMax());
    }

    public static ByteSlice doDecompress(DataPack pack, DataPackNode dpn, ByteSlice cmpData) {
        return NumOp.decompress(dpn.numType(), cmpData, pack.objCount, dpn.uniformMin(), dpn.uniformMax());
    }

    private static PackRSIndexNum createPackRSIndex(int version, boolean isFloat) {
        if (version < Version.VERSION_6_ID) {
            return new RSIndex_Histogram.PackIndex(isFloat);
        } else {
            return new RSIndex_Histogram_V2.PackIndex(isFloat);
        }
    }

    private static PackExtIndexNum createPackExtIndex(int version, boolean useExtIndex) {
        return new PackExtIndex_Unused();
    }

    private static boolean withDefault(boolean[] values, boolean defaultVal) {
        return values.length == 0 ? defaultVal : values[0];
    }

    public static PackBundle from(int version, long[] values, int offset, int size, boolean... useExtIndex) {
        long min, max;
        min = max = values[offset];
        for (int i = 0; i < size; i++) {
            long v = values[offset + i];
            min = Math.min(v, min);
            max = Math.max(v, max);
        }
        switch (version) {
            case Version.VERSION_0_ID:
                return _from_v0(version, values, offset, size, NumType.NLong, min, max);
            default:
                return _from_v1(version, withDefault(useExtIndex, true), values, offset, size, NumType.NLong, min, max);
        }
    }

    public static PackBundle from(int version, int[] values, int offset, int size, boolean... useExtIndex) {
        int min, max;
        min = max = values[offset];
        for (int i = offset; i < offset + size; i++) {
            int v = values[i];
            min = Math.min(v, min);
            max = Math.max(v, max);
        }
        switch (version) {
            case Version.VERSION_0_ID:
                return _from_v0(version, values, offset, size, min, max);
            default:
                return _from_v1(version, withDefault(useExtIndex, true), values, offset, size, min, max);
        }
    }

    public static PackBundle from(int version, float[] values, int offset, int size, boolean... useExtIndex) {
        float min, max;
        min = max = values[offset];
        for (int i = offset; i < offset + size; i++) {
            float v = values[i];
            min = Math.min(v, min);
            max = Math.max(v, max);
        }
        switch (version) {
            case Version.VERSION_0_ID:
                return _from_v0(version, values, offset, size, min, max);
            default:
                return _from_v1(version, withDefault(useExtIndex, true), values, offset, size, min, max);
        }
    }

    public static PackBundle from(int version, double[] values, int offset, int size, boolean... useExtIndex) {
        double min, max;
        min = max = values[offset];
        for (int i = offset; i < offset + size; i++) {
            double v = values[i];
            min = Math.min(v, min);
            max = Math.max(v, max);
        }
        switch (version) {
            case Version.VERSION_0_ID:
                return _from_v0(version, values, offset, size, min, max);
            default:
                return _from_v1(version, withDefault(useExtIndex, true), values, offset, size, min, max);
        }
    }

    private static PackBundle _from_v1(int version,
                                       boolean useExtIndex,
                                       int[] values,
                                       int offset,
                                       int size,
                                       int origMin,
                                       int origMax) {
        assert values.length > 0;
        assert size > 0 && size <= DataPack.MAX_COUNT;

        PackRSIndexNum index = createPackRSIndex(version, false);
        PackExtIndexNum extIndex = createPackExtIndex(version, useExtIndex);
        int min = 0; // Useless here.
        int max = values[offset];
        for (int i = offset; i < offset + size; i++) {
            int val = values[i];
            if (Integer.compareUnsigned(max, val) < 0) {
                max = val;
            }
            index.putValue(val, origMin, origMax);
            extIndex.putValue(i, val);
        }

        byte type = NumType.NInt;
        DataPackNode dpn = new DataPackNode(version);
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

        return new PackBundle(new DataPack(data, null, dpn), dpn, index, extIndex);
    }

    private static PackBundle _from_v1(int version,
                                       boolean useExtIndex,
                                       long[] values,
                                       int offset,
                                       int size,
                                       byte suggestType,
                                       long origMin,
                                       long origMax) {
        assert values.length > 0;
        assert size > 0 && size <= DataPack.MAX_COUNT;

        PackRSIndexNum index = createPackRSIndex(version, false);
        PackExtIndexNum extIndex = createPackExtIndex(version, useExtIndex);
        long min = 0;
        long max = values[offset];
        for (int i = offset; i < offset + size; i++) {
            long val = values[i];
            if (Long.compareUnsigned(max, val) < 0) {
                max = val;
            }
            index.putValue(val, origMin, origMax);
            extIndex.putValue(i, val);
        }

        byte type = NumType.NLong;
        DataPackNode dpn = new DataPackNode(version);
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

        return new PackBundle(new DataPack(data, null, dpn), dpn, index, extIndex);
    }

    private static PackBundle _from_v1(int version,
                                       boolean useExtIndex,
                                       float[] values,
                                       int offset,
                                       int size,
                                       float origMin,
                                       float origMax) {
        assert values.length > 0;
        assert size > 0 && size <= DataPack.MAX_COUNT;

        PackRSIndexNum index = createPackRSIndex(version, true);
        PackExtIndexNum extIndex = createPackExtIndex(version, useExtIndex);
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
            extIndex.putValue(i, uniforVal);
        }

        byte type = NumType.NInt;
        DataPackNode dpn = new DataPackNode(version);
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

        return new PackBundle(new DataPack(data, null, dpn), dpn, index, extIndex);
    }

    private static PackBundle _from_v1(int version,
                                       boolean useExtIndex,
                                       double[] values,
                                       int offset,
                                       int size,
                                       double origMin,
                                       double origMax) {
        assert values.length > 0;
        assert size > 0 && size <= DataPack.MAX_COUNT;

        PackRSIndexNum index = createPackRSIndex(version, true);
        PackExtIndexNum extIndex = createPackExtIndex(version, useExtIndex);
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
            extIndex.putValue(i, val);
        }

        byte type = NumType.NLong;
        DataPackNode dpn = new DataPackNode(version);
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

        return new PackBundle(new DataPack(data, null, dpn), dpn, index, extIndex);
    }

    //=======================================================================
    // VERSION_0 deprecated

    private static PackBundle _from_v0(int version,
                                       int[] values,
                                       int offset,
                                       int size,
                                       int origMin,
                                       int origMax) {
        assert values.length > 0;
        assert size > 0 && size <= DataPack.MAX_COUNT;

        PackRSIndexNum index = createPackRSIndex(version, false);
        PackExtIndexNum extIndex = createPackExtIndex(version, false);
        long min, max;
        min = max = values[offset];
        for (int i = offset; i < offset + size; i++) {
            int val = values[i];
            min = Math.min(min, val);
            max = Math.max(max, val);
            index.putValue(val, origMin, origMax);
            extIndex.putValue(i, val);
        }

        byte type = NumType.NInt;
        // Simple check for overflow.
        if (max - min < 0) {
            type = NumType.NLong;
        } else {
            type = NumType.fromMaxValue(max - min);
        }
        DataPackNode dpn = new DataPackNode(Version.VERSION_0_ID);
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

        return new PackBundle(new DataPack(data, null, dpn), dpn, index, extIndex);
    }

    private static PackBundle _from_v0(int version,
                                       long[] values,
                                       int offset,
                                       int size,
                                       byte suggestType,
                                       long origMin,
                                       long origMax) {
        assert values.length > 0;
        assert size > 0 && size <= DataPack.MAX_COUNT;

        PackRSIndexNum index = createPackRSIndex(version, false);
        PackExtIndexNum extIndex = createPackExtIndex(version, false);
        long min, max;
        min = max = values[offset];
        for (int i = offset; i < offset + size; i++) {
            long val = values[i];
            min = Math.min(min, val);
            max = Math.max(max, val);
            index.putValue(val, origMin, origMax);
            extIndex.putValue(i, val);
        }

        byte type = NumType.NLong;
        // Simple check for overflow.
        if (max - min < 0) {
            type = NumType.NLong;
        } else {
            type = NumType.fromMaxValue(max - min);
        }
        DataPackNode dpn = new DataPackNode(Version.VERSION_0_ID);
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

        return new PackBundle(new DataPack(data, null, dpn), dpn, index, extIndex);
    }

    private static PackBundle _from_v0(int version,
                                       float[] values,
                                       int offset,
                                       int size,
                                       float origMin,
                                       float origMax) {
        assert values.length > 0;
        assert size > 0 && size <= DataPack.MAX_COUNT;

        PackRSIndexNum index = createPackRSIndex(version, true);
        PackExtIndexNum extIndex = createPackExtIndex(version, false);
        long uniformOrignMin = NumType.floatToLong(origMin);
        long uniformOrignMax = NumType.floatToLong(origMax);
        long min, max;
        min = max = NumType.floatToLong(values[offset]);
        for (int i = offset; i < offset + size; i++) {
            long val = NumType.floatToLong(values[i]);
            min = Math.min(min, val);
            max = Math.max(max, val);
            index.putValue(val, uniformOrignMin, uniformOrignMax);
            extIndex.putValue(i, val);
        }

        byte type = NumType.NInt;
        // Simple check for overflow.
        if (max - min < 0) {
            type = NumType.NLong;
        } else {
            type = NumType.fromMaxValue(max - min);
        }
        DataPackNode dpn = new DataPackNode(Version.VERSION_0_ID);
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

        return new PackBundle(new DataPack(data, null, dpn), dpn, index, extIndex);
    }

    private static PackBundle _from_v0(int version,
                                       double[] values,
                                       int offset,
                                       int size,
                                       double origMin,
                                       double origMax) {
        assert values.length > 0;
        assert size > 0 && size <= DataPack.MAX_COUNT;

        PackRSIndexNum index = createPackRSIndex(version, true);
        PackExtIndexNum extIndex = createPackExtIndex(version, false);
        long uniformOrignMin = NumType.doubleToLong(origMin);
        long uniformOrignMax = NumType.doubleToLong(origMax);
        long min, max;
        min = max = NumType.doubleToLong(values[offset]);
        for (int i = offset; i < offset + size; i++) {
            long val = NumType.doubleToLong(values[i]);
            min = Math.min(min, val);
            max = Math.max(max, val);
            index.putValue(val, uniformOrignMin, uniformOrignMax);
            extIndex.putValue(i, val);
        }

        byte type = NumType.NLong;
        // Simple check for overflow.
        if (max - min < 0) {
            type = NumType.NLong;
        } else {
            type = NumType.fromMaxValue(max - min);
        }
        DataPackNode dpn = new DataPackNode(Version.VERSION_0_ID);
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

        return new PackBundle(new DataPack(data, null, dpn), dpn, index, extIndex);
    }
}
