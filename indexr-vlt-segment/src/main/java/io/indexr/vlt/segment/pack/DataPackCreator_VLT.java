package io.indexr.vlt.segment.pack;

import com.google.common.base.Preconditions;

import org.apache.spark.unsafe.types.UTF8String;

import java.util.List;

import io.indexr.io.ByteSlice;
import io.indexr.segment.ColumnType;
import io.indexr.segment.PackExtIndex;
import io.indexr.segment.PackRSIndexNum;
import io.indexr.segment.PackRSIndexStr;
import io.indexr.segment.SegmentMode;
import io.indexr.segment.pack.DataPack;
import io.indexr.segment.pack.NumType;
import io.indexr.segment.storage.PackBundle;
import io.indexr.util.MemoryUtil;

public class DataPackCreator_VLT {
    public static PackBundle from(int version, SegmentMode mode, boolean isIndexed, int[] values, int offset, int size) {
        ByteSlice data = ByteSlice.allocateDirect(size << 2);
        int min, max;
        long sum = 0;
        min = max = values[offset];
        for (int i = 0; i < size; i++) {
            int v = values[offset + i];
            min = Math.min(v, min);
            max = Math.max(v, max);
            sum += v;
            data.putInt(i << 2, v);
        }

        byte dataType = ColumnType.INT;
        PackRSIndexNum index = (PackRSIndexNum) mode.versionAdapter.createPackRSIndex(version, mode, dataType);
        for (int i = offset; i < offset + size; i++) {
            int val = values[i];
            index.putValue(val, min, max);
        }

        DataPackNode_VLT dpn = (DataPackNode_VLT) mode.versionAdapter.createDPN(version, mode);
        dpn.setObjCount(size);
        dpn.setMinValue(min);
        dpn.setMaxValue(max);
        dpn.setSum(sum);
        dpn.setDataSize(data.size());

        DataPack dataPack = new DataPack(data, null, dpn);
        Object extraInfo = dataPack.compress(dataType, isIndexed, dpn);

        PackExtIndex extIndex = mode.versionAdapter.createExtIndex(version, mode, dataType, isIndexed, dpn, dataPack, extraInfo);

        return new PackBundle(dataPack, dpn, index, extIndex);
    }

    public static PackBundle from(int version, SegmentMode mode, boolean isIndexed, long[] values, int offset, int size) {
        ByteSlice data = ByteSlice.allocateDirect(size << 3);
        long min, max;
        long sum = 0;
        min = max = values[offset];
        for (int i = 0; i < size; i++) {
            long v = values[offset + i];
            min = Math.min(v, min);
            max = Math.max(v, max);
            sum += v;
            data.putLong(i << 3, v);
        }

        byte dataType = ColumnType.LONG;
        PackRSIndexNum index = (PackRSIndexNum) mode.versionAdapter.createPackRSIndex(version, mode, dataType);
        for (int i = offset; i < offset + size; i++) {
            long val = values[i];
            index.putValue(val, min, max);
        }

        DataPackNode_VLT dpn = (DataPackNode_VLT) mode.versionAdapter.createDPN(version, mode);
        dpn.setObjCount(size);
        dpn.setMinValue(min);
        dpn.setMaxValue(max);
        dpn.setSum(sum);
        dpn.setDataSize(data.size());

        DataPack dataPack = new DataPack(data, null, dpn);
        Object extraInfo = dataPack.compress(dataType, isIndexed, dpn);

        PackExtIndex extIndex = mode.versionAdapter.createExtIndex(version, mode, dataType, isIndexed, dpn, dataPack, extraInfo);

        return new PackBundle(dataPack, dpn, index, extIndex);
    }

    public static PackBundle from(int version, SegmentMode mode, boolean isIndexed, float[] values, int offset, int size) {
        ByteSlice data = ByteSlice.allocateDirect(size << 2);
        float min, max;
        float sum = 0;
        min = max = values[offset];
        for (int i = 0; i < size; i++) {
            float v = values[offset + i];
            min = Math.min(v, min);
            max = Math.max(v, max);
            sum += v;
            data.putFloat(i << 2, v);
        }
        long uniformMin = NumType.floatToLong(min);
        long uniformMax = NumType.floatToLong(max);

        byte dataType = ColumnType.FLOAT;
        PackRSIndexNum index = (PackRSIndexNum) mode.versionAdapter.createPackRSIndex(version, mode, dataType);
        for (int i = offset; i < offset + size; i++) {
            long uniformVal = NumType.floatToLong(values[i]);
            index.putValue(uniformVal, uniformMin, uniformMax);
        }

        DataPackNode_VLT dpn = (DataPackNode_VLT) mode.versionAdapter.createDPN(version, mode);
        dpn.setObjCount(size);
        dpn.setMinValue(uniformMin);
        dpn.setMaxValue(uniformMax);
        dpn.setSum(Double.doubleToRawLongBits(sum));
        dpn.setDataSize(data.size());

        DataPack dataPack = new DataPack(data, null, dpn);
        Object extraInfo = dataPack.compress(dataType, isIndexed, dpn);

        PackExtIndex extIndex = mode.versionAdapter.createExtIndex(version, mode, dataType, isIndexed, dpn, dataPack, extraInfo);

        return new PackBundle(dataPack, dpn, index, extIndex);
    }

    public static PackBundle from(int version, SegmentMode mode, boolean isIndexed, double[] values, int offset, int size) {
        ByteSlice data = ByteSlice.allocateDirect(size << 3);
        double min, max;
        double sum = 0;
        min = max = values[offset];
        for (int i = 0; i < size; i++) {
            double v = values[offset + i];
            min = Math.min(v, min);
            max = Math.max(v, max);
            sum += v;
            data.putDouble(i << 3, v);
        }
        long uniformMin = NumType.doubleToLong(min);
        long uniformMax = NumType.doubleToLong(max);

        byte dataType = ColumnType.DOUBLE;
        PackRSIndexNum index = (PackRSIndexNum) mode.versionAdapter.createPackRSIndex(version, mode, dataType);
        for (int i = offset; i < offset + size; i++) {
            long uniformVal = NumType.doubleToLong(values[i]);
            index.putValue(uniformVal, uniformMin, uniformMax);
        }

        DataPackNode_VLT dpn = (DataPackNode_VLT) mode.versionAdapter.createDPN(version, mode);
        dpn.setObjCount(size);
        dpn.setMinValue(uniformMin);
        dpn.setMaxValue(uniformMax);
        dpn.setSum(Double.doubleToRawLongBits(sum));
        dpn.setDataSize(data.size());

        DataPack dataPack = new DataPack(data, null, dpn);
        Object extraInfo = dataPack.compress(dataType, isIndexed, dpn);

        PackExtIndex extIndex = mode.versionAdapter.createExtIndex(version, mode, dataType, isIndexed, dpn, dataPack, extraInfo);

        return new PackBundle(dataPack, dpn, index, extIndex);
    }

    public static PackBundle from(int version, SegmentMode mode, boolean isIndexed, List<UTF8String> strings) {
        int size = strings.size();
        Preconditions.checkArgument(size > 0 && size <= DataPack.MAX_COUNT);

        byte dataType = ColumnType.STRING;
        PackRSIndexStr index = (PackRSIndexStr) mode.versionAdapter.createPackRSIndex(version, mode, dataType);

        int strTotalLen = 0;
        for (int i = 0; i < size; i++) {
            UTF8String s = strings.get(i);
            int byteCount = s.numBytes();
            Preconditions.checkState(
                    byteCount <= ColumnType.MAX_STRING_UTF8_SIZE,
                    "string in utf-8 should be smaller than %s bytes",
                    ColumnType.MAX_STRING_UTF8_SIZE);
            strTotalLen += byteCount;

            index.putValue(s);
        }

        int indexLen = (size + 1) << 2;

        ByteSlice data = ByteSlice.allocateDirect(indexLen + strTotalLen);
        long addr = data.address();
        int offset = 0;
        for (int i = 0; i < size; i++) {
            UTF8String s = strings.get(i);
            int start = offset;
            int end = start + s.numBytes();

            MemoryUtil.setInt(addr + (i << 2), start);
            MemoryUtil.copyMemory(s.getBaseObject(), s.getBaseOffset(), null, addr + indexLen + start, s.numBytes());

            offset = end;
        }
        assert offset == strTotalLen;
        MemoryUtil.setInt(addr + (size << 2), offset);

        DataPackNode_VLT dpn = (DataPackNode_VLT) mode.versionAdapter.createDPN(version, mode);
        dpn.setObjCount(size);
        dpn.setDataSize(data.size());

        DataPack dataPack = new DataPack(data, null, dpn);
        Object extraInfo = dataPack.compress(dataType, isIndexed, dpn);

        PackExtIndex extIndex = mode.versionAdapter.createExtIndex(version, mode, dataType, isIndexed, dpn, dataPack, extraInfo);

        return new PackBundle(dataPack, dpn, index, extIndex);
    }
}
