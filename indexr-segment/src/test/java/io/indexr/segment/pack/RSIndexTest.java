package io.indexr.segment.pack;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import io.indexr.data.LikePattern;
import io.indexr.segment.Column;
import io.indexr.segment.ColumnSchema;
import io.indexr.segment.ColumnType;
import io.indexr.segment.DPValues;
import io.indexr.segment.PackExtIndexNum;
import io.indexr.segment.PackExtIndexStr;
import io.indexr.segment.PackRSIndexNum;
import io.indexr.segment.PackRSIndexStr;
import io.indexr.segment.RSIndex;
import io.indexr.segment.RSIndexNum;
import io.indexr.segment.RSIndexStr;
import io.indexr.segment.RSValue;

public class RSIndexTest {

    @Test
    public void hist_float_test() {
        for (Version version : Version.values()) {
            hist_float_test(version.id);
        }
    }

    private void hist_float_test(int version) {
        Random r = new Random();
        double[] valList = new double[DataPack.MAX_COUNT];
        for (int i = 0; i < DataPack.MAX_COUNT; i++) {
            valList[i] = r.nextDouble();
        }
        PackBundle pb = DataPack_N.from(version, valList, 0, valList.length);
        for (double v : valList) {
            long uv = Double.doubleToRawLongBits(v);
            Assert.assertEquals(RSValue.Some, ((PackRSIndexNum) pb.rsIndex).isValue(uv, uv, pb.dpn.minValue(), pb.dpn.maxValue()));
        }

        double[] vals = new double[]{1, 2, 3, 9, 1111, -333.4444444, 0};
        double[] notVals = new double[]{4, 33, 100.933, 44.22, -44.33, -88.333, Float.MAX_VALUE};
        PackBundle p = DataPack_N.from(version, vals, 0, vals.length);
        DataPackNode dpn = p.dpn;
        PackRSIndexNum index = (PackRSIndexNum) p.rsIndex;
        for (double v : vals) {
            Assert.assertEquals(RSValue.Some, index.isValue(Double.doubleToRawLongBits(v), Double.doubleToRawLongBits(v), dpn.minValue(), dpn.maxValue()));
        }
        for (double v : notVals) {
            Assert.assertEquals(RSValue.None, index.isValue(Double.doubleToRawLongBits(v), Double.doubleToRawLongBits(v), dpn.minValue(), dpn.maxValue()));
        }
    }

    @Test
    public void hist_number_test() {
        for (Version version : Version.values()) {
            hist_number_test(version.id);
        }
    }

    private void hist_number_test(int version) {
        Random r = new Random();
        long[] valList = new long[DataPack.MAX_COUNT];
        for (int i = 0; i < DataPack.MAX_COUNT; i++) {
            valList[i] = r.nextLong();
        }
        PackBundle pb = DataPack_N.from(version, valList, 0, valList.length);
        for (long v : valList) {
            Assert.assertEquals(RSValue.Some, ((PackRSIndexNum) pb.rsIndex).isValue(v, v, pb.dpn.minValue(), pb.dpn.maxValue()));
        }

        long[] vals = new long[]{333, 111, 556, 6732, 1, 33, -22, -566, 113};
        long[] notVals = new long[]{-4, 354, 67, 1993, -56, 11};
        PackBundle p = DataPack_N.from(version, vals, 0, vals.length);
        DataPackNode dpn = p.dpn;
        PackRSIndexNum index = (PackRSIndexNum) p.rsIndex;
        for (long v : vals) {
            byte res = index.isValue(v, v, dpn.minValue(), dpn.maxValue());

            Assert.assertEquals(RSValue.Some, res);
        }
        for (long v : notVals) {
            Assert.assertEquals(RSValue.None, index.isValue(v, v, dpn.minValue(), dpn.maxValue()));
        }
    }

    @Test
    public void cmap_test() {
        for (Version version : Version.values()) {
            // Version 0 doesn't have index for strings.
            if (version != Version.VERSION_0) {
                cmap_test(version.id);
            }
        }
    }

    private void cmap_test(int version) {
        List<UTF8String> stringList = new ArrayList<>();
        Random r = new Random();
        for (int i = 0; i < DataPack.MAX_COUNT; i++) {
            stringList.add(UTF8String.fromString(RandomStringUtils.random(r.nextInt(100))));
        }
        PackBundle pb = DataPack_R.from(version, stringList);
        for (UTF8String s : stringList) {
            Assert.assertEquals(RSValue.Some, ((PackRSIndexStr) pb.rsIndex).isValue(s));
        }


        String[] strs = {"jgqfucaEFDbPnzED", "ZxcyhMu", "aa", "aabbbccc", "134567", "##$@%@%223##", "硙硙年费=", "aabb2bcccaabb2bccc  hhaabb2bcccaabb2bcccaabb2bcccaabb2bcccaabb21111"};
        String[] not_strs = {"aa1", "aabnb2bccc", "0134567", "##$@%@%223##_", "8",};
        String[] like_strs = {"aa", "aabb%", "a_b_%", "134_67", "aa", "%$$^^", "硙硙%"};
        String[] not_like_strs = {"0aabb % ", "0aa_b_ % ", "13467"};

        PackBundle p = DataPack_R.fromJavaString(version, Arrays.asList(strs));
        DataPackNode dpn = p.dpn;
        PackRSIndexStr index = (PackRSIndexStr) p.rsIndex;

        for (String s : strs) {
            UTF8String us = UTF8String.fromString(s);
            Assert.assertEquals(RSValue.Some, index.isValue(us));
        }
        for (String s : not_strs) {
            UTF8String us = UTF8String.fromString(s);
            Assert.assertEquals(RSValue.None, index.isValue(us));
        }

        for (String s : like_strs) {
            UTF8String us = UTF8String.fromString(s);
            Assert.assertEquals(RSValue.Some, index.isLike(new LikePattern(us)));
        }

        for (String s : not_like_strs) {
            UTF8String us = UTF8String.fromString(s);
            Assert.assertEquals(RSValue.None, index.isLike(new LikePattern(us)));
        }

        if (version >= Version.VERSION_4_ID) {
            UTF8String us = UTF8String.fromString("");
            Assert.assertEquals(RSValue.None, index.isValue(us));
            Assert.assertEquals(RSValue.None, index.isLike(new LikePattern(us)));
        }
    }

    public static void checkIndex(StorageSegment segment) throws IOException {
        int colId = 0;
        for (ColumnSchema cs : segment.schema().getColumns()) {
            Column column = segment.column(colId);
            RSIndex index = column.rsIndex();
            byte dataType = cs.getDataType();
            if (ColumnType.STRING == dataType) {
                RSIndexStr strIndex = (RSIndexStr) index;
                for (int packId = 0; packId < column.packCount(); packId++) {
                    DPValues pack = column.pack(packId);
                    DataPackNode dpn = column.dpn(packId);
                    PackExtIndexStr extIndex = column.extIndex(packId);
                    for (int rowId = 0; rowId < pack.count(); rowId++) {
                        UTF8String val = pack.stringValueAt(rowId);
                        Assert.assertEquals(RSValue.Some, strIndex.isValue(packId, val));
                        Assert.assertEquals(RSValue.Some, extIndex.isValue(rowId, val));
                    }
                }
            } else {
                RSIndexNum numIndex = (RSIndexNum) index;
                for (int packId = 0; packId < column.packCount(); packId++) {
                    DPValues pack = column.pack(packId);
                    DataPackNode dpn = column.dpn(packId);
                    PackExtIndexNum extIndex = column.extIndex(packId);
                    for (int rowId = 0; rowId < pack.count(); rowId++) {
                        long val = pack.uniformValAt(rowId, dataType);
                        Assert.assertEquals(RSValue.Some, numIndex.isValue(packId, val, val, dpn.minValue(), dpn.maxValue()));
                        Assert.assertEquals(RSValue.Some, extIndex.isValue(rowId, val));
                    }
                }
            }
            colId++;
        }
    }
}