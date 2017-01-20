package io.indexr.segment.pack;

import org.apache.spark.unsafe.types.UTF8String;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

import io.indexr.data.LikePattern;
import io.indexr.segment.PackRSIndexStr;
import io.indexr.segment.RSValue;
import io.indexr.util.Pair;

public class RSIndexTest {

    @Test
    public void hist_float_test() {
        for (Version version : Version.values()) {
            hist_float_test(version.id);
        }
    }

    private void hist_float_test(int version) {
        RSIndex_Histogram.HistPackIndex index = new RSIndex_Histogram.HistPackIndex(true);
        double[] vals = new double[]{1, 2, 3, 9, 1111, -333.4444444, 0};
        double[] notVals = new double[]{4, 33, 100.933, 44.22, -44.33, -88.333, Float.MAX_VALUE};
        Pair<DataPack, DataPackNode> p = DataPack_N.from(version, vals, 0, vals.length, index);
        DataPackNode dpn = p.second;
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
        RSIndex_Histogram.HistPackIndex index = new RSIndex_Histogram.HistPackIndex(false);
        long[] vals = new long[]{333, 111, 556, 6732, 1, 33, -22, -566, 113};
        long[] notVals = new long[]{-4, 354, 67, 1993, -56, 11};
        Pair<DataPack, DataPackNode> p = DataPack_N.from(version, vals, 0, vals.length, index);
        DataPackNode dpn = p.second;
        for (long v : vals) {
            Assert.assertEquals(RSValue.Some, index.isValue(v, v, dpn.minValue(), dpn.maxValue()));
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
        PackRSIndexStr index;
        switch (version) {
            case Version.VERSION_0_ID:
                return;
            case Version.VERSION_1_ID:
            case Version.VERSION_2_ID:
                index = new RSIndex_CMap.CMapPackIndex();
                break;
            default:
                index = new RSIndex_CMap_V2.CMapPackIndex();
                break;
        }
        String[] strs = {"jgqfucaEFDbPnzED", "aa", "aabbbccc", "134567", "##$@%@%223##", "硙硙年费=", "aabb2bcccaabb2bccc  hhaabb2bcccaabb2bcccaabb2bcccaabb2bcccaabb21111"};
        String[] not_strs = {"aa1", "aabnb2bccc", "0134567", "##$@%@%223##_", "8",};
        String[] like_strs = {"aa", "aabb%", "a_b_%", "134_67", "aa", "%$$^^", "硙硙%"};
        String[] not_like_strs = {"0aabb % ", "0aa_b_ % ", "13467"};

        Pair<DataPack, DataPackNode> p = DataPack_R.fromJavaString(version, Arrays.asList(strs), index);
        DataPackNode dpn = p.second;

        for (String s : strs) {
            Assert.assertEquals(RSValue.Some, index.isValue(UTF8String.fromString(s)));
        }
        for (String s : not_strs) {
            Assert.assertEquals(RSValue.None, index.isValue(UTF8String.fromString(s)));
        }

        for (String s : like_strs) {
            Assert.assertEquals(RSValue.Some, index.isLike(new LikePattern(UTF8String.fromString(s))));
        }

        for (String s : not_like_strs) {
            Assert.assertEquals(RSValue.None, index.isLike(new LikePattern(UTF8String.fromString(s))));
        }

        if (version >= Version.VERSION_4_ID) {
            Assert.assertEquals(RSValue.None, index.isValue(UTF8String.fromString("")));
            Assert.assertEquals(RSValue.None, index.isLike(new LikePattern(UTF8String.fromString(""))));
        }
    }
}