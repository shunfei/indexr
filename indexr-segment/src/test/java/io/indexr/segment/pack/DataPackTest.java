package io.indexr.segment.pack;


import com.google.common.collect.Lists;

import com.sun.jna.NativeLibrary;

import org.apache.spark.unsafe.types.UTF8String;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.util.List;

import io.indexr.io.ByteBufferWriter;
import io.indexr.io.ByteSlice;
import io.indexr.segment.ColumnType;
import io.indexr.segment.SegmentMode;
import io.indexr.segment.storage.PackBundle;
import io.indexr.segment.storage.Version;

public class DataPackTest {
    static {
        NativeLibrary.addSearchPath("bhcompress", "lib");
    }

    @Test
    public void testUtils() {
        Assert.assertEquals(1, DataPack.packRowCount(65537, 1));
        Assert.assertEquals(65536, DataPack.packRowCount(65536, 0));
    }

    public void testNumber(int version, SegmentMode mode, byte dataType, boolean isIndexed, DataPackNode dpn, DataPack pack) throws IOException {
        pack.compress(dataType, isIndexed, dpn);
        dpn.setPackSize(pack.serializedSize());

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        pack.write(ByteBufferWriter.of(Channels.newChannel(baos), null));
        byte[] bytes = baos.toByteArray();
        ByteSlice tmp = ByteSlice.allocateDirect(bytes.length);
        tmp.put(0, bytes);
        //DataPack newPack = DataPack.from(tmp, dpn);
        DataPack newPack = new DataPack(null, tmp, dpn);
        newPack.decompress(dataType, dpn);

        pack.decompress(dataType, dpn);
        Assert.assertEquals(true, ByteSlice.checkEquals(pack.data(), newPack.data()));
    }

    public void testRaw(int version, SegmentMode mode, byte dataType, boolean isIndexed, DataPackNode dpn, DataPack pack, List<String> strs) throws IOException {
        pack.compress(dataType, isIndexed, dpn);
        dpn.setPackSize(pack.serializedSize());

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        pack.write(ByteBufferWriter.of(Channels.newChannel(baos), null));
        byte[] bytes = baos.toByteArray();
        ByteSlice tmp = ByteSlice.allocateDirect(bytes.length);
        tmp.put(0, bytes);
        DataPack newPack = new DataPack(null, tmp, dpn);
        newPack.decompress(dataType, dpn);

        cmpStr(newPack, strs);
    }

    private void cmpNumber(DataPack pack, int[] vals) {
        for (int i = 0; i < vals.length; i++) {
            Assert.assertEquals(vals[i], pack.intValueAt(i));
        }
    }

    private void cmpNumber(DataPack pack, long[] vals) {
        for (int i = 0; i < vals.length; i++) {
            Assert.assertEquals(vals[i], pack.longValueAt(i));
        }
    }

    private void cmpNumber(DataPack pack, float[] vals) {
        for (int i = 0; i < vals.length; i++) {
            Assert.assertEquals(vals[i], pack.floatValueAt(i), 0);
        }
    }

    private void cmpNumber(DataPack pack, double[] vals) {
        for (int i = 0; i < vals.length; i++) {
            Assert.assertEquals(vals[i], pack.doubleValueAt(i), 0);
        }
    }

    private void cmpStr(DataPack pack, List<String> strs) {
        for (int i = 0; i < strs.size(); i++) {
            Assert.assertEquals(strs.get(i), pack.stringValueAt(i).toString());
        }
    }

    @Test
    public void test_pack() throws IOException {
        for (Version version : Version.values()) {
            for (SegmentMode mode : SegmentMode.values()) {
                test_pack(version.id, mode, false);
                test_pack(version.id, mode, true);
            }
        }
    }

    private void test_pack(int version, SegmentMode mode, boolean isIndexed) throws IOException {
        int[] int_vals = new int[]{1, 2, 3, 5, 6, 7, 8, 9, -1111, 333, 0};
        //PackBundle p = DataPack_N.from(version, mode, int_vals, 0, int_vals.length);
        PackBundle p = mode.versionAdapter.createPackBundle(version, mode, ColumnType.INT, isIndexed,
                new VirtualDataPack(ColumnType.INT, int_vals, int_vals.length));
        cmpNumber(p.dataPack, int_vals);
        testNumber(version, mode, ColumnType.INT, isIndexed, p.dpn, p.dataPack);

        int[] int_vals1 = new int[]{0};
        //p = DataPack_N.from(version, mode, int_vals1, 0, int_vals1.length);
        p = mode.versionAdapter.createPackBundle(version, mode, ColumnType.INT, isIndexed,
                new VirtualDataPack(ColumnType.INT, int_vals1, int_vals1.length));
        cmpNumber(p.dataPack, int_vals1);
        testNumber(version, mode, ColumnType.INT, isIndexed, p.dpn, p.dataPack);

        int[] int_vals2 = new int[]{10, 10, 10, 10};
        //p = DataPack_N.from(version, mode, int_vals2, 0, int_vals2.length);
        p = mode.versionAdapter.createPackBundle(version, mode, ColumnType.INT, isIndexed,
                new VirtualDataPack(ColumnType.INT, int_vals2, int_vals2.length));
        cmpNumber(p.dataPack, int_vals2);
        testNumber(version, mode, ColumnType.INT, isIndexed, p.dpn, p.dataPack);

        long[] long_vals = new long[]{1, Long.MAX_VALUE, 3, Long.MIN_VALUE, 6, 7, 8, 9, 1111, 333, 0};
        //p = DataPack_N.from(version, mode, long_vals, 0, long_vals.length);
        p = mode.versionAdapter.createPackBundle(version, mode, ColumnType.LONG, isIndexed,
                new VirtualDataPack(ColumnType.LONG, long_vals, long_vals.length));
        cmpNumber(p.dataPack, long_vals);
        testNumber(version, mode, ColumnType.LONG, isIndexed, p.dpn, p.dataPack);

        float[] float_vals = new float[]{1, 2, 3, 5, 6, 7, 8, 9, 1111.4666F, 333, 0};
        //p = DataPack_N.from(version, mode, float_vals, 0, float_vals.length);
        p = mode.versionAdapter.createPackBundle(version, mode, ColumnType.FLOAT, isIndexed,
                new VirtualDataPack(ColumnType.FLOAT, float_vals, float_vals.length));
        cmpNumber(p.dataPack, float_vals);
        testNumber(version, mode, ColumnType.FLOAT, isIndexed, p.dpn, p.dataPack);

        float_vals = new float[]{1.1f, 1.1f};
        //p = DataPack_N.from(version, mode, float_vals, 0, float_vals.length);
        p = mode.versionAdapter.createPackBundle(version, mode, ColumnType.FLOAT, isIndexed,
                new VirtualDataPack(ColumnType.FLOAT, float_vals, float_vals.length));
        cmpNumber(p.dataPack, float_vals);
        testNumber(version, mode, ColumnType.FLOAT, isIndexed, p.dpn, p.dataPack);

        double[] double_vals = new double[]{1, 2, 3, 5, Double.MIN_VALUE, 7, Double.MAX_VALUE, 9, 1111, -333.4444444, 0};
        //p = DataPack_N.from(version, mode, double_vals, 0, double_vals.length);
        p = mode.versionAdapter.createPackBundle(version, mode, ColumnType.DOUBLE, isIndexed,
                new VirtualDataPack(ColumnType.DOUBLE, double_vals, double_vals.length));
        cmpNumber(p.dataPack, double_vals);
        testNumber(version, mode, ColumnType.DOUBLE, isIndexed, p.dpn, p.dataPack);

        List<String> str_vals = Lists.newArrayList("qwfwrfwe", "121343%$#22342342", "2211", "sdfdsgkwkek00-\t\t\5\3\1\bsdaasds", "");
        //PackBundle ps = DataPack_R.fromJavaString(version, mode, str_vals);
        PackBundle ps = mode.versionAdapter.createPackBundle(version, mode, ColumnType.STRING, isIndexed,
                new VirtualDataPack(ColumnType.STRING, Lists.transform(str_vals, UTF8String::fromString).toArray(new UTF8String[0]), str_vals.size()));
        cmpStr(ps.dataPack, str_vals);
        testRaw(version, mode, ColumnType.STRING, isIndexed, ps.dpn, ps.dataPack, str_vals);

        List<String> str_vals2 = Lists.newArrayList("", "", "", "");
        //PackBundle ps2 = DataPack_R.fromJavaString(version, mode, str_vals2);
        PackBundle ps2 = mode.versionAdapter.createPackBundle(version, mode, ColumnType.STRING, isIndexed,
                new VirtualDataPack(ColumnType.STRING, Lists.transform(str_vals2, UTF8String::fromString).toArray(new UTF8String[0]), str_vals2.size()));
        cmpStr(ps2.dataPack, str_vals2);
        testRaw(version, mode, ColumnType.STRING, isIndexed, ps2.dpn, ps2.dataPack, str_vals2);
    }

    @Test
    public void test_float_to_int() {
        float a = -1.42f;
        int b = Float.floatToRawIntBits(a);
        long c = (long) b;
        float a2 = Float.intBitsToFloat((int) c);
        Assert.assertEquals(0, Float.compare(a, a2));

        double d = Double.longBitsToDouble(c);
        a2 = (float) d;
        Assert.assertNotEquals(0, Float.compare(a, a2));
    }

    @Test
    public void test_str() {
        for (Version version : Version.values()) {
            for (SegmentMode mode : SegmentMode.values()) {
                test_str(version.id, mode, false);
                test_str(version.id, mode, true);
            }
        }
    }

    private void test_str(int version, SegmentMode mode, boolean isIndexed) {
        String[] strs = {"121343%$#22342342", "#$%#%242rwef23r23dfsdf", "", "2211", "sdfdsgkwkek00-\t\t\5\3\1\bsdaasds", "", "323weewew", "[][][][[]]", "windows", "mac", "linux", "android", "windows", "mac", "linux", "android", "windows", "mac", "linux", "android", "qwfwrfwe"};
        List<String> str_vals = Lists.newArrayList(strs);
        //PackBundle dpr = DataPack_R.fromJavaString(version, mode, str_vals);
        PackBundle pb = mode.versionAdapter.createPackBundle(version, mode, ColumnType.STRING, isIndexed,
                new VirtualDataPack(ColumnType.STRING, Lists.transform(str_vals, UTF8String::fromString).toArray(new UTF8String[0]), strs.length));
        DataPack pack = pb.dataPack;
        DataPackNode dpn = pb.dpn;

        pack.compress(ColumnType.STRING, isIndexed, dpn);
        dpn.setPackSize(pack.serializedSize());

        pack.decompress(ColumnType.STRING, dpn);

        cmpStr(pack, str_vals);
    }
}