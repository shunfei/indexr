package io.indexr.segment.pack;


import com.google.common.collect.Lists;

import com.sun.jna.NativeLibrary;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.util.List;

import io.indexr.io.ByteBufferWriter;
import io.indexr.io.ByteSlice;

public class DataPackTest {
    static {
        NativeLibrary.addSearchPath("bhcompress", "lib");
    }

    @Test
    public void testUtils() {
        Assert.assertEquals(1, DataPack.packRowCount(65537, 1));
        Assert.assertEquals(65536, DataPack.packRowCount(65536, 0));
    }

    public void testNumber(DataPackNode dpn, DataPack pack) throws IOException {
        ByteSlice packData = pack.data();
        dpn.setCompress(true);
        pack.compress(dpn);
        dpn.setPackSize(pack.serializedSize());

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        pack.write(ByteBufferWriter.of(Channels.newChannel(baos), null));
        byte[] bytes = baos.toByteArray();
        ByteSlice tmp = ByteSlice.allocateDirect(bytes.length);
        tmp.put(0, bytes);
        DataPack newPack = DataPack.from(tmp, dpn);
        newPack.decompress(dpn);

        pack.decompress(dpn);
        packData = pack.data();
        Assert.assertEquals(true, ByteSlice.checkEquals(packData, newPack.data()));

    }

    public void testRaw(DataPackNode dpn, DataPack pack, List<String> strs) throws IOException {
        pack.compress(dpn);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        pack.write(ByteBufferWriter.of(Channels.newChannel(baos), null));
        byte[] bytes = baos.toByteArray();
        ByteSlice tmp = ByteSlice.allocateDirect(bytes.length);
        tmp.put(0, bytes);
        DataPack new_dpn = DataPack.from(tmp, dpn);
        new_dpn.decompress(dpn);

        cmpStr(new_dpn, strs);
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
            test_pack(version.id);
        }
    }

    private void test_pack(int version) throws IOException {
        int[] int_vals = new int[]{1, 2, 3, 5, 6, 7, 8, 9, -1111, 333, 0};
        PackBundle p = DataPack_N.from(version, int_vals, 0, int_vals.length);
        cmpNumber(p.dataPack, int_vals);
        testNumber(p.dpn, p.dataPack);

        int[] int_vals1 = new int[]{0};
        p = DataPack_N.from(version, int_vals1, 0, int_vals1.length);
        cmpNumber(p.dataPack, int_vals1);
        testNumber(p.dpn, p.dataPack);

        int[] int_vals2 = new int[]{10, 10, 10, 10};
        p = DataPack_N.from(version, int_vals2, 0, int_vals2.length);
        cmpNumber(p.dataPack, int_vals2);
        testNumber(p.dpn, p.dataPack);

        long[] long_vals = new long[]{1, Long.MAX_VALUE, 3, Long.MIN_VALUE, 6, 7, 8, 9, 1111, 333, 0};
        p = DataPack_N.from(version, long_vals, 0, long_vals.length);
        cmpNumber(p.dataPack, long_vals);
        testNumber(p.dpn, p.dataPack);

        float[] float_vals = new float[]{1, 2, 3, 5, 6, 7, 8, 9, 1111.4666F, 333, 0};
        p = DataPack_N.from(version, float_vals, 0, float_vals.length);
        cmpNumber(p.dataPack, float_vals);
        testNumber(p.dpn, p.dataPack);

        float_vals = new float[]{1.1f, 1.1f};
        p = DataPack_N.from(version, float_vals, 0, float_vals.length);
        cmpNumber(p.dataPack, float_vals);
        testNumber(p.dpn, p.dataPack);

        double[] double_vals = new double[]{1, 2, 3, 5, Double.MIN_VALUE, 7, Double.MAX_VALUE, 9, 1111, -333.4444444, 0};
        p = DataPack_N.from(version, double_vals, 0, double_vals.length);
        cmpNumber(p.dataPack, double_vals);
        testNumber(p.dpn, p.dataPack);

        List<String> str_vals = Lists.newArrayList("qwfwrfwe", "121343%$#22342342", "2211", "sdfdsgkwkek00-\t\t\5\3\1\bsdaasds", "");
        PackBundle ps = DataPack_R.fromJavaString(version, str_vals);
        cmpStr(ps.dataPack, str_vals);
        testRaw(ps.dpn, ps.dataPack, str_vals);

        List<String> str_vals2 = Lists.newArrayList("", "", "", "");
        PackBundle ps2 = DataPack_R.fromJavaString(version, str_vals2);
        cmpStr(ps2.dataPack, str_vals2);
        testRaw(ps2.dpn, ps2.dataPack, str_vals2);
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
            test_str(version.id);
        }
    }

    private void test_str(int version) {
        String[] strs = {"121343%$#22342342", "#$%#%242rwef23r23dfsdf", "", "2211", "sdfdsgkwkek00-\t\t\5\3\1\bsdaasds", "", "323weewew", "[][][][[]]", "windows", "mac", "linux", "android", "windows", "mac", "linux", "android", "windows", "mac", "linux", "android", "qwfwrfwe"};
        List<String> str_vals = Lists.newArrayList(strs);
        PackBundle dpr = DataPack_R.fromJavaString(version, str_vals);
        DataPack pr = dpr.dataPack;
        DataPackNode view = dpr.dpn;

        pr.compress(view);
        pr.decompress(view);

        cmpStr(pr, str_vals);
        pr.stringValueAt(0);
    }
}