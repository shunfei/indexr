package io.indexr.compress.bh;

import com.sun.jna.Native;
import com.sun.jna.Pointer;

import org.junit.Assert;
import org.junit.Test;

import java.util.Random;

import io.indexr.io.ByteSlice;

public class BHCompressTest {
    private static final Random random = new Random();

    @Test
    public void test_byte() {
        int item_size = 50;
        byte[] data = new byte[item_size];
        for (int i = 0; i < item_size; i++) {
            data[i] = (byte) (random.nextInt(256) - 128);
        }

        ByteSlice bb = ByteSlice.allocateDirect(item_size);
        long max_val = 0;
        for (int i = 0; i < item_size; i++) {
            byte v = data[i];
            bb.put(i, v);
            max_val = Math.max(v & 0xFFL, max_val);
        }

        Pointer p = new Pointer(BhcompressLibrary.INSTANCE.JavaCompress_Number_Byte_NP(bb.address(), item_size, max_val));

        int err = p.getByte(0);
        long new_max_val = p.getLong(1);
        int cmp_data_size = p.getInt(9);

        Assert.assertEquals(BhcompressLibrary.CprsErr_J.CPRS_SUCCESS_J, err);

        long native_dp = BhcompressLibrary.INSTANCE.JavaDecompress_Number_Byte_NP(Pointer.nativeValue(p), item_size);
        ByteSlice dp = ByteSlice.fromAddress(native_dp, item_size);

        byte[] new_data = new byte[item_size];
        for (int i = 0; i < item_size; i++) {
            new_data[i] = dp.get(i);
        }

        Assert.assertArrayEquals(data, new_data);
        Native.free(Pointer.nativeValue(p));
    }

    @Test
    public void test_byte_wrap() {
        int item_size = 50;
        byte[] data = new byte[item_size];
        for (int i = 0; i < item_size; i++) {
            data[i] = (byte) (random.nextInt(256) - 128);
        }

        ByteSlice bb = ByteSlice.allocateDirect(item_size);
        long max_val = 0;
        for (int i = 0; i < item_size; i++) {
            byte v = data[i];
            bb.put(i, v);
            max_val = Math.max(v & 0xFFL, max_val);
        }


        ByteSlice cmp = BHCompressor.compressByte(bb, item_size, max_val);
        ByteSlice dp = BHCompressor.decompressByte(cmp, item_size, max_val);

        Assert.assertEquals(true, ByteSlice.checkEquals(bb, dp));
    }


    @Test
    public void test_short() {
        ByteSlice s = ByteSlice.allocateDirect(100);
        s.putShort(0, (short) 2);


        int item_size = 50;
        short[] data = new short[item_size];
        for (int i = 0; i < item_size; i++) {
            data[i] = (short) (random.nextInt(65536) - 32768);
        }

        ByteSlice bb = ByteSlice.allocateDirect(item_size * 2);
        long max_val = 0;
        for (int i = 0; i < item_size; i++) {
            short v = data[i];
            bb.putShort(i * 2, v);
            max_val = Math.max(v & 0xFFFFL, max_val);
        }


        Pointer p = new Pointer(BhcompressLibrary.INSTANCE.JavaCompress_Number_Short_NP(bb.address(), item_size, max_val));
        Assert.assertNotEquals(p, null);
        int err = p.getByte(0);
        long new_max_val = p.getLong(1);
        int cmp_data_size = p.getInt(9);

        Assert.assertEquals(BhcompressLibrary.CprsErr_J.CPRS_SUCCESS_J, err);

        long native_dp = BhcompressLibrary.INSTANCE.JavaDecompress_Number_Short_NP(Pointer.nativeValue(p), item_size);
        ByteSlice dp = ByteSlice.fromAddress(native_dp, item_size * 2);

        short[] new_data = new short[item_size];
        for (int i = 0; i < item_size; i++) {
            new_data[i] = dp.getShort(i * 2);
        }

        Assert.assertArrayEquals(data, new_data);

        Native.free(Pointer.nativeValue(p));
    }

    @Test
    public void test_short_wrap() {
        ByteSlice s = ByteSlice.allocateDirect(100);
        s.putShort(0, (short) 2);


        int item_size = 50;
        short[] data = new short[item_size];
        for (int i = 0; i < item_size; i++) {
            data[i] = (short) (random.nextInt(65536) - 32768);
        }

        ByteSlice bb = ByteSlice.allocateDirect(item_size * 2);
        long max_val = 0;
        for (int i = 0; i < item_size; i++) {
            short v = data[i];
            bb.putShort(i * 2, v);
            max_val = Math.max(v & 0xFFFFL, max_val);
        }


        ByteSlice cmp = BHCompressor.compressShort(bb, item_size, max_val);
        ByteSlice dp = BHCompressor.decompressShort(cmp, item_size, max_val);

        Assert.assertEquals(true, ByteSlice.checkEquals(bb, dp));
    }

    @Test
    public void test_int() {
        int item_size = 50;
        int[] data = new int[item_size];
        for (int i = 0; i < item_size; i++) {
            data[i] = random.nextInt();
        }

        ByteSlice bb = ByteSlice.allocateDirect(item_size * 4);
        int max_val = 0;
        for (int i = 0; i < item_size; i++) {
            int v = data[i];
            bb.putInt(i * 4, v);
            if (Integer.compareUnsigned(max_val, v) < 0) {
                max_val = v;
            }
        }
        Pointer p = new Pointer(BhcompressLibrary.INSTANCE.JavaCompress_Number_Int_NP(bb.address(), item_size, max_val));
        Assert.assertNotEquals(p, null);
        int err = p.getByte(0);
        long new_max_val = p.getLong(1);
        int cmp_data_size = p.getInt(9);

        Assert.assertEquals(BhcompressLibrary.CprsErr_J.CPRS_SUCCESS_J, err);

        long native_dp = BhcompressLibrary.INSTANCE.JavaDecompress_Number_Int_NP(Pointer.nativeValue(p), item_size);
        ByteSlice dp = ByteSlice.fromAddress(native_dp, item_size * 4);

        int[] new_data = new int[item_size];
        for (int i = 0; i < item_size; i++) {
            new_data[i] = dp.getInt(i * 4);
        }

        Assert.assertArrayEquals(data, new_data);
        Native.free(Pointer.nativeValue(p));
    }


    @Test
    public void test_int_wrap() {
        int item_size = 50;
        int[] data = new int[item_size];
        for (int i = 0; i < item_size; i++) {
            data[i] = (int) (i * 2);
        }

        ByteSlice bb = ByteSlice.allocateDirect(item_size * 4);
        long max_val = 0;
        for (int i = 0; i < item_size; i++) {
            int v = data[i];
            bb.putInt(i * 4, v);
            max_val = Math.max(max_val, v);
        }
        ByteSlice cmp = BHCompressor.compressInt(bb, item_size, max_val);
        ByteSlice dp = BHCompressor.decompressInt(cmp, item_size, max_val);

        Assert.assertEquals(true, ByteSlice.checkEquals(bb, dp));
    }

    @Test
    public void test_long() {
        int item_size = 50;
        long[] data = new long[item_size];
        for (int i = 0; i < item_size; i++) {
            data[i] = random.nextLong();
        }

        ByteSlice bb = ByteSlice.allocateDirect(item_size * 8);
        long max_val = 0;
        for (int i = 0; i < item_size; i++) {
            long v = data[i];
            bb.putLong(i * 8, v);
            if (Long.compareUnsigned(max_val, v) < 0) {
                max_val = v;
            }
        }
        Pointer p = new Pointer(BhcompressLibrary.INSTANCE.JavaCompress_Number_Long_NP(bb.address(), item_size, max_val));
        int err = p.getByte(0);
        long new_max_val = p.getLong(1);
        int cmp_data_size = p.getInt(9);

        Assert.assertEquals(BhcompressLibrary.CprsErr_J.CPRS_SUCCESS_J, err);

        long native_dp = BhcompressLibrary.INSTANCE.JavaDecompress_Number_Long_NP(Pointer.nativeValue(p), item_size);
        ByteSlice dp = ByteSlice.fromAddress(native_dp, item_size * 8);

        long[] new_data = new long[item_size];
        for (int i = 0; i < item_size; i++) {
            new_data[i] = dp.getLong(i * 8);
        }

        Assert.assertArrayEquals(data, new_data);
        Native.free(Pointer.nativeValue(p));
    }


    @Test
    public void test_long_wrap() {
        int item_size = 50;
        long[] data = new long[item_size];
        for (int i = 0; i < item_size; i++) {
            data[i] = random.nextLong();
        }

        ByteSlice bb = ByteSlice.allocateDirect(item_size * 8);
        long max_val = 0;
        for (int i = 0; i < item_size; i++) {
            long v = data[i];
            bb.putLong(i * 8, v);
            if (Long.compareUnsigned(max_val, v) < 0) {
                max_val = v;
            }
        }

        ByteSlice cmp = BHCompressor.compressLong(bb, item_size, max_val);
        ByteSlice dp = BHCompressor.decompressLong(cmp, item_size, max_val);

        Assert.assertEquals(true, ByteSlice.checkEquals(bb, dp));
    }

    @Test
    public void test_double_wrap() {
        int item_size = 65536;
        double[] data = new double[item_size];
        for (int i = 0; i < item_size; i++) {
            if (i == 0) {
                data[i] = 0.0;
            } else {
                data[i] = 157.058 + i;// random.nextDouble();
            }
        }

        ByteSlice bb = ByteSlice.allocateDirect(item_size * 8);
        long max_val = 0;
        for (int i = 0; i < item_size; i++) {
            double v = data[i];
            bb.putDouble(i * 8, v);
            long uv = Double.doubleToRawLongBits(v);
            if (Long.compareUnsigned(max_val, uv) < 0) {
                max_val = uv;
            }
        }

        ByteSlice cmp = BHCompressor.compressLong(bb, item_size, max_val);
        ByteSlice dp = BHCompressor.decompressLong(cmp, item_size, max_val);

        Assert.assertEquals(true, ByteSlice.checkEquals(bb, dp));
    }
}
