package io.indexr.compress.bh;

import com.google.common.base.Preconditions;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import io.indexr.io.ByteSlice;
import io.indexr.segment.pack.DataPack;

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


        ByteSlice cmp = BHCompressor.compressShort(bb, item_size, max_val);
        ByteSlice dp = BHCompressor.decompressShort(cmp, item_size, max_val);

        Assert.assertEquals(true, ByteSlice.checkEquals(bb, dp));
    }

    @Test
    public void test_int() {
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

        ByteSlice cmp = BHCompressor.compressLong(bb, item_size, max_val);
        ByteSlice dp = BHCompressor.decompressLong(cmp, item_size, max_val);

        Assert.assertEquals(true, ByteSlice.checkEquals(bb, dp));
    }

    @Test
    public void test_double() {
        int item_size = 65536;
        double[] data = new double[item_size];
        for (int i = 0; i < item_size; i++) {
            data[i] = random.nextDouble();
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

    @Test
    public void test_string() {
        int item_size = 65536;
        List<UTF8String> strings = new ArrayList<>();
        for (int i = 0; i < item_size; i++) {
            strings.add(UTF8String.fromString(RandomStringUtils.random(20)));
        }
        ByteSlice bb = _from_v1(strings);
        ByteSlice cmp = BHCompressor.compressIndexedStr_v1(bb, item_size);
        ByteSlice dp = BHCompressor.decompressIndexedStr_v1(cmp, item_size);

        Assert.assertEquals(true, ByteSlice.checkEquals(bb, dp));
    }

    private static ByteSlice _from_v1(List<UTF8String> strings) {
        int size = strings.size();
        Preconditions.checkArgument(size > 0 && size <= DataPack.MAX_COUNT);

        int strTotalLen = 0;
        for (UTF8String s : strings) {
            int byteCount = s.numBytes();
            strTotalLen += byteCount;
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

        return data;
    }
}
