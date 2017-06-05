package io.indexr.vlt.codec.delta;

import io.indexr.vlt.codec.Codec;
import io.indexr.vlt.codec.CodecType;
import io.indexr.vlt.codec.UnsafeUtil;

import org.apache.commons.lang.RandomStringUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import io.indexr.data.StringsStruct;
import io.indexr.io.ByteSlice;
import io.indexr.util.UTF8Util;
import io.indexr.util.Wrapper;

public class DeltaCodecTest {
    private Random random = new Random();

    @Test
    public void testInts() {
        long a = 0;
        for (int i = 0; i < 100; i++) {
            a += _testInts();
        }
        System.out.println(a);
    }

    public long _testInts() {
        // We don't compress too small group of values.
        int valCount = random.nextInt(65536);
        int dataSize = valCount << 2;
        ByteSlice data = ByteSlice.allocateDirect(dataSize);
        int prevVal = random.nextInt();
        for (int i = 0; i < valCount; i++) {
            prevVal = prevVal + (random.nextInt(100) - 50);
            data.putInt(i << 2, prevVal);
        }
        int[] a1 = new int[valCount];
        UnsafeUtil.copyMemory(null, data.address(), a1, UnsafeUtil.INT_ARRAY_OFFSET, dataSize);

        ByteSlice encoded = ByteSlice.allocateDirect(Codec.outputBufferSize(CodecType.DELTA, valCount, dataSize));

        DeltaCodec codec = new DeltaCodec();
        int encodedSize = codec.encodeInts(valCount, data.address(), encoded.address(), encoded.size());
        Assert.assertTrue(encodedSize > 0);
        //System.out.println((float) encodedSize / dataSize);

        ByteSlice decoded = ByteSlice.allocateDirect(dataSize);
        long now = System.currentTimeMillis();
        int consumedSize = codec.decodeInts(valCount, encoded.address(), decoded.address(), decoded.size());
        Assert.assertEquals(encodedSize, consumedSize);

        long t = System.currentTimeMillis() - now;

        int[] a2 = new int[valCount];
        UnsafeUtil.copyMemory(null, decoded.address(), a2, UnsafeUtil.INT_ARRAY_OFFSET, dataSize);

        Assert.assertArrayEquals(a1, a2);
        Assert.assertTrue(ByteSlice.checkEquals(data, decoded));

        // Those not only for a good habit, but also stop jvm from reclaiming they too early!
        data.free();
        encoded.free();
        decoded.free();

        return t;
    }

    @Test
    public void testLongs() {
        long a = 0;
        for (int i = 0; i < 100; i++) {
            a += _testLongs();
        }
        System.out.println(a);
    }

    public long _testLongs() {
        int valCount = random.nextInt(65536);
        int dataSize = valCount << 3;
        ByteSlice data = ByteSlice.allocateDirect(dataSize);
        long prevVal = random.nextLong();
        for (int i = 0; i < valCount; i++) {
            prevVal = prevVal + (random.nextInt(1000) - 500);
            data.putLong(i << 3, prevVal);
        }
        long[] a1 = new long[valCount];
        UnsafeUtil.copyMemory(null, data.address(), a1, UnsafeUtil.LONG_ARRAY_OFFSET, dataSize);

        ByteSlice encoded = ByteSlice.allocateDirect(Codec.outputBufferSize(CodecType.DELTA, valCount, dataSize));

        DeltaCodec codec = new DeltaCodec();
        int encodedSize = codec.encodeLongs(valCount, data.address(), encoded.address(), encoded.size());
        Assert.assertTrue(encodedSize > 0);
        //System.out.println((float) encodedSize / dataSize);

        ByteSlice decoded = ByteSlice.allocateDirect(dataSize);
        long now = System.currentTimeMillis();
        int consumedSize = codec.decodeLongs(valCount, encoded.address(), decoded.address(), decoded.size());
        Assert.assertEquals(encodedSize, consumedSize);

        long t = System.currentTimeMillis() - now;

        long[] a2 = new long[valCount];
        UnsafeUtil.copyMemory(null, decoded.address(), a2, UnsafeUtil.LONG_ARRAY_OFFSET, dataSize);

        Assert.assertArrayEquals(a1, a2);
        Assert.assertTrue(ByteSlice.checkEquals(data, decoded));

        // Those not only for a good habit, but also stop jvm from reclaiming them too early!
        data.free();
        encoded.free();
        decoded.free();

        return t;
    }


    @Test
    public void testStrings() {
        long a = 0;
        for (int i = 0; i < 100; i++) {
            a += _testStringsEncodeDecode();
        }
        System.out.println(a);
    }

    public long _testStringsEncodeDecode() {
        int valCount = random.nextInt(65536);
        String[] prefixes = {"www.sunteng.com", "www.github.com", "www.sunteng.com/indexr"};
        List<String> distinctStrings = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            distinctStrings.add(prefixes[random.nextInt(prefixes.length)] + RandomStringUtils.randomAlphabetic(10));
        }
        List<byte[]> strings = new ArrayList<>();
        for (int i = 0; i < valCount; i++) {
            byte[] s = UTF8Util.toUtf8(distinctStrings.get(random.nextInt(100)));
            strings.add(s);
        }

        Wrapper<ByteSlice> _data = new Wrapper<>();
        StringsStruct.from(strings, _data);
        ByteSlice data = _data.value;

        ByteSlice encoded = ByteSlice.allocateDirect(Codec.outputBufferSize(CodecType.DELTA, valCount, data.size()));

        DeltaCodec codec = new DeltaCodec();
        int encodedSize = codec.encodeStrings(valCount, data.address(), encoded.address(), encoded.size());
        Assert.assertTrue(encodedSize > 0);
        //System.out.println((float) encodedSize / data.size());

        ByteSlice decoded = ByteSlice.allocateDirect(data.size());
        long now = System.currentTimeMillis();
        int consumedSize = codec.decodeStrings(valCount, encoded.address(), decoded.address(), decoded.size());
        Assert.assertEquals(encodedSize, consumedSize);
        long t = System.currentTimeMillis() - now;

        Assert.assertTrue(ByteSlice.checkEquals(data, decoded));

        data.free();
        encoded.free();
        decoded.free();

        return t;
    }
}
