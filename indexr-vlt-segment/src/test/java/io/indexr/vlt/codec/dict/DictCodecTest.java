package io.indexr.vlt.codec.dict;

import org.apache.commons.lang.RandomStringUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import io.indexr.vlt.codec.Codec;
import io.indexr.vlt.codec.ErrorCode;
import io.indexr.data.StringsStruct;
import io.indexr.io.ByteSlice;
import io.indexr.util.UTF8Util;
import io.indexr.util.Wrapper;

public class DictCodecTest {
    private Random random = new Random();

    private static interface CodecProvider {
        Codec createCodec(float ratio);
    }

    @Test
    public void testIntsEncodeDecode() {
        long[] a = new long[3];
        for (int unique = 10; unique < 65536; unique += 1000) {
            a[0] += _testIntsEncodeDecode(SimpleDictCodec::new, unique);
            a[1] += _testIntsEncodeDecode(DictCodec::new, unique);
            a[2] += _testIntsEncodeDecode(DictCompressCodec::new, unique);
            System.out.println();
        }
        for (int i = 0; i < 3; i++) {
            a[i] = a[i] / 100000;
        }
        System.out.println("time: " + Arrays.toString(a));
    }

    public long _testIntsEncodeDecode(CodecProvider codecProvider, int unique) {
        int valCount = 65536;
        int dataSize = valCount << 2;
        ByteSlice data = ByteSlice.allocateDirect(dataSize);
        for (int i = 0; i < valCount; i++) {
            data.putInt(i << 2, Math.abs(random.nextInt()) % unique);
        }
        Codec codec = codecProvider.createCodec(0);
        ByteSlice encoded = ByteSlice.allocateDirect(Codec.outputBufferSize(codec.type(), valCount, dataSize));

        int encodedSize = codec.encodeInts(valCount, data.address(), encoded.address(), encoded.size());
        Assert.assertEquals(ErrorCode.DATA_IMPROPER, encodedSize);

        codec = codecProvider.createCodec(1);
        encodedSize = codec.encodeInts(valCount, data.address(), encoded.address(), encoded.size());
        Assert.assertTrue(encodedSize > 0);
        System.out.println((float) encodedSize / dataSize);

        ByteSlice decoded = ByteSlice.allocateDirect(dataSize);
        long now = System.nanoTime();
        int consumedSize = codec.decodeInts(valCount, encoded.address(), decoded.address(), decoded.size());
        Assert.assertEquals(encodedSize, consumedSize);

        long t = System.nanoTime() - now;

        Assert.assertTrue(ByteSlice.checkEquals(data, decoded));

        data.free();
        encoded.free();
        decoded.free();

        return t;
    }

    @Test
    public void testLongsEncodeDecode() {
        long[] a = new long[3];
        for (int unique = 10; unique < 65536; unique += 1000) {
            a[0] += _testLongsEncodeDecode(SimpleDictCodec::new, unique);
            a[1] += _testLongsEncodeDecode(DictCodec::new, unique);
            a[2] += _testLongsEncodeDecode(DictCompressCodec::new, unique);
            System.out.println();
        }
        for (int i = 0; i < 3; i++) {
            a[i] = a[i] / 100000;
        }
        System.out.println("time: " + Arrays.toString(a));
    }

    public long _testLongsEncodeDecode(CodecProvider codecProvider, int unique) {
        int valCount = 65536;
        int dataSize = valCount << 3;
        ByteSlice data = ByteSlice.allocateDirect(dataSize);
        for (int i = 0; i < valCount; i++) {
            data.putLong(i << 3, Math.abs(random.nextLong()) % unique);
        }
        Codec codec = codecProvider.createCodec(0);
        ByteSlice encoded = ByteSlice.allocateDirect(Codec.outputBufferSize(codec.type(), valCount, dataSize));

        int encodedSize = codec.encodeLongs(valCount, data.address(), encoded.address(), encoded.size());
        Assert.assertEquals(ErrorCode.DATA_IMPROPER, encodedSize);

        codec = codecProvider.createCodec(1);
        encodedSize = codec.encodeLongs(valCount, data.address(), encoded.address(), encoded.size());
        Assert.assertTrue(encodedSize > 0);
        System.out.println((float) encodedSize / dataSize);

        ByteSlice decoded = ByteSlice.allocateDirect(dataSize);
        long now = System.nanoTime();
        int consumedSize = codec.decodeLongs(valCount, encoded.address(), decoded.address(), decoded.size());
        Assert.assertEquals(encodedSize, consumedSize);
        long t = System.nanoTime() - now;

        Assert.assertTrue(ByteSlice.checkEquals(data, decoded));

        data.free();
        encoded.free();
        decoded.free();

        return t;
    }

    @Test
    public void testFloatsEncodeDecode() {
        long[] a = new long[3];
        for (int unique = 10; unique < 65536; unique += 1000) {
            a[0] += _testFloatsEncodeDecode(SimpleDictCodec::new, unique);
            a[1] += _testFloatsEncodeDecode(DictCodec::new, unique);
            a[2] += _testFloatsEncodeDecode(DictCompressCodec::new, unique);
            System.out.println();
        }
        for (int i = 0; i < 3; i++) {
            a[i] = a[i] / 100000;
        }
        System.out.println("time: " + Arrays.toString(a));
    }

    public long _testFloatsEncodeDecode(CodecProvider codecProvider, int unique) {
        int valCount = 65536;
        int dataSize = valCount << 2;
        ByteSlice data = ByteSlice.allocateDirect(dataSize);
        for (int i = 0; i < valCount; i++) {
            data.putFloat(i << 2, (float) (Math.abs(random.nextInt()) % unique) / 10);
        }
        Codec codec = codecProvider.createCodec(0);
        ByteSlice encoded = ByteSlice.allocateDirect(Codec.outputBufferSize(codec.type(), valCount, dataSize));

        int encodedSize = codec.encodeFloats(valCount, data.address(), encoded.address(), encoded.size());
        Assert.assertEquals(ErrorCode.DATA_IMPROPER, encodedSize);

        codec = codecProvider.createCodec(1);
        encodedSize = codec.encodeFloats(valCount, data.address(), encoded.address(), encoded.size());
        Assert.assertTrue(encodedSize > 0);
        System.out.println((float) encodedSize / dataSize);

        ByteSlice decoded = ByteSlice.allocateDirect(dataSize);
        long now = System.nanoTime();
        int consumedSize = codec.decodeFloats(valCount, encoded.address(), decoded.address(), decoded.size());
        Assert.assertEquals(encodedSize, consumedSize);
        long t = System.nanoTime() - now;

        Assert.assertTrue(ByteSlice.checkEquals(data, decoded));

        data.free();
        encoded.free();
        decoded.free();

        return t;
    }

    @Test
    public void testDoublesEncodeDecode() {
        long[] a = new long[3];
        for (int unique = 10; unique < 65536; unique += 1000) {
            a[0] += _testDoublesEncodeDecode(SimpleDictCodec::new, unique);
            a[1] += _testDoublesEncodeDecode(DictCodec::new, unique);
            a[2] += _testDoublesEncodeDecode(DictCompressCodec::new, unique);
            System.out.println();
        }
        for (int i = 0; i < 3; i++) {
            a[i] = a[i] / 100000;
        }
        System.out.println("time: " + Arrays.toString(a));
    }

    public long _testDoublesEncodeDecode(CodecProvider codecProvider, int unique) {
        int valCount = 65536;
        int dataSize = valCount << 3;
        ByteSlice data = ByteSlice.allocateDirect(dataSize);
        for (int i = 0; i < valCount; i++) {
            data.putDouble(i << 3, (double) (Math.abs(random.nextLong()) % unique) / 10);
        }
        Codec codec = codecProvider.createCodec(0);
        ByteSlice encoded = ByteSlice.allocateDirect(Codec.outputBufferSize(codec.type(), valCount, dataSize));

        int encodedSize = codec.encodeDoubles(valCount, data.address(), encoded.address(), encoded.size());
        Assert.assertEquals(ErrorCode.DATA_IMPROPER, encodedSize);

        codec = codecProvider.createCodec(1);
        encodedSize = codec.encodeDoubles(valCount, data.address(), encoded.address(), encoded.size());
        Assert.assertTrue(encodedSize > 0);
        System.out.println((float) encodedSize / dataSize);

        ByteSlice decoded = ByteSlice.allocateDirect(dataSize);
        long now = System.nanoTime();
        int consumedSize = codec.decodeDoubles(valCount, encoded.address(), decoded.address(), decoded.size());
        Assert.assertEquals(encodedSize, consumedSize);
        long t = System.nanoTime() - now;

        Assert.assertTrue(ByteSlice.checkEquals(data, decoded));

        data.free();
        encoded.free();
        decoded.free();

        return t;
    }

    @Test
    public void testStringsEncodeDecode() {
        long[] a = new long[3];
        for (int unique = 10; unique < 65536; unique += 1000) {
            a[0] += _testStringsEncodeDecode(SimpleDictCodec::new, unique);
            a[1] += _testStringsEncodeDecode(DictCodec::new, unique);
            a[2] += _testStringsEncodeDecode(DictCompressCodec::new, unique);
            System.out.println();
        }
        for (int i = 0; i < 3; i++) {
            a[i] = a[i] / 100000;
        }
        System.out.println("time: " + Arrays.toString(a));
    }

    public long _testStringsEncodeDecode(CodecProvider codecProvider, int unique) {
        int valCount = 65536;
        List<String> distinctStrings = new ArrayList<>();
        for (int i = 0; i < unique; i++) {
            distinctStrings.add(RandomStringUtils.randomAlphabetic(random.nextInt(30)));
        }
        List<byte[]> strings = new ArrayList<>();
        for (int i = 0; i < valCount; i++) {
            byte[] s = UTF8Util.toUtf8(distinctStrings.get(random.nextInt(unique)));
            strings.add(s);
        }

        Wrapper<ByteSlice> _data = new Wrapper<>();
        StringsStruct.from(strings, _data);
        ByteSlice data = _data.value;

        Codec codec = codecProvider.createCodec(0);
        ByteSlice encoded = ByteSlice.allocateDirect(Codec.outputBufferSize(codec.type(), valCount, data.size()));

        int encodedSize = codec.encodeStrings(valCount, data.address(), encoded.address(), encoded.size());
        Assert.assertEquals(ErrorCode.DATA_IMPROPER, encodedSize);

        codec = codecProvider.createCodec(1);
        encodedSize = codec.encodeStrings(valCount, data.address(), encoded.address(), encoded.size());
        Assert.assertTrue(encodedSize > 0);
        System.out.println((float) encodedSize / data.size());

        ByteSlice decoded = ByteSlice.allocateDirect(data.size());
        long now = System.nanoTime();
        int consumedSize = codec.decodeStrings(valCount, encoded.address(), decoded.address(), decoded.size());
        Assert.assertEquals(encodedSize, consumedSize);
        long t = System.nanoTime() - now;

        Assert.assertTrue(ByteSlice.checkEquals(data, decoded));

        data.free();
        encoded.free();
        decoded.free();

        return t;
    }
}
