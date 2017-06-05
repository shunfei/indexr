package io.indexr.vlt.codec.rle;

import io.indexr.vlt.codec.Codec;
import io.indexr.vlt.codec.CodecType;
import io.indexr.vlt.codec.UnsafeUtil;
import io.indexr.vlt.codec.VarInteger;

import org.junit.Assert;
import org.junit.Test;

import java.util.Random;

import io.indexr.io.ByteSlice;

public class RLEPackingHybridCodecTest {
    private Random random = new Random();

    @Test
    public void test() {
        long a = 0;
        for (int i = 0; i < 100; i++) {
            a += _test();
        }
        System.out.println(a);
    }

    public long _test() {
        int valCount = random.nextInt(65536);
        int dataSize = valCount << 2;
        int max = random.nextInt(10000000);
        ByteSlice data = ByteSlice.allocateDirect(dataSize);
        for (int i = 0; i < valCount; ) {
            int v = random.nextInt(max);
            int count = random.nextInt(16);
            for (int k = 0; k < count & i + k < valCount; k++) {
                data.putInt((i + k) << 2, v);
            }
            i += count;
        }
        int[] a1 = new int[valCount];
        UnsafeUtil.copyMemory(null, data.address(), a1, UnsafeUtil.INT_ARRAY_OFFSET, dataSize);

        ByteSlice encoded = ByteSlice.allocateDirect(Codec.outputBufferSize(CodecType.DELTA, valCount, dataSize));

        Codec codec = new RLEPackingHybridCodec(VarInteger.bitWidth(max));
        int encodedSize = codec.encodeInts(valCount, data.address(), encoded.address(), encoded.size());
        Assert.assertTrue(encodedSize > 0);
        //System.out.println((float) res / dataSize);

        ByteSlice decoded = ByteSlice.allocateDirect(valCount << 2);
        long now = System.currentTimeMillis();
        int consumedSize = codec.decodeInts(valCount, encoded.address(), decoded.address(), decoded.size());
        Assert.assertEquals(encodedSize, consumedSize);

        long t = System.currentTimeMillis() - now;

        int[] a2 = new int[valCount];
        UnsafeUtil.copyMemory(null, decoded.address(), a2, UnsafeUtil.INT_ARRAY_OFFSET, dataSize);

        Assert.assertArrayEquals(a1, a2);
        Assert.assertTrue(ByteSlice.checkEquals(data, decoded));

        // Those not only for a good habit, but also stop jvm from reclaiming them too early!
        data.free();
        encoded.free();
        decoded.free();

        return t;
    }
}
