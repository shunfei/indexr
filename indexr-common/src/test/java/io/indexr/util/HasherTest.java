package io.indexr.util;

import org.apache.spark.unsafe.Platform;
import org.junit.Assert;
import org.junit.Test;

public class HasherTest {
    @Test
    public void testHash() {
        byte[] bytes1 = new byte[]{2, 3, -14, 42, -3, 19};
        byte[] bytes2 = new byte[]{44, 2, 3, -14, 42, -3, 19, 25, 18};
        Hasher32 hasher = new Hasher32(0);
        //Murmur3_x86_32 hasher = new Murmur3_x86_32(0);
        int h1 = hasher.hashUnsafeWords(bytes1, 0, 5);
        int h2 = hasher.hashUnsafeWords(bytes2, 1, 5);
        Assert.assertEquals(h1, h2);

        long[] longs = new long[]{2222, -234};
        Assert.assertEquals(longs[1], Platform.getLong(longs, Platform.LONG_ARRAY_OFFSET + 8));

    }
}
