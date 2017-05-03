package io.indexr.util;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.spark.unsafe.Platform;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Random;

public class BytesUtilTest {

    Comparator<byte[]> comparator = new Comparator<byte[]>() {
        @Override
        public int compare(byte[] s1, byte[] s2) {
            return BytesUtil.compareBytes(
                    s1, Platform.BYTE_ARRAY_OFFSET, s1.length,
                    s2, Platform.BYTE_ARRAY_OFFSET, s2.length);
        }
    };

    @Test
    public void test() {
        int valCount = 65536;
        Random random = new Random();
        ArrayList<byte[]> strings = new ArrayList<>();
        for (int i = 0; i < valCount; i++) {
            byte[] s = UTF8Util.toUtf8(RandomStringUtils.randomAlphabetic(random.nextInt(30)));
            strings.add(s);
        }

        Collections.sort(strings, comparator);

        byte[] last = null;
        for (byte[] v : strings) {
            if (last != null) {
                Assert.assertTrue(comparator.compare(last, v) <= 0);
            }
            last = v;
        }
    }
}
