package io.indexr.compress;


import com.google.common.io.CountingInputStream;
import com.google.common.io.CountingOutputStream;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import io.indexr.io.BitWrappedInputStream;
import io.indexr.io.BitWrappedOutputStream;


public class ArithmeticCoderTest {
    int[] counts = new int[]{1, 1, 1, 1, 1, 1, 1, 1, 1, 1};
    int[] symbols = new int[]{0, 1, 4, 6, 7, 3, 4, 1, 2, 5, 6, 3, 4, 6, 7, 3, 3, 7, 8, 4, 6, 7, 2, 4, 7, 2, 3, 6, 4, 0, 0, 0, 0, 0, 0, 0, 9, 9, 1};

    // int[] counts = new int[]{1, 1};
    // int[] symbols = new int[]{1, 0, 1, 0,1,0,0,1,0,1,0,1,0,1,0,1,0,1,0,1,0,1,1, 0, 1, 0,1,0,0,1,0,1,0,1,0,1,0,1,0,1,0,1,0,0,0,0,0,1};

    @Test
    public void encodeDecodeTest() throws IOException {
        ArthmeticCoder.SimpleFrequency freq = new ArthmeticCoder.SimpleFrequency(counts);

        ByteArrayOutputStream encodedPool = new ByteArrayOutputStream();
        CountingOutputStream outputCounting = new CountingOutputStream(encodedPool);
        ArthmeticCoder.Encoder encoder = new ArthmeticCoder.Encoder(freq, new BitWrappedOutputStream(outputCounting));
        for (int s : symbols) {
            encoder.write(s);
        }
        encoder.seal();

        ByteArrayInputStream decodedPool = new ByteArrayInputStream(encodedPool.toByteArray());
        CountingInputStream inputCounting = new CountingInputStream(decodedPool);
        ArthmeticCoder.Decoder decoder = new ArthmeticCoder.Decoder(freq, new BitWrappedInputStream(inputCounting));
        int[] symbols2 = new int[symbols.length];
        for (int i = 0; i < symbols.length; i++) {
            symbols2[i] = decoder.read();
        }

        Assert.assertEquals(outputCounting.getCount(), inputCounting.getCount());
        Assert.assertArrayEquals(symbols, symbols2);
    }
}
