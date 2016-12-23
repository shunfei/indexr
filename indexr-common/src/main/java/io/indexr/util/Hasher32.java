package io.indexr.util;

import org.apache.spark.unsafe.Platform;

import java.nio.ByteOrder;

/**
 * 32-bit Murmur3 hasher.  This is based on Guava's Murmur3_32HashFunction.
 */
public final class Hasher32 {
    private static final int C1 = 0xcc9e2d51;
    private static final int C2 = 0x1b873593;

    private final int seed;

    public Hasher32(int seed) {
        this.seed = seed;
    }

    @Override
    public String toString() {
        return "Murmur3_32(seed=" + seed + ")";
    }

    public int hashInt(int input) {
        int k1 = mixK1(input);
        int h1 = mixH1(seed, k1);

        return fmix(h1, 4);
    }

    public int hashUnsafeWords(byte[] bytes) {
        return hashUnsafeWords((Object) bytes, Platform.BYTE_ARRAY_OFFSET, bytes.length);
    }

    public int hashUnsafeWords(byte[] bytes, int offset, int len) {
        return hashUnsafeWords((Object) bytes, Platform.BYTE_ARRAY_OFFSET + offset, len);
    }

    public int hashUnsafeWords(Object base, long offset, int lengthInBytes) {
        return hashUnsafeWords(base, offset, lengthInBytes, seed);
    }

    private static final boolean LITTLE_ENDIAN = ByteOrder.nativeOrder().equals(ByteOrder.LITTLE_ENDIAN);

    public static int hashUnsafeWords(Object base, long offset, int lengthInBytes, int seed) {
        // This is based on Guava's `Murmur32_Hasher.processRemaining(ByteBuffer)` method.

        int t = lengthInBytes & 0xFFFF_FFFC;
        int h1 = seed;
        for (int i = 0; i < t; i += 4) {
            int halfWord = Platform.getInt(base, offset + i);
            int k1 = mixK1(halfWord);
            h1 = mixH1(h1, k1);
        }

        int l = lengthInBytes & 0x03;
        if (l != 0) {
            // Risk cross memory boundary.
            //int tail = LITTLE_ENDIAN
            //        ? Platform.getByte(base, offset + t) & (0xFFFFFFFF >>> ((4 - l) << 3))
            //        : Platform.getByte(base, offset + t) & (0xFFFFFFFF << ((4 - l) << 3));

            int tail = 0;
            for (int i = t; i < lengthInBytes; i++) {
                tail = (tail << 8) | (Platform.getByte(base, offset + i) & 0xFF);
            }
            int k1 = mixK1(tail);
            h1 = mixH1(h1, k1);
        }

        return fmix(h1, lengthInBytes);
    }

    public int hashLong(long input) {
        int low = (int) input;
        int high = (int) (input >>> 32);

        int k1 = mixK1(low);
        int h1 = mixH1(seed, k1);

        k1 = mixK1(high);
        h1 = mixH1(h1, k1);

        return fmix(h1, 8);
    }

    private static int mixK1(int k1) {
        k1 *= C1;
        k1 = Integer.rotateLeft(k1, 15);
        k1 *= C2;
        return k1;
    }

    private static int mixH1(int h1, int k1) {
        h1 ^= k1;
        h1 = Integer.rotateLeft(h1, 13);
        h1 = h1 * 5 + 0xe6546b64;
        return h1;
    }

    // Finalization mix - force all bits of a hash block to avalanche
    private static int fmix(int h1, int length) {
        h1 ^= length;
        h1 ^= h1 >>> 16;
        h1 *= 0x85ebca6b;
        h1 ^= h1 >>> 13;
        h1 *= 0xc2b2ae35;
        h1 ^= h1 >>> 16;
        return h1;
    }
}
