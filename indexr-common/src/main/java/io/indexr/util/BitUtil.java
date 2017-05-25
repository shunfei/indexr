package io.indexr.util;

/*
 * Copied from com.carrotsearch.hppc.BitUtil and make it public,
 */

import org.apache.spark.unsafe.Platform;

/**
 * A variety of high efficiency bit twiddling routines.
 */
public final class BitUtil {
    private BitUtil() {} // no instance

    public static void fillZero(Object base, long arrAddr, int fromWord, int toWord) {
        MemoryUtil.setMemory(base, arrAddr + (fromWord << 3), (toWord - fromWord) << 3, (byte) 0);
    }

    public static void fillOne(Object base, long arrAddr, int fromWord, int toWord) {
        MemoryUtil.setMemory(base, arrAddr + (fromWord << 3), (toWord - fromWord) << 3, (byte) 0xFF);
    }

    // The pop methods used to rely on bit-manipulation tricks for speed but it
    // turns out that it is faster to use the Long.bitCount method (which is an
    // intrinsic since Java 6u18) in a naive loop, see LUCENE-2221

    /** Returns the number of set bits in an array of longs. */
    public static long pop_array(Object base, long arrAddr, int wordOffset, int numWords) {
        long popCount = 0;
        for (int i = wordOffset, end = wordOffset + numWords; i < end; ++i) {
            popCount += Long.bitCount(Platform.getLong(base, arrAddr + (i << 3)));
        }
        return popCount;
    }

    /**
     * Returns the popcount or cardinality of the two sets after an intersection.
     * Neither array is modified.
     */
    public static long pop_intersect(Object base1, long arr1Addr, Object base2, long arr2Addr, int wordOffset, int numWords) {
        long popCount = 0;
        for (int i = wordOffset, end = wordOffset + numWords; i < end; ++i) {
            popCount += Long.bitCount(Platform.getLong(base1, arr1Addr + (i << 3)) & Platform.getLong(base2, arr2Addr + (i << 3)));
        }
        return popCount;
    }

    /**
     * Returns the popcount or cardinality of the union of two sets.
     * Neither array is modified.
     */
    public static long pop_union(Object base1, long arr1Addr, Object base2, long arr2Addr, int wordOffset, int numWords) {
        long popCount = 0;
        for (int i = wordOffset, end = wordOffset + numWords; i < end; ++i) {
            popCount += Long.bitCount(Platform.getLong(base1, arr1Addr + (i << 3)) | Platform.getLong(base2, arr2Addr + (i << 3)));
        }
        return popCount;
    }

    /**
     * Returns the popcount or cardinality of A & ~B.
     * Neither array is modified.
     */
    public static long pop_andnot(Object base1, long arr1Addr, Object base2, long arr2Addr, int wordOffset, int numWords) {
        long popCount = 0;
        for (int i = wordOffset, end = wordOffset + numWords; i < end; ++i) {
            popCount += Long.bitCount(Platform.getLong(base1, arr1Addr + (i << 3)) & ~Platform.getLong(base2, arr2Addr + (i << 3)));
        }
        return popCount;
    }

    /**
     * Returns the popcount or cardinality of A ^ B
     * Neither array is modified.
     */
    public static long pop_xor(Object base1, long arr1Addr, Object base2, long arr2Addr, int wordOffset, int numWords) {
        long popCount = 0;
        for (int i = wordOffset, end = wordOffset + numWords; i < end; ++i) {
            popCount += Long.bitCount(Platform.getLong(base1, arr1Addr + (i << 3)) ^ Platform.getLong(base2, arr2Addr + (i << 3)));
        }
        return popCount;
    }

    /** returns the next highest power of two, or the current value if it's already a power of two or zero */
    public static int nextHighestPowerOfTwo(int v) {
        v--;
        v |= v >> 1;
        v |= v >> 2;
        v |= v >> 4;
        v |= v >> 8;
        v |= v >> 16;
        v++;
        return v;
    }

    /** returns the next highest power of two, or the current value if it's already a power of two or zero */
    public static long nextHighestPowerOfTwo(long v) {
        v--;
        v |= v >> 1;
        v |= v >> 2;
        v |= v >> 4;
        v |= v >> 8;
        v |= v >> 16;
        v |= v >> 32;
        v++;
        return v;
    }
}


