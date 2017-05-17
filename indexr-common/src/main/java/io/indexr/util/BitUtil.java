package io.indexr.util;

/*
 * Copied from com.carrotsearch.hppc.BitUtil and make it public,
 */

/**
 * A variety of high efficiency bit twiddling routines.
 */
public final class BitUtil {
    private BitUtil() {} // no instance

    public static void fillZero(long arrAddr, int fromWord, int toWord) {
        MemoryUtil.setMemory(arrAddr + (fromWord << 3), (toWord - fromWord) << 3, (byte) 0);
    }

    public static void fillOne(long arrAddr, int fromWord, int toWord) {
        MemoryUtil.setMemory(arrAddr + (fromWord << 3), (toWord - fromWord) << 3, (byte) 0xFF);
    }

    // The pop methods used to rely on bit-manipulation tricks for speed but it
    // turns out that it is faster to use the Long.bitCount method (which is an
    // intrinsic since Java 6u18) in a naive loop, see LUCENE-2221

    /** Returns the number of set bits in an array of longs. */
    public static long pop_array(long arrAddr, int wordOffset, int numWords) {
        long popCount = 0;
        for (int i = wordOffset, end = wordOffset + numWords; i < end; ++i) {
            popCount += Long.bitCount(MemoryUtil.getLong(arrAddr + (i << 3)));
        }
        return popCount;
    }

    /**
     * Returns the popcount or cardinality of the two sets after an intersection.
     * Neither array is modified.
     */
    public static long pop_intersect(long arr1Addr, long arr2Addr, int wordOffset, int numWords) {
        long popCount = 0;
        for (int i = wordOffset, end = wordOffset + numWords; i < end; ++i) {
            popCount += Long.bitCount(MemoryUtil.getLong(arr1Addr + (i << 3)) & MemoryUtil.getLong(arr2Addr + (i << 3)));
        }
        return popCount;
    }

    /**
     * Returns the popcount or cardinality of the union of two sets.
     * Neither array is modified.
     */
    public static long pop_union(long arr1Addr, long arr2Addr, int wordOffset, int numWords) {
        long popCount = 0;
        for (int i = wordOffset, end = wordOffset + numWords; i < end; ++i) {
            popCount += Long.bitCount(MemoryUtil.getLong(arr1Addr + (i << 3)) | MemoryUtil.getLong(arr2Addr + (i << 3)));
        }
        return popCount;
    }

    /**
     * Returns the popcount or cardinality of A & ~B.
     * Neither array is modified.
     */
    public static long pop_andnot(long arr1Addr, long arr2Addr, int wordOffset, int numWords) {
        long popCount = 0;
        for (int i = wordOffset, end = wordOffset + numWords; i < end; ++i) {
            popCount += Long.bitCount(MemoryUtil.getLong(arr1Addr + (i << 3)) & ~MemoryUtil.getLong(arr2Addr + (i << 3)));
        }
        return popCount;
    }

    /**
     * Returns the popcount or cardinality of A ^ B
     * Neither array is modified.
     */
    public static long pop_xor(long arr1Addr, long arr2Addr, int wordOffset, int numWords) {
        long popCount = 0;
        for (int i = wordOffset, end = wordOffset + numWords; i < end; ++i) {
            popCount += Long.bitCount(MemoryUtil.getLong(arr1Addr + (i << 3)) ^ MemoryUtil.getLong(arr2Addr + (i << 3)));
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


