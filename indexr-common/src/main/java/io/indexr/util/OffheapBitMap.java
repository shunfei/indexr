package io.indexr.util;


import java.nio.ByteBuffer;

import io.indexr.data.Freeable;

/**
 * A fixed len BitMap stores words in off-heap memory.
 */
public class OffheapBitMap implements Freeable {
    /**
     * The initial default number of bits ({@value #DEFAULT_NUM_BITS}).
     */
    private static final long DEFAULT_NUM_BITS = 64;

    /**
     * Internal representation of bits in this bit set.
     */
    //public long[] bits;

    public long bitsAddr;

    /**
     * The object attached to this bitmap, e.g. a DirectByteBuffer which holds the memory this
     * bitmap use.
     */
    public Object attach;

    /**
     * The number of words (longs).
     */
    public int wlen;

    public OffheapBitMap(long bitsAddr, int wlen, Object attach) {
        this.bitsAddr = bitsAddr;
        this.wlen = wlen;
        this.attach = attach;
    }

    public OffheapBitMap(long cap) {
        this.wlen = bits2words(cap);
        ByteBuffer buffer = ByteBufferUtil.allocateDirect(wlen << 3);
        this.bitsAddr = MemoryUtil.getAddress(buffer);
        this.attach = buffer;
    }

    @Override
    public void free() {
        if (attach != null && attach instanceof ByteBuffer) {
            ByteBufferUtil.free((ByteBuffer) attach);
        }

        bitsAddr = 0;
        wlen = -1;
        attach = null;
    }

    public long word(int wordIndex) {
        assert wordIndex >= 0 && wordIndex < wlen;

        return MemoryUtil.getLong(bitsAddr + (wordIndex << 3));
    }

    public void setWord(int wordIndex, long word) {
        assert wordIndex >= 0 && wordIndex < wlen;

        MemoryUtil.setLong(bitsAddr + (wordIndex << 3), word);
    }

    /**
     * @return Returns an iterator over all set bits of this bitset. The iterator should
     * be faster than using a loop around {@link #nextSetBit(int)}.
     */
    public OffheapBitMapIterator iterator() {
        return new OffheapBitMapIterator(bitsAddr, wlen);
    }

    /**
     * @return Returns the current capacity in bits (1 greater than the index of the last bit).
     */
    public long capacity() {
        return wlen << 6;
    }

    /**
     * @return Returns the current capacity of this set. Included for compatibility. This is <b>not</b>
     * equal to {@link #cardinality}.
     * @see #cardinality()
     * @see java.util.BitSet#size()
     */
    public long size() {
        return capacity();
    }

    /**
     * @return Returns the "logical size" of this {@code BitSet}: the index of
     * the highest set bit in the {@code BitSet} plus one.
     * @see java.util.BitSet#length()
     */
    public long length() {
        trimTrailingZeros();
        if (wlen == 0) return 0;
        return (((long) wlen - 1) << 6)
                + (64 - Long.numberOfLeadingZeros(word(wlen - 1)));
    }

    /**
     * @return Returns true if there are no set bits
     */
    public boolean isEmpty() {
        return cardinality() == 0;
    }

    /**
     * @param index The index.
     * @return Returns true or false for the specified bit index.
     */
    public boolean get(int index) {
        assert index >= 0 && index < (wlen << 6);

        int i = index >> 6; // div 64
        // signed shift will keep a negative index and force an
        // array-index-out-of-bounds-exception, removing the need for an explicit check.
        if (i >= wlen) return false;

        int bit = index & 0x3f; // mod 64
        long bitmask = 1L << bit;
        return (word(i) & bitmask) != 0;
    }

    /**
     * @param index The index.
     * @return Returns true or false for the specified bit index.
     */
    public boolean get(long index) {
        assert index >= 0 && index < (wlen << 6);

        int i = (int) (index >> 6); // div 64
        if (i >= wlen) return false;
        int bit = (int) index & 0x3f; // mod 64
        long bitmask = 1L << bit;
        return (word(i) & bitmask) != 0;
    }

    /**
     * Sets a bit.
     *
     * @param index the index to set
     */
    public void set(long index) {
        assert index >= 0 && index < (wlen << 6);

        int wordNum = expandingWordNum(index);
        int bit = (int) index & 0x3f;
        long bitmask = 1L << bit;
        setWord(wordNum, word(wordNum) | bitmask);
    }

    /**
     * Sets a range of bits: [startIndex, endIndex).
     *
     * @param startIndex include
     * @param endIndex   exclude
     */
    public void set(long startIndex, long endIndex) {
        assert startIndex >= 0;
        assert endIndex <= (wlen << 6);

        if (endIndex <= startIndex) return;

        int startWord = (int) (startIndex >> 6);

        // since endIndex is one past the end, this is index of the last
        // word to be changed.
        int endWord = expandingWordNum(endIndex - 1);

        long startmask = -1L << startIndex;
        long endmask = -1L >>> -endIndex; // 64-(endIndex&0x3f) is the same as -endIndex
        // due to wrap

        if (startWord == endWord) {
            setWord(startWord, word(startWord) | (startmask & endmask));
            return;
        }

        setWord(startWord, word(startWord) | startmask);
        BitUtil.fillOne(bitsAddr, startWord + 1, endWord);
        setWord(endWord, word(endWord) | endmask);
    }

    protected int expandingWordNum(long index) {
        int wordNum = (int) (index >> 6);
        assert wordNum < wlen;
        return wordNum;
    }

    /** Clears all bits. */
    public void clear() {
        BitUtil.fillZero(bitsAddr, 0, wlen);
        this.wlen = 0;
    }

    /**
     * clears a bit
     *
     * @param index the index to clear
     */
    public void clear(long index) {
        assert index >= 0 && index < (wlen << 6);

        int wordNum = (int) (index >> 6); // div 64
        if (wordNum >= wlen) return;
        int bit = (int) index & 0x3f; // mod 64
        long bitmask = 1L << bit;
        setWord(wordNum, word(wordNum) & (~bitmask));
    }

    /**
     * Clears a range of bits
     *
     * @param startIndex lower index
     * @param endIndex   one-past the last bit to clear
     */
    public void clear(int startIndex, int endIndex) {
        assert startIndex >= 0;
        assert endIndex <= (wlen << 6);

        if (endIndex <= startIndex) return;

        int startWord = (startIndex >> 6);
        if (startWord >= wlen) return;

        // since endIndex is one past the end, this is index of the last
        // word to be changed.
        int endWord = ((endIndex - 1) >> 6);

        long startmask = -1L << startIndex;
        long endmask = -1L >>> -endIndex; // 64-(endIndex&0x3f) is the same as -endIndex
        // due to wrap

        // invert masks since we are clearing
        startmask = ~startmask;
        endmask = ~endmask;

        if (startWord == endWord) {
            setWord(startWord, word(startWord) & (startmask | endmask));
            return;
        }

        setWord(startWord, word(startWord) & startmask);

        int middle = Math.min(wlen, endWord);
        BitUtil.fillZero(bitsAddr, startWord + 1, middle);
        if (endWord < wlen) {
            setWord(endWord, word(endIndex) & endmask);
        }
    }

    /**
     * Clears a range of bits. [startIndex, endIndex)
     *
     * @param startIndex lower index
     * @param endIndex   one-past the last bit to clear
     */
    public void clear(long startIndex, long endIndex) {
        assert startIndex >= 0;
        assert endIndex <= (wlen << 6);

        if (endIndex <= startIndex) return;

        int startWord = (int) (startIndex >> 6);
        if (startWord >= wlen) return;

        // since endIndex is one past the end, this is index of the last
        // word to be changed.
        int endWord = (int) ((endIndex - 1) >> 6);

        long startmask = -1L << startIndex;
        long endmask = -1L >>> -endIndex; // 64-(endIndex&0x3f) is the same as -endIndex
        // due to wrap

        // invert masks since we are clearing
        startmask = ~startmask;
        endmask = ~endmask;

        if (startWord == endWord) {
            setWord(startWord, word(startWord) & (startmask | endmask));
            return;
        }

        setWord(startWord, word(startWord) & startmask);

        int middle = Math.min(wlen, endWord);
        BitUtil.fillZero(bitsAddr, startWord + 1, middle);
        if (endWord < wlen) {
            setWord(endWord, word(endWord) & endmask);
        }
    }

    /**
     * Sets a bit and returns the previous value. The index should be less than the BitSet
     * size.
     *
     * @param index the index to set
     * @return previous state of the index
     */
    public boolean getAndSet(int index) {
        assert index >= 0 && index < (wlen << 6);

        int wordNum = index >> 6; // div 64
        int bit = index & 0x3f; // mod 64
        long bitmask = 1L << bit;
        boolean val = (word(wordNum) & bitmask) != 0;
        setWord(wordNum, word(wordNum) | bitmask);
        return val;
    }

    /**
     * Sets a bit and returns the previous value. The index should be less than the BitSet
     * size.
     *
     * @param index the index to set
     * @return previous state of the index
     */
    public boolean getAndSet(long index) {
        assert index >= 0 && index < (wlen << 6);

        int wordNum = (int) (index >> 6); // div 64
        int bit = (int) index & 0x3f; // mod 64
        long bitmask = 1L << bit;
        boolean val = (word(wordNum) & bitmask) != 0;
        setWord(wordNum, word(wordNum) | bitmask);
        return val;
    }

    /**
     * Flips a bit, expanding the set size if necessary.
     *
     * @param index the index to flip
     */
    public void flip(long index) {
        assert index >= 0 && index < (wlen << 6);

        int wordNum = expandingWordNum(index);
        int bit = (int) index & 0x3f; // mod 64
        long bitmask = 1L << bit;
        setWord(wordNum, word(wordNum) ^ bitmask);
    }

    /**
     * flips a bit and returns the resulting bit value. The index should be less than the
     * BitSet size.
     *
     * @param index the index to flip
     * @return previous state of the index
     */
    public boolean flipAndGet(int index) {
        assert index >= 0 && index < (wlen << 6);

        int wordNum = index >> 6; // div 64
        int bit = index & 0x3f; // mod 64
        long bitmask = 1L << bit;
        setWord(wordNum, word(wordNum) ^ bitmask);
        return (word(wordNum) & bitmask) != 0;
    }

    /**
     * flips a bit and returns the resulting bit value. The index should be less than the
     * BitSet size.
     *
     * @param index the index to flip
     * @return previous state of the index
     */
    public boolean flipAndGet(long index) {
        assert index >= 0 && index < (wlen << 6);

        int wordNum = (int) (index >> 6); // div 64
        int bit = (int) index & 0x3f; // mod 64
        long bitmask = 1L << bit;
        setWord(wordNum, word(wordNum) ^ bitmask);
        return (word(wordNum) & bitmask) != 0;
    }

    /**
     * Flips a range of bits. [startIndex, endIndex)
     *
     * @param startIndex lower index
     * @param endIndex   one-past the last bit to flip
     */
    public void flip(long startIndex, long endIndex) {
        assert startIndex >= 0;
        assert endIndex <= (wlen << 6);

        if (endIndex <= startIndex) return;
        int startWord = (int) (startIndex >> 6);

        // since endIndex is one past the end, this is index of the last
        // word to be changed.
        int endWord = expandingWordNum(endIndex - 1);

        long startmask = -1L << startIndex;
        long endmask = -1L >>> -endIndex; // 64-(endIndex&0x3f) is the same as -endIndex
        // due to wrap

        if (startWord == endWord) {
            setWord(startWord, word(startWord) ^ (startmask & endmask));
            return;
        }

        setWord(startWord, word(startWord) ^ startmask);

        for (int i = startWord + 1; i < endWord; i++) {
            setWord(i, ~word(i));
        }

        setWord(endWord, word(endWord) ^ endmask);
    }

    /** @return the number of set bits */
    public long cardinality() {
        return BitUtil.pop_array(bitsAddr, 0, wlen);
    }

    /**
     * @param a The first set
     * @param b The second set
     * @return Returns the popcount or cardinality of the intersection of the two sets. Neither
     * set is modified.
     */
    public static long intersectionCount(OffheapBitMap a, OffheapBitMap b) {
        return BitUtil.pop_intersect(a.bitsAddr, b.bitsAddr, 0, Math.min(a.wlen, b.wlen));
    }

    /**
     * @param a The first set
     * @param b The second set
     * @return Returns the popcount or cardinality of the union of the two sets. Neither set is
     * modified.
     */
    public static long unionCount(OffheapBitMap a, OffheapBitMap b) {
        long tot = BitUtil.pop_union(a.bitsAddr, b.bitsAddr, 0, Math.min(a.wlen, b.wlen));
        if (a.wlen < b.wlen) {
            tot += BitUtil.pop_array(b.bitsAddr, a.wlen, b.wlen - a.wlen);
        } else if (a.wlen > b.wlen) {
            tot += BitUtil.pop_array(a.bitsAddr, b.wlen, a.wlen - b.wlen);
        }
        return tot;
    }

    /**
     * @param a The first set
     * @param b The second set
     * @return Returns the popcount or cardinality of "a and not b" or "intersection(a, not(b))".
     * Neither set is modified.
     */
    public static long andNotCount(OffheapBitMap a, OffheapBitMap b) {
        long tot = BitUtil.pop_andnot(a.bitsAddr, b.bitsAddr, 0, Math.min(a.wlen, b.wlen));
        if (a.wlen > b.wlen) {
            tot += BitUtil.pop_array(a.bitsAddr, b.wlen, a.wlen - b.wlen);
        }
        return tot;
    }

    /**
     * @param a The first set
     * @param b The second set
     * @return Returns the popcount or cardinality of the exclusive-or of the two sets. Neither
     * set is modified.
     */
    public static long xorCount(OffheapBitMap a, OffheapBitMap b) {
        long tot = BitUtil.pop_xor(a.bitsAddr, b.bitsAddr, 0, Math.min(a.wlen, b.wlen));
        if (a.wlen < b.wlen) {
            tot += BitUtil.pop_array(b.bitsAddr, a.wlen, b.wlen - a.wlen);
        } else if (a.wlen > b.wlen) {
            tot += BitUtil.pop_array(a.bitsAddr, b.wlen, a.wlen - b.wlen);
        }
        return tot;
    }

    /**
     * @param index The index to start scanning from, inclusive.
     * @return Returns the index of the first set bit starting at the index specified. -1 is
     * returned if there are no more set bits.
     */
    public int nextSetBit(int index) {
        int i = index >> 6;
        if (i >= wlen) return -1;
        int subIndex = index & 0x3f; // index within the word
        long word = word(i) >> subIndex; // skip all the bits to the right of index

        if (word != 0) {
            return (i << 6) + subIndex + Long.numberOfTrailingZeros(word);
        }

        while (++i < wlen) {
            word = word(i);
            if (word != 0) return (i << 6) + Long.numberOfTrailingZeros(word);
        }

        return -1;
    }

    /**
     * @param index The index to start scanning from, inclusive.
     * @return Returns the index of the first set bit starting at the index specified. -1 is
     * returned if there are no more set bits.
     */
    public long nextSetBit(long index) {
        int i = (int) (index >>> 6);
        if (i >= wlen) return -1;
        int subIndex = (int) index & 0x3f; // index within the word
        long word = word(i) >>> subIndex; // skip all the bits to the right of index

        if (word != 0) {
            return (((long) i) << 6) + (subIndex + Long.numberOfTrailingZeros(word));
        }

        while (++i < wlen) {
            word = word(i);
            if (word != 0) return (((long) i) << 6) + Long.numberOfTrailingZeros(word);
        }

        return -1;
    }

    /**
     * this = this AND other
     *
     * @param other The bitset to intersect with.
     */
    public void intersect(OffheapBitMap other) {
        assert wlen == other.wlen;
        // testing against zero can be more efficient
        int pos = wlen;
        while (--pos >= 0) {
            setWord(pos, word(pos) & other.word(pos));
        }
    }

    /**
     * this = this OR other
     *
     * @param other The bitset to union with.
     */
    public void union(OffheapBitMap other) {
        assert wlen == other.wlen;

        int pos = wlen;
        while (--pos >= 0) {
            setWord(pos, word(pos) | other.word(pos));
        }
    }

    /**
     * Remove all elements set in other: this = this AND_NOT other
     *
     * @param other The other bitset.
     */
    public void remove(OffheapBitMap other) {
        assert wlen == other.wlen;

        int idx = wlen;
        while (--idx >= 0) {
            setWord(idx, word(idx) & (~other.word(idx)));
        }
    }

    /**
     * this = this XOR other
     *
     * @param other The other bitset.
     */
    public void xor(OffheapBitMap other) {
        assert wlen == other.wlen;

        int pos = wlen;
        while (--pos >= 0) {
            setWord(pos, word(pos) ^ other.word(pos));
        }
    }

    // some BitSet compatibility methods

    // ** see {@link intersect} */
    public void and(OffheapBitMap other) {
        intersect(other);
    }

    // ** see {@link union} */
    public void or(OffheapBitMap other) {
        union(other);
    }

    // ** see {@link andNot} */
    public void andNot(OffheapBitMap other) {
        remove(other);
    }

    /**
     * @param other The other bitset.
     * @return true if the sets have any elements in common
     */
    public boolean intersects(OffheapBitMap other) {
        assert wlen == other.wlen;
        int pos = wlen;
        while (--pos >= 0) {
            if ((word(pos) & other.word(pos)) != 0) return true;
        }
        return false;
    }


    public static int getNextSize(int targetSize) {
        /*
         * This over-allocates proportional to the list size, making room for additional
         * growth. The over-allocation is mild, but is enough to give linear-time
         * amortized behavior over a long sequence of appends() in the presence of a
         * poorly-performing system realloc(). The growth pattern is: 0, 4, 8, 16, 25, 35,
         * 46, 58, 72, 88, ...
         */
        return (targetSize >> 3) + (targetSize < 9 ? 3 : 6) + targetSize;
    }

    /**
     * Lowers {@link #wlen}, the number of words in use, by checking for trailing zero
     * words.
     */
    public void trimTrailingZeros() {
        int idx = wlen - 1;
        while (idx >= 0 && word(idx) == 0)
            idx--;
        wlen = idx + 1;
    }

    /*
     * returns the number of 64 bit words it would take to hold numBits
     */
    public static int bits2words(long numBits) {
        return (int) (((numBits - 1) >>> 6) + 1);
    }

    /* returns true if both sets have the same bits set */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof OffheapBitMap)) return false;

        OffheapBitMap a;
        OffheapBitMap b = (OffheapBitMap) o;

        // make a the larger set.
        if (b.wlen > this.wlen) {
            a = b;
            b = this;
        } else {
            a = this;
        }

        // check for any set bits out of the range of b
        for (int i = a.wlen - 1; i >= b.wlen; i--) {
            if (a.word(i) != 0) return false;
        }

        for (int i = b.wlen - 1; i >= 0; i--) {
            if (a.word(i) != b.word(i)) return false;
        }

        return true;
    }

    @Override
    public String toString() {
        long bit = nextSetBit(0);
        if (bit < 0) {
            return "{}";
        }

        final StringBuilder builder = new StringBuilder();
        builder.append("{");

        builder.append(Long.toString(bit));
        while ((bit = nextSetBit(bit + 1)) >= 0) {
            builder.append(", ");
            builder.append(Long.toString(bit));
        }
        builder.append("}");

        return builder.toString();
    }

    public static int numberOfLeadingZeros(long i) {
        // HD, Figure 5-6
        if (i == 0)
            return 64;
        int n = 1;
        int x = (int) (i >>> 32);
        if (x == 0) {
            n += 32;
            x = (int) i;
        }
        if (x >>> 16 == 0) {
            n += 16;
            x <<= 16;
        }
        if (x >>> 24 == 0) {
            n += 8;
            x <<= 8;
        }
        if (x >>> 28 == 0) {
            n += 4;
            x <<= 4;
        }
        if (x >>> 30 == 0) {
            n += 2;
            x <<= 2;
        }
        n -= x >>> 31;
        return n;
    }

    public static int numberOfTrailingZeros(long i) {
        // HD, Figure 5-14
        int x, y;
        if (i == 0) return 64;
        int n = 63;
        y = (int) i;
        if (y != 0) {
            n = n - 32;
            x = y;
        } else x = (int) (i >>> 32);
        y = x << 16;
        if (y != 0) {
            n = n - 16;
            x = y;
        }
        y = x << 8;
        if (y != 0) {
            n = n - 8;
            x = y;
        }
        y = x << 4;
        if (y != 0) {
            n = n - 4;
            x = y;
        }
        y = x << 2;
        if (y != 0) {
            n = n - 2;
            x = y;
        }
        return n - ((x << 1) >>> 31);
    }
}
