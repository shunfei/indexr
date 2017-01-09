package io.indexr.segment.io;

import com.google.common.base.Preconditions;

import io.indexr.io.ByteSlice;

/**
 * Longs that can represented by less bits, i.e 0~63 bits. Only max value in [0, NLong.MAX_VALUE) is supported.
 * 
 * offset:    the offset of bits in the source.
 * width:     the actual bits to store value.
 * gap:       bits between the values.
 * <pre>
 *  offset: 0, width: 2, gap: 0
 *  |..|..|..|..|..|
 *  0  1  2  3  4
 *
 *  offset: 4, width: 2, gap:3
 *  ....|..'...|..'...|..'...
 *      0      1      2
 * </pre>
 */
public class FixedBitWidthNumbers implements FixedWidthNumbers {
    private final ByteSlice source;
    private final int offset;
    private final int width;
    private final int gap;
    private final int skipWidth;
    private final long max;

    public FixedBitWidthNumbers(ByteSlice source, int width) {
        this(source, 0, width, 0);
    }

    public FixedBitWidthNumbers(ByteSlice source, int offset, int width, int gap) {
        Preconditions.checkArgument(source != null);
        Preconditions.checkArgument(offset >= 0);
        Preconditions.checkArgument(width >= 0 && width <= 63);
        Preconditions.checkArgument(gap >= 0);

        long theMax = 0;
        for (int i = 0; i < width; i++) {
            theMax = (theMax << 1) | 1;
        }

        this.source = source;
        this.offset = offset;
        this.width = width;
        this.gap = gap;
        this.skipWidth = width + gap;
        this.max = theMax;
    }

    public static int calWidth(long max) {
        Preconditions.checkArgument(max >= 0);

        int width = 0;
        while (max != 0) {
            max >>>= 1;
            width++;
        }
        return width;
    }

    public static int calByteSize(int itemSize, int bitWidth) {
        Preconditions.checkArgument(itemSize >= 0 && bitWidth >= 0);

        if (itemSize == 0 || bitWidth == 0) {
            return 0;
        } else {
            return ((itemSize * bitWidth - 1) >>> 3) + 1;
        }
    }

    public long max() {
        return max;
    }

    public int width() {
        return width;
    }

    public int gap() {
        return gap;
    }

    public long get(int index) {
        Preconditions.checkArgument(index >= 0);

        int startOffsetBits = index * skipWidth + offset;

        int byteStart = startOffsetBits >> 3; // div 8, 8 bits a byte.
        int bitStart = startOffsetBits & 0x07; // mod 8

        int byteEnd = (startOffsetBits + width) >> 3;
        int bitEnd = (startOffsetBits + width) & 0x07;

        long value = 0;
        int bitOffset = bitStart;
        for (int byteOffset = byteStart; byteOffset <= byteEnd; byteOffset++) {
            int theBitEnd = byteOffset == byteEnd ? bitEnd : 8;
            if (theBitEnd > 0) {
                int b = source.get(byteOffset) & 0xFF;
                while (bitOffset < theBitEnd) {
                    value = (value << 1) | ((b >>> (7 - bitOffset)) & 0x01);
                    bitOffset++;
                }
                bitOffset = 0;
            }
        }

        return value;
    }

    public void set(int index, long value) {
        Preconditions.checkArgument(index >= 0);
        Preconditions.checkArgument(value >= 0 && value <= max);

        int startOffsetBits = index * skipWidth + offset;

        int byteStart = startOffsetBits >>> 3; // div 8, 8 bits a byte.
        int bitStart = startOffsetBits & 0x07; // mod 8

        int byteEnd = (startOffsetBits + width) >>> 3;
        int bitEnd = (startOffsetBits + width) & 0x07;

        int bitOffset = bitStart;
        int addedBit = 0;
        for (int byteOffset = byteStart; byteOffset <= byteEnd; byteOffset++) {
            int theBitEnd = byteOffset == byteEnd ? bitEnd : 8;
            if (theBitEnd > 0) {
                int addBitCount = theBitEnd - bitOffset;
                addedBit += addBitCount;

                int leftShift = 8 - theBitEnd;
                int rightShift = width - addedBit;

                int mask = 0;
                for (int i = 0; i < addBitCount; i++) {
                    mask = (mask << 1) | 1;
                }
                int addBits = (int) ((value >>> rightShift & mask) << leftShift);

                int b = source.get(byteOffset) & 0xFF;
                b = b & ~(mask << leftShift);
                b = b | addBits;

                source.put(byteOffset, (byte) b);
                bitOffset = 0;
            }
        }
    }
}
