package io.indexr.segment.pack;

import java.io.IOException;
import java.nio.ByteBuffer;

import io.indexr.io.ByteBufferWriter;
import io.indexr.io.ByteSlice;
import io.indexr.segment.PackRSIndex;
import io.indexr.segment.PackRSIndexNum;
import io.indexr.segment.RSIndexNum;
import io.indexr.segment.RSValue;
import io.indexr.util.MemoryUtil;

/**
 * Hash + Histogram, each takes 1024 bits.
 *
 * | hash index0 | histo index0 | hash index1 | histo index1 | ...
 * |  1024 bits  |  1024 bits   |  1024 bits  |  1024 bits   | ...
 */
public class RSIndex_Histogram_V2 implements RSIndexNum {
    private static final int PACK_SIZE_BYTE_SHIFT = 8; // 1 << 8 = 1024 * 2 / 8

    private static final int PACK_HASH_SIZE_BYTE = 128; // 1024 bit, 128 byte
    private static final int PACK_HISTO_SIZE_BIT = 1024; // 1024 bit, 128 byte
    private static final int MOD_1024 = 0x03FF;
    private static final int MOD_32 = 0x1F;
    private static final int MOD_8 = 0x03;

    private ByteSlice buffer; // Hold a reference, prevent memory from gc.
    private long bufferAddr;
    private final int packCount;
    private final boolean isFloat; // Floating-point numbers or not.

    public RSIndex_Histogram_V2(int packCount, boolean isFloat) {
        this(ByteSlice.allocateDirect(packCount << PACK_SIZE_BYTE_SHIFT), packCount, isFloat);
    }

    public RSIndex_Histogram_V2(ByteSlice buffer, int packCount, boolean isFloat) {
        this.buffer = buffer;
        this.bufferAddr = this.buffer.address();
        this.packCount = packCount;
        this.isFloat = isFloat;
    }

    @Override
    public long size() {
        return buffer.size();
    }

    private static boolean exactMode(long packMin, long packMax) {
        return (packMax - packMin) <= PACK_HISTO_SIZE_BIT - 2;
    }

    private static boolean intervalTooLarge(long min, long max) {
        // about 2^62
        return min < -4611686018427387900L && max > 4611686018427387900L;
    }

    @Override
    public byte isValue(int packId, long minVal, long maxVal, long packMin, long packMax) {
        assert packId >= 0 && packId < packCount;
        int packOffset = packId << PACK_SIZE_BYTE_SHIFT;
        if (minVal == maxVal) {
            byte res = checkHash(bufferAddr + packOffset, minVal, packMin, packMax, isFloat);
            if (res == RSValue.None) {
                return RSValue.None;
            }
            return RSValue.and(
                    res,
                    checkHistogram(bufferAddr + packOffset + PACK_HASH_SIZE_BYTE, minVal, maxVal, packMin, packMax, isFloat));
        } else {
            return checkHistogram(bufferAddr + packOffset + PACK_HASH_SIZE_BYTE, minVal, maxVal, packMin, packMax, isFloat);
        }
    }

    public void putValue(int packId, long value, long packMin, long packMax) {
        assert packId >= 0 && packId < packCount;
        int packOffset = packId << PACK_SIZE_BYTE_SHIFT;

        putHash(bufferAddr + packOffset, value, packMin, packMax, isFloat);
        putHistogram(bufferAddr + packOffset + PACK_HASH_SIZE_BYTE, value, packMin, packMax, isFloat);
    }

    private static byte checkHash(long packAddr, long value, long packMin, long packMax, boolean isFloat) {
        if (intervalTooLarge(packMin, packMax)) {
            return RSValue.Some;
        }
        long intervalValue;
        if (isFloat) {
            double dValue = Double.longBitsToDouble(value);
            double dpackMin = Double.longBitsToDouble(packMin);
            intervalValue = Double.doubleToRawLongBits(dValue - dpackMin);
        } else {
            intervalValue = value - packMin;
        }
        int bit = (int) (intervalValue & MOD_1024);
        return ((MemoryUtil.getByte(packAddr + (bit >>> 3)) >>> (bit & MOD_8)) & 1) != 0 ? RSValue.Some : RSValue.None;
    }

    private static void putHash(long packAddr, long value, long packMin, long packMax, boolean isFloat) {
        if (intervalTooLarge(packMin, packMax)) {
            return;
        }
        long intervalValue;
        if (isFloat) {
            double dValue = Double.longBitsToDouble(value);
            double dpackMin = Double.longBitsToDouble(packMin);
            intervalValue = Double.doubleToRawLongBits(dValue - dpackMin);
        } else {
            intervalValue = value - packMin;
        }
        int bit = (int) (intervalValue & MOD_1024);
        byte oldVal = MemoryUtil.getByte(packAddr + (bit >>> 3));
        MemoryUtil.setByte(packAddr + (bit >>> 3), (byte) (oldVal | (1 << (bit & MOD_8))));
    }

    private static byte checkHistogram(long packAddr,
                                       long minVal, long maxVal,
                                       long packMin, long packMax,
                                       boolean isFloat) {
        if (intervalTooLarge(minVal, maxVal) || intervalTooLarge(packMin, packMax)) {
            return RSValue.Some;
        }
        int minBit, maxBit;
        if (isFloat) {
            double dminVal = Double.longBitsToDouble(minVal);
            double dmaxVal = Double.longBitsToDouble(maxVal);
            double dpackMin = Double.longBitsToDouble(packMin);
            double dpackMax = Double.longBitsToDouble(packMax);
            assert dminVal <= dmaxVal;
            assert dpackMin <= dpackMax;
            if (dmaxVal < dpackMin || dminVal > dpackMax) {
                return RSValue.None;
            }
            if (dmaxVal >= dpackMax && dminVal <= dpackMin) {
                return RSValue.All;
            }
            if (dmaxVal >= dpackMax || dminVal <= dpackMin) {
                return RSValue.Some;
            }
            double intervalLen = (dpackMax - dpackMin) / (double) PACK_HISTO_SIZE_BIT;
            minBit = (int) ((dminVal - dpackMin) / intervalLen);
            maxBit = (int) ((dmaxVal - dpackMin) / intervalLen);
        } else {
            if (maxVal < packMin || minVal > packMax) {
                return RSValue.None;
            }
            if (maxVal >= packMax && minVal <= packMin) {
                return RSValue.All;
            }
            if (maxVal >= packMax || minVal <= packMin) {
                return RSValue.Some;
            }
            if (exactMode(packMin, packMax)) {
                minBit = (int) (minVal - packMin - 1);
                maxBit = (int) (maxVal - packMin - 1);
            } else {
                double intervalLen = (packMax - packMin) / (double) PACK_HISTO_SIZE_BIT;
                minBit = (int) ((minVal - packMin - 1) / intervalLen);
                maxBit = (int) ((maxVal - packMin - 1) / intervalLen);
            }
        }
        assert minBit >= 0;
        assert maxBit >= 0;
        if (maxBit >= PACK_HISTO_SIZE_BIT) {
            return RSValue.Some;
        }

        int index = -1;
        int histIntValue = 0;
        for (int bit = minBit; bit <= maxBit; bit++) {
            int newIndex = bit >>> 5;
            if (newIndex != index) {
                index = newIndex;
                histIntValue = MemoryUtil.getInt(packAddr + (index << 2));
                //System.out.printf("index: %s, histIntValue: %s\n", index, histIntValue);
            }
            if (((histIntValue >>> (bit & MOD_32)) & 1) != 0) {
                return RSValue.Some;
            }
        }
        return RSValue.None;
    }

    private static void putHistogram(long packAddr,
                                     long value,
                                     long packMin, long packMax,
                                     boolean isFloat) {
        if (intervalTooLarge(packMin, packMax)) {
            return;
        }
        int bit;
        if (isFloat) {
            double dvalue = Double.longBitsToDouble(value);
            double dpackMin = Double.longBitsToDouble(packMin);
            double dpackMax = Double.longBitsToDouble(packMax);
            assert dpackMin <= dpackMax;
            assert dvalue >= dpackMin && dvalue <= dpackMax;
            if (dvalue == dpackMin || dvalue == dpackMax) {
                return;
            }
            double intervalLen = (dpackMax - dpackMin) / (double) PACK_HISTO_SIZE_BIT;
            bit = (int) ((dvalue - dpackMin) / intervalLen);
        } else {
            assert packMin <= packMax;
            assert value >= packMin && value <= packMax;
            if (value == packMin || value == packMax) {
                return;
            }
            if (exactMode(packMin, packMax)) {
                bit = (int) (value - packMin - 1);
            } else {
                double intervalLen = (packMax - packMin) / (double) PACK_HISTO_SIZE_BIT;
                bit = (int) ((value - packMin - 1) / intervalLen);
            }
        }
        if (bit > PACK_HISTO_SIZE_BIT) {
            return;
        }
        assert bit >= 0;
        // An int contains 4 bytes, i.e. 32 bits.
        // index = bit / 32 * 4 + packOffset
        long addr = packAddr + ((bit >>> 5) << 2);
        int oldVal = MemoryUtil.getInt(addr);
        MemoryUtil.setInt(addr, oldVal | (1 << (bit & MOD_32)));
    }

    @Override
    public PackRSIndex packIndex(int packId) {
        assert packId >= 0 && packId < packCount;
        int packOffset = packId << PACK_SIZE_BYTE_SHIFT;
        return new PackIndex(bufferAddr + packOffset, isFloat);
    }

    @Override
    public void free() {
        buffer.free();
        buffer = null;
        bufferAddr = 0;
    }

    @Override
    public void write(ByteBufferWriter writer) throws IOException {
        writer.write(buffer.toByteBuffer(), buffer.size());
    }

    /**
     * Note that this instance could be reused.
     */
    public final static class PackIndex implements PackRSIndexNum {

        private ByteSlice buffer;
        private long bufferAddr;
        private final boolean isFloat;

        public PackIndex(boolean isFloat) {
            this.buffer = ByteSlice.allocateDirect(1 << PACK_SIZE_BYTE_SHIFT);
            this.bufferAddr = buffer.address();
            this.isFloat = isFloat;
            clear();
        }

        /**
         * A index only generated from RSIndex_Histogram.
         */
        private PackIndex(long bufferAddr, boolean isFloat) {
            ByteBuffer bb = MemoryUtil.getHollowDirectByteBuffer();
            // Without cleaner.
            MemoryUtil.setByteBuffer(bb, bufferAddr, 1 << PACK_SIZE_BYTE_SHIFT, null);

            this.buffer = ByteSlice.wrap(bb);
            this.bufferAddr = bufferAddr;
            this.isFloat = isFloat;
        }

        @Override
        public int serializedSize() {
            return buffer.size();
        }

        @Override
        public void write(ByteBufferWriter writer) throws IOException {
            writer.write(buffer.toByteBuffer(), buffer.size());
        }

        @Override
        public void clear() {
            buffer.clear();
        }

        @Override
        public void free() {
            buffer.free();
            buffer = null;
            bufferAddr = 0;
        }

        @Override
        public void putValue(long value, long packMin, long packMax) {
            putHash(bufferAddr, value, packMin, packMax, isFloat);
            putHistogram(bufferAddr + PACK_HASH_SIZE_BYTE, value, packMin, packMax, isFloat);
        }

        @Override
        public byte isValue(long minVal, long maxVal, long packMin, long packMax) {
            if (minVal == maxVal) {
                byte res = checkHash(bufferAddr, minVal, packMin, packMax, isFloat);
                if (res == RSValue.None) {
                    return RSValue.None;
                }
                return RSValue.and(
                        res,
                        checkHistogram(bufferAddr + PACK_HASH_SIZE_BYTE, minVal, maxVal, packMin, packMax, isFloat));
            } else {
                return checkHistogram(bufferAddr + PACK_HASH_SIZE_BYTE, minVal, maxVal, packMin, packMax, isFloat);
            }
        }
    }
}
