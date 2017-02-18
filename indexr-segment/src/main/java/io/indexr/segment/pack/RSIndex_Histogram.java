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

// This class is implemented after RSI_Histogram.cpp from ICE-4.0.7.

/**
 * Each pack (with 65536 values), we use 128 bytes, i.e. 1024 bits to index.
 */
final class RSIndex_Histogram implements RSIndexNum {
    private static final int PACK_HIST_SIZE_BYTE_SHIFT = 7; // * 128
    private static final int PACK_HIST_SIZE_BIT = 1 << (PACK_HIST_SIZE_BYTE_SHIFT + 3); // 1024 bit, 128 byte
    private static final int INT_BIT_MASK = 0b11111; // mod 32

    private ByteSlice histBuffer; // Hold a reference, prevent memory from gc.
    private long bufferAddr;
    private final int packCount;
    private final boolean isFloat; // Floating-point numbers or not.

    public RSIndex_Histogram(int packCount, boolean isFloat) {
        this(ByteSlice.allocateDirect(packCount << PACK_HIST_SIZE_BYTE_SHIFT), packCount, isFloat);
    }

    public RSIndex_Histogram(ByteSlice buffer, int packCount, boolean isFloat) {
        this.histBuffer = buffer;
        this.bufferAddr = histBuffer.address();
        this.packCount = packCount;
        this.isFloat = isFloat;
    }

    @Override
    public long size() {
        return histBuffer.size();
    }

    private static boolean exactMode(long packMin, long packMax) {
        return (packMax - packMin) <= PACK_HIST_SIZE_BIT - 2;
    }

    private static boolean intervalTooLarge(long min, long max) {
        // about 2^62
        return min < -4611686018427387900L && max > 4611686018427387900L;
    }

    @Override
    public byte isValue(int packId, long minVal, long maxVal, long packMin, long packMax) {
        assert packId >= 0 && packId < packCount;
        int packOffset = packId << PACK_HIST_SIZE_BYTE_SHIFT;
        //System.out.println("\npackId: " + packId);
        return doCheckValue(bufferAddr + packOffset, minVal, maxVal, packMin, packMax, isFloat);
    }

    public void putValue(int packId, long value, long packMin, long packMax) {
        assert packId >= 0 && packId < packCount;
        int packOffset = packId << PACK_HIST_SIZE_BYTE_SHIFT;
        doPutValue(bufferAddr + packOffset, value, packMin, packMax, isFloat);
    }

    private static byte doCheckValue(long packAddr,
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
            double intervalLen = (dpackMax - dpackMin) / (double) PACK_HIST_SIZE_BIT;
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
                double intervalLen = (packMax - packMin) / (double) PACK_HIST_SIZE_BIT;
                minBit = (int) ((minVal - packMin - 1) / intervalLen);
                maxBit = (int) ((maxVal - packMin - 1) / intervalLen);
            }
        }
        assert minBit >= 0;
        assert maxBit >= 0;
        if (maxBit >= PACK_HIST_SIZE_BIT) {
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
            if (((histIntValue >>> (bit & INT_BIT_MASK)) & 1) != 0) {
                return RSValue.Some;
            }
        }
        return RSValue.None;
    }

    private static void doPutValue(long packAddr,
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
            double intervalLen = (dpackMax - dpackMin) / (double) PACK_HIST_SIZE_BIT;
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
                double intervalLen = (packMax - packMin) / (double) PACK_HIST_SIZE_BIT;
                bit = (int) ((value - packMin - 1) / intervalLen);
            }
        }
        if (bit > PACK_HIST_SIZE_BIT) {
            return;
        }
        assert bit >= 0;
        // An int contains 4 bytes, i.e. 32 bits.
        // index = bit / 32 * 4 + packOffset
        long addr = packAddr + ((bit >>> 5) << 2);
        int oldVal = MemoryUtil.getInt(addr);
        MemoryUtil.setInt(addr, oldVal | (1 << (bit & INT_BIT_MASK)));
    }

    @Override
    public PackRSIndex packIndex(int packId) {
        assert packId >= 0 && packId < packCount;
        int packOffset = packId << PACK_HIST_SIZE_BYTE_SHIFT;
        return new PackIndex(bufferAddr + packOffset, isFloat);
    }

    @Override
    public void free() {
        histBuffer.free();
        histBuffer = null;
        bufferAddr = 0;
    }

    @Override
    public void write(ByteBufferWriter writer) throws IOException {
        writer.write(histBuffer.toByteBuffer(), histBuffer.size());
    }

    /**
     * Note that this instance could be reused.
     */
    public final static class PackIndex implements PackRSIndexNum {

        private ByteSlice histBuffer;
        private long bufferAddr;
        private final boolean isFloat;

        public PackIndex(boolean isFloat) {
            this.histBuffer = ByteSlice.allocateDirect(1 << PACK_HIST_SIZE_BYTE_SHIFT);
            this.bufferAddr = histBuffer.address();
            this.isFloat = isFloat;
            clear();
        }

        /**
         * A index only generated from RSIndex_Histogram.
         */
        private PackIndex(long bufferAddr, boolean isFloat) {
            ByteBuffer bb = MemoryUtil.getHollowDirectByteBuffer();
            // Without cleaner.
            MemoryUtil.setByteBuffer(bb, bufferAddr, 1 << PACK_HIST_SIZE_BYTE_SHIFT, null);

            this.histBuffer = ByteSlice.wrap(bb);
            this.bufferAddr = bufferAddr;
            this.isFloat = isFloat;
        }

        @Override
        public int serializedSize() {
            return histBuffer.size();
        }

        @Override
        public void write(ByteBufferWriter writer) throws IOException {
            writer.write(histBuffer.toByteBuffer(), histBuffer.size());
        }

        @Override
        public void clear() {
            histBuffer.clear();
        }

        @Override
        public void free() {
            histBuffer.free();
            histBuffer = null;
            bufferAddr = 0;
        }

        @Override
        public void putValue(long value, long packMin, long packMax) {
            doPutValue(bufferAddr, value, packMin, packMax, isFloat);
        }

        @Override
        public byte isValue(long minVal, long maxVal, long packMin, long packMax) {
            return doCheckValue(bufferAddr, minVal, maxVal, packMin, packMax, isFloat);
        }
    }
}
