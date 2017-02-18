package io.indexr.segment.pack;

import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.types.UTF8String;

import java.io.IOException;
import java.nio.ByteBuffer;

import io.indexr.data.LikePattern;
import io.indexr.io.ByteBufferWriter;
import io.indexr.io.ByteSlice;
import io.indexr.segment.PackRSIndex;
import io.indexr.segment.PackRSIndexStr;
import io.indexr.segment.RSIndexStr;
import io.indexr.segment.RSValue;
import io.indexr.util.MemoryUtil;

// This class is implemented after RSI_CMap.cpp from ICE-4.0.7.

class RSIndex_CMap implements RSIndexStr {
    private static final int POSITION_BYTE_SHIFT = 5; // 32. i.e. 256 / 8
    private static final int POSISTIONS = 64; // We only index the first 64 chars here.
    private static final byte ESCAPE_CHARACTOR = '\\';

    private ByteSlice buffer;
    private long bufferAddr;
    private final int packCount;

    public RSIndex_CMap(ByteSlice buffer, int packCount) {
        this.buffer = buffer;
        this.bufferAddr = buffer.address();
        this.packCount = packCount;
    }

    @Override
    public long size() {
        return buffer.size();
    }

    @Override
    public PackRSIndex packIndex(int packId) {
        long packAddr = bufferAddr + (packId * (POSISTIONS << POSITION_BYTE_SHIFT));
        return new PackIndex(packAddr);
    }

    public void putValue(int packId, UTF8String value) {
        assert packId >= 0 && packId < packCount;
        long packAddr = bufferAddr + (packId * (POSISTIONS << POSITION_BYTE_SHIFT));
        _putValue(packAddr, value);
    }

    @Override
    public byte isValue(int packId, UTF8String value) {
        assert packId >= 0 && packId < packCount;

        long packAddr = bufferAddr + (packId * (POSISTIONS << POSITION_BYTE_SHIFT));
        return _isValue(packAddr, value);
    }

    @Override
    public byte isLike(int packId, LikePattern pattern) {
        assert packId >= 0 && packId < packCount;
        long packAddr = bufferAddr + (packId * (POSISTIONS << POSITION_BYTE_SHIFT));

        return _isLike(packAddr, pattern);
    }

    private static void _putValue(long packAddr, UTF8String value) {
        int bytes = value.numBytes();
        Object valueBase = value.getBaseObject();
        long valueOffset = value.getBaseOffset();
        int indexSize = bytes < POSISTIONS ? bytes : POSISTIONS;

        for (int pos = 0; pos < indexSize; pos++) {
            set(packAddr, Platform.getByte(valueBase, valueOffset + pos), pos);
        }
    }

    private static byte _isValue(long packAddr, UTF8String value) {
        int bytes = value.numBytes();
        Object valueBase = value.getBaseObject();
        long valueOffset = value.getBaseOffset();
        int indexSize = bytes < POSISTIONS ? bytes : POSISTIONS;

        for (int pos = 0; pos < indexSize; pos++) {
            if (!isSet(packAddr, Platform.getByte(valueBase, valueOffset + pos), pos)) {
                return RSValue.None;
            }
        }
        return RSValue.Some;
    }

    public static byte _isLike(long packAddr, LikePattern pattern) {
        // We can exclude cases like "ala%" and "a_l_a%"

        UTF8String original = pattern.original;
        int bytes = original.numBytes();
        Object valueBase = original.getBaseObject();
        long valueOffset = original.getBaseOffset();
        int indexSize = bytes < POSISTIONS ? bytes : POSISTIONS;

        for (int pos = 0; pos < indexSize; pos++) {
            byte c = Platform.getByte(valueBase, valueOffset + pos);
            // The ESCAPE_CHARACTOR case can be optimized. But I'm too lazy...
            if (c == '%' || c == ESCAPE_CHARACTOR) {
                break;
            }
            if (c != '_' && !isSet(packAddr, c, pos)) {
                return RSValue.None;
            }
        }
        return RSValue.Some;
    }

    /**
     * @param charVal 0~255
     */
    private static void set(long packAddr, byte charVal, int pos) {
        int charUnsigned = charVal & 0xFF;
        // packAddr + (pos * 256 / 8) + charUnsigned / 32;
        long addr = packAddr + (pos << POSITION_BYTE_SHIFT) + (charUnsigned >>> POSITION_BYTE_SHIFT);
        int oldVal = MemoryUtil.getInt(addr);
        MemoryUtil.setInt(addr, oldVal | (1 << (charUnsigned % 32)));
    }

    private static boolean isSet(long packAddr, byte charVal, int pos) {
        int charUnsigned = charVal & 0xFF;
        long addr = packAddr + (pos << POSITION_BYTE_SHIFT) + (charUnsigned >>> POSITION_BYTE_SHIFT);
        return ((MemoryUtil.getInt(addr) >>> (charUnsigned % 32)) & 1) == 1;
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

    public static class PackIndex implements PackRSIndexStr {
        private ByteSlice buffer;
        private long bufferAddr;

        public PackIndex() {
            this.buffer = ByteSlice.allocateDirect(POSISTIONS << POSITION_BYTE_SHIFT);
            this.bufferAddr = buffer.address();
            clear();
        }

        /**
         * A index only generated from RSIndex_CMap.
         */
        private PackIndex(long bufferAddr) {
            ByteBuffer bb = MemoryUtil.getHollowDirectByteBuffer();
            // Without cleaner.
            MemoryUtil.setByteBuffer(bb, bufferAddr, POSISTIONS << POSITION_BYTE_SHIFT, null);

            this.buffer = ByteSlice.wrap(bb);
            this.bufferAddr = bufferAddr;
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
        public int serializedSize() {
            return buffer.size();
        }

        @Override
        public void write(ByteBufferWriter writer) throws IOException {
            writer.write(buffer.toByteBuffer(), buffer.size());
        }

        @Override
        public void putValue(UTF8String value) {
            _putValue(bufferAddr, value);
        }

        @Override
        public byte isValue(UTF8String value) {
            return _isValue(bufferAddr, value);
        }

        @Override
        public byte isLike(LikePattern pattern) {
            return _isLike(bufferAddr, pattern);
        }

    }
}
