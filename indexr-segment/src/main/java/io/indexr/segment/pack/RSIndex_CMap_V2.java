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
import io.indexr.util.FastMath;
import io.indexr.util.MemoryUtil;

/**
 * Seperate the index by value size.
 * We have 8 index chunk here.
 *
 * 0, 1, 2, 4, 8, 16, 32, 64
 * total posistion: 64 * 2
 *
 * e.g. when comes a value with size 5, put it into "8" size chunk.
 */
class RSIndex_CMap_V2 implements RSIndexStr {
    private static final int POSITION_BYTE_SHIFT = 5; // 32. i.e. 256 / 8
    private static final int MAX_POSISTIONS = 64; // We only index the first max 64 chars here.
    private static final int TOTAL_POSISTIONS = 64 * 2;
    private static final byte ESCAPE_CHARACTOR = '\\';

    private static final int[] INDEX_POS_COUNT = new int[]{0, 1, 2, 4, 8, 16, 32, 64};
    private static final int[] INDEX_OFFSET = new int[MAX_POSISTIONS + 1];

    static {
        for (int size = 0; size <= MAX_POSISTIONS; size++) {
            INDEX_OFFSET[size] = size == 0 ? 0 : 1 << FastMath.ceilLog2(size);
        }
    }

    private static int indexOffsetBySize(int valueSize) {
        if (valueSize > MAX_POSISTIONS) {
            return INDEX_OFFSET[MAX_POSISTIONS];
        }
        return INDEX_OFFSET[valueSize];
    }

    private ByteSlice buffer;
    private long bufferAddr;
    private final int packCount;

    public RSIndex_CMap_V2(int packCount) {
        this(ByteSlice.allocateDirect(packCount * (TOTAL_POSISTIONS << POSITION_BYTE_SHIFT)), packCount);
    }

    public RSIndex_CMap_V2(ByteSlice buffer, int packCount) {
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
        return new CMapPackIndex();
    }

    public void putValue(int packId, UTF8String value) {
        assert packId >= 0 && packId < packCount;

        int valueSize = value.numBytes();
        Object valueBase = value.getBaseObject();
        long valueOffset = value.getBaseOffset();

        long packAddr = bufferAddr + (packId * (TOTAL_POSISTIONS << POSITION_BYTE_SHIFT));
        long offset = packAddr + indexOffsetBySize(valueSize);
        int checkSize = valueSize < MAX_POSISTIONS ? valueSize : MAX_POSISTIONS;

        if (checkSize == 0) {
            // mark empty string exists.
            set(offset, (byte) 1, 0);
        } else {
            for (int pos = 0; pos < checkSize; pos++) {
                set(offset, Platform.getByte(valueBase, valueOffset + pos), pos);
            }
        }
    }

    @Override
    public byte isValue(int packId, UTF8String value) {
        assert packId >= 0 && packId < packCount;

        int valueSize = value.numBytes();
        Object valueBase = value.getBaseObject();
        long valueOffset = value.getBaseOffset();

        long packAddr = bufferAddr + (packId * (TOTAL_POSISTIONS << POSITION_BYTE_SHIFT));
        long offset = packAddr + indexOffsetBySize(valueSize);
        int checkSize = valueSize < MAX_POSISTIONS ? valueSize : MAX_POSISTIONS;

        if (checkSize == 0) {
            if (!isSet(offset, (byte) 1, 0)) {
                return RSValue.None;
            }
        } else {
            for (int pos = 0; pos < checkSize; pos++) {
                if (!isSet(offset, Platform.getByte(valueBase, valueOffset + pos), pos)) {
                    return RSValue.None;
                }
            }
        }
        return RSValue.Some;
    }

    @Override
    public byte isLike(int packId, LikePattern pattern) {
        assert packId >= 0 && packId < packCount;

        // We can exclude cases like "ala%" and "a_l_a%"

        if (pattern.original.numBytes() == 0
                || (!pattern.hasAny && !pattern.hasOne)) {
            return isValue(packId, pattern.original);
        }

        long packAddr = bufferAddr + (packId * (TOTAL_POSISTIONS << POSITION_BYTE_SHIFT));
        int analyzedLen = pattern.analyzed.length;

        for (int posCount : INDEX_POS_COUNT) {
            if (posCount < pattern.minMatchLen && posCount != MAX_POSISTIONS) {
                continue;
            }

            long offset = packAddr + indexOffsetBySize(posCount);
            int checkSize = analyzedLen < posCount ? analyzedLen : posCount;
            boolean match = true;
            for (int pos = 0; pos < checkSize; pos++) {
                if (pattern.one[pos]) {
                    continue;
                }
                if (pattern.any[pos]) {
                    break;
                }
                byte c = pattern.analyzed[pos];
                assert c != 0;
                if (!isSet(offset, c, pos)) {
                    match = false;
                    break;
                }
            }
            if (match) {
                return RSValue.Some;
            }
        }
        return RSValue.None;
    }

    /**
     * @param charVal 0~255
     */
    private static void set(long offset, byte charVal, int pos) {
        int charUnsigned = charVal & 0xFF;
        // offset + (pos * 256 / 8) + charUnsigned / 32;
        long addr = offset + (pos << POSITION_BYTE_SHIFT) + (charUnsigned >> POSITION_BYTE_SHIFT);
        int oldVal = MemoryUtil.getInt(addr);
        MemoryUtil.setInt(addr, oldVal | (1 << (charUnsigned % 32)));
    }

    private static boolean isSet(long offset, byte charVal, int pos) {
        int charUnsigned = charVal & 0xFF;
        long addr = offset + (pos << POSITION_BYTE_SHIFT) + (charUnsigned >> POSITION_BYTE_SHIFT);
        return ((MemoryUtil.getInt(addr) >> (charUnsigned % 32)) & 1) == 1;
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

    public static class CMapPackIndex implements PackRSIndexStr {
        private ByteSlice buffer;
        private long bufferAddr;

        public CMapPackIndex() {
            this.buffer = ByteSlice.allocateDirect(TOTAL_POSISTIONS << POSITION_BYTE_SHIFT + 1);
            this.bufferAddr = buffer.address();
            clear();
        }

        private CMapPackIndex(long bufferAddr) {
            ByteBuffer bb = MemoryUtil.getHollowDirectByteBuffer();
            // Without cleaner.
            MemoryUtil.setByteBuffer(bb, bufferAddr, TOTAL_POSISTIONS << POSITION_BYTE_SHIFT, null);

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
            int valueSize = value.numBytes();
            Object valueBase = value.getBaseObject();
            long valueOffset = value.getBaseOffset();

            long offset = bufferAddr + indexOffsetBySize(valueSize);
            int checkSize = valueSize < MAX_POSISTIONS ? valueSize : MAX_POSISTIONS;

            if (checkSize == 0) {
                // mark empty string exits.
                set(offset, (byte) 1, 0);
            } else {
                for (int pos = 0; pos < checkSize; pos++) {
                    set(offset, Platform.getByte(valueBase, valueOffset + pos), pos);
                }
            }
        }

        @Override
        public byte isValue(UTF8String value) {
            int valueSize = value.numBytes();
            Object valueBase = value.getBaseObject();
            long valueOffset = value.getBaseOffset();

            long offset = bufferAddr + indexOffsetBySize(valueSize);
            int checkSize = valueSize < MAX_POSISTIONS ? valueSize : MAX_POSISTIONS;

            if (checkSize == 0) {
                if (!isSet(offset, (byte) 1, 0)) {
                    return RSValue.None;
                }
            } else {
                for (int pos = 0; pos < checkSize; pos++) {
                    if (!isSet(offset, Platform.getByte(valueBase, valueOffset + pos), pos)) {
                        return RSValue.None;
                    }
                }
            }
            return RSValue.Some;
        }

        @Override
        public byte isLike(LikePattern pattern) {
            if (pattern.original.numBytes() == 0
                    || (!pattern.hasAny && !pattern.hasOne)) {
                return isValue(pattern.original);
            }

            int analyzedLen = pattern.analyzed.length;

            for (int posCount : INDEX_POS_COUNT) {
                if (posCount < pattern.minMatchLen && posCount != MAX_POSISTIONS) {
                    continue;
                }

                long offset = bufferAddr + indexOffsetBySize(posCount);
                int checkSize = analyzedLen < posCount ? analyzedLen : posCount;
                boolean match = true;
                for (int pos = 0; pos < checkSize; pos++) {
                    if (pattern.one[pos]) {
                        continue;
                    }
                    if (pattern.any[pos]) {
                        break;
                    }
                    byte c = pattern.analyzed[pos];
                    assert c != 0;
                    if (!isSet(offset, c, pos)) {
                        match = false;
                        break;
                    }
                }
                if (match) {
                    return RSValue.Some;
                }
            }
            return RSValue.None;
        }

    }
}
