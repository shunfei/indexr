package io.indexr.vlt.codec.delta;

import java.nio.ByteBuffer;

import io.indexr.data.OffheapBytes;
import io.indexr.data.StringsStruct;
import io.indexr.io.ByteSlice;
import io.indexr.util.MemoryUtil;
import io.indexr.util.Wrapper;
import io.indexr.vlt.codec.Codec;
import io.indexr.vlt.codec.CodecType;
import io.indexr.vlt.codec.ErrorCode;
import io.indexr.vlt.codec.VarInteger;
import io.indexr.vlt.codec.pack.IntPacker;
import io.indexr.vlt.codec.pack.LongPacker;
import io.indexr.vlt.codec.pack.PackingUtil;

import static io.indexr.vlt.codec.UnsafeUtil.INT_ARRAY_OFFSET;
import static io.indexr.vlt.codec.UnsafeUtil.LONG_ARRAY_OFFSET;
import static io.indexr.vlt.codec.UnsafeUtil.copyMemory;
import static io.indexr.vlt.codec.UnsafeUtil.getByte;
import static io.indexr.vlt.codec.UnsafeUtil.getInt;
import static io.indexr.vlt.codec.UnsafeUtil.getLong;
import static io.indexr.vlt.codec.UnsafeUtil.setInt;
import static io.indexr.vlt.codec.UnsafeUtil.setLong;


public class DeltaCodec implements Codec {
    public static final CodecType TYPE = CodecType.DELTA;

    private static final int BLOCK_SPLIT_SIZE = 128;
    private static final int MINI_BLOCK_SPLIT_SIZE = 32;

    @Override
    public CodecType type() {
        return TYPE;
    }

    @Override
    public int encodeInts(int valCount, long inputAddr, long outputAddr, int outputLimit, Wrapper extraInfo) {
        if (valCount == 0) return 0;
        // First 4 bytes store the size of encoded data.
        ByteBuffer outputBuffer = MemoryUtil.getByteBuffer(outputAddr + 4, outputLimit, false);

        int prevVal = getInt(inputAddr);
        VarInteger.writeZigZagVarInt(prevVal, outputBuffer);
        int[] deltaBuffer = new int[BLOCK_SPLIT_SIZE];
        for (int i = 0; i < valCount; i += BLOCK_SPLIT_SIZE) {
            int blockSize = Math.min(i + BLOCK_SPLIT_SIZE, valCount) - i;
            prevVal = encodeIntsBlock(prevVal, blockSize, inputAddr + (i << 2), outputBuffer, deltaBuffer);
        }

        setInt(outputAddr, outputBuffer.position());
        return 4 + outputBuffer.position();
    }

    private int encodeIntsBlock(int prevVal, int count, long inputAddr, ByteBuffer outputBuffer, int[] deltaBuffer) {
        int minDelta = Integer.MAX_VALUE;
        for (int i = 0; i < count; i++) {
            int v = getInt(inputAddr + (i << 2));
            int delta = v - prevVal;
            deltaBuffer[i] = delta;
            minDelta = Math.min(minDelta, delta);
            prevVal = v;
        }
        VarInteger.writeZigZagVarInt(minDelta, outputBuffer);

        for (int i = 0; i < count; i++) {
            // Sub the minDelta, try best to make deltas into positive integers.
            // Overflowing cases are rare, besides we can also handle.
            deltaBuffer[i] -= minDelta;
        }

        for (int i = 0; i < count; i += MINI_BLOCK_SPLIT_SIZE) {
            int to = Math.min(i + MINI_BLOCK_SPLIT_SIZE, count);
            encodeIntsMiniBlock(i, to, deltaBuffer, outputBuffer);
        }

        return prevVal;
    }

    private void encodeIntsMiniBlock(int from, int to, int[] deltaBuffer, ByteBuffer outputBuffer) {
        int mask = 0;
        for (int i = from; i < to; i++) {
            mask |= deltaBuffer[i];
        }
        int bitWidth = VarInteger.bitWidth(mask);
        outputBuffer.put((byte) bitWidth);

        IntPacker intPacker = PackingUtil.intPacker(bitWidth);
        int pos = outputBuffer.position();
        int packedSize = intPacker.packUnsafe(
                to - from,
                deltaBuffer, INT_ARRAY_OFFSET + (from << 2),
                null, MemoryUtil.getAddress(outputBuffer) + pos);
        outputBuffer.position(pos + packedSize);
    }

    @Override
    public int decodeInts(int valCount, long inputAddr, long outputAddr, int ouputLimit) {
        if (valCount == 0) return 0;
        int encodedLen = getInt(inputAddr);
        ByteBuffer inputBuffer = MemoryUtil.getByteBuffer(inputAddr + 4, encodedLen, false);
        int prevVal = VarInteger.readZigZagVarInt(inputBuffer);
        for (int i = 0; i < valCount; i += BLOCK_SPLIT_SIZE) {
            int blockSize = Math.min(i + BLOCK_SPLIT_SIZE, valCount) - i;
            prevVal = decodeIntsBlock(prevVal, blockSize, inputBuffer, outputAddr + (i << 2));
        }
        return 4 + inputBuffer.position();
    }

    private int decodeIntsBlock(int prevVal, int count, ByteBuffer inputBuffer, long outputAddr) {
        int minDelta = VarInteger.readZigZagVarInt(inputBuffer);
        for (int i = 0; i < count; i += MINI_BLOCK_SPLIT_SIZE) {
            int to = Math.min(i + MINI_BLOCK_SPLIT_SIZE, count);
            decodeIntsMiniBlock(prevVal, i, to, inputBuffer, outputAddr + (i << 2));
        }

        for (int i = 0; i < count; i++) {
            int delta = getInt(outputAddr + (i << 2));
            int v = delta + minDelta + prevVal;
            setInt(outputAddr + (i << 2), v);
            prevVal = v;
        }

        return prevVal;
    }

    private void decodeIntsMiniBlock(int prevVal, int from, int to, ByteBuffer inputBuffer, long outputAddr) {
        int bitWidth = (int) inputBuffer.get();
        IntPacker intPacker = PackingUtil.intPacker(bitWidth);

        int pos = inputBuffer.position();
        int count = to - from;
        int consumedSize = intPacker.unpackUnsafe(
                count,
                null, MemoryUtil.getAddress(inputBuffer) + pos,
                null, outputAddr);

        inputBuffer.position(pos + consumedSize);
    }

    @Override
    public int encodeLongs(int valCount, long inputAddr, long outputAddr, int outputLimit, Wrapper extraInfo) {
        if (valCount == 0) return 0;
        // First 4 bytes store the size of encoded data.
        ByteBuffer outputBuffer = MemoryUtil.getByteBuffer(outputAddr + 4, outputLimit, false);

        long prevVal = getLong(inputAddr);
        VarInteger.writeZigZagVarLong(prevVal, outputBuffer);
        long[] deltaBuffer = new long[BLOCK_SPLIT_SIZE];
        for (int i = 0; i < valCount; i += BLOCK_SPLIT_SIZE) {
            int blockSize = Math.min(i + BLOCK_SPLIT_SIZE, valCount) - i;
            prevVal = encodeLongsBlock(prevVal, blockSize, inputAddr + (i << 3), outputBuffer, deltaBuffer);
        }

        setInt(outputAddr, outputBuffer.position());
        return 4 + outputBuffer.position();
    }

    private long encodeLongsBlock(long prevVal, int count, long inputAddr, ByteBuffer outputBuffer, long[] deltaBuffer) {
        long minDelta = Long.MAX_VALUE;
        for (int i = 0; i < count; i++) {
            long v = getLong(inputAddr + (i << 3));
            long delta = v - prevVal;
            deltaBuffer[i] = delta;
            minDelta = Math.min(minDelta, delta);
            prevVal = v;
        }
        VarInteger.writeZigZagVarLong(minDelta, outputBuffer);

        for (int i = 0; i < count; i++) {
            // Sub the minDelta, try best to make deltas into positive integers.
            // Overflowing cases are rare, besides we can also handle.
            deltaBuffer[i] -= minDelta;
        }

        for (int i = 0; i < count; i += MINI_BLOCK_SPLIT_SIZE) {
            int to = Math.min(i + MINI_BLOCK_SPLIT_SIZE, count);
            encodeLongsMiniBlock(i, to, deltaBuffer, outputBuffer);
        }

        return prevVal;
    }

    private void encodeLongsMiniBlock(int from, int to, long[] deltaBuffer, ByteBuffer outputBuffer) {
        long mask = 0;
        for (int i = from; i < to; i++) {
            mask |= deltaBuffer[i];
        }
        int bitWidth = VarInteger.bitWidth(mask);
        outputBuffer.put((byte) bitWidth);

        LongPacker longPacker = PackingUtil.longPacker(bitWidth);
        int pos = outputBuffer.position();
        int packedSize = longPacker.packUnsafe(
                to - from,
                deltaBuffer, LONG_ARRAY_OFFSET + (from << 3),
                null, MemoryUtil.getAddress(outputBuffer) + pos);
        outputBuffer.position(pos + packedSize);
    }

    @Override
    public int decodeLongs(int valCount, long inputAddr, long outputAddr, int ouputLimit) {
        if (valCount == 0) return 0;
        int encodedLen = getInt(inputAddr);
        ByteBuffer inputBuffer = MemoryUtil.getByteBuffer(inputAddr + 4, encodedLen, false);
        long prevVal = VarInteger.readZigZagVarLong(inputBuffer);
        for (int i = 0; i < valCount; i += BLOCK_SPLIT_SIZE) {
            int blockSize = Math.min(i + BLOCK_SPLIT_SIZE, valCount) - i;
            prevVal = decodeLongsBlock(prevVal, blockSize, inputBuffer, outputAddr + (i << 3));
        }
        return 4 + inputBuffer.position();
    }

    private long decodeLongsBlock(long prevVal, int count, ByteBuffer inputBuffer, long outputAddr) {
        long minDelta = VarInteger.readZigZagVarLong(inputBuffer);
        for (int i = 0; i < count; i += MINI_BLOCK_SPLIT_SIZE) {
            int to = Math.min(i + MINI_BLOCK_SPLIT_SIZE, count);
            decodeLongsMiniBlock(prevVal, i, to, inputBuffer, outputAddr + (i << 3));
        }

        for (int i = 0; i < count; i++) {
            long delta = getLong(outputAddr + (i << 3));
            long v = delta + minDelta + prevVal;
            setLong(outputAddr + (i << 3), v);
            prevVal = v;
        }

        return prevVal;
    }

    private void decodeLongsMiniBlock(long prevVal, int from, int to, ByteBuffer inputBuffer, long outputAddr) {
        int bitWidth = (int) inputBuffer.get();
        LongPacker longPacker = PackingUtil.longPacker(bitWidth);

        int pos = inputBuffer.position();
        int count = to - from;
        int consumedSize = longPacker.unpackUnsafe(
                count,
                null, MemoryUtil.getAddress(inputBuffer) + pos,
                null, outputAddr);

        inputBuffer.position(pos + consumedSize);
    }

    @Override
    public int encodeFloats(int valCount, long inputAddr, long outputAddr, int outputLimit, Wrapper extraInfo) {
        return encodeInts(valCount, inputAddr, outputAddr, outputLimit);
    }

    @Override
    public int decodeFloats(int valCount, long inputAddr, long outputAddr, int outputLimit) {
        return decodeInts(valCount, inputAddr, outputAddr, outputLimit);
    }

    @Override
    public int encodeDoubles(int valCount, long inputAddr, long outputAddr, int outputLimit, Wrapper extraInfo) {
        return encodeLongs(valCount, inputAddr, outputAddr, outputLimit);
    }

    @Override
    public int decodeDoubles(int valCount, long inputAddr, long outputAddr, int ouputLimit) {
        return decodeLongs(valCount, inputAddr, outputAddr, ouputLimit);
    }

    @Override
    public int encodeStrings(int valCount, long inputAddr, long outputAddr, int outputLimit, Wrapper extraInfo) {
        if (valCount == 0) return 0;

        ByteSlice prefixLenBuffer = ByteSlice.allocateDirect(valCount << 2);
        ByteSlice deltaLenBuffer = ByteSlice.allocateDirect(valCount << 2);
        OffheapBytes[] deltaBuffer = new OffheapBytes[valCount];

        StringsStruct strings = new StringsStruct(valCount, inputAddr);
        OffheapBytes prevVal = new OffheapBytes();
        for (int i = 0; i < valCount; i++) {
            OffheapBytes val = strings.getString(i);
            int minLen = Math.min(prevVal.len(), val.len());
            int prefixLen = 0;
            while (prefixLen < minLen
                    && getByte(val.addr() + prefixLen) == getByte(prevVal.addr() + prefixLen)) {
                prefixLen++;
            }

            prevVal.set(val); // prevVal = val

            prefixLenBuffer.putInt(i << 2, prefixLen);

            int deltaLen = val.len() - prefixLen;
            deltaLenBuffer.putInt(i << 2, deltaLen);

            val.set(val.addr() + prefixLen, deltaLen); // [0, len) slice to [prefixLen, len)
            deltaBuffer[i] = val;
        }

        // First 4 bytes store the size of encoded data.
        long dataOutputAddr = outputAddr + 4;
        long dataOutputEnd = outputAddr + outputLimit;

        // Compress prefix lens.
        int prefixLenEncodedSize = encodeInts(
                valCount,
                prefixLenBuffer.address(),
                dataOutputAddr,
                (int) (dataOutputEnd - dataOutputAddr));
        if (prefixLenEncodedSize < 0) {
            prefixLenBuffer.free();
            deltaLenBuffer.free();
            return prefixLenEncodedSize;
        }

        // Compress delta lens.
        long deltaLenOutputAddr = dataOutputAddr + prefixLenEncodedSize;
        int deltaLenEncodedSize = encodeInts(
                valCount,
                deltaLenBuffer.address(),
                deltaLenOutputAddr,
                (int) (dataOutputEnd - deltaLenOutputAddr));
        if (deltaLenEncodedSize < 0) {
            prefixLenBuffer.free();
            deltaLenBuffer.free();
            return deltaLenEncodedSize;
        }

        // Store plain delta contents.
        long deltaContentOutputAddr = deltaLenOutputAddr + deltaLenEncodedSize;
        boolean overflow = false;
        int deltaTotalLen = 0;
        for (int i = 0; i < valCount; i++) {
            OffheapBytes delta = deltaBuffer[i];
            if (deltaContentOutputAddr + delta.len() > dataOutputEnd) {
                overflow = true;
                break;
            }
            copyMemory(delta.addr(), deltaContentOutputAddr, delta.len());
            deltaContentOutputAddr += delta.len();
            deltaTotalLen += delta.len();
        }

        prefixLenBuffer.free();
        deltaLenBuffer.free();

        if (overflow) {
            return ErrorCode.BUFFER_OVERFLOW;
        } else {
            int encodedDataSize = prefixLenEncodedSize + deltaLenEncodedSize + deltaTotalLen;
            assert deltaContentOutputAddr - dataOutputAddr == encodedDataSize;
            setInt(outputAddr, encodedDataSize);
            return 4 + encodedDataSize;
        }
    }

    @Override
    public int decodeStrings(int valCount, long inputAddr, long outputAddr, int ouputLimit) {
        if (valCount == 0) return 0;

        ByteSlice prefixLenBuffer = ByteSlice.allocateDirect(valCount << 2);
        ByteSlice deltaLenBuffer = ByteSlice.allocateDirect(valCount << 2);
        OffheapBytes[] deltaBuffer = new OffheapBytes[valCount];

        long inputDataAddr = inputAddr + 4;
        int prefixLenEncodedSize = decodeInts(valCount, inputDataAddr, prefixLenBuffer.address(), prefixLenBuffer.size());

        long deltaLenInputAddr = inputDataAddr + prefixLenEncodedSize;
        int deltaLenEncodedSize = decodeInts(valCount, deltaLenInputAddr, deltaLenBuffer.address(), deltaLenBuffer.size());

        OffheapBytes prevVal = new OffheapBytes();
        StringsStruct.Builder stringsBuilder = new StringsStruct.Builder(valCount, outputAddr);
        long deltaContentInputAddr = deltaLenInputAddr + deltaLenEncodedSize;
        int deltaTotalLen = 0;
        for (int i = 0; i < valCount; i++) {
            int prefixLen = prefixLenBuffer.getInt(i << 2);
            int deltaLen = deltaLenBuffer.getInt(i << 2);

            stringsBuilder.addString(
                    null, prevVal.addr(), prefixLen,
                    null, deltaContentInputAddr, deltaLen, prevVal);

            deltaTotalLen += deltaLen;
            deltaContentInputAddr += deltaLen;
        }
        stringsBuilder.build();

        assert deltaContentInputAddr - inputDataAddr == prefixLenEncodedSize + deltaLenEncodedSize + deltaTotalLen;

        return 4 + prefixLenEncodedSize + deltaLenEncodedSize + deltaTotalLen;
    }
}
