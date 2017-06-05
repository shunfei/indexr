package io.indexr.vlt.codec.rle;

import com.google.common.base.Preconditions;

import io.indexr.vlt.codec.Codec;
import io.indexr.vlt.codec.CodecType;
import io.indexr.vlt.codec.ErrorCode;
import io.indexr.vlt.codec.VarInteger;
import io.indexr.vlt.codec.pack.IntPacker;
import io.indexr.vlt.codec.pack.PackingUtil;

import java.nio.ByteBuffer;

import io.indexr.util.MemoryUtil;
import io.indexr.util.Wrapper;

import static io.indexr.vlt.codec.UnsafeUtil.getInt;
import static io.indexr.vlt.codec.UnsafeUtil.setInt;

public class RLEPackingHybridCodec implements Codec {
    public static final CodecType TYPE = CodecType.RLE;

    private static final int RLE_THRESHOLD = 8;
    private final IntPacker intPacker;
    private final int byteWidth;

    public RLEPackingHybridCodec(int bitWidth) {
        this.intPacker = PackingUtil.intPacker(bitWidth);
        this.byteWidth = VarInteger.bitWidthToByteWidth(bitWidth);
    }

    @Override
    public CodecType type() {
        return TYPE;
    }

    @Override
    public int encodeInts(int valCount, long inputAddr, long outputAddr, int outputLimit, Wrapper extraInfo) {
        if (valCount == 0) return 0;
        // First 4 bytes store the size of encoded data.
        // Convenient for write bytes without managing pos :)
        ByteBuffer outputBuffer = MemoryUtil.getByteBuffer(outputAddr + 4, outputLimit, false);

        int curPackingChunkStart = 0;
        int curRepeatChunkStart = 0;
        int curRepeatVal = getInt(inputAddr);
        for (int i = 0; i < valCount; i++) {
            int v = getInt(inputAddr + (i << 2));
            assert VarInteger.bitWidth(v) <= intPacker.bitWidth();
            if (v < 0) {
                // Only support positive integers.
                return ErrorCode.DATA_IMPROPER;
            }
            if (v != curRepeatVal) {
                int repeat = i - curRepeatChunkStart;
                if (repeat >= RLE_THRESHOLD) {
                    packingEncode(curPackingChunkStart, curRepeatChunkStart, inputAddr, outputBuffer);
                    rleEncode(curRepeatVal, repeat, inputAddr, outputBuffer);

                    curPackingChunkStart = i;
                }

                curRepeatChunkStart = i;
                curRepeatVal = v;
            }
        }
        packingEncode(curPackingChunkStart, valCount, inputAddr, outputBuffer);

        setInt(outputAddr, outputBuffer.position());
        return 4 + outputBuffer.position();
    }

    // packing-header = (count << 1) | 1
    private void packingEncode(int fromIndex, int toIndex, long inputAddr, ByteBuffer outputBuffer) {
        if (fromIndex == toIndex) {
            return;
        }
        int count = toIndex - fromIndex;
        VarInteger.writeUnsignedVarInt((count << 1) | 1, outputBuffer);
        int pos = outputBuffer.position();
        int packedSize = intPacker.packUnsafe(
                count,
                null,
                inputAddr + (fromIndex << 2),
                null,
                MemoryUtil.getAddress(outputBuffer) + pos);
        outputBuffer.position(pos + packedSize);
    }

    // rle-header = repeat << 1
    private void rleEncode(int val, int repeat, long inputAddr, ByteBuffer outputBuffer) {
        VarInteger.writeUnsignedVarInt(repeat << 1, outputBuffer);
        VarInteger.writeFixedByteWidthInt(byteWidth, val, outputBuffer);
    }

    @Override
    public int decodeInts(int valCount, long inputAddr, long outputAddr, int ouputLimit) {
        if (valCount == 0) return 0;
        int encodedLen = getInt(inputAddr);
        ByteBuffer inputBuffer = MemoryUtil.getByteBuffer(inputAddr + 4, encodedLen, false);
        int valIndex = 0;
        while (inputBuffer.hasRemaining()) {
            int header = VarInteger.readUnsignedVarInt(inputBuffer);
            if ((header & 1) == 0) {
                // rle
                int repeat = header >>> 1;
                final int val = VarInteger.readFixedByteWidthInt(byteWidth, inputBuffer);
                for (int i = 0; i < repeat; i++) {
                    setInt(outputAddr + ((valIndex + i) << 2), val);
                }
                valIndex += repeat;
            } else {
                // packing
                int count = header >>> 1;
                int pos = inputBuffer.position();
                int consumedSize = intPacker.unpackUnsafe(
                        count,
                        null,
                        MemoryUtil.getAddress(inputBuffer) + pos,
                        null,
                        outputAddr + (valIndex << 2));
                inputBuffer.position(pos + consumedSize);
                valIndex += count;
            }
        }
        Preconditions.checkState(valIndex == valCount);
        return 4 + inputBuffer.position();
    }
}
