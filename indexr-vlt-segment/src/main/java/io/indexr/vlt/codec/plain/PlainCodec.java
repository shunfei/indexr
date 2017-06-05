package io.indexr.vlt.codec.plain;

import io.indexr.vlt.codec.Codec;
import io.indexr.vlt.codec.CodecType;
import io.indexr.vlt.codec.UnsafeUtil;

import io.indexr.util.Wrapper;

public class PlainCodec implements Codec {
    public static final CodecType TYPE = CodecType.PLAIN;

    @Override
    public CodecType type() {
        return TYPE;
    }

    @Override
    public int encodeInts(int valCount, long inputAddr, long outputAddr, int outputLimit, Wrapper extraInfo) {
        UnsafeUtil.copyMemory(inputAddr, outputAddr, valCount << 2);
        return valCount << 2;
    }

    @Override
    public int encodeLongs(int valCount, long inputAddr, long outputAddr, int outputLimit, Wrapper extraInfo) {
        UnsafeUtil.copyMemory(inputAddr, outputAddr, valCount << 3);
        return valCount << 3;
    }

    @Override
    public int encodeFloats(int valCount, long inputAddr, long outputAddr, int outputLimit, Wrapper extraInfo) {
        UnsafeUtil.copyMemory(inputAddr, outputAddr, valCount << 2);
        return valCount << 2;
    }

    @Override
    public int encodeDoubles(int valCount, long inputAddr, long outputAddr, int outputLimit, Wrapper extraInfo) {
        UnsafeUtil.copyMemory(inputAddr, outputAddr, valCount << 3);
        return valCount << 3;
    }

    @Override
    public int encodeStrings(int valCount, long inputAddr, long outputAddr, int outputLimit, Wrapper extraInfo) {
        int totalLen = ((valCount + 1) << 2) + (UnsafeUtil.getInt(inputAddr + (valCount << 2)));
        UnsafeUtil.copyMemory(inputAddr, outputAddr, totalLen);
        return totalLen;
    }

    @Override
    public int decodeInts(int valCount, long inputAddr, long outputAddr, int outputLimit) {
        UnsafeUtil.copyMemory(inputAddr, outputAddr, valCount << 2);
        return valCount << 2;
    }

    @Override
    public int decodeLongs(int valCount, long inputAddr, long outputAddr, int outputLimit) {
        UnsafeUtil.copyMemory(inputAddr, outputAddr, valCount << 3);
        return valCount << 3;
    }

    @Override
    public int decodeFloats(int valCount, long inputAddr, long outputAddr, int outputLimit) {
        UnsafeUtil.copyMemory(inputAddr, outputAddr, valCount << 2);
        return valCount << 2;
    }

    @Override
    public int decodeDoubles(int valCount, long inputAddr, long outputAddr, int outputLimit) {
        UnsafeUtil.copyMemory(inputAddr, outputAddr, valCount << 3);
        return valCount << 3;
    }

    @Override
    public int decodeStrings(int valCount, long inputAddr, long outputAddr, int outputLimit) {
        int totalLen = ((valCount + 1) << 2) + (UnsafeUtil.getInt(inputAddr + (valCount << 2)));
        UnsafeUtil.copyMemory(inputAddr, outputAddr, totalLen);
        return totalLen;
    }
}
