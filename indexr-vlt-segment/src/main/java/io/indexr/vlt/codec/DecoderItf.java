package io.indexr.vlt.codec;

public interface DecoderItf {
    int decode(int valCount, long inputAddr, long outputAddr, int outputLimit);
}
