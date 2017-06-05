package io.indexr.vlt.codec;

import io.indexr.util.Wrapper;

public interface EncoderItf {
    int encode(int valCount, long inputAddr, long outputAddr, int outputLimit, Wrapper extraInfo);
}
