package io.indexr.vlt.codec;

import io.indexr.util.Wrapper;

/**
 * Returns the encoded data size.
 */
public interface Encoder {
    default int encodeInts(int valCount, long inputAddr, long outputAddr, int outputLimit) {
        return encodeInts(valCount, inputAddr, outputAddr, outputLimit, new Wrapper());
    }

    default int encodeInts(int valCount, long inputAddr, long outputAddr, int outputLimit, Wrapper extraInfo) {
        throw new UnsupportedOperationException();
    }

    default int encodeLongs(int valCount, long inputAddr, long outputAddr, int outputLimit) {
        return encodeLongs(valCount, inputAddr, outputAddr, outputLimit, new Wrapper());
    }

    default int encodeLongs(int valCount, long inputAddr, long outputAddr, int outputLimit, Wrapper extraInfo) {
        throw new UnsupportedOperationException();
    }

    default int encodeFloats(int valCount, long inputAddr, long outputAddr, int outputLimit) {
        return encodeFloats(valCount, inputAddr, outputAddr, outputLimit, new Wrapper());
    }

    default int encodeFloats(int valCount, long inputAddr, long outputAddr, int outputLimit, Wrapper extraInfo) {
        throw new UnsupportedOperationException();
    }

    default int encodeDoubles(int valCount, long inputAddr, long outputAddr, int outputLimit) {
        return encodeDoubles(valCount, inputAddr, outputAddr, outputLimit, new Wrapper());
    }

    default int encodeDoubles(int valCount, long inputAddr, long outputAddr, int outputLimit, Wrapper extraInfo) {
        throw new UnsupportedOperationException();
    }

    default int encodeStrings(int valCount, long inputAddr, long outputAddr, int outputLimit) {
        return encodeStrings(valCount, inputAddr, outputAddr, outputLimit, new Wrapper());
    }

    default int encodeStrings(int valCount, long inputAddr, long outputAddr, int outputLimit, Wrapper extraInfo) {
        throw new UnsupportedOperationException();
    }

}
