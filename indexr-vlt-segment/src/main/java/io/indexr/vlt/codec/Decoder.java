package io.indexr.vlt.codec;

// Returns how many bytes it consumed. Which should be equal to the return value of Encoder,
// otherwise, you caught a bug.
public interface Decoder {
    default int decodeInts(int valCount, long inputAddr, long outputAddr, int outputLimit) {
        throw new UnsupportedOperationException();
    }

    default int decodeLongs(int valCount, long inputAddr, long outputAddr, int outputLimit) {
        throw new UnsupportedOperationException();
    }

    default int decodeFloats(int valCount, long inputAddr, long outputAddr, int outputLimit) {
        throw new UnsupportedOperationException();
    }

    default int decodeDoubles(int valCount, long inputAddr, long outputAddr, int ouputLimit) {
        throw new UnsupportedOperationException();
    }

    default int decodeStrings(int valCount, long inputAddr, long outputAddr, int ouputLimit) {
        throw new UnsupportedOperationException();
    }
}
