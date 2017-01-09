package io.indexr.io;

import com.google.common.base.Preconditions;

import java.io.IOException;
import java.io.InputStream;

/**
 * A stream of bits that can be read.
 * 
 * <p>Because they come from an underlying byte stream, the total number of bits is always a
 * multiple of 8. The bits are read in big endian.
 */
public class BitWrappedInputStream implements BitInputStream {
    // Underlying byte stream to read from.
    private InputStream input;
    // Either in the range 0x00 to 0xFF if bits are available,
    // or is -1 if the end of stream is reached.
    private int nextBits;
    // Always between 0 and 7, inclusive.
    private int numBitsRemaining;
    private boolean isEndOfStream;

    public BitWrappedInputStream(InputStream in) {
        Preconditions.checkArgument(in != null);
        input = in;
        numBitsRemaining = 0;
        isEndOfStream = false;
    }

    @Override
    public int read() throws IOException {
        if (isEndOfStream)
            return -1;
        if (numBitsRemaining == 0) {
            nextBits = input.read();
            if (nextBits == -1) {
                isEndOfStream = true;
                return -1;
            }
            numBitsRemaining = 8;
        }
        numBitsRemaining--;
        return (nextBits >>> numBitsRemaining) & 1;
    }
}
