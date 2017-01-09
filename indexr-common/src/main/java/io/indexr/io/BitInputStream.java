package io.indexr.io;

import java.io.IOException;

public interface BitInputStream {
    /**
     * Reads a bit from the stream.
     *
     * <p>Returns 0 or 1 if a bit is available, or -1 if the end of stream is reached. The end of
     * stream always occurs on a byte boundary.
     */
    public int read() throws IOException;

}
