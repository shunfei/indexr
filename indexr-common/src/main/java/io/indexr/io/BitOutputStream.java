package io.indexr.io;

import java.io.IOException;

public interface BitOutputStream {
    /**
     * Writes a bit to the stream.
     *
     * <p>Only the last bit counted, i.e. <code>b</code> should be 0 or 1.
     */
    void write(int b) throws IOException;

    /**
     * Call this method to indicate an end of writting to this stream.
     *
     * <p>Note that calling this method should <b>not</b> close the underlying OutputStream!
     */
    default void seal() throws IOException {
    }
}
