package io.indexr.segment;

import java.io.IOException;

import io.indexr.data.Freeable;
import io.indexr.io.ByteBufferWriter;

/**
 * Index for one data pack.
 */
public interface PackRSIndex extends Freeable {

    int serializedSize();

    void write(ByteBufferWriter writer) throws IOException;

    void clear();
}
