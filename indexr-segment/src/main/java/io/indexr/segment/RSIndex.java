package io.indexr.segment;

import java.io.IOException;

import io.indexr.data.Freeable;
import io.indexr.data.Sizable;
import io.indexr.io.ByteBufferWriter;

/**
 * Rough set index.
 */
public interface RSIndex extends Freeable, Sizable {

    default PackRSIndex packIndex(int packId) {
        throw new UnsupportedOperationException();
    }

    default void write(ByteBufferWriter writer) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    default void free() {}

    @Override
    default long size() {return 0;}
}
