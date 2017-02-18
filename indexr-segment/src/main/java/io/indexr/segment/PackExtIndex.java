package io.indexr.segment;

import java.io.IOException;

import io.indexr.data.Freeable;
import io.indexr.io.ByteBufferWriter;
import io.indexr.segment.pack.DataPackNode;

/**
 * Extend index for one data pack.
 */
public interface PackExtIndex extends Freeable {
    int serializedSize();

    default void compress(DataPackNode dpn) {}

    default void decompress(DataPackNode dpn) {}

    default void write(ByteBufferWriter writer) throws IOException {}

    @Override
    default void free() {}
}
