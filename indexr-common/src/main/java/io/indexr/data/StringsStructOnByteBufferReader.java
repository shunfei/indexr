package io.indexr.data;

import org.apache.spark.unsafe.Platform;

import java.io.IOException;
import java.nio.channels.FileChannel;

import io.indexr.io.ByteBufferReader;

/**
 * The ByteBuffer here is normally comes from {@link java.nio.channels.FileChannel#map(FileChannel.MapMode, long, long)}.
 * If it is a pure memory byte reader, then you probably should use {@link StringsStruct}.
 */
public class StringsStructOnByteBufferReader {
    private final int count;
    private final ByteBufferReader reader;

    private byte[] intValBuffer = new byte[4];

    public StringsStructOnByteBufferReader(int count, ByteBufferReader buffer) {
        this.count = count;
        this.reader = buffer;
    }

    public ByteBufferReader reader() {
        return reader;
    }

    public int valCount() {
        return count;
    }

    public int totalLen() throws IOException {
        return totalIndexLen() + totalStringLen();
    }

    public int totalIndexLen() {
        return (count + 1) << 2;
    }

    public int totalStringLen() throws IOException {
        return readInt(count << 2);
    }

    private int readInt(int pos) throws IOException {
        reader.read(pos, intValBuffer, 0, 4);
        return Platform.getInt(intValBuffer, Platform.BYTE_ARRAY_OFFSET);
    }

    public int getString(int index, byte[] container) throws IOException {
        int totalIndexLen = (count + 1) << 2;
        int strOffset = readInt(index << 2);
        int nextOffset = readInt((index + 1) << 2);
        reader.read(totalIndexLen + strOffset, container, 0, nextOffset - strOffset);
        //reader.position(totalIndexLen + strOffset);
        //reader.get(container, 0, nextOffset - strOffset);
        return nextOffset - strOffset;
    }
}
