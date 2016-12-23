package io.indexr.util;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;

public class OffsetFileChannel implements WritableByteChannel {
    private final FileChannel fileChannel;
    private long offset;

    public OffsetFileChannel(FileChannel fileChannel, long offset) {
        this.fileChannel = fileChannel;
        this.offset = offset;
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        int size = src.remaining();
        IOUtil.writeFully(fileChannel, offset, src, size);
        return size;
    }

    @Override
    public boolean isOpen() {
        return fileChannel.isOpen();
    }

    @Override
    public void close() throws IOException {
        fileChannel.close();
    }
}
