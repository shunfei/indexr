package io.indexr.util;

import org.apache.commons.io.IOUtils;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * Load data from file which stored in format: [size][data][size][data]....
 */
public class ObjectLoader implements Closeable {
    private FileChannel file;
    private long fileSize;
    private ByteBuffer buffer;

    private long fileOffset = 0;
    private int nextEntrySize = -1;
    private boolean done = false;

    public ObjectLoader(Path path) throws IOException {
        this.file = FileChannel.open(path, StandardOpenOption.READ);
        this.fileSize = file.size();
        this.buffer = ByteBufferUtil.allocateDirect(4 << 20); // 4MB.
        buffer.flip(); // Make it empty.
    }

    public boolean hasNext() throws IOException {
        if (nextEntrySize >= 0) {
            return true;
        }
        if (done || fileOffset >= fileSize) {
            return false;
        }
        int entrySize;
        if (buffer.remaining() <= 4) {
            buffer.clear();
            int toRead = (int) Math.min(fileSize - fileOffset, buffer.capacity());
            if (toRead < 4) {
                // File corruption.
                done = true;
                return false;
            }
            buffer.limit(toRead);
            IOUtil.readFully(file, fileOffset, buffer);
            buffer.flip();
        }
        entrySize = buffer.getInt();
        if (entrySize > buffer.remaining()) {
            if (entrySize + 4 > buffer.capacity()) {
                // buffer too small.
                ByteBufferUtil.free(buffer);
                buffer = ByteBufferUtil.allocateDirect(entrySize << 1);
            } else {
                buffer.clear();
            }
            int toRead = (int) Math.min(fileSize - fileOffset, buffer.capacity());
            if (toRead < entrySize + 4) {
                // File corruption.
                done = true;
                return false;
            }
            buffer.limit(toRead);
            IOUtil.readFully(file, fileOffset, buffer);
            buffer.flip();

            // move 4 bytes forward.
            int es = buffer.getInt();
            assert entrySize == es;
        }

        fileOffset += 4 + entrySize;
        nextEntrySize = entrySize;
        return true;
    }

    public void next(EntryListener listener) throws IOException {
        if (!hasNext()) {
            throw new IllegalStateException("no more entry");
        }
        int size = nextEntrySize;
        nextEntrySize = -1;
        listener.onEntry(buffer, size);
    }

    @Override
    public void close() throws IOException {
        IOUtils.closeQuietly(file);
        ByteBufferUtil.free(buffer);
    }

    public static interface EntryListener {
        /**
         * The listener should take <code>size</code>s bytes from <code>buffer</code> and move the pos forward.
         */
        void onEntry(ByteBuffer buffer, int size);
    }
}
