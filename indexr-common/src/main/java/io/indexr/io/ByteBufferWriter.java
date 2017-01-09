package io.indexr.io;

import org.apache.hadoop.fs.FSDataOutputStream;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;

import io.indexr.util.IOUtil;

public interface ByteBufferWriter extends Closeable {
    /**
     * Write exactly <i>size</i>s bytes from <i>src</i> into this writer.
     * The position of the writer is implementation specific and cannot be decided by client.
     * 
     * Alfter return, position down there should move forward <i>size</i>s bytes.
     */
    void write(ByteBuffer src, int size) throws IOException;

    /**
     * Convenient method to write some bytes in byte array.
     * This method is not encouraged as it could make people lazy and hurt performance.
     */
    default void write(byte[] bytes) throws IOException {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        write(buffer, bytes.length);
    }

    default void flush() throws IOException {}

    @Override
    default void close() throws IOException {}

    default void setName(String name) {}

    @FunctionalInterface
    public static interface Opener {
        ByteBufferWriter open() throws IOException;
    }

    @FunctionalInterface
    public static interface PredictSizeOpener {
        /**
         * Open a writer with known incomming data size.
         */
        ByteBufferWriter open(long predictSize) throws IOException;
    }

    public static ByteBufferWriter of(ByteBufferWriter writer, Closeable close) throws IOException {
        return new ByteBufferWriter() {
            @Override
            public void write(ByteBuffer src, int size) throws IOException {
                writer.write(src, size);
            }

            @Override
            public void flush() throws IOException {
                writer.flush();
            }

            @Override
            public void close() throws IOException {
                if (close != null) {
                    close.close();
                }
            }
        };
    }

    public static ByteBufferWriter of(FSDataOutputStream ouput) throws IOException {
        return of(ouput, ouput);
    }

    public static ByteBufferWriter of(FSDataOutputStream ouput, Closeable close) throws IOException {
        return new ByteBufferWriter() {
            String name;

            @Override
            public void write(ByteBuffer src, int size) throws IOException {
                try {
                    IOUtil.writeFully(ouput, src, size);
                } catch (Exception e) {
                    throw new IOException(String.format("name: %s", name), e);
                }
            }

            @Override
            public void flush() throws IOException {
                ouput.hflush();
            }

            @Override
            public void close() throws IOException {
                if (close != null) {
                    close.close();
                }
            }

            @Override
            public void setName(String name) {
                this.name = name;
            }
        };
    }

    public static ByteBufferWriter of(FileChannel file, long offset) throws IOException {
        return of(file, offset, file);
    }

    public static ByteBufferWriter of(FileChannel file, long offset, Closeable close) throws IOException {
        return new ByteBufferWriter() {
            String name;
            long curOffset = offset;

            @Override
            public void write(ByteBuffer src, int size) throws IOException {
                try {
                    IOUtil.writeFully(file, curOffset, src, size);
                } catch (Exception e) {
                    throw new IOException(String.format("name: %s", name), e);
                }
                curOffset += size;
            }

            @Override
            public void flush() throws IOException {
                file.force(false);
            }

            @Override
            public void close() throws IOException {
                if (close != null) {
                    close.close();
                }
            }

            @Override
            public void setName(String name) {
                this.name = name;
            }
        };
    }

    public static ByteBufferWriter of(WritableByteChannel channel) throws IOException {
        return of(channel, channel);
    }

    public static ByteBufferWriter of(WritableByteChannel channel, Closeable close) throws IOException {
        return new ByteBufferWriter() {
            @Override
            public void write(ByteBuffer src, int size) throws IOException {
                IOUtil.writeFully(channel, src, size);
            }

            @Override
            public void close() throws IOException {
                if (close != null) {
                    close.close();
                }
            }
        };
    }
}
