package io.indexr.io;

import com.google.common.base.Preconditions;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import io.indexr.util.ByteBufferUtil;
import io.indexr.util.IOUtil;

public interface ByteBufferReader extends Closeable {
    /**
     * Read exactly <i>size</i>s bytes from this ByteBufferReader from <i>offset</i> position into <i>dst</i>.
     * 
     * Whether this function may or may not change the position of datasource is implementation specific.
     */
    void read(long offset, ByteBuffer dst, int size) throws IOException;

    /**
     * Read exactly <i>size</i>s bytes.
     * Normally used to fetch small info data. Slower than {@link #read(long, ByteBuffer, int)} is expected.
     */
    default byte[] read(long offset, int size) throws IOException {
        ByteBuffer buffer = ByteBufferUtil.allocateHeap(size);
        read(offset, buffer, size);
        return buffer.array();
    }

    default boolean exists(long offset) throws IOException {
        return true;
    }

    @Override
    default void close() throws IOException {}

    default void setName(String name) {}

    @FunctionalInterface
    public static interface Opener {
        ByteBufferReader open(long readBase) throws IOException;

        /**
         * Wrap a ByteBufferReader into an {@link Opener}.
         * Note that the close will <b>not</b> passed to the opener,
         */
        static Opener create(ByteBufferReader reader) {
            // Usually the user can open many times,
            // and most of the time we don't want the reader realy be closed,
            // so lets set it null.
            return b -> ByteBufferReader.of(reader, b, null);
        }

        static Opener create(FSDataInputStream input, long size) {
            return b -> ByteBufferReader.of(input, size, b, null);
        }

        static Opener create(FileChannel file) {
            return b -> ByteBufferReader.of(file, b, null);
        }

        static Opener create(ByteBuffer buffer) {
            return b -> ByteBufferReader.of(buffer, (int) b, null);
        }

        static Opener create(Path path) {
            return b -> {
                FileChannel fileChannel = FileChannel.open(path, StandardOpenOption.READ);
                ByteBufferReader bbr = ByteBufferReader.of(fileChannel, b, fileChannel::close);
                bbr.setName(path.toString());
                return bbr;
            };
        }

        static Opener create(org.apache.hadoop.fs.FileSystem fileSystem, org.apache.hadoop.fs.Path path) {
            return b -> {
                FileStatus status = fileSystem.getFileStatus(path);
                Preconditions.checkState(status != null, "File on %s not exists", path.toString());
                Preconditions.checkState(status.isFile(), "%s should be a file", path.toString());

                FSDataInputStream stream = fileSystem.open(path);
                ByteBufferReader bbr = ByteBufferReader.of(stream, status.getLen(), b, stream);
                bbr.setName(path.toString());
                return bbr;
            };
        }

        static Opener create(org.apache.hadoop.fs.FileSystem fileSystem, org.apache.hadoop.fs.Path path, long size) {
            return b -> {
                FSDataInputStream stream = fileSystem.open(path);
                ByteBufferReader bbr = ByteBufferReader.of(stream, size, b, stream);
                bbr.setName(path.toString());
                return bbr;
            };
        }
    }

    public static ByteBufferReader of(ByteBufferReader reader, long readBase, Closeable close) throws IOException {
        return new ByteBufferReader() {
            @Override
            public void read(long offset, ByteBuffer dst, int size) throws IOException {
                reader.read(readBase + offset, dst, size);
            }

            @Override
            public boolean exists(long offset) throws IOException {
                return reader.exists(readBase + offset);
            }

            @Override
            public void close() throws IOException {
                if (close != null) {
                    close.close();
                }
            }
        };
    }

    /**
     * Create a ByteBufferReader by {@link FSDataInputStream}.
     * Unfortunately, the reader returned is not multi-thread safe because {@link FSDataInputStream} doesn't provide
     * appropriate api. And the pos will move to last read position.
     */
    public static ByteBufferReader of(FSDataInputStream input, long fileSize, long readBase, Closeable close) throws IOException {
        return new ByteBufferReader() {
            String name;

            @Override
            public void read(long offset, ByteBuffer dst, int size) throws IOException {
                try {
                    IOUtil.readFully(input, readBase + offset, dst, size);
                } catch (Exception e) {
                    throw new IOException(String.format("name: %s", name), e);
                }
            }

            @Override
            public byte[] read(long offset, int size) throws IOException {
                try {
                    byte[] bytes = new byte[size];
                    input.readFully(offset, bytes);
                    return bytes;
                } catch (Exception e) {
                    throw new IOException(String.format("name: %s", name), e);
                }
            }

            @Override
            public boolean exists(long offset) throws IOException {
                return offset >= 0 && readBase + offset < fileSize;
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

    public static ByteBufferReader of(FileChannel file, long readBase, Closeable close) throws IOException {
        return new ByteBufferReader() {
            String name;

            @Override
            public void read(long offset, ByteBuffer dst, int size) throws IOException {
                try {
                    IOUtil.readFully(file, readBase + offset, dst, size);
                } catch (Exception e) {
                    throw new IOException(String.format("name: %s", name), e);
                }
            }

            @Override
            public boolean exists(long offset) throws IOException {
                return offset >= 0 && readBase + offset < file.size();
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

    public static ByteBufferReader of(ByteBuffer buffer, int readBase, Closeable close) throws IOException {
        return new ByteBufferReader() {
            @Override
            public void read(long offset, ByteBuffer dst, int size) throws IOException {
                IOUtil.readFully(buffer, (int) (readBase + offset), dst, size);
            }

            @Override
            public boolean exists(long offset) throws IOException {
                return offset >= 0 && readBase + offset < buffer.limit();
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
