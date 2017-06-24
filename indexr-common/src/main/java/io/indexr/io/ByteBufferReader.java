package io.indexr.io;

import com.google.common.base.Preconditions;

import org.apache.hadoop.fs.FileStatus;
import org.apache.spark.unsafe.Platform;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import io.indexr.util.IOUtil;

public interface ByteBufferReader extends Closeable {
    /**
     * Read exactly <i>size</i>s bytes from this ByteBufferReader from <i>position</i> into <i>dst</i>.
     *
     * Whether this function may or may not change the position of datasource is implementation specific.
     */
    void read(long position, ByteBuffer dst) throws IOException;

    default void read(long position, byte[] buffer, int offset, int length) throws IOException {
        ByteBuffer byteBuffer = ByteBuffer.wrap(buffer);
        byteBuffer.position(offset);
        byteBuffer.limit(offset + length);
        read(position, byteBuffer);
    }

    // Those method sshould only used in test/assert senario.

    default int readInt(long position) throws IOException {
        byte[] valBuffer = new byte[4];
        read(position, valBuffer, 0, 4);
        return Platform.getInt(valBuffer, Platform.BYTE_ARRAY_OFFSET);
    }

    default long readLong(long position) throws IOException {
        byte[] valBuffer = new byte[8];
        read(position, valBuffer, 0, 8);
        return Platform.getLong(valBuffer, Platform.BYTE_ARRAY_OFFSET);
    }

    default boolean exists(long position) throws IOException {
        return true;
    }

    @Override
    default void close() throws IOException {}

    default void setName(String name) {}

    @FunctionalInterface
    public static interface Opener {
        ByteBufferReader open(long readBase) throws IOException;

        /**
         * Only open when read methods are called.
         */
        default ByteBufferReader openOnRead(long readBase) throws IOException {
            return new OpenOnReadBBR(this, readBase);
        }

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

        //static Opener create(FSDataInputStream input, long size) {
        //    return b -> ByteBufferReader.of(input, size, b, null);
        //}

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

        static Opener create(org.apache.hadoop.fs.FileSystem fileSystem,
                             org.apache.hadoop.fs.Path path) throws IOException {
            FileStatus status = fileSystem.getFileStatus(path);
            Preconditions.checkState(status != null, "File on %s not exists", path.toString());
            Preconditions.checkState(status.isFile(), "%s should be a file", path.toString());

            long size = status.getLen();
            int blockCount = fileSystem.getFileBlockLocations(status, 0, size).length;
            return b -> {
                return DFSByteBufferReader.open(fileSystem, path, size, blockCount, b);
            };
        }

        static Opener create(org.apache.hadoop.fs.FileSystem fileSystem,
                             org.apache.hadoop.fs.Path path,
                             long size,
                             int blockCount) throws IOException {
            return b -> {
                return DFSByteBufferReader.open(fileSystem, path, size, blockCount, b);
            };
        }
    }

    public static ByteBufferReader of(ByteBufferReader reader, long readBase, Closeable close) throws IOException {
        return new ByteBufferReader() {
            @Override
            public void read(long position, ByteBuffer dst) throws IOException {
                reader.read(readBase + position, dst);
            }

            @Override
            public void read(long position, byte[] buffer, int offset, int length) throws IOException {
                reader.read(readBase + position, buffer, offset, length);
            }

            @Override
            public boolean exists(long position) throws IOException {
                return reader.exists(readBase + position);
            }

            @Override
            public void close() throws IOException {
                if (close != null) {
                    close.close();
                }
            }
        };
    }

    public static ByteBufferReader of(FileChannel file, long readBase, Closeable close) throws IOException {
        return new ByteBufferReader() {
            String name;

            @Override
            public void read(long position, ByteBuffer dst) throws IOException {
                try {
                    IOUtil.readFully(file, readBase + position, dst);
                } catch (Exception e) {
                    throw new IOException(String.format("name: %s", name), e);
                }
            }

            @Override
            public boolean exists(long position) throws IOException {
                return position >= 0 && readBase + position < file.size();
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

    public static ByteBufferReader of(ByteBuffer byteBuffer, int readBase, Closeable close) throws IOException {
        return new ByteBufferReader() {
            @Override
            public void read(long position, ByteBuffer dst) throws IOException {
                IOUtil.readFully(byteBuffer, (int) (readBase + position), dst, dst.remaining());
            }

            @Override
            public void read(long position, byte[] buffer, int offset, int length) {
                byteBuffer.position((int) position);
                byteBuffer.get(buffer, offset, length);
            }

            @Override
            public boolean exists(long position) throws IOException {
                return position >= 0 && readBase + position < byteBuffer.limit();
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
