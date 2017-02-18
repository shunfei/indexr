package io.indexr.util;

import com.google.common.base.Preconditions;

import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.apache.hadoop.fs.ByteBufferReadable;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

public class IOUtil {
    private static final Logger logger = LoggerFactory.getLogger(IOUtil.class);

    public static void closeQuietly(AutoCloseable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (Exception e) {
                // Ignore
            }
        }
    }

    public static void closeQuietly(Object closeable) {
        if (closeable == null || !(closeable instanceof AutoCloseable)) {
            return;
        }
        closeQuietly((AutoCloseable) closeable);
    }

    public static List<String> wildcardFiles(String path) {
        File file = new File(path);
        String dirName = file.getParent();
        if (dirName == null) {
            dirName = ".";
        }
        String fileName = file.getName();
        File dir = new File(dirName);
        FileFilter fileFilter = new WildcardFileFilter(fileName);
        File[] files = dir.listFiles((File f) -> {
            return f.isFile() && fileFilter.accept(f);
        });
        if (files == null) {
            throw new RuntimeException(String.format("[%s] is not a valid path", path));
        }
        List<String> acceptFiles = new ArrayList<>();
        for (File f : files) {
            acceptFiles.add(f.getAbsolutePath());
        }
        return acceptFiles;
    }

    /**
     * Slow function, should only used in test. Unless, you choose to optimize it :).
     * This function will not move the position of <i>from</i>.
     */
    public static void readFully(ByteBuffer from, int offset, ByteBuffer to, int size) {
        for (int i = 0; i < size; i++) {
            to.put(from.get(offset + i));
        }
    }

    public static void readFully(FSDataInputStream reader, ByteBuffer buffer, int size) throws IOException {
        readFully(reader, -1, buffer, size);
    }

    public static void readFully(FSDataInputStream reader, long offset, ByteBuffer buffer, int size) throws IOException {
        Preconditions.checkState(buffer.remaining() >= size,
                "buffer.remaining() >= size");
        if (offset >= 0) {
            reader.seek(offset);
        }

        InputStream is = reader.getWrappedStream();
        if (!(is instanceof ByteBufferReadable)) {
            logger.trace("Using read bytes method");
            byte[] bytes = new byte[size];
            reader.readFully(bytes);
            buffer.put(bytes);
        } else {
            int read = 0;
            while (read < size) {
                int rt = reader.read(buffer);
                if (rt < 0) {
                    throw new IOException("End of stream");
                }
                read += rt;
            }
        }
    }

    public static void readFully(FileChannel file, ByteBuffer buffer, int size) throws IOException {
        Preconditions.checkState(file.size() - file.position() >= size && buffer.remaining() >= size,
                "file.size() - file.position() >= size && buffer.remaining() >= size");

        readFully((ReadableByteChannel) file, buffer, size);
    }

    public static void readFullyWithRE(FileChannel file, ByteBuffer buffer, int size) {
        try {
            readFully(file, buffer, size);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void readFully(FileChannel file, long offset, ByteBuffer buffer, int size) throws IOException {
        Preconditions.checkState(file.size() - offset >= size && buffer.remaining() >= size,
                "file.size() - offset >= size && buffer.remaining() >= size");

        int read = 0;
        while (read < size) {
            int rt = file.read(buffer, offset + read);
            if (rt < 0) {
                throw new IOException("End of file");
            }
            read += rt;
        }
    }

    public static void readFullyWithRE(FileChannel file, long offset, ByteBuffer buffer, int size) {
        try {
            readFully(file, offset, buffer, size);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void readFully(ReadableByteChannel channel, ByteBuffer buffer, int size) throws IOException {
        Preconditions.checkState(buffer.remaining() >= size,
                "buffer.remaining() >= size");

        int read = 0;
        while (read < size) {
            int rt = channel.read(buffer);
            if (rt < 0) {
                throw new IOException("End of channel");
            }
            read += rt;
        }
    }


    public static void writeFully(FileChannel file, ByteBuffer buffer, int size) throws IOException {
        Preconditions.checkState(buffer.remaining() >= size,
                "buffer.remaining() >= size");

        int write = 0;
        while (write < size) {
            write += file.write(buffer);
        }
    }

    public static void writeFullyWithRE(FileChannel file, ByteBuffer buffer, int size) {
        try {
            writeFully(file, buffer, size);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void writeFully(FSDataOutputStream ouput, ByteBuffer buffer, int size) throws IOException {
        Preconditions.checkState(buffer.remaining() >= size,
                "buffer.remaining() >= size");

        byte[] bytes = new byte[2048];
        int count = 0;
        while (count < size) {
            int step = Math.min(2048, size - count);
            count += step;
            buffer.get(bytes, 0, step);
            ouput.write(bytes, 0, step);
        }
    }

    public static void writeFully(WritableByteChannel channel, ByteBuffer buffer, int size) throws IOException {
        Preconditions.checkState(buffer.remaining() >= size,
                "buffer.remaining() >= size");

        int write = 0;
        while (write < size) {
            write += channel.write(buffer);
        }
        assert write == size;
    }

    public static void writeFully(FileChannel file, long offset, ByteBuffer buffer, int size) throws IOException {
        Preconditions.checkState(buffer.remaining() >= size,
                "buffer.remaining() >= size");

        int write = 0;
        while (write < size) {
            write += file.write(buffer, offset + write);
        }
    }

    public static void writeFullyWithRE(FileChannel file, long offset, ByteBuffer buffer, int size) {
        try {
            writeFully(file, offset, buffer, size);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void createFileIfNotExist(Path path) throws IOException {
        try (FileChannel f = FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {}
    }
}
