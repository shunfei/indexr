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
     * Slow function, should only used in test. Unless, you choose to optimize it.
     * This function will not move the position of <i>from</i>.
     */
    public static void readFully(ByteBuffer from, int offset, ByteBuffer to, int size) {
        for (int i = 0; i < size; i++) {
            to.put(from.get(offset + i));
        }
    }

    public static void readFully(FSDataInputStream reader, ByteBuffer buffer) throws IOException {
        readFully(reader, -1, buffer);
    }

    public static void readFully(FSDataInputStream reader, long offset, ByteBuffer buffer) throws IOException {
        if (offset >= 0) {
            reader.seek(offset);
        }
        //reader.skip()

        InputStream is = reader.getWrappedStream();
        if (!(is instanceof ByteBufferReadable)) {
            logger.trace("Using read bytes method");
            byte[] bytes = new byte[buffer.remaining()];
            reader.readFully(bytes);
            buffer.put(bytes);
        } else {
            while (buffer.hasRemaining()) {
                int pos = buffer.position();
                int rt = reader.read(buffer);
                if (rt < 0) {
                    throw new IOException("End of stream");
                }
                buffer.position(pos + rt);
            }
        }
        Preconditions.checkState(!buffer.hasRemaining());
    }

    public static void readFully(FileChannel file, ByteBuffer buffer) throws IOException {
        Preconditions.checkState(file.size() - file.position() >= buffer.remaining());

        readFully((ReadableByteChannel) file, buffer);
    }

    public static void readFullyWithRE(FileChannel file, ByteBuffer buffer) {
        try {
            readFully(file, buffer);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void readFully(FileChannel file, long offset, ByteBuffer buffer) throws IOException {
        Preconditions.checkState(file.size() - offset >= buffer.remaining());

        int read = 0;
        while (buffer.hasRemaining()) {
            int rt = file.read(buffer, offset + read);
            if (rt < 0) {
                throw new IOException("End of file");
            }
            read += rt;
        }
    }

    public static void readFullyWithRE(FileChannel file, long offset, ByteBuffer buffer) {
        try {
            readFully(file, offset, buffer);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void readFully(ReadableByteChannel channel, ByteBuffer buffer) throws IOException {
        while (buffer.hasRemaining()) {
            int rt = channel.read(buffer);
            if (rt < 0) {
                throw new IOException("End of channel");
            }
        }
    }


    public static void writeFully(FileChannel file, ByteBuffer buffer) throws IOException {
        while (buffer.hasRemaining()) {
            int rt = file.write(buffer);
            if (rt < 0) {
                throw new IOException("Failed to write");
            }
        }
    }

    public static void writeFullyWithRE(FileChannel file, ByteBuffer buffer) {
        try {
            writeFully(file, buffer);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void writeFully(FSDataOutputStream ouput, ByteBuffer buffer) throws IOException {
        byte[] bytes = new byte[2048];
        int count = 0;
        int size = buffer.remaining();
        while (count < size) {
            int step = Math.min(2048, size - count);
            count += step;
            buffer.get(bytes, 0, step);
            ouput.write(bytes, 0, step);
        }
        assert !buffer.hasRemaining();
    }

    public static void writeFully(WritableByteChannel channel, ByteBuffer buffer) throws IOException {
        while (buffer.hasRemaining()) {
            int rt = channel.write(buffer);
            if (rt < 0) {
                throw new IOException("Failed to write");
            }
        }
    }

    public static void writeFully(FileChannel file, long offset, ByteBuffer buffer) throws IOException {
        int write = 0;
        int size = buffer.remaining();
        while (write < size) {
            write += file.write(buffer, offset + write);
        }
        assert !buffer.hasRemaining();
    }

    public static void writeFullyWithRE(FileChannel file, long offset, ByteBuffer buffer) {
        try {
            writeFully(file, offset, buffer);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void createFileIfNotExist(Path path) throws IOException {
        try (FileChannel f = FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {}
    }
}
