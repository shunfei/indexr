package org.apache.spark.util.collection.unsafe.sort;

import com.google.common.base.Preconditions;

import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

import io.indexr.util.ByteBufferUtil;

public final class UnsafeSorterSpillWriter {
    private final int numRecordsToWrite;
    private int numRecordsSpilled = 0;
    private Path filePath;
    private FileChannel file;
    private ByteBuffer buffer = ByteBuffer.allocateDirect(1024 * 1024);

    public UnsafeSorterSpillWriter(int numRecordsToWrite) throws IOException {
        this.numRecordsToWrite = numRecordsToWrite;
        filePath = Files.createTempFile(Paths.get("indexr_tmp"), "sort_", null);
        file = FileChannel.open(filePath,
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING);

        buffer.order(ByteOrder.nativeOrder());
        buffer.putInt(numRecordsToWrite);
        flushBuffer();
    }

    private void flushBuffer() throws IOException {
        buffer.flip();
        file.write(buffer);
        buffer.clear();
    }

    public void write(
            Object baseObject,
            long baseOffset,
            int recordLength,
            long keyPrefix) throws IOException {
        Preconditions.checkState(numRecordsSpilled < numRecordsToWrite,
                "Number of records written exceeded numRecordsToWrite = " + numRecordsToWrite);
        buffer.clear();
        buffer.putInt(recordLength);
        buffer.putLong(keyPrefix);
        int written = 0;
        while (written < recordLength) {
            int writeBytes = Math.min(recordLength - written, buffer.remaining());
            ByteBufferUtil.copyIntoDirectByteBuffer(baseObject, baseOffset + written, buffer, writeBytes);
            flushBuffer();
            written += writeBytes;
        }
        assert written == recordLength;
        numRecordsSpilled++;
    }

    public void flush() throws IOException {
        file.force(false);
    }

    public Path getFile() {
        return filePath;
    }

    public void close() throws IOException {
        file.force(false);
        IOUtils.closeQuietly(file);
        file = null;
        buffer = null;
    }

    public void removeFile() throws IOException {
        Files.deleteIfExists(filePath);
    }

    public UnsafeSorterSpillReader getReader() throws IOException {
        return new UnsafeSorterSpillReader(filePath);
    }
}
