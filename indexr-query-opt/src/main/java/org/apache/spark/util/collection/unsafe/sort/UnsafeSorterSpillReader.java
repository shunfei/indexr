package org.apache.spark.util.collection.unsafe.sort;

import org.apache.commons.io.IOUtils;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import io.indexr.util.IOUtil;
import io.indexr.util.MemoryUtil;

public final class UnsafeSorterSpillReader extends UnsafeSorterIterator implements Closeable {
    private FileChannel file;
    private ByteBuffer buffer;
    private long bufferAddr;

    // Variables that change with every record read:
    private long baseOffset;
    private int recordLength;
    private long keyPrefix;
    private int numRecords;
    private int numRecordsRemaining;

    public UnsafeSorterSpillReader(
            Path path) throws IOException {
        file = FileChannel.open(path, StandardOpenOption.READ);
        assert file.size() >= 4;

        allocateBuffer(1024 * 1024);

        buffer.order(ByteOrder.nativeOrder());
        fillBuffer(4);
        numRecords = numRecordsRemaining = buffer.getInt();
    }

    private void allocateBuffer(int cap) {
        buffer = ByteBuffer.allocateDirect(cap);
        bufferAddr = MemoryUtil.getAddress(buffer);
    }

    private void fillBuffer(int atLeastBytes) throws IOException {
        buffer.clear();
        IOUtil.readFully(file, buffer, atLeastBytes);
        buffer.flip();
    }

    @Override
    public int getNumRecords() {
        return numRecords;
    }

    @Override
    public boolean hasNext() {
        return (numRecordsRemaining > 0);
    }

    @Override
    public void loadNext() throws IOException {
        if (buffer.remaining() < 4 + 8) {
            fillBuffer(4 + 8);
        }
        recordLength = buffer.getInt();
        keyPrefix = buffer.getLong();

        if (recordLength > 0) {
            if (buffer.remaining() < recordLength) {
                if (buffer.capacity() > 4 + 8 + recordLength) {
                    allocateBuffer(4 + 8 + recordLength);
                }
                fillBuffer(recordLength);
            }
        }
        baseOffset = bufferAddr;

        numRecordsRemaining--;
        if (numRecordsRemaining == 0) {
            close();
        }
    }

    @Override
    public Object getBaseObject() {
        return null;
    }

    @Override
    public long getBaseOffset() {
        return baseOffset;
    }

    @Override
    public int getRecordLength() {
        return recordLength;
    }

    @Override
    public long getKeyPrefix() {
        return keyPrefix;
    }

    @Override
    public void close() throws IOException {
        IOUtils.closeQuietly(file);
    }
}
