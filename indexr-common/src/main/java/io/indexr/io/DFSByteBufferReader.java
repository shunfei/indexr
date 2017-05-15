package io.indexr.io;

import com.google.common.base.Preconditions;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.hdfs.BlockReader;
import org.apache.hadoop.hdfs.DFSInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import io.indexr.util.IOUtil;
import io.indexr.util.MemoryUtil;

public class DFSByteBufferReader implements ByteBufferReader {
    private static boolean HDFS_READ_HACK_ENABLE = MemoryUtil.HDFS_READ_HACK_ENABLE;
    private static Boolean IS_SHORT_CIRCUIT_LOCAL_READ_ENABLE = null;
    private static final Logger logger = LoggerFactory.getLogger(DFSByteBufferReader.class);

    private String name;
    private FSDataInputStream input;
    private long fileSize;
    private long readBase;
    private Closeable close;
    private boolean isSingleBlock;

    // Only used in short circuit local read.
    private FileChannel localFile;

    private long fastReadDuration = 0;
    private int fastReadCount = 0;
    private long normalReadDuration = 0;
    private int normalReadCount = 0;

    public DFSByteBufferReader(String name,
                               FSDataInputStream input,
                               long fileSize,
                               long readBase,
                               Closeable close,
                               int blockCount) throws IOException {
        Preconditions.checkState(input != null);
        Preconditions.checkState(fileSize >= 0);

        this.name = name;
        this.input = input;
        this.fileSize = fileSize;
        this.readBase = readBase;
        this.close = close;
        this.isSingleBlock = blockCount == 1;

        logger.debug("{}, size: {}, base: {}, block: {}", name, fileSize, readBase, blockCount);
    }

    @Override
    public void read(long offset, ByteBuffer dst) throws IOException {
        try {
            if (isSingleBlock && HDFS_READ_HACK_ENABLE) {
                readSingleBlock(offset, dst);
            } else {
                normalRead(offset, dst);
            }
        } catch (Exception e) {
            throw new IOException(String.format("name: %s", name), e);
        }
    }

    private void readSingleBlock(long offset, ByteBuffer dst) throws IOException {
        // Fast read if local file already exists.
        if (localFile != null) {
            fastRead(offset, dst);
            return;
        }

        InputStream is = input.getWrappedStream();
        if (is instanceof DFSInputStream) {
            BlockReader blockReader = MemoryUtil.getDFSInputStream_blockReader(is);
            if (blockReader != null && blockReader.isShortCircuit()) {
                localFile = MemoryUtil.getBlockReaderLocal_dataIn(blockReader);
            }
        }

        if (localFile != null) {
            fastRead(offset, dst);
        } else {
            normalRead(offset, dst);
        }
    }

    private void fastRead(long offset, ByteBuffer dst) throws IOException {
        logger.trace("fast read");
        long time = System.nanoTime();

        IOUtil.readFully(localFile, readBase + offset, dst);

        fastReadDuration += System.nanoTime() - time;
        fastReadCount++;
    }

    private void normalRead(long offset, ByteBuffer dst) throws IOException {
        logger.trace("normal read");
        long time = System.nanoTime();

        IOUtil.readFully(input, readBase + offset, dst);

        normalReadDuration += System.nanoTime() - time;
        normalReadCount++;
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
        logger.debug("fast read. avg cost: {}, total cost: {}, count: {}",
                fastReadCount == 0 ? 0 : fastReadDuration / fastReadCount,
                fastReadDuration,
                fastReadCount);
        logger.debug("normal read. avg cost: {}, total cost: {}, count: {}",
                normalReadCount == 0 ? 0 : normalReadDuration / normalReadCount,
                normalReadDuration,
                normalReadCount);
    }

    @Override
    public void setName(String name) {
        this.name = name;
    }

    public static ByteBufferReader open(org.apache.hadoop.fs.FileSystem fileSystem,
                                        org.apache.hadoop.fs.Path path,
                                        long size,
                                        int blockCount,
                                        long readBase) throws IOException {
        FSDataInputStream stream = fileSystem.open(path);

        if (HDFS_READ_HACK_ENABLE) {
            if (IS_SHORT_CIRCUIT_LOCAL_READ_ENABLE == null) {
                IS_SHORT_CIRCUIT_LOCAL_READ_ENABLE = Boolean.parseBoolean(fileSystem.getConf().get("dfs.client.read.shortcircuit", "false"));
            }
            if (IS_SHORT_CIRCUIT_LOCAL_READ_ENABLE) {
                InputStream is = stream.getWrappedStream();
                if (is instanceof DFSInputStream) {
                    // Close check sum if short circuit local read is enabled.
                    MemoryUtil.setDFSInputStream_verifyChecksum(is, false);
                    logger.debug("disable read check sum for: {}", path);
                }
            }
        }

        return new DFSByteBufferReader(
                path.toString(),
                stream,
                size,
                readBase,
                stream,
                blockCount);
    }
}
