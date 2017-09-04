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
    private static int TRY_GET_LOCAL_FILE_LIMIT = 2;
    private static final Logger logger = LoggerFactory.getLogger(DFSByteBufferReader.class);

    private String name;
    private FSDataInputStream input;
    private long fileSize;
    private long readBase;
    private Closeable close;
    private boolean isSingleBlock;

    // Only used in short circuit local read.
    private FileChannel localFile;
    private int tryGetLocalFileTimes = 0;

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

    private void tryGetLocalFile() {
        if (tryGetLocalFileTimes >= TRY_GET_LOCAL_FILE_LIMIT) {
            return;
        }
        if (isSingleBlock && HDFS_READ_HACK_ENABLE) {
            try {
                InputStream is = input.getWrappedStream();
                if (is instanceof DFSInputStream) {
                    BlockReader blockReader = MemoryUtil.getDFSInputStream_blockReader(is);
                    if (blockReader != null && blockReader.isShortCircuit()) {
                        localFile = MemoryUtil.getBlockReaderLocal_dataIn(blockReader);
                    }
                }
            } catch (Throwable e) {
                logger.debug("HDFS READ HACK failed.", e);
            }
        }
        tryGetLocalFileTimes++;
    }

    private void fastRead(long position, ByteBuffer dst) throws IOException {
        logger.trace("fast read");
        long time = System.nanoTime();

        IOUtil.readFully(localFile, readBase + position, dst);

        fastReadDuration += System.nanoTime() - time;
        fastReadCount++;
    }

    private void fastRead(long position, byte[] buffer, int offset, int length) throws IOException {
        logger.trace("fast read");
        long time = System.nanoTime();

        ByteBuffer bb = ByteBuffer.wrap(buffer, offset, length);
        localFile.read(bb, readBase + position);

        fastReadDuration += System.nanoTime() - time;
        fastReadCount++;
    }

    private void normalRead(long position, ByteBuffer dst) throws IOException {
        logger.trace("normal read");
        long time = System.nanoTime();

        IOUtil.readFully(input, readBase + position, dst);

        normalReadDuration += System.nanoTime() - time;
        normalReadCount++;
    }

    private void normalRead(long position, byte[] buffer, int offset, int length) throws IOException {
        logger.trace("normal read");
        long time = System.nanoTime();

        input.seek(readBase + position);
        input.readFully(buffer, offset, length);

        normalReadDuration += System.nanoTime() - time;
        normalReadCount++;
    }

    @Override
    public void read(long position, ByteBuffer dst) throws IOException {
        try {
            tryGetLocalFile();
            if (localFile != null) {
                final int dst_pos = dst.position();
                final int dst_limit = dst.limit();
                try {
                    fastRead(position, dst);
                } catch (Exception e) {
                    localFile = null;
                    tryGetLocalFileTimes = TRY_GET_LOCAL_FILE_LIMIT;
                    logger.warn("fast read failed, roll back to normal read: {}", name);
                    dst.position(dst_pos);
                    dst.limit(dst_limit);
                    normalRead(position, dst);
                }
            } else {
                normalRead(position, dst);
            }
        } catch (Exception e) {
            throw new IOException(String.format("name: %s", name), e);
        }
    }

    @Override
    public void read(long position, byte[] buffer, int offset, int length) throws IOException {
        try {
            tryGetLocalFile();
            if (localFile != null) {
                try {
                    fastRead(position, buffer, offset, length);
                } catch (Exception e) {
                    localFile = null;
                    logger.warn("fast read failed, roll back to normal read: {}", name);
                    normalRead(position, buffer, offset, length);
                }
            } else {
                normalRead(position, buffer, offset, length);
            }
        } catch (Exception e) {
            throw new IOException(String.format("name: %s", name), e);
        }
    }

    @Override
    public boolean exists(long position) throws IOException {
        return position >= 0 && readBase + position < fileSize;
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
