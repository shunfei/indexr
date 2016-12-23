package io.indexr.hive;

import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.EnumSet;

import io.indexr.io.ByteBufferWriter;
import io.indexr.segment.pack.IntegratedSegment;
import io.indexr.segment.pack.StorageSegment;

public class SegmentHelper {
    private static final Logger logger = LoggerFactory.getLogger(SegmentHelper.class);

    public static long getSegmentBlockSize(FileSystem fileSystem, long fileSize) {
        long blockSize = fileSystem.getDefaultBlockSize();
        while (blockSize < fileSize) {
            blockSize <<= 1;
        }
        return blockSize;
    }

    public static void uploadSegment(StorageSegment segment,
                                     FileSystem fileSystem,
                                     Path path,
                                     Path tableLocation) throws IOException {
        short replica = fileSystem.getDefaultReplication(tableLocation);
        if (replica <= 0) {
            logger.warn("Failed to get replication from {}", tableLocation);
            replica = fileSystem.getDefaultReplication();
        }
        short _replica = replica;

        ByteBufferWriter.PredictSizeOpener writeOpener = size -> {
            long blockSize = getSegmentBlockSize(fileSystem, size);
            FSDataOutputStream outputStream = fileSystem.create(
                    path,
                    FsPermission.getFileDefault().applyUMask(FsPermission.getUMask(fileSystem.getConf())),
                    EnumSet.of(CreateFlag.CREATE, CreateFlag.OVERWRITE),
                    4096 * 10,
                    _replica,
                    blockSize,
                    null);
            ByteBufferWriter writer = ByteBufferWriter.of(outputStream, outputStream::close);
            writer.setName(path.toString());
            return writer;
        };
        IntegratedSegment.Fd.create(segment, writeOpener, null);
    }

    public static void notifyUpdate(FileSystem fileSystem, String tableLocation) throws IOException {
        // Touch the update file to notify segment change.
        Path updateFilePath = new Path(String.format("%s/__UPDATE__", tableLocation));
        try (FSDataOutputStream os = fileSystem.create(updateFilePath, true)) {
            os.hsync();
        }
    }

    public static boolean checkSegmentByPath(Path path) {
        String[] ss = path.toString().split(Path.SEPARATOR);
        if (ss.length == 0) {
            return false;
        }
        //for (String s : ss) {
        //    if (s.startsWith(".")) {
        //        return false;
        //    }
        //}
        String fileName = ss[ss.length - 1];
        return !fileName.endsWith("__");
    }
}
