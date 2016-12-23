package io.indexr.server;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.function.Consumer;

import io.indexr.io.ByteBufferReader;
import io.indexr.io.ByteBufferWriter;
import io.indexr.segment.SegmentFd;
import io.indexr.segment.pack.IntegratedSegment;
import io.indexr.segment.pack.StorageSegment;
import io.indexr.util.Try;

public class SegmentHelper {
    private static final Logger logger = LoggerFactory.getLogger(SegmentHelper.class);

    public static long getSegmentBlockSize(FileSystem fileSystem, long fileSize) {
        long blockSize = fileSystem.getDefaultBlockSize();
        while (blockSize < fileSize) {
            blockSize <<= 1;
        }
        return blockSize;
    }

    public static Path segmentPath(String dataRoot, String tableName, String segmentName) {
        String segmentRootDir = IndexRConfig.segmentRootPath(dataRoot, tableName);
        return new org.apache.hadoop.fs.Path(segmentRootDir, segmentName);
    }

    public static void uploadSegment(StorageSegment segment,
                                     FileSystem fileSystem,
                                     String dataRoot,
                                     String tableName) throws IOException {
        uploadSegment(segment, fileSystem, dataRoot, tableName, false, true);
    }

    public static SegmentFd uploadSegment(StorageSegment segment,
                                          FileSystem fileSystem,
                                          String dataRoot,
                                          String tableName,
                                          boolean openSegment,
                                          boolean notifyUpdate) throws IOException {
        Path path = segmentPath(dataRoot, tableName, segment.name());
        SegmentFd fd = uploadSegment(segment, fileSystem, path, openSegment);
        if (notifyUpdate) {
            notifyUpdate(fileSystem, dataRoot, tableName);
        }
        return fd;
    }

    public static SegmentFd uploadSegment(StorageSegment segment, FileSystem fileSystem, Path path, boolean openSegment) throws IOException {
        // First upload to a tmp file, then rename to real path after completely transfer all data, to avoid file corruption.

        Path tmpPath = new Path(path.toString() + ".__TMP__");
        SegmentFd fd;
        short replica = fileSystem.getDefaultReplication(path);
        if (replica <= 0) {
            logger.warn("Failed to get replication from {}", path);
            replica = fileSystem.getDefaultReplication();
        }
        short _replica = replica;

        ByteBufferWriter.PredictSizeOpener writeOpener = size -> {
            long blockSize = getSegmentBlockSize(fileSystem, size);
            FSDataOutputStream outputStream = fileSystem.create(
                    tmpPath,
                    FsPermission.getFileDefault().applyUMask(FsPermission.getUMask(fileSystem.getConf())),
                    EnumSet.of(CreateFlag.CREATE, CreateFlag.OVERWRITE),
                    4096 * 10,
                    _replica,
                    blockSize,
                    null);
            ByteBufferWriter writer = ByteBufferWriter.of(outputStream, outputStream::close);
            writer.setName(tmpPath.toString());
            return writer;
        };
        ByteBufferReader.Opener readerOpener = openSegment ? ByteBufferReader.Opener.create(fileSystem, path) : null;
        fd = IntegratedSegment.Fd.create(segment, writeOpener, readerOpener);

        int times = 0;
        Boolean ok = false;
        while (times < 5) {
            if (fileSystem instanceof DistributedFileSystem) {
                ok = Try.on(() -> ((DistributedFileSystem) fileSystem).rename(tmpPath, path, Options.Rename.OVERWRITE), 1, logger);
            } else {
                ok = Try.on(() -> fileSystem.rename(tmpPath, path), 1, logger);
            }
            if (ok != null && ok) {
                break;
            }
            times++;
            if (fileSystem.exists(path)) {
                Try.on(() -> fileSystem.delete(path), 1, logger, String.format("delete old segment failed: %s", path.toString()));
            }
        }
        if (ok == null || !ok) {
            throw new IOException(String.format("Failed to rename from [%s] to [%s]", tmpPath, path));
        }

        return fd;
    }

    public static void notifyUpdate(FileSystem fileSystem, String dataRoot, String tableName) throws IOException {
        // Touch the update file to notify segment change.
        Path updateFilePath = new Path(IndexRConfig.segmentUpdateFilePath(dataRoot, tableName));
        try (FSDataOutputStream os = fileSystem.create(updateFilePath, true)) {
            os.hsync();
        }
    }

    public static void notifyUpdate(FileSystem fileSystem, String tableLocation) throws IOException {
        // Touch the update file to notify segment change.
        Path updateFilePath = new Path(tableLocation, "__UPDATE__");
        try (FSDataOutputStream os = fileSystem.create(updateFilePath, true)) {
            os.hsync();
        }
    }

    public static List<String> listSegmentNames(FileSystem fileSystem, Path dir) throws IOException {
        dir = fileSystem.resolvePath(dir);
        String dirStr = dir.toString() + "/";
        List<String> names = new ArrayList<>(2048);
        literalAllSegments(fileSystem, dir, f -> {
            String name = StringUtils.removeStart(f.getPath().toString(), dirStr);
            names.add(name);
        });
        return names;
    }

    public static void literalAllSegments(FileSystem fileSystem, Path dir, Consumer<LocatedFileStatus> consumer) throws IOException {
        RemoteIterator<LocatedFileStatus> files = fileSystem.listFiles(dir, true);
        while (files.hasNext()) {
            LocatedFileStatus fileStatus = files.next();
            if (!fileStatus.isFile()) {
                continue;
            }


            Path path = fileStatus.getPath();
            if (checkSegmentByPath(path)) {
                consumer.accept(fileStatus);
            }
        }
    }

    public static boolean checkSegmentByPath(Path path) {
        String[] ss = path.toString().split(Path.SEPARATOR);
        if (ss.length == 0) {
            return false;
        }
        for (String s : ss) {
            if (s.startsWith(".")) {
                return false;
            }
        }
        String fileName = ss[ss.length - 1];
        return !fileName.endsWith("__");
    }
}
