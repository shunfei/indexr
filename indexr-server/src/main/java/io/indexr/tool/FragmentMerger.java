package io.indexr.tool;

import com.google.common.base.Preconditions;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.datanucleus.store.rdbms.query.AbstractRDBMSQueryResult;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import io.indexr.io.ByteBufferReader;
import io.indexr.segment.Segment;
import io.indexr.segment.SegmentFd;
import io.indexr.segment.SegmentMode;
import io.indexr.segment.SegmentSchema;
import io.indexr.segment.rt.Metric;
import io.indexr.segment.rt.RTSMerge;
import io.indexr.segment.storage.StorageSegment;
import io.indexr.segment.storage.itg.Integrate;
import io.indexr.segment.storage.itg.IntegratedSegment;
import io.indexr.segment.storage.itg.SegmentMeta;
import io.indexr.server.FileSegmentPool;
import io.indexr.server.IndexRConfig;
import io.indexr.server.SegmentHelper;
import io.indexr.server.TableSchema;
import io.indexr.server.ZkTableManager;
import io.indexr.server.rt.RealtimeConfig;
import io.indexr.util.Pair;
import io.indexr.util.Strings;
import io.indexr.util.Try;

public class FragmentMerger {
    private static final Logger logger = LoggerFactory.getLogger(FragmentMerger.class);
    private static final long MERGE_THRESHOLD = 1L << 31;
    private static final DateTimeFormatter timeFormatter = DateTimeFormatter.ofPattern("yyyyMMddHHmm");

    private FileSystem fileSystem;
    private Path tableRootPath;
    private SegmentSchema schema;
    private boolean grouping;
    private SegmentMode mode;
    private List<String> dims;
    private List<Metric> metrics;

    public FragmentMerger(FileSystem fileSystem,
                          Path tableRootPath,
                          SegmentSchema schema,
                          boolean grouping,
                          SegmentMode mode,
                          List<String> dims,
                          List<Metric> metrics) {
        this.fileSystem = fileSystem;
        this.tableRootPath = tableRootPath;
        this.schema = schema;
        this.grouping = grouping;
        this.mode = mode;
        this.dims = dims;
        this.metrics = metrics;
    }

    private static class ToMerge implements Comparable {
        private FileStatus fileStatus;
        private IntegratedSegment.Fd fd;

        public ToMerge(FileStatus fileStatus, IntegratedSegment.Fd fd) {
            this.fileStatus = fileStatus;
            this.fd = fd;
        }

        @Override
        public int compareTo(Object o) {
            ToMerge other = (ToMerge) o;
            return Long.compare(fileStatus.getLen(), other.fileStatus.getLen());
        }

        @Override
        public String toString() {
            return fileStatus.getPath().toString();
        }
    }

    static {
        RealtimeConfig.loadSubtypes();
    }


    public static void mergeTable(String tableName, IndexRConfig config) throws Exception {
        ZkTableManager tm = new ZkTableManager(config.getZkClient());
        Preconditions.checkState(tm.getTableSchema(tableName) != null, "Table [%s] not exists!", tableName);
        TableSchema tableSchema = tm.getTableSchema(tableName);

        FileSystem fileSystem = config.getFileSystem();
        String tableRootPath = IndexRConfig.segmentRootPath(
                config.getDataRoot(),
                tableName,
                tableSchema.location).toString();
        SegmentSchema schema = tableSchema.schema;
        boolean grouping = tableSchema.aggSchema.grouping;
        SegmentMode mode = tableSchema.mode;
        List<String> dims = tableSchema.aggSchema.dims;
        List<Metric> metrics = tableSchema.aggSchema.metrics;

        FragmentMerger merger = new FragmentMerger(
                fileSystem,
                new Path(tableRootPath),
                schema,
                grouping,
                mode,
                dims,
                metrics);
        merger.mergeFolder(merger.tableRootPath);
    }

    private void mergeFolder(Path path) throws IOException {
        logger.debug("mergeFolder: {}", path);

        FileStatus[] fileStatuses = fileSystem.listStatus(path);

        List<FileStatus> files = new ArrayList<>();
        List<FileStatus> folders = new ArrayList<>();
        for (int i = 0; i < fileStatuses.length; i++) {
            FileStatus fileStatus = fileStatuses[i];
            if (!SegmentHelper.checkSegmentByPath(fileStatus.getPath())) {
                continue;
            }
            if (fileStatus.isFile()) {
                files.add(fileStatus);
            } else if (fileStatus.isDirectory()) {
                folders.add(fileStatus);
            }
        }

        //logger.debug("files: {}", files);
        //logger.debug("folders: {}", folders);

        // Merge files under current folder
        if (files.size() > 1) {
            mergeFiles(path, files);
        }

        // Merge sub folders
        for (FileStatus folder : folders) {
            mergeFolder(folder.getPath());
        }
    }

    private void mergeFiles(Path parentPath, List<FileStatus> files) throws IOException {
        logger.debug("mergeFiles: parentPath: {}, files: {}", parentPath, files.stream().map(FileStatus::getPath).collect(Collectors.toList()));

        List<ToMerge> toMerges = new ArrayList<>();
        for (FileStatus fileStatus : files) {
            ByteBufferReader.Opener readerOpener = ByteBufferReader.Opener.create(
                    fileSystem,
                    fileStatus.getPath(),
                    fileStatus.getLen(),
                    1);
            try (ByteBufferReader reader = readerOpener.open(0)) {
                SegmentMeta sectionInfo = Integrate.INSTANCE.read(reader);
                if (sectionInfo == null) {
                    logger.debug("ignore: {}", fileStatus.getPath());
                    // Not a segment.
                    continue;
                }
                IntegratedSegment.Fd fd = IntegratedSegment.Fd.create("", sectionInfo, readerOpener);
                toMerges.add(new ToMerge(fileStatus, fd));
            }
        }

        if (toMerges.size() <= 1) {
            return;
        }

        long totalSize = 0;
        int splitStart = 0;
        for (int i = 0; i < toMerges.size(); i++) {
            ToMerge toMerge = toMerges.get(i);
            totalSize += toMerge.fileStatus.getLen();

            if (totalSize >= MERGE_THRESHOLD) {
                doMerge(parentPath, toMerges.subList(splitStart, i));

                splitStart = i;
                totalSize = 0;
            }
        }

        doMerge(parentPath, toMerges.subList(splitStart, toMerges.size()));
    }

    private void doMerge(Path parentPath, List<ToMerge> toMerges) throws IOException {
        logger.info("toMerges: {}", toMerges);
        if (toMerges.size() <= 1) {
            // No need to merge.
            return;
        }
        List<SegmentFd> mergeList = toMerges.stream().map(m -> m.fd).collect(Collectors.toList());
        java.nio.file.Path localPath = Files.createTempDirectory("indexr_merge_");
        logger.debug("localPath: {}", localPath);
        try {
            StorageSegment mergeSegment = RTSMerge.merge(
                    grouping,
                    schema,
                    mode,
                    dims,
                    metrics,
                    mergeList,
                    localPath,
                    "MergeTemp");

            String segmentName = String.format("Merge_%s_%s", LocalDateTime.now().format(timeFormatter), RandomStringUtils.randomAlphabetic(16));
            SegmentHelper.uploadSegment(mergeSegment, fileSystem, new Path(parentPath, segmentName), false);
            SegmentHelper.notifyUpdate(fileSystem, tableRootPath);

            for (ToMerge toMerge : toMerges) {
                Try.on(() -> fileSystem.delete(toMerge.fileStatus.getPath()), 2, logger);
            }
        } finally {
            FileUtils.deleteDirectory(localPath.toFile());
        }
    }
}
