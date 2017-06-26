package io.indexr.server;

import com.google.common.base.Preconditions;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

import io.indexr.segment.Segment;
import io.indexr.segment.SegmentFd;
import io.indexr.segment.SegmentManager;
import io.indexr.segment.SegmentUploader;
import io.indexr.segment.storage.StorageSegment;

public class FileSegmentManager implements SegmentManager, SegmentUploader {
    private static final Logger logger = LoggerFactory.getLogger(FileSegmentManager.class);

    String tableName;
    FileSystem fileSystem;
    Path segmentRootPath;

    public FileSegmentManager(String tableName, FileSystem fileSystem, Path segmentRootPath) throws Exception {
        this.tableName = tableName;
        this.fileSystem = fileSystem;

        this.segmentRootPath = segmentRootPath;
        if (!fileSystem.exists(segmentRootPath)) {
            fileSystem.mkdirs(segmentRootPath);
        }
        this.segmentRootPath = fileSystem.resolvePath(segmentRootPath);
    }

    @Override
    public boolean exists(String name) throws IOException {
        return fileSystem.exists(new Path(segmentRootPath, name));
    }

    @Override
    public List<String> allSegmentNames() throws IOException {
        return SegmentHelper.listSegmentNames(fileSystem, segmentRootPath);
    }

    @Override
    public void add(Segment segment) throws Exception {
        Preconditions.checkState(segment instanceof StorageSegment);
        upload((StorageSegment) segment, false);
    }

    @Override
    public SegmentFd upload(StorageSegment segment, boolean openFd) throws IOException {
        return SegmentHelper.uploadSegment((StorageSegment) segment, fileSystem, segmentRootPath, false, true);
    }

    @Override
    public void remove(String name) throws Exception {
        fileSystem.delete(new Path(segmentRootPath, name));
        SegmentHelper.notifyUpdate(fileSystem, segmentRootPath);
    }
}
