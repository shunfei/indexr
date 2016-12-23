package io.indexr.server;

import com.google.common.base.Preconditions;

import org.apache.commons.lang.StringUtils;
import org.apache.directory.api.util.Strings;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.ClosedByInterruptException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import io.indexr.io.ByteBufferReader;
import io.indexr.segment.SegmentFd;
import io.indexr.segment.SegmentLocality;
import io.indexr.segment.SegmentPool;
import io.indexr.segment.pack.Integrated;
import io.indexr.segment.pack.IntegratedSegment;
import io.indexr.util.Try;

public class FileSegmentPool extends FileSegmentManager implements SegmentPool, SegmentLocality {
    private static final Logger logger = LoggerFactory.getLogger(FileSegmentPool.class);

    private static final Random random = new Random();
    private static final long RefreshSegmentPeriod = TimeUnit.SECONDS.toMillis(10);
    private static final long RefreshLocalityPeriod = TimeUnit.SECONDS.toMillis(30 * 60);

    private ScheduledFuture refreshSegment;
    private ScheduledFuture refreshLocality;
    private long lastRefreshTime = 0;

    private final FileSystem fileSystem;
    private final Path segmentRootPath;
    private final String segmentRootPathStr;
    private final Path updateFilePath;
    private final java.nio.file.Path localCachePath;

    // segment name -> host list.
    private Map<String, List<String>> hostMap = new HashMap<>();
    private final Map<String, SegmentFdAndTime> segmentFdMap = new ConcurrentHashMap<>(2048);
    // A segment list duplication for fast traversal and to avoid muti-thread unsafe while updating segmengFdMap.
    private List<SegmentFd> segmentFdList = Collections.emptyList();

    private long lastSaveCacheTime = 0;
    private int totalUpdateCount = 0;

    private static final long SaveCachePeriod = TimeUnit.MINUTES.toMillis(15);
    private static final int SaveCacheUpdateCount = 30;
    private static final int RetryTimes = 3;

    public FileSegmentPool(String tableName,
                           FileSystem fileSystem,
                           String dataRoot,
                           java.nio.file.Path localDataRoot,
                           ScheduledExecutorService notifyService) throws Exception {
        super(tableName, fileSystem, dataRoot);
        Path rootPath = new Path(IndexRConfig.segmentRootPath(dataRoot, tableName));
        if (!fileSystem.exists(rootPath)) {
            fileSystem.mkdirs(rootPath);
        }
        this.segmentRootPath = fileSystem.resolvePath(rootPath);
        this.segmentRootPathStr = segmentRootPath.toString() + "/";
        this.fileSystem = fileSystem;
        this.localCachePath = IndexRConfig.localCacheSegmentFdPath(localDataRoot, tableName);
        this.updateFilePath = new Path(IndexRConfig.segmentUpdateFilePath(dataRoot, tableName));

        if (!fileSystem.exists(updateFilePath)) {
            fileSystem.create(updateFilePath, true);
        }
        if (!Files.exists(localCachePath.getParent())) {
            Files.createDirectories(localCachePath.getParent());
        }

        // Load segments before doing any query.
        Try.on(this::loadFromLocalCache,
                1, logger,
                String.format("Load %s segmentFds from local cache failed", tableName));

        this.refreshSegment = notifyService.scheduleWithFixedDelay(
                () -> this.refresh(false),
                TimeUnit.SECONDS.toMillis(1),
                RefreshSegmentPeriod + random.nextInt(1000),
                TimeUnit.MILLISECONDS);
        this.refreshLocality = notifyService.scheduleWithFixedDelay(
                this::refreshLocalities,
                TimeUnit.SECONDS.toMillis(1) + random.nextInt(5000),
                RefreshLocalityPeriod + random.nextInt(5000),
                TimeUnit.MILLISECONDS);
    }

    private Path segmentPath(String segmentName) {
        return new Path(segmentRootPath, segmentName);
    }

    @Override
    public boolean exists(String name) {
        return segmentFdMap.containsKey(name);
    }

    @Override
    public SegmentFd get(String name) {
        SegmentFdAndTime st = segmentFdMap.get(name);
        return st == null ? null : st.fd;
    }

    @Override
    public List<SegmentFd> all() {
        return segmentFdList;
    }

    @Override
    public void refresh(boolean force) {
        refreshSegments(force);
    }

    private void refreshSegments(boolean force) {
        try {
            if (force) {
                doRefreshSegments();
                return;
            }

            FileStatus fileStatus = fileSystem.getFileStatus(updateFilePath);
            long modifyTime = fileStatus != null ? fileStatus.getModificationTime() : 0;
            boolean modifyTimeOk = lastRefreshTime < modifyTime;
            if (modifyTimeOk) {
                if (doRefreshSegments()) {
                    lastRefreshTime = modifyTime;
                }
            }

        } catch (IOException e) {
            if (e instanceof ClosedByInterruptException) {
                logger.warn("Load segments of table [{}] failed by ClosedByInterruptException.", tableName);
                return;
            }
            String msg = e.getMessage();
            if (msg != null && Strings.equals(msg.trim(), "Filesystem closed")) {
                logger.warn("Load segments of table [{}] failed by Filesystem closed.", tableName);
                return;
            }
            logger.error("", e);
            logger.error("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<");
            logger.error("Load segments of table [{}] failed, system in inconsistent state", tableName);
            logger.error(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
        }
    }

    private synchronized boolean doRefreshSegments() throws IOException {
        RemoteIterator<LocatedFileStatus> files = fileSystem.listFiles(segmentRootPath, true);

        Set<String> nameSet = new HashSet<>(2048);
        int updateCount = 0;
        boolean hasError = false;

        // Load segment from storage.

        while (files.hasNext()) {
            LocatedFileStatus fileStatus = files.next();
            String name = getSegmentName(fileStatus);
            if (name == null) {
                continue;
            }

            nameSet.add(name);

            long modifyTime = fileStatus.getModificationTime();
            SegmentFdAndTime st = segmentFdMap.get(name);
            if (st != null && st.modifyTime == modifyTime && st.fileSize == fileStatus.getLen()) {
                // Segment exist and is up to dated.
                continue;
            }
            Path segmentPath = segmentPath(name);
            ByteBufferReader.Opener readerOpener = ByteBufferReader.Opener.create(
                    fileSystem,
                    segmentPath,
                    fileStatus.getLen());
            Integrated.SectionInfo sectionInfo = null;
            try (ByteBufferReader reader = readerOpener.open(0)) {
                sectionInfo = Integrated.read(reader);
                if (sectionInfo == null) {
                    // Not a segment.
                    continue;
                }
                IntegratedSegment.Fd fd = IntegratedSegment.Fd.create(name, sectionInfo, readerOpener);

                segmentFdMap.put(name, new SegmentFdAndTime(fd, fileStatus.getModificationTime(), fileStatus.getLen()));
                logger.info("table [{}] add new segment [{}]", tableName, name);

                updateCount++;
            } catch (IOException e) {
                hasError = true;
                logger.error("", e);
                logger.error("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<");
                logger.error("Load segment [{}: {}] failed, system in inconsistent state", tableName, name);
                logger.error(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
            }
        }

        // Remove outdated segments.
        Iterator<String> it = segmentFdMap.keySet().iterator();
        while (it.hasNext()) {
            String name = it.next();
            if (!nameSet.contains(name)) {
                logger.info("table [{}] remove segment [{}]", tableName, name);
                it.remove();
                updateCount++;
            }
        }

        if (updateCount > 0) {
            updateSegmentFdList();
        }

        // Save to local cache.
        totalUpdateCount += updateCount;
        if (totalUpdateCount >= SaveCacheUpdateCount
                || lastSaveCacheTime + SaveCachePeriod <= System.currentTimeMillis()) {
            Try.on(this::saveToLocalCache,
                    1, logger,
                    String.format("Save %s segment fds to local cache failed", tableName));
            logger.debug("save [{}] segment fds to local cache", tableName);
            totalUpdateCount = 0;
            lastSaveCacheTime = System.currentTimeMillis();
        }
        return !hasError;
    }

    private void updateSegmentFdList() {
        List<SegmentFd> newFds = new ArrayList<>(segmentFdMap.size());
        for (SegmentFdAndTime st : segmentFdMap.values()) {
            newFds.add(st.fd);
        }
        segmentFdList = newFds;
    }

    private ByteBufferReader.Opener createBBROpener(String name) {
        return ByteBufferReader.Opener.create(fileSystem, segmentPath(name));
    }

    private String getSegmentName(FileStatus fileStatus) {
        if (!fileStatus.isFile()) {
            return null;
        }
        Path path = fileStatus.getPath();
        if (!SegmentHelper.checkSegmentByPath(path)) {
            return null;
        }
        return StringUtils.removeStart(path.toString(), segmentRootPathStr);
    }

    private List<String> loadHosts(String segmentName) throws IOException {
        Path path = segmentPath(segmentName);
        BlockLocation[] locations = fileSystem.getFileBlockLocations(path, 0, Long.MAX_VALUE);
        if (locations == null) {
            return null;
        }
        Preconditions.checkState(locations.length == 1, "A segment should only consisted by one block");
        return Arrays.asList(locations[0].getHosts());
    }

    public void refreshLocalities() {
        try {
            // HashMap taks muti-thread risk here. Change to ConcurrentHashMap if it happens.
            Map<String, List<String>> newHostMap = new HashMap<>(segmentFdMap.size());

            RemoteIterator<LocatedFileStatus> files = fileSystem.listFiles(segmentRootPath, true);
            while (files.hasNext()) {
                LocatedFileStatus fileStatus = files.next();
                String name = getSegmentName(fileStatus);
                if (name == null) {
                    continue;
                }
                BlockLocation[] locations = fileStatus.getBlockLocations();
                Preconditions.checkState(locations.length == 1, "A segment should only consisted by one block");
                List<String> hosts = Arrays.asList(locations[0].getHosts());
                newHostMap.put(name, hosts);
            }

            hostMap = newHostMap;
        } catch (IOException e) {
            if (e instanceof ClosedByInterruptException) {
                logger.warn("Refresh [{}] segment locality failed by ClosedByInterruptException.", tableName);
                // Normally close interrupt.
                return;
            }
            String msg = e.getMessage();
            if (msg != null && Strings.equals(msg.trim(), "Filesystem closed")) {
                logger.warn("Refresh [{}] segment locality failed by Filesystem closed.", tableName);
                // Normally close interrupt.
                return;
            }
            logger.warn("Refresh [{}] segment locality failed.", tableName, e);
        }
    }

    @Override
    public List<String> getHosts(String segmentName, boolean isRealtime) throws IOException {
        assert !isRealtime;

        Map<String, List<String>> hostMap = this.hostMap;
        List<String> hosts = hostMap.get(segmentName);
        if (hosts == null) {
            hosts = loadHosts(segmentName);
            if (hosts != null) {
                hostMap.put(segmentName, hosts);
            }
        }
        return hosts;
    }

    @Override
    public void close() {
        refreshSegment.cancel(true);
        refreshLocality.cancel(true);

        hostMap.clear();
        segmentFdMap.clear();
        segmentFdList.clear();
    }

    public void loadFromLocalCache() throws IOException {
        Map<String, Integrated.SectionInfo> sectionInfos = Integrated.SectionInfo.loadFromLocalFile(localCachePath);
        for (Map.Entry<String, Integrated.SectionInfo> entry : sectionInfos.entrySet()) {
            String timeAndName = entry.getKey();
            Integrated.SectionInfo info = entry.getValue();
            String[] strs = StringUtils.split(timeAndName, "|", 3);
            long time = Long.parseLong(strs[0]);
            long fileSize = Long.parseLong(strs[1]);
            String name = strs[2];
            ByteBufferReader.Opener reader = ByteBufferReader.Opener.create(
                    fileSystem,
                    segmentPath(name),
                    fileSize);
            IntegratedSegment.Fd fd = IntegratedSegment.Fd.create(name, info, reader);

            logger.debug("table [{}] load cache segmentfd [{}]", tableName, fd.name());

            segmentFdMap.put(name, new SegmentFdAndTime(fd, time, fileSize));
        }
        updateSegmentFdList();
    }

    public void saveToLocalCache() throws IOException {
        Map<String, Integrated.SectionInfo> sectionInfos = new HashMap<>(segmentFdMap.size());
        for (Map.Entry<String, SegmentFdAndTime> entry : segmentFdMap.entrySet()) {
            String name = entry.getKey();
            SegmentFdAndTime st = entry.getValue();
            sectionInfos.put(st.modifyTime + "|" + st.fileSize + "|" + name, ((IntegratedSegment.Fd) st.fd).sectionInfo());
        }
        Integrated.SectionInfo.saveToLocalFile(localCachePath, sectionInfos);
    }

    private static class SegmentFdAndTime {
        private final SegmentFd fd;
        private final long modifyTime;
        private final long fileSize;

        SegmentFdAndTime(SegmentFd fd, long modifyTime, long fileSize) {
            this.fd = fd;
            this.modifyTime = modifyTime;
            this.fileSize = fileSize;
        }
    }
}
