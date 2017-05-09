package io.indexr.segment.rt;

import com.google.common.base.Preconditions;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.indexr.segment.InfoSegment;
import io.indexr.segment.Row;
import io.indexr.segment.RowTraversal;
import io.indexr.segment.Segment;
import io.indexr.segment.SegmentFd;
import io.indexr.segment.SegmentMode;
import io.indexr.segment.SegmentSchema;
import io.indexr.segment.SegmentUploader;
import io.indexr.segment.cache.ExtIndexMemCache;
import io.indexr.segment.cache.IndexMemCache;
import io.indexr.segment.cache.PackMemCache;
import io.indexr.segment.storage.ColumnNode;
import io.indexr.segment.storage.StorageSegment;
import io.indexr.segment.storage.Version;
import io.indexr.segment.storage.itg.IntegratedSegment;
import io.indexr.util.DelayTask;
import io.indexr.util.IOUtil;
import io.indexr.util.JsonUtil;
import io.indexr.util.Strings;
import io.indexr.util.Try;

public class RTSGroup implements InfoSegment, SegmentFd {
    private static final Logger logger = LoggerFactory.getLogger(RTSGroup.class);

    private final Path path;
    private final String tableName;

    private final Metadata metadata;

    private long handlePeriod = TimeUnit.SECONDS.toMillis(10);
    private long fastPeriod = TimeUnit.MILLISECONDS.toMillis(300);
    private long deleteDelayPeriod = TimeUnit.SECONDS.toMillis(15);

    // In recoving state or not. The handle thread must finish recovring before doing anything else.
    private volatile boolean isRecovering;
    // Whether allow new rts add into this rtsg or not. The handle thread won't start merging if it is true.
    private volatile boolean allowAddSegment;

    private final Map<String, RealtimeSegment> segments = new ConcurrentHashMap<>();
    private volatile SegmentFd mergeSegment;
    private volatile State state;

    public static enum State {
        /**
         * Before creation.
         */
        Begin("_ST_BEGIN"),
        /**
         * Metadata and other files have been written, from now on it is a valid realtime segment.
         */
        Created("_ST_CREATED"),
        /**
         * Rows in memory has dump to local disk.
         */
        Merged("_ST_MERGED"),
        /**
         * Segment has uploaded to indexr storage system.
         */
        Uploaded("_ST_UPLOADED"),
        /**
         * The end state.
         */
        Done("_ST_DONE");

        public final String fileName;

        State(String fileName) {
            this.fileName = fileName;
        }

        public static void commit(Path path, State state) throws IOException {
            IOUtil.createFileIfNotExist(path.resolve(state.fileName));
        }

        public static State getState(Path path) throws IOException {
            if (!Files.exists(path)) {
                return Begin;
            }
            try (Stream<Path> paths = Files.list(path)) {
                Set<String> stateFiles = paths
                        .map(p -> p.getFileName().toString())
                        .filter(n -> n.startsWith("_ST_") || n.startsWith("_st_"))
                        .collect(Collectors.toSet());
                State stat = Begin;
                for (State st : State.values()) {
                    if (stateFiles.contains(st.fileName) || stateFiles.contains(st.fileName.toLowerCase())) {
                        stat = st;
                    }
                }
                return stat;
            }
        }
    }

    private RTSGroup(Path path,
                     String tableName,
                     Metadata metadata,
                     State state,
                     boolean isRecovering,
                     boolean allowAddSegment) {
        this.path = path;
        this.tableName = tableName;
        this.metadata = metadata;
        this.state = state;
        this.isRecovering = isRecovering;
        this.allowAddSegment = allowAddSegment;
    }

    private static class Metadata {
        @JsonProperty("version")
        public final int version;
        @JsonProperty("name")
        public final String name;
        @JsonProperty("schema")
        public final SegmentSchema schema;
        @JsonProperty("createTime")
        public final long createTime;
        @JsonProperty("dims")
        public final List<String> dims;
        @JsonProperty("metrics")
        public final List<Metric> metrics;
        @JsonProperty("nameToAlias")
        public final Map<String, String> nameToAlias;
        @JsonProperty("grouping")
        public final boolean grouping;
        @JsonProperty("mode")
        public final String modeName;
        @JsonIgnore
        public final SegmentMode mode;

        @JsonCreator
        public Metadata(@JsonProperty("version") int version,
                        @JsonProperty("name") String name,
                        @JsonProperty("schema") SegmentSchema schema,
                        @JsonProperty("createTime") long createTime,
                        @JsonProperty("dims") List<String> dims,
                        @JsonProperty("metrics") List<Metric> metrics,
                        @JsonProperty("nameToAlias") Map<String, String> nameToAlias,
                        @JsonProperty("grouping") boolean grouping,
                        @JsonProperty("compress") Boolean compress,
                        @JsonProperty("mode") String modeName) {
            this.version = version;
            this.name = name;
            this.schema = schema;
            this.createTime = createTime;
            this.dims = dims;
            this.metrics = metrics;
            this.nameToAlias = nameToAlias;
            this.grouping = grouping;
            this.mode = SegmentMode.fromNameWithCompress(modeName, compress);
            this.modeName = this.mode.name();
        }
    }

    public static RTSGroup open(Path path, String tableName) throws IOException {
        return open(path, tableName, null, null, null, null, null, false, null);
    }

    public static RTSGroup open(Path path,
                                String tableName,
                                String name,
                                SegmentSchema schema,
                                List<String> dims,
                                List<Metric> metrics,
                                Map<String, String> nameToAlias,
                                boolean grouping,
                                SegmentMode mode) throws IOException {
        Preconditions.checkState(path != null && !Strings.isEmpty(tableName));

        State state = State.getState(path);
        switch (state) {
            case Begin: {
                if (name == null || schema == null) {
                    logger.info("Remove invalid rts group({}) folder. [table: {}, path: {}]",
                            state.name(), tableName, path);
                    Try.on(() -> FileUtils.deleteDirectory(path.toFile()),
                            1, logger,
                            String.format("Delete rts group folder failed. [table: %s, rtsg: %s]",
                                    tableName, name));
                    return null;
                }
                if (!Files.exists(path)) {
                    Files.createDirectories(path);
                }
                long createTime = System.currentTimeMillis();
                Metadata metadata = new Metadata(
                        Version.LATEST_ID,
                        name,
                        schema,
                        createTime,
                        dims,
                        metrics,
                        nameToAlias,
                        grouping,
                        null,
                        mode.name()
                );
                JsonUtil.save(path.resolve("metadata.json"), metadata);
                RTSGroup rtsGroup = new RTSGroup(path, tableName, metadata, state, false, false);
                rtsGroup.commitState(State.Created);
                rtsGroup.allowAddSegment = true;
                return rtsGroup;
            }
            case Created:
            case Merged: {
                Metadata metadata = JsonUtil.load(path.resolve("metadata.json"), Metadata.class);
                RTSGroup rtsGroup = new RTSGroup(
                        path,
                        tableName,
                        metadata,
                        state,
                        true,
                        state == State.Created);
                rtsGroup.loadLocalSegments();
                return rtsGroup;
            }
            case Uploaded:
            case Done:
                logger.info("Remove invalid rts group({}) folder. [table: {}, path{}]",
                        state.name(), tableName, path);
                Try.on(() -> FileUtils.deleteDirectory(path.toFile()),
                        1, logger,
                        String.format("Delete rts group folder failed. [table: %s, rtsg: %s]",
                                tableName, name));
                return null;
            default:
                return null;
        }
    }

    public List<String> dims() {
        return metadata.dims;
    }

    public List<Metric> metrics() {
        return metadata.metrics;
    }

    public boolean grouping() {
        return metadata.grouping;
    }

    public void setHandlePeriod(long period) {
        handlePeriod = period;
    }

    public void setFastPeriod(long period) {
        fastPeriod = period;
    }

    public Path path() {
        return path;
    }

    public String tableName() {
        return tableName;
    }

    public long createTime() {
        return metadata.createTime;
    }

    public State state() {
        return state;
    }

    public boolean isRecovering() {
        return isRecovering;
    }

    public Map<String, RealtimeSegment> realtimeSegments() {
        return segments;
    }

    public boolean hasUploaded() {
        return state == State.Uploaded || state == State.Done;
    }

    public boolean timeToUpload(long uploadPeriodMS, long maxRow) {
        return System.currentTimeMillis() - metadata.createTime >= uploadPeriodMS || rowCount() >= maxRow;
    }

    public synchronized RealtimeSegment addSegment(Supplier<RealtimeSegment> supplier) {
        if (!allowAddSegment) {
            return null;
        }
        RealtimeSegment rts = supplier.get();
        if (rts == null) {
            return null;
        } else {
            segments.put(rts.name(), rts);
            return rts;
        }
    }

    public void disallowAddSegment() {
        allowAddSegment = false;
    }

    public boolean allowAddSegment() {
        return allowAddSegment;
    }

    public synchronized boolean tryStartMerge() {
        if (!allowAddSegment) {
            return true;
        }
        if (isIngesting()) {
            return false;
        }
        allowAddSegment = false;
        return true;
    }

    private void commitState(State state) throws IOException {
        this.state = state;
        State.commit(path, state);
    }

    private boolean isIngesting() {
        boolean ok = false;
        for (RealtimeSegment rts : segments.values()) {
            if (rts.state() == RealtimeSegment.State.Created) {
                if (ok) {
                    // It could happen when ingesting segment is failed.
                    logger.warn("More than one segment is ingesting. [table: {}, rtsg: {}]",
                            tableName, metadata.name);
                }
                ok = true;
            }
        }
        return ok;
    }

    private void loadLocalSegments() throws IOException {
        List<Path> segmentPaths;
        try (Stream<Path> paths = Files.list(path)) {
            segmentPaths = paths
                    .filter(p -> Files.isDirectory(p) && p.getFileName().toString().startsWith("rts."))
                    .collect(Collectors.toList());
        }
        List<RealtimeSegment> localSegments = new ArrayList<>();
        for (Path path : segmentPaths) {
            RealtimeSegment rts = RealtimeSegment.open(path, tableName, path.getFileName().toString(), metadata.schema);
            localSegments.add(rts);
        }
        localSegments.forEach(rts -> segments.put(rts.name(), rts));
    }

    private boolean recoverFromMergeSegment() {
        Preconditions.checkState(state == State.Merged);
        logger.debug("Start recover rts group data from merge segment. [table: {}, rtsg: {}]",
                tableName, metadata.name);
        // Merge segemnt already exists, we just need to load it.
        mergeSegment = Try.on(
                () -> IntegratedSegment.Fd.create(metadata.name, path.resolve("integrated")),
                1, logger,
                String.format("Open merge segment failed. [table: %s, rtsg: %s]",
                        tableName, metadata.name));
        if (mergeSegment != null) {
            logger.debug("Recover rts group from merge segment completed. [table: {}, rtsg: {}]",
                    tableName, metadata.name);
            return true;
        } else {
            logger.debug("Recover rts group from merge segment failed. [table: {}, rtsg: {}]",
                    tableName, metadata.name);
            return false;
        }
    }

    private boolean recoverFromSubSegments(RTResources rtResources) {
        Preconditions.checkState(state == State.Created);
        logger.debug("Recover data from sub segments . [table: {}, rtsg: {}]",
                tableName, metadata.name);

        boolean ok = true;
        List<RealtimeSegment> toRemove = new ArrayList<>();
        for (RealtimeSegment rts : segments.values()) {
            if (!rts.isRecovering()) {
                continue;
            }
            switch (rts.state()) {
                case Begin:
                    // A rtseg in begin status means it is empty, safe to be removed.
                    toRemove.add(rts);
                    break;
                case Created:
                case RTIFinished:
                case Saved:
                    ok = Try.on(
                            () -> rts.recover(metadata.dims, metadata.metrics, metadata.nameToAlias, metadata.grouping, rtResources),
                            1, logger,
                            String.format("Recover segment failed. [table: %s, rtsg: %s, segment: %s]",
                                    tableName, metadata.name, rts.name()));
                    break;
                default:
                    throw new IllegalStateException("illegal state: " + rts.state().name());
            }
            if (!ok) {
                break;
            }
        }
        toRemove.forEach(rts -> segments.remove(rts.name()));

        if (ok) {
            logger.debug("Recover rts group from sub segments completed. [table: {}, rtsg: {}]",
                    tableName, metadata.name);
        } else {
            logger.debug("Recover rts group from sub segment failed. [table: {}, rtsg: {}]",
                    tableName, metadata.name);
        }

        return ok;
    }

    private boolean recover(RTResources rtResources) {
        Preconditions.checkState(isRecovering);
        boolean ok;
        switch (state) {
            case Created:
                ok = recoverFromSubSegments(rtResources);
                break;
            case Merged:
                ok = recoverFromMergeSegment();
                break;
            default:
                throw new IllegalStateException("illegal state: " + state.name());
        }
        if (ok) {
            isRecovering = false;
        }
        return ok;
    }

    private boolean saveToDisk(RTResources rtResources) {
        boolean allOk = true;
        List<RealtimeSegment> toRemove = new ArrayList<>();
        for (RealtimeSegment rts : segments.values()) {
            boolean ok = false;
            switch (rts.state()) {
                case Begin:
                    toRemove.add(rts);
                    ok = true;
                    break;
                case Created:
                    // Still ingesting.
                    //logger.debug("Segment is ingesting, ignore. [table: {}, rtsg: {}, segment: {}]", tableName, name, rts.name());
                    break;
                case RTIFinished:
                    SegmentFd savedSegment = Try.on(
                            () -> rts.saveToDisk(metadata.version, metadata.mode, rtResources),
                            1, logger,
                            String.format("Save in memory rows to disk failed. [table: %s, rtsg: %s, segment: %s]",
                                    tableName, metadata.name, rts.name()));
                    if (savedSegment != null) {
                        ok = Try.on(
                                () -> rts.commitState(RealtimeSegment.State.Saved),
                                1, logger,
                                String.format("Commit saved state failed. [table: %s, rtsg: %s, segment: %s]",
                                        tableName, metadata.name, rts.name()));
                    }
                    break;
                case Saved:
                    ok = true;
                    break;
                default:
                    throw new IllegalStateException("illegal state: " + rts.state().name());
            }
            allOk &= ok;
        }
        toRemove.forEach(rts -> segments.remove(rts.name()));
        return allOk;
    }

    private SegmentFd doMerge() {
        boolean ok = true;

        ArrayList<RealtimeSegment> sortedRTSs = new ArrayList<>(segments.values());
        sortedRTSs.sort((s1, s2) -> s1.name().compareTo(s2.name()));

        List<SegmentFd> savedSegments = new ArrayList<>();
        for (RealtimeSegment rts : sortedRTSs) {
            Preconditions.checkState(rts.state() == RealtimeSegment.State.Saved,
                    "Segment [%s] should be in [%s] state, but [%s]",
                    rts.name(), RealtimeSegment.State.Saved, rts.state());
            SegmentFd savedSegment = Try.on(
                    rts::getSavedSegment,
                    1, logger,
                    String.format("Get saved segmet failed. [table: %s, rtsg: %s, segment: %s]",
                            tableName, metadata.name, rts.name()));
            if (savedSegment == null) {
                ok = false;
                break;
            } else {
                savedSegments.add(savedSegment);
            }
        }
        if (!ok) {
            //IOUtils.closeQuietly(mergeSegment);
            return null;
        }

        Path mergePath = path.resolve("merge");
        StorageSegment mergeSegment = Try.on(
                () -> RTSMerge.merge(
                        metadata.grouping,
                        metadata.schema,
                        metadata.mode,
                        metadata.dims,
                        metadata.metrics,
                        savedSegments,
                        mergePath,
                        metadata.name
                ),
                1, logger,
                String.format("Merge segment failed. [table: %s, rtsg: %s]",
                        tableName, metadata.name));
        if (mergeSegment == null) {
            return null;
        }

        Path integratedPath = path.resolve("integrated");
        SegmentFd integratedSegment = Try.on(
                () -> IntegratedSegment.Fd.create(mergeSegment, integratedPath, true),
                1, logger,
                String.format("Integrate merge segmet failed. [table: %s, rtsg: %s]",
                        tableName, metadata.name));

        IOUtil.closeQuietly(mergeSegment);
        return integratedSegment;
    }

    private boolean merge() {
        Preconditions.checkState(state == State.Created);
        logger.debug("Start merge rts group. [table: {}, rtsg: {}]", tableName, metadata.name);

        long lastTime = System.currentTimeMillis();
        long rowCount = rowCount();
        if (mergeSegment == null) {
            this.mergeSegment = doMerge();
        }
        boolean ok = mergeSegment != null;
        if (ok) {
            ok = Try.on(
                    () -> commitState(State.Merged),
                    1, logger,
                    String.format("Commit merge state failed. [table: %s, rtsg: %s]",
                            tableName, metadata.name));
            long mergeRowCount = mergeSegment.info().rowCount();
            logger.info("Merge rts group completed. [took {}, merge {}, rows {} ({})]. [table: {}, rtsg: {}]",
                    String.format("%.2fs", (double) (System.currentTimeMillis() - lastTime) / 1000),
                    rowCount,
                    mergeSegment.info().rowCount(),
                    String.format("%.2f%%", (double) mergeRowCount / rowCount * 100),
                    tableName, metadata.name);
        }
        return ok;
    }

    private boolean upload(SegmentUploader uploader) {
        Preconditions.checkState(state == State.Merged && mergeSegment != null);
        logger.debug("Start upload rts group. [table: {}, rtsg: {}]",
                tableName, metadata.name);

        boolean ok = true;
        if (mergeSegment.info().rowCount() > 0) {
            ok = Try.on(
                    () -> {
                        try (Segment segment = mergeSegment.open()) {
                            uploader.upload((StorageSegment) segment, false);
                        }
                    },
                    1, logger,
                    String.format("Upload merge segment failed. [table: %s, rtsg: %s]",
                            tableName, metadata.name));
        } else {
            logger.info("Empty rts group, ignore. [table: {}, rtsg: {}]",
                    tableName, metadata.name);
        }
        if (!ok) {
            return false;
        }

        ok = Try.on(
                () -> commitState(State.Uploaded),
                1, logger,
                String.format("Commit upload state failed. [table: %s, rtsg: %s]",
                        tableName, metadata.name));
        if (ok) {
            logger.info("Upload rts group completed. [table: {}, rtsg: {}]",
                    tableName, metadata.name);
        }
        return ok;
    }

    private boolean delete() {
        String tn = tableName;
        String rtsgName = metadata.name;
        File deletePath = path.toFile();

        new DelayTask(() ->
        {
            logger.debug("Delete rts group files. [table: {}, rtsg: {}]",
                    tn, rtsgName);
            FileUtils.deleteDirectory(deletePath);
        }, 5 * 60 * 1000).submit();

        return true;
    }

    /**
     * A thread may constantly call this method to finally push the segment into storage system.
     *
     * @return -1: This rts group is done it's job, no more calls to handle; 0: should immediately call again; else: Next call period.
     */
    public long handle(long uploadPeriodMS,
                       long maxRow,
                       SegmentUploader uploader,
                       RTResources rtResources) {
        Preconditions.checkState(state != State.Begin);

        if (isRecovering) {
            return recover(rtResources) ? fastPeriod : handlePeriod;
        }

        switch (state) {
            case Created: {
                if (!saveToDisk(rtResources)) {
                    return handlePeriod;
                }
                if (!allowAddSegment
                        || (timeToUpload(uploadPeriodMS, maxRow) && tryStartMerge())) {
                    // Check again, some new rts could be added into.
                    if (!saveToDisk(rtResources)) {
                        return handlePeriod;
                    }
                    return merge() ? fastPeriod : handlePeriod;
                } else {
                    return handlePeriod;
                }
            }
            case Merged: {
                if (!upload(uploader)) {
                    return handlePeriod;
                } else {
                    // Remove this rtsg later, wait for other nodes to load its segment.
                    return deleteDelayPeriod;
                }
            }
            case Uploaded: {
                return delete() ? -1 : handlePeriod;
            }
            default:
                throw new IllegalStateException("illegal state: " + state.name());
        }
    }


    // ============================================
    // Segment interface implementation
    // ============================================

    @Override
    public int version() {
        return metadata.version;
    }

    @Override
    public SegmentMode mode() {
        return metadata.mode;
    }

    @Override
    public boolean isRealtime() {
        return true;
    }

    @Override
    public String name() {
        return metadata.name;
    }

    @Override
    public SegmentSchema schema() {
        return metadata.schema;
    }

    @Override
    public boolean isColumned() {
        // Always false.
        return false;
    }

    @Override
    public long rowCount() {
        // We always get row valueCount from sub rts, as the merge segment could be less than sum of
        // sub rts because of grouping.
        long rowCount = 0;
        for (RealtimeSegment rts : segments.values()) {
            rowCount += rts.rowCount();
        }
        return rowCount;
    }

    @Override
    public ColumnNode columnNode(int colId) throws IOException {
        List<ColumnNode> subNodes = new ArrayList<>();
        for (RealtimeSegment rts : segments.values()) {
            subNodes.add(rts.columnNode(colId));
        }
        return ColumnNode.merge(subNodes, metadata.schema.columns.get(colId).getDataType());
    }

    @Override
    public InfoSegment info() {
        return this;
    }

    /**
     * This method is only used for testing. Never use it in real production.
     */
    @Override
    public Segment open(IndexMemCache indexMemCache, ExtIndexMemCache extIndexMemCache, PackMemCache packMemCache) throws IOException {
        if (mergeSegment != null) {
            return mergeSegment.open(indexMemCache, extIndexMemCache, packMemCache);
        }
        return new Segment() {
            SegmentFdRowIterator iterator = new SegmentFdRowIterator(
                    new ArrayList<>(segments.values()),
                    indexMemCache,
                    packMemCache);
            SegmentFdRowIterator rt;

            @Override
            public int version() {
                return metadata.version;
            }

            @Override
            public SegmentMode mode() {
                return metadata.mode;
            }

            @Override
            public String name() {
                return metadata.name;
            }

            @Override
            public SegmentSchema schema() {
                return metadata.schema;
            }

            @Override
            public long rowCount() {
                return RTSGroup.this.rowCount();
            }

            @Override
            public RowTraversal rowTraversal() {
                rt = iterator;
                return new RowTraversal() {
                    @Override
                    public Iterator<Row> iterator() {
                        return iterator;
                    }
                };
            }

            @Override
            public void close() throws IOException {
                if (rt != null) {
                    rt.close();
                    rt = null;
                }
            }
        };
    }
}
