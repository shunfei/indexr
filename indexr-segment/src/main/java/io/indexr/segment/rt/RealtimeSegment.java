package io.indexr.segment.rt;

import com.google.common.base.Preconditions;

import org.apache.commons.io.IOUtils;
import org.apache.directory.api.util.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.indexr.io.ByteBufferWriter;
import io.indexr.segment.InfoSegment;
import io.indexr.segment.RowTraversal;
import io.indexr.segment.Segment;
import io.indexr.segment.SegmentFd;
import io.indexr.segment.SegmentSchema;
import io.indexr.segment.pack.ColumnNode;
import io.indexr.segment.pack.DPSegment;
import io.indexr.segment.pack.IndexMemCache;
import io.indexr.segment.pack.IntegratedSegment;
import io.indexr.segment.pack.OpenOption;
import io.indexr.segment.pack.PackMemCache;
import io.indexr.util.ByteBufferUtil;
import io.indexr.util.Holder;
import io.indexr.util.IOUtil;
import io.indexr.util.Serializable;
import io.indexr.util.Try;

public class RealtimeSegment implements InfoSegment, SegmentFd {
    private static final Logger logger = LoggerFactory.getLogger(RealtimeSegment.class);
    private static final Map<UTF8Row, UTF8Row> zeroRows = new HashMap<>(0);
    private static final int ROW_BUFFER_SIZE = 1 << 16;

    private final Path path;
    private final String table;
    private final String name;
    private final SegmentSchema schema;
    private final RealtimeIndex index;

    // Rows in memory.
    private volatile Map<UTF8Row, UTF8Row> rowsInMemory = zeroRows;
    private long rowInMemoryCount = 0;
    private long rowsMemoryUsage = 0;
    // Rows on disk.
    private volatile SegmentFd savedSegment;

    private volatile State state;
    private volatile boolean isRecovering = false;

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
         * Realtime ingestion has finished.
         */
        RTIFinished("_ST_RTIFINISHED"),
        /**
         * Rows in memory has dump to local disk.
         */
        Saved("_ST_SAVED");

        public final String fileName;

        State(String fileName) {
            this.fileName = fileName;
        }

        public static void commit(Path segmentPath, State state) throws IOException {
            IOUtil.createFileIfNotExist(segmentPath.resolve(state.fileName));
        }

        public static State getState(Path segmentPath) throws IOException {
            try (Stream<Path> paths = Files.list(segmentPath)) {
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

    private RealtimeSegment(Path path,
                            String table,
                            String name,
                            SegmentSchema schema,
                            State state,
                            boolean isRecovering) {
        this.path = path;
        this.table = table;
        this.name = name;
        this.schema = schema;
        this.state = state;
        this.isRecovering = isRecovering;
        this.index = new RealtimeIndex(schema);
    }

    public static RealtimeSegment open(Path path, String table, String name, SegmentSchema schema) throws IOException {
        Preconditions.checkState(path != null && !Strings.isEmpty(name) && schema != null);

        if (!Files.exists(path)) {
            Files.createDirectories(path);
        }
        State state = State.getState(path);
        switch (state) {
            case Begin: {
                RealtimeSegment segment = new RealtimeSegment(path, table, name, schema, state, false);
                segment.commitState(State.Created);
                return segment;
            }
            case Created:
            case RTIFinished:
            case Saved: {
                // A segment recovered from disk will never be ingested again.
                if (state == State.Created) {
                    state = State.RTIFinished;
                }
                RealtimeSegment segment = new RealtimeSegment(path, table, name, schema, state, true);
                segment.commitState(state);
                return segment;
            }
            default:
                return null;
        }
    }

    private Map<UTF8Row, UTF8Row> createMap() {
        return new ConcurrentSkipListMap<>(UTF8Row.dimBytesComparator());
    }

    public Path path() {
        return path;
    }

    public State state() {
        return state;
    }

    public boolean isRecovering() {
        return isRecovering;
    }

    public void commitState(State state) throws IOException {
        this.state = state;
        State.commit(path, state);
    }

    public long realtimeIngest(List<String> dims,
                               List<Metric> metrics,
                               Map<String, String> nameToAlias,
                               TagSetting tagSetting,
                               int ignoreStrategy,
                               boolean grouping,
                               Fetcher fetcher,
                               int maxRow,
                               long maxPeriod,
                               RTResources rtResources) throws Exception {
        Preconditions.checkState(state == State.Created);
        return ingestRows(
                dims,
                metrics,
                nameToAlias,
                grouping,
                tagSetting,
                ignoreStrategy,
                fetcher,
                maxRow,
                maxPeriod,
                true,
                rtResources);
    }

    /**
     * Ingest some rows from fetcher.
     *
     * @param dims           those felds used for sorting and grouping.
     * @param nameToAlias    fields can specify another name.
     * @param grouping       need grouping or not.
     * @param fetcher        the datasource.
     * @param maxRow         -1 means no limit.
     * @param maxPeriod      -1 means no limit.
     * @param writeCommitLog write commit log or not.
     * @return The row count we got in realtime segment.
     */
    private long ingestRows(
            List<String> dims,
            List<Metric> metrics,
            Map<String, String> nameToAlias,
            boolean grouping,
            TagSetting tagSetting,
            int ignoreStrategy,
            Fetcher fetcher,
            int maxRow,
            long maxPeriod,
            boolean writeCommitLog,
            RTResources rtResource
    ) throws Exception {
        logger.debug("Start ingest rows. [segment: {}]", name);
        // It could happens that recover process fail and then try more times.
        if (rowsInMemory != zeroRows) {
            Map map = rowsInMemory;
            rowsInMemory = zeroRows;
            map.clear();
        }
        long lastTime = System.currentTimeMillis();

        Thread thread = Thread.currentThread();
        if (thread instanceof IngestThread) {
            ((IngestThread) thread).setInterrupValid(true);
        }

        int realRowCount = 0;
        long ingestRowCount = 0;
        Map<UTF8Row, UTF8Row> rowsPool = createMap();
        rowsInMemory = rowsPool;
        rowsMemoryUsage = 0;
        rowInMemoryCount = 0;
        fetcher.setRowCreator(
                String.format("table:%s", table),
                new UTF8Row.Creator(grouping, schema.columns, dims, metrics, nameToAlias, tagSetting, ignoreStrategy));

        maxRow = maxRow < 0 ? Integer.MAX_VALUE : maxRow;
        long endTime = maxPeriod < 0 ? Long.MAX_VALUE : System.currentTimeMillis() + maxPeriod;
        endTime = endTime < 0 ? Long.MAX_VALUE : endTime;

        Holder<ByteBuffer> rowBuffer = null;
        ByteBufferWriter commitLog = null;
        Path commitLogPath = path.resolve("commitlog.bin");
        try {
            if (writeCommitLog) {
                FileChannel commitLogFile = FileChannel.open(
                        commitLogPath,
                        StandardOpenOption.WRITE,
                        StandardOpenOption.CREATE,
                        StandardOpenOption.TRUNCATE_EXISTING);
                commitLog = ByteBufferWriter.of(commitLogFile, 0);
                commitLog.setName(commitLogPath.toString());
                rowBuffer = new Holder<>(ByteBufferUtil.allocateDirect(ROW_BUFFER_SIZE));
            }

            boolean stop = false;
            while (!stop
                    && realRowCount < maxRow
                    && System.currentTimeMillis() < endTime) {
                try {
                    if (!fetcher.hasNext()) {
                        break;
                    }
                    List<UTF8Row> rows = fetcher.next();
                    if (rows.size() == 0) {
                        continue;
                    }
                    // Check thread interrupt after fetcher return and before save commitlog.
                    if (Thread.interrupted()) {
                        stop = true;
                    }
                    if (writeCommitLog) {
                        try {
                            Serializable.save(commitLog, rowBuffer, rows);
                        } catch (ClosedByInterruptException e) {
                            logger.warn("Failed to write commit log of rows: {}", rows, e);
                        }
                    }
                    long memory = 0;
                    for (UTF8Row row : rows) {
                        UTF8Row oldRow = rowsPool.get(row);
                        if (oldRow != null) {
                            oldRow.merge(row);
                            index.update(oldRow);
                            row.free();
                        } else {
                            rowsPool.put(row, row);
                            index.update(row);
                            rowInMemoryCount++;
                            realRowCount++;
                            memory += row.memoryUsage();
                        }
                        ingestRowCount++;
                    }
                    if (memory > 0) {
                        rowsMemoryUsage += memory;
                        rtResource.addMemoryUsageEx(memory);
                    }
                } catch (Exception e) {
                    if (e instanceof InterruptedException
                            || e instanceof ClosedByInterruptException) {
                        break;
                    } else {
                        throw e;
                    }
                }
            }
        } finally {
            // Prevent flush operation from being interrupted.
            if (thread instanceof IngestThread) {
                ((IngestThread) thread).setInterrupValid(false);
            }

            if (writeCommitLog && commitLog != null) {
                ByteBufferWriter _commitLog = commitLog;
                Try.on(() -> {
                            // Clear interrup flag before flush, try best to avoid ClosedByInterruptException.
                            Thread.interrupted();
                            _commitLog.flush();
                        },
                        2, // Try more times.
                        logger,
                        String.format("Failed to flush commitlog file: %s", commitLogPath.toString()));
            }
            if (commitLog != null) {
                Try.on(commitLog::close, logger);
            }
            Try.on(fetcher::commit, logger);
            if (rowBuffer != null) {
                ByteBufferUtil.free(rowBuffer.get());
            }

            logger.info("Table [{}]: consume msgs [{}], fail msgs [{}], ignore rows [{}], produce rows [{}], " +
                            "ingest rows [{}], final rows [{}], took time [{}], consume memory [{}K], " +
                            "total rt memoryusage [{}M], avaliable memory [{}M], segment: [{}]",
                    table,
                    fetcher.statConsume(),
                    fetcher.statFail(),
                    fetcher.statIgnore(),
                    fetcher.statProduce(),
                    ingestRowCount,
                    realRowCount,
                    String.format("%.2fs", (double) (System.currentTimeMillis() - lastTime) / 1000),
                    rowsMemoryUsage >>> 10,
                    rtResource.memoryUsage() >>> 20,
                    rtResource.freeMemory() >>> 20,
                    name);

            fetcher.statReset();
        }
        return ingestRowCount;
    }

    public void recover(List<String> dims,
                        List<Metric> metrics,
                        Map<String, String> nameToAlias,
                        boolean grouping,
                        RTResources rtResources) throws Exception {
        switch (state) {
            case Created:
            case RTIFinished:
                recoverFromCommitLog(dims, metrics, nameToAlias, grouping, rtResources);
                break;
            case Saved:
                //case Uploaded:
                recoverFromSavedSegment(rtResources);
                break;
            default:
                throw new IllegalStateException("illegal recover state: " + state.name());
        }
        isRecovering = false;
    }

    /**
     * Load data from commit log.
     */
    private long recoverFromCommitLog(List<String> dims,
                                      List<Metric> metrics,
                                      Map<String, String> nameToAlias,
                                      boolean grouping,
                                      RTResources rtResources) throws Exception {
        Preconditions.checkState(state == State.Created || state == State.RTIFinished);
        logger.debug("Recover realtime segment from commit log. [segment: {}]", name);
        long lastTime = System.currentTimeMillis();

        long rowCount;
        Path logPath = path.resolve("commitlog.bin");
        if (!Files.exists(logPath)) {
            logger.warn("Bin log file not found. [segment: {}]", name);
            rowCount = 0;
        } else {
            // At least 4 mb.
            rtResources.reserveMemoryEx(4 << 20);
            try (CommitLogFetcher commitLog = new CommitLogFetcher(logPath)) {
                commitLog.ensure(schema);
                rowCount = ingestRows(
                        dims,
                        metrics,
                        nameToAlias,
                        grouping,
                        null,
                        EventIgnoreStrategy.NO_IGNORE,
                        commitLog,
                        -1,
                        -1,
                        false,
                        rtResources);
            }
        }

        logger.debug("Recover from commit log [{}] rows completed, took [{}]. [segment: {}]",
                rowCount,
                String.format("%.2fs", (double) (System.currentTimeMillis() - lastTime) / 1000),
                name);
        return rowCount;
    }

    private void recoverFromSavedSegment(RTResources rtResources) throws Exception {
        Preconditions.checkState(state == State.Saved);
        logger.debug("Recover realtime segment from saved segment. [segment: {}]", name);

        SegmentFd segment = loadSavedSegment();
        setSavedSegment(segment, rtResources);

        logger.debug("Recover from segment [{}] rows completed. [segment: {}]", segment.info().rowCount(), name);
    }

    /**
     * Persistence rows in memory to local disk.
     */
    public SegmentFd saveToDisk(int version, boolean compress, RTResources rtResources) throws IOException {
        logger.debug("Start save in-memory rows to disk. [segment: {}]", name);
        long lastTime = System.currentTimeMillis();

        SegmentFd integratedSegment;
        Path dumpPath = path.resolve("dump");
        try (DPSegment dpSegment = DPSegment.open(version, dumpPath, name, schema, OpenOption.Overwrite)) {
            dpSegment.update().setCompress(compress);
            boolean ok = true;
            long rowCount = 0;
            for (UTF8Row row : rowsInMemory.values()) {
                if (Thread.interrupted()) {
                    ok = false;
                    break;
                } else {
                    dpSegment.add(row);
                    rowCount++;
                }
            }
            if (!ok) {
                logger.warn("Fail saveToDisk. [segment: {}]", name);
                IOUtils.closeQuietly(dpSegment);
                return null;
            }
            dpSegment.seal();

            Path integratedPath = path.resolve("integrated");
            integratedSegment = IntegratedSegment.Fd.create(dpSegment, integratedPath, true);

            Preconditions.checkState(dpSegment.rowCount() == rowCount());
        }

        long memory = rowsMemoryUsage;
        setSavedSegment(integratedSegment, rtResources);

        logger.debug("Save [{}] rows completed, took [{}], release memory [{}K]. [segment: {}]",
                rowCount(),
                String.format("%.2fs", (double) (System.currentTimeMillis() - lastTime) / 1000),
                memory >>> 10,
                name);
        return integratedSegment;
    }

    private void setSavedSegment(SegmentFd segment, RTResources rtResources) {
        this.savedSegment = segment;
        if (rowsInMemory != zeroRows) {
            Map<UTF8Row, UTF8Row> map = rowsInMemory;
            long memory = rowsMemoryUsage;

            rowsInMemory = zeroRows;
            rowInMemoryCount = 0;
            rowsMemoryUsage = 0;

            // Remove all rows and free its memory, help GC.
            Iterator<UTF8Row> it = map.values().iterator();
            while (it.hasNext()) {
                UTF8Row row = it.next();
                it.remove();
                row.free();
            }
            Preconditions.checkState(map.isEmpty(), "Realtime rows map should be emptpy now.");
            rtResources.decMemoryUsage(memory);
        }
    }

    /**
     * Try load saved segment from local disk.
     */
    private SegmentFd loadSavedSegment() throws IOException {
        Preconditions.checkState(state == State.Saved);
        return IntegratedSegment.Fd.create(name, path.resolve("integrated"));
    }

    public SegmentFd getSavedSegment() throws IOException {
        Preconditions.checkState(state == State.Saved);
        if (savedSegment != null) {
            return savedSegment;
        } else {
            return loadSavedSegment();
        }
    }

    // ============================================
    // Segment interface implementation
    // ============================================


    @Override
    public boolean isRealtime() {
        return true;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public SegmentSchema schema() {
        return schema;
    }

    @Override
    public long rowCount() {
        return savedSegment != null ? savedSegment.info().rowCount() : rowInMemoryCount;
    }

    @Override
    public ColumnNode columnNode(int colId) throws IOException {
        return savedSegment != null ? savedSegment.info().columnNode(colId) : index.columnNode(colId);
    }

    @Override
    public boolean isColumned() {
        return savedSegment != null && savedSegment.info().isColumned();
    }

    @Override
    public InfoSegment info() {
        if (savedSegment != null) {
            return savedSegment.info();
        } else {
            return this;
        }
    }

    @Override
    public Segment open(IndexMemCache indexMemCache, PackMemCache packMemCache) throws IOException {
        if (savedSegment != null) {
            return savedSegment.open(indexMemCache, packMemCache);
        } else {
            return new RTSeg(name, schema, rowsInMemory, rowInMemoryCount);
        }
    }

    private static class RTSeg implements Segment {
        final String name;
        final SegmentSchema schema;
        final Map<UTF8Row, UTF8Row> rows;
        final long size;

        public RTSeg(String name, SegmentSchema schema, Map<UTF8Row, UTF8Row> rows, long size) {
            this.name = name;
            this.schema = schema;
            this.rows = rows;
            this.size = size;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public SegmentSchema schema() {
            return schema;
        }

        @Override
        public long rowCount() {
            return size;
        }

        @Override
        public RowTraversal rowTraversal() {
            return () -> (Iterator) rows.values().iterator();
        }
    }
}
