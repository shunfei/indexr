package io.indexr.segment.rt;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.indexr.segment.SegmentUploader;
import io.indexr.segment.storage.Version;
import io.indexr.util.DelayRepeatTask;
import io.indexr.util.Trick;
import io.indexr.util.Try;

public class RealtimeTable implements Closeable {
    private static final Logger logger = LoggerFactory.getLogger(RealtimeTable.class);
    private static final ScheduledExecutorService notifyService = Executors.newScheduledThreadPool(1);

    // Too many rts group usually means the delay of handle process.
    private static final int WarnRTSGroupSize = 3;
    // Make it looks like a number, easy to do arithmetical comparation. Like time >= from.
    private static final DateTimeFormatter timeFormatter = DateTimeFormatter.ofPattern("yyyyMMddHHmm");
    private static final long ingestRetryPeriod = TimeUnit.SECONDS.toMillis(5);
    private static final long handleRetryPeriod = TimeUnit.SECONDS.toMillis(5);
    private static final long handleFastPeriod = TimeUnit.MILLISECONDS.toMillis(200);
    private static final long rtsgDeleteDelayPeriod = TimeUnit.MINUTES.toMillis(10);

    private final String tableName;
    private final Path localDir;
    private final SegmentUploader uploader;
    private final ScheduledExecutorService rtHandleService;
    private final RTResources rtResources;

    private final Map<String, RTSGroup> rtsGroups = new ConcurrentHashMap<>();

    private volatile RealtimeSetting setting;
    private volatile State state;

    private ThreadLocal<Long> ingestPhase = ThreadLocal.withInitial(() -> 0L);
    private volatile IngestThread ingestThread;
    private final Object ingestThreadLock = new Object();

    public static enum State {
        RECOVER,
        RUN,
        STOP,
        CLOSE;
    }

    public RealtimeTable(String tableName,
                         Path localDir,
                         SegmentUploader uploader,
                         ScheduledExecutorService rtHandleService,
                         RTResources rtResources) {
        this.tableName = tableName;
        this.localDir = localDir;
        this.uploader = uploader;
        this.rtHandleService = rtHandleService;
        this.rtResources = rtResources;
        this.state = State.STOP;

        if (!Files.exists(localDir)) {
            Try.on(() -> Files.createDirectories(localDir), logger);
        }
        loadLocalRTSGroups().forEach(this::addRTSGroup);
    }

    /**
     * Start to ingest evnets from datasource.
     */
    public synchronized boolean start() {
        if (state == State.CLOSE || setting == null) {
            return false;
        }
        if (state == State.RECOVER || state == State.RUN) {
            return true;
        }

        logger.info("Starting realtime table. [table: {}]", tableName);

        state = containsRecoverRTSG() ? State.RECOVER : State.RUN;

        safeInterruptIngest();
        startIngest();

        return true;
    }

    /**
     * Stop ingesting events into this realtime table.
     */
    public synchronized boolean stop() {
        if (state == State.CLOSE) {
            return false;
        }
        if (state == State.STOP) {
            return true;
        }
        logger.info("Stoping realtime table and upload local realtime segments. [table: {}]", tableName);

        state = State.STOP;
        if (setting != null) {
            Try.on(setting.fetcher::close, logger);
        }
        //safeInterruptIngest();

        for (RTSGroup rtsGroup : rtsGroups.values()) {
            rtsGroup.setHandlePeriod(200);
            rtsGroup.setFastPeriod(0);
        }

        return true;
    }

    public synchronized void updateSetting(RealtimeSetting newSetting) {
        if (state == State.CLOSE) {
            return;
        }
        logger.info("Updating realtime table setting. [table: {}]", tableName);

        if (newSetting == null) {
            stop();
            setting = null;
            state = State.STOP;
            return;
        }
        if (!newSetting.equals(setting)) {
            RealtimeSetting oldSetting = setting;
            setting = newSetting;
            if (oldSetting != null) {
                Try.on(oldSetting.fetcher::close, logger);
            }
            //safeInterruptIngest();
        }
    }

    /**
     * Close this realtime table. Job is done.
     * This method only called before process exit.
     */
    @Override
    public synchronized void close() {
        if (state == State.CLOSE) {
            return;
        }
        logger.info("Closing realtime table. [table: {}]", tableName);
        state = State.CLOSE;

        if (setting != null) {
            Try.on(setting.fetcher::close, logger);
        }
        //safeInterruptIngest();
    }

    private void startIngest() {
        synchronized (ingestThreadLock) {
            if (ingestThread != null && ingestThread.isAlive()) {
                return;
            }
            DelayRepeatTask task = new DelayRepeatTask() {
                @Override
                public long run() {
                    Long nextPeriod = Try.on(
                            RealtimeTable.this::ingest,
                            1, logger,
                            String.format("Ingest failed. [table: %s]", tableName));
                    return nextPeriod == null ? ingestRetryPeriod : nextPeriod;
                }

                @Override
                public void onStart() {
                    logger.info("Ingest progress start. [table: {}]", tableName);
                }

                @Override
                public void onComplete() {
                    logger.info("Ingest progress exit. [table: {}]", tableName);
                }
            };

            IngestThread thread = new IngestThread(
                    () -> DelayRepeatTask.runTaskInThread(task, 0),
                    tableName);
            thread.start();
            this.ingestThread = thread;
        }
    }

    private long ingest() {
        synchronized (ingestThreadLock) {
            if (state == State.CLOSE
                    || (!isIngestState() || !setting.ingest)) {
                if (Thread.currentThread() == ingestThread) {
                    ingestThread = null;
                }
                return -1;
            }
            if (Thread.currentThread() != ingestThread) {
                return -1;
            }
        }
        if (!rtResources.reserveMemory(4 << 20)) {
            logger.info("Not enough memory, wait... [table: {}]", tableName);
            return ingestRetryPeriod;
        }

        RTSGroup ingestRTSG = latestRTSG();
        if (ingestRTSG == null || !isRTSGIngestable(ingestRTSG)) {
            if (ingestRTSG != null) {
                // Make the last rtsg upload faster.
                ingestRTSG.disallowAddSegment();
            }
            logger.info("Create new rts group. [table: {}]", tableName);

            ZonedDateTime now = ZonedDateTime.now(setting.timeZone.toZoneId());
            String rtsgPathStr = String.format("rtsg.%s.%s.seg", now.format(timeFormatter), UUID.randomUUID());
            Path rtsgPath = localDir.resolve(rtsgPathStr);
            String rtsgName = "rt/" + rtsgPathStr;
            RTSGroup rtsGroup = Try.on(
                    () -> RTSGroup.open(
                            rtsgPath,
                            tableName,
                            rtsgName,
                            setting.schema,
                            setting.dims,
                            setting.metrics,
                            setting.nameToAlias,
                            setting.grouping,
                            setting.mode),
                    1, logger,
                    String.format("Create new rts group failed. [table: %s, rtsg: %s]", tableName, rtsgName));
            if (rtsGroup == null) {
                return ingestRetryPeriod;
            }

            logger.info("New rts group. [table: {}, rtsg: {}]", tableName, rtsgName);

            addRTSGroup(rtsGroup);
            ingestRTSG = rtsGroup;
        }
        return doIngest(ingestRTSG, setting);
    }

    private long doIngest(RTSGroup rtsGroup, RealtimeSetting setting) {
        Boolean ok = Try.on(
                () -> setting.fetcher.ensure(setting.schema),
                1, logger,
                String.format("Ensure ingest fecther stream failed. [table: %s]", tableName));
        if (ok == null || !ok) {
            logger.warn("Fetcher not ready yet, wait. [table: {}, fetcher: {}]", tableName, setting.fetcher);
            return ingestRetryPeriod;
        }
        // The open/close state should match.
        if (!isIngestState()) {
            Try.on(setting.fetcher::close, logger);
            return ingestRetryPeriod;
        }
        Boolean hasNext = Try.on(setting.fetcher::hasNext, 1, logger);
        if (hasNext == null) {
            return ingestRetryPeriod;
        }
        if (!hasNext) {
            return ingestRetryPeriod;
        }
        RealtimeSegment rts = rtsGroup.addSegment(() -> Try.on(
                () -> {
                    ZonedDateTime now = ZonedDateTime.now(setting.timeZone.toZoneId());
                    String segmentName = String.format("rts.%s.%s.seg", now.format(timeFormatter), UUID.randomUUID());
                    Path segmentPath = rtsGroup.path().resolve(segmentName);
                    return RealtimeSegment.open(segmentPath, tableName, segmentName, setting.schema);
                },
                1, logger,
                String.format("Create new segment failed. [table: %s, rtsg: %s]", tableName, rtsGroup.name())));
        if (rts == null) {
            // If rts group is not allow to add new segment any more, then jump out and create a new one.
            return rtsGroup.allowAddSegment() ? ingestRetryPeriod : 0;
        }

        IngestThread thread = (IngestThread) Thread.currentThread();
        Runnable notifyTask = new Runnable() {
            long phase = ingestPhase.get();

            @Override
            public void run() {
                // Make sure the notify task only fire to current phase.
                if (phase == ingestPhase.get() && !thread.isInterrupted() && thread.isInterrupValid()) {
                    thread.interrupt();
                }
            }
        };

        ScheduledFuture f = notifyService.schedule(
                syncRun(notifyTask),
                setting.savePeriodMS + 1000, // Wait a little time before interrupt the thread. It is better that the ingest process could return itself.
                TimeUnit.MILLISECONDS);
        long rt = 0;
        try {
            logger.info("Start ingest new segment. [table: {}, rtsg: {}, segment: {}]", tableName, rtsGroup.name(), rts.name());
            long rowCount = rts.realtimeIngest(
                    setting.dims,
                    setting.metrics,
                    setting.nameToAlias,
                    setting.tagSetting,
                    setting.ignoreStrategy,
                    setting.grouping,
                    setting.fetcher,
                    setting.maxRowInMemory,
                    setting.savePeriodMS,
                    rtResources);
            logger.info("Finish ingest segment. [ingested: {}, got: {} ({})]. [table: {}, rtsg: {}, segment: {}]",
                    rowCount, rts.rowCount(), String.format("%.2f%%", (double) rts.rowCount() / rowCount * 100), tableName, rtsGroup.name(), rts.name());
        } catch (Exception e) {
            logger.error("Ingest segment failed. [table: {}, rtsg: {}, segment: {}]", tableName, rtsGroup.name(), rts.name(), e);
            rt = ingestRetryPeriod;
        } finally {
            f.cancel(false);
            // Clear interrupt flag.
            syncRun(() -> {
                ingestPhase.set(ingestPhase.get() + 1);
                Thread.interrupted();
            }).run();
            Try.on(() -> rts.commitState(RealtimeSegment.State.RTIFinished),
                    1, logger,
                    String.format("Create new segment failed. [table: %s, segment: %s]", tableName, rtsGroup.name()));
        }
        return rt;
    }

    private void addRTSGroup(RTSGroup rtsGroup) {
        if (rtsGroups.containsKey(rtsGroup.name())) {
            return;
        }
        rtsGroups.put(rtsGroup.name(), rtsGroup);
        startHandle(rtsGroup);

        int count = 0;
        for (RTSGroup rtsg : rtsGroups.values()) {
            if (!rtsg.hasUploaded()) {
                count++;
            }
        }
        if (count >= WarnRTSGroupSize) {
            logger.warn("Too many rts groups! [table: {}, rtsg valueCount: {}]", tableName, count);
        }
    }

    private void startHandle(RTSGroup rtsGroup) {
        DelayRepeatTask task = new DelayRepeatTask() {
            @Override
            public long run() {
                if (Thread.interrupted()) {
                    return -1;
                }
                long uploadPeriod = isIngestState() ? setting.uploadPeriodMS : 0;
                long maxRow = isIngestState() ? setting.maxRowInRealtime : 0;
                Long nextPeriod = Try.on(
                        () -> rtsGroup.handle(
                                uploadPeriod,
                                maxRow,
                                uploader,
                                rtResources),
                        1, logger,
                        String.format("Handle rts group failed. [table: %s, rtsg: %s]", tableName, rtsGroup.name()));

                if (state == State.RECOVER && !containsRecoverRTSG()) {
                    logger.info("Recover realtime table completed. [table: {}]", tableName);
                    state = State.RUN;
                }

                return nextPeriod == null ? handleRetryPeriod : nextPeriod;
            }

            @Override
            public void onStart() {
                logger.info("Handle progress start. [table: {}, rtsg: {}]", tableName, rtsGroup.name());
            }

            @Override
            public void onComplete() {
                logger.info("Handle progress exit. [table: {}, rtsg: {}]", tableName, rtsGroup.name());
                logger.info("Remove rts group. [table: {}, rtsg: {}]", tableName, rtsGroup.name());
                rtsGroups.remove(rtsGroup.name());
            }
        };
        rtsGroup.setHandlePeriod(handleRetryPeriod);
        rtsGroup.setFastPeriod(handleFastPeriod);
        DelayRepeatTask.runTask(task, 200, rtHandleService);
    }

    private void safeInterruptIngest() {
        IngestThread thread = this.ingestThread;
        if (thread != null && thread.isInterrupValid()) {
            thread.interrupt();
        }
    }

    private Runnable syncRun(Runnable r) {
        return () -> {
            synchronized (RealtimeTable.this) {
                r.run();
            }
        };
    }

    // Wether this rtsg can be ingested or not.
    private boolean isRTSGIngestable(RTSGroup rtsGroup) {
        return rtsGroup.version() == Version.LATEST_ID
                && isRTSGSettingOk(rtsGroup)
                && rtsGroup.allowAddSegment()
                && !rtsGroup.timeToUpload(setting.uploadPeriodMS, setting.maxRowInRealtime);
    }

    // Wether this rtsg holds the latest setting.
    private boolean isRTSGSettingOk(RTSGroup rtsGroup) {
        Path rtsgPath = rtsGroup.path();
        return rtsgPath.getParent().equals(localDir)
                && rtsGroup.schema().equals(setting.schema)
                && rtsGroup.grouping() == setting.grouping
                && Trick.equals(rtsGroup.dims(), setting.dims)
                && Trick.equals(rtsGroup.metrics(), setting.metrics);
    }

    public String tableName() {
        return tableName;
    }

    public Map<String, RTSGroup> rtsGroups() {
        return rtsGroups;
    }

    public State curState() {
        return state;
    }

    private boolean containsRecoverRTSG() {
        for (RTSGroup rtsg : rtsGroups.values()) {
            if (rtsg.isRecovering()) {
                return true;
            }
        }
        return false;
    }

    public boolean isIngestState() {
        return state == State.RECOVER || state == State.RUN;
    }

    public boolean isSafeToExit() {
        if (state != State.CLOSE && state != State.STOP) {
            return false;
        }
        for (RTSGroup rtsg : rtsGroups.values()) {
            if (!rtsg.hasUploaded()) {
                return false;
            }
        }
        return true;
    }

    private List<RTSGroup> loadLocalRTSGroups() {
        List<Path> rtsgPaths = Try.on(
                () -> {
                    try (Stream<Path> paths = Files.list(localDir)) {
                        return paths.filter(p -> {
                            if (!Files.isDirectory(p)) {
                                return false;
                            }
                            String name = p.getFileName().toString();
                            if (!name.startsWith("rtsg.")) {
                                return false;
                            }
                            if (rtsGroups.containsKey("rt/" + name)) {
                                return false;
                            }
                            return true;
                        }).collect(Collectors.toList());
                    }
                },
                1, logger,
                String.format("List rts group failed, ignored. [table: %s, local path: %s]", tableName, localDir));
        if (rtsgPaths == null) {
            return Collections.emptyList();
        }

        List<RTSGroup> localRTSGroups = new ArrayList<>();
        for (Path path : rtsgPaths) {
            RTSGroup rtsGroup = Try.on(
                    () -> RTSGroup.open(path, tableName),
                    1, logger,
                    String.format("Open rts group failed, ignored. [table: %s, path: %s]", tableName, path));
            if (rtsGroup != null) {
                localRTSGroups.add(rtsGroup);
            }
        }

        logger.info("load {} rts groups. [table: {}]", localRTSGroups.size(), tableName);
        return localRTSGroups;
    }

    private RTSGroup latestRTSG() {
        RTSGroup latest = null;
        for (RTSGroup rtsg : rtsGroups.values()) {
            latest = latest == null ? rtsg : rtsg.createTime() > latest.createTime() ? rtsg : latest;
        }
        return latest;
    }
}
