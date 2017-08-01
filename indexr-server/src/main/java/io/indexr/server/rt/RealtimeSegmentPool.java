package io.indexr.server.rt;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.curator.framework.CuratorFramework;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.array.ByteArrayMethods;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import io.indexr.segment.SegmentFd;
import io.indexr.segment.SegmentLocality;
import io.indexr.segment.SegmentPool;
import io.indexr.segment.SegmentSchema;
import io.indexr.segment.SegmentUploader;
import io.indexr.segment.rt.RTSGroup;
import io.indexr.segment.rt.RTSGroupInfo;
import io.indexr.segment.rt.RealtimeSegment;
import io.indexr.segment.rt.RealtimeSetting;
import io.indexr.segment.rt.RealtimeTable;
import io.indexr.segment.storage.ColumnNode;
import io.indexr.server.IndexRConfig;
import io.indexr.server.TableSchema;
import io.indexr.server.ZkHelper;
import io.indexr.server.ZkHelper.ChildrenRefresher;
import io.indexr.util.GenericCompression;
import io.indexr.util.JsonUtil;
import io.indexr.util.Strings;
import io.indexr.util.Try;

/**
 * This segment pool cache all realtime segment fds.
 */
public class RealtimeSegmentPool implements SegmentPool, SegmentLocality {
    private static final Logger logger = LoggerFactory.getLogger(RealtimeSegmentPool.class);
    private static final Random random = new Random();
    private static final long RefreshSegmentPeriod = TimeUnit.SECONDS.toMillis(2);

    private ScheduledFuture refreshSegment;

    // The realtime table work in localhost.
    private final RealtimeTable localRealtimeTable;

    // The all realtime segments in system. name -> rtsg.
    private Map<String, RTSGroupInfo> rtsGroupInfoMap = Collections.emptyMap();
    // Cache all hosts which has realtime segments.
    private List<String> rttNodes = Collections.emptyList();

    private final ChildrenRefresher<HostSegmentInfo> rtsRefresher;

    private final String hostName;
    private final String tableName;
    private final IndexRConfig indexRConfig;
    private final CuratorFramework zkClient;
    private final String declarePath;

    private byte[] lastRTSGInfo;

    private boolean rtIngest;

    public RealtimeSegmentPool(String hostName,
                               String tableName,
                               IndexRConfig indexRConfig,
                               SegmentUploader uploader,
                               CuratorFramework zkClient,
                               ScheduledExecutorService notifyService,
                               ScheduledExecutorService rtHandleService) throws Exception {
        this.hostName = hostName;
        this.zkClient = zkClient;
        this.tableName = tableName;
        this.indexRConfig = indexRConfig;
        this.declarePath = IndexRConfig.zkRTTablePath(tableName);
        this.localRealtimeTable = new RealtimeTable(
                tableName,
                IndexRConfig.localRTPath(indexRConfig.getLocalDataRoot(), tableName),
                uploader,
                rtHandleService,
                indexRConfig.getRtResources());
        this.rtsRefresher = new ChildrenRefresher<HostSegmentInfo>(
                zkClient,
                declarePath,
                d -> JsonUtil.fromJson(GenericCompression.decomress(d), HostSegmentInfo.class));

        ZkHelper.createIfNotExist(zkClient, declarePath);
        refresh(true);

        long t = RefreshSegmentPeriod + random.nextInt(1000);
        this.refreshSegment = notifyService.scheduleWithFixedDelay(
                () -> this.refresh(false),
                t,
                t,
                TimeUnit.MILLISECONDS);
    }

    public void updateSchema(TableSchema schema) {
        localRealtimeTable.updateSetting(getRTSetting(schema));
        if (rtIngest) {
            localRealtimeTable.start();
        }
    }

    public void setRTIngest(boolean rtIngest) {
        this.rtIngest = rtIngest;
        if (rtIngest && !localRealtimeTable.isIngestState()) {
            localRealtimeTable.start();
        }
        if (!rtIngest && localRealtimeTable.isIngestState()) {
            localRealtimeTable.stop();
        }
    }

    private RealtimeSetting getRTSetting(TableSchema tableSchema) {
        RealtimeConfig rtConf = tableSchema.realtimeConfig;
        if (tableSchema.realtimeConfig == null) {
            return null;
        }
        return new RealtimeSetting(
                tableSchema.schema,
                rtConf.aggSchema.dims,
                rtConf.aggSchema.metrics,
                rtConf.nameToAlias,
                rtConf.tagSetting,
                rtConf.ignoreStrategy,
                rtConf.savePeriodMinutes * 60 * 1000,
                rtConf.uploadPeriodMinutes * 60 * 1000,
                rtConf.maxRowInMemory,
                rtConf.maxRowInRealtime,
                indexRConfig.getTimeZone(),
                rtConf.aggSchema.grouping,
                rtConf.ingest,
                tableSchema.mode,
                rtConf.fetcher);
    }

    public boolean isSafeToExit() {
        return localRealtimeTable.isSafeToExit();
    }

    @Override
    public SegmentFd get(String name) {
        if (name.startsWith("rts.")) {
            // Asking for realtime segment(the child segment in realtime segment group), which can only be searched localhost.
            for (RTSGroup rtsg : localRealtimeTable.rtsGroups().values()) {
                RealtimeSegment rts = rtsg.realtimeSegments().get(name);
                if (rts != null) {
                    return rts;
                }
            }
        } else {
            return rtsGroupInfoMap.get(name);
        }
        return null;
    }

    @Override
    public List<SegmentFd> all() {
        return new ArrayList<>(rtsGroupInfoMap.values());
    }

    @Override
    public List<String> getHosts(String segmentName, boolean isRealtime) throws IOException {
        assert isRealtime;

        RTSGroupInfo info = rtsGroupInfoMap.get(segmentName);
        if (info == null) {
            return null;
        }
        return Collections.singletonList(info.host());
    }

    @Override
    public void close() {
        refreshSegment.cancel(true);
        localRealtimeTable.close();
    }

    @Override
    public List<String> realtimeHosts() {
        return rttNodes;
    }

    @Override
    public synchronized void refresh(boolean force) {
        Try.on(this::refreshLocalRT, logger);
        Try.on(this::updateRTCache, logger);
    }

    private void refreshLocalRT() throws Exception {
        // Declare local segments to zk
        List<RTSGroupInfo> localInfos = localRealtimeTable
                .rtsGroups()
                .values()
                .stream()
                // We keep the uploaded segments and wait for deletion delay exceed.
                //.filter(rtsg -> !rtsg.hasUploaded())
                .map(rtsg -> {
                    try {
                        SegmentSchema schema = rtsg.schema();
                        ColumnNode[] columnNodes = new ColumnNode[schema.columns.size()];
                        for (int i = 0; i < columnNodes.length; i++) {
                            columnNodes[i] = rtsg.columnNode(i);
                        }
                        return new RTSGroupInfo(rtsg.version(), rtsg.mode().name(), rtsg.name(), schema, rtsg.rowCount(), columnNodes, hostName);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                })
                .collect(Collectors.toList());
        String json = JsonUtil.toJson(new HostSegmentInfo(localInfos));
        String zkPath = declarePath + "/" + hostName;

        byte[] rtsgInfo = GenericCompression.compress(json.getBytes("utf-8"));
        if (lastRTSGInfo != null
                && rtsgInfo.length == lastRTSGInfo.length
                && ByteArrayMethods.arrayEquals(
                rtsgInfo, Platform.BYTE_ARRAY_OFFSET,
                lastRTSGInfo, Platform.BYTE_ARRAY_OFFSET, rtsgInfo.length)) {
            // No need to update
            return;
        }
        // Replace.
        try {
            if (zkClient.checkExists().forPath(zkPath) == null) {
                zkClient.create().withMode(CreateMode.EPHEMERAL).forPath(zkPath, rtsgInfo);
            } else {
                zkClient.setData().forPath(zkPath, rtsgInfo);
            }
        } catch (KeeperException.NodeExistsException e) {
            zkClient.setData().forPath(zkPath, rtsgInfo);
        }
        lastRTSGInfo = rtsgInfo;
    }

    private void updateRTCache() throws Exception {
        Map<String, RTSGroupInfo> groupInfoMap = new HashMap<>();
        List<String> hostWithRTS = new ArrayList<>();

        Map<String, HostSegmentInfo> hostRTSInfos = rtsRefresher.refresh();

        for (Map.Entry<String, HostSegmentInfo> e : hostRTSInfos.entrySet()) {
            String host = e.getKey();
            HostSegmentInfo hostInfo = e.getValue();
            boolean isLocalHost = Strings.equals(host, this.hostName);
            for (RTSGroupInfo info : hostInfo.rtsGroupInfos) {
                if (isLocalHost) {
                    RTSGroup rtsg = localRealtimeTable.rtsGroups().get(info.name());
                    if (rtsg == null) {
                        logger.warn("rtsg not found in localhost. [host: {}, rtsg: {}]", host, info.name());
                        continue;
                    }
                    info.setRTSGroup(rtsg);
                }
                groupInfoMap.put(info.name(), info);
            }
            if (hostInfo.rtsGroupInfos.size() > 0) {
                hostWithRTS.add(host);
            }
        }

        this.rtsGroupInfoMap = groupInfoMap;
        this.rttNodes = hostWithRTS;
    }

    public static class HostSegmentInfo {
        @JsonProperty("rtsGroupInfos")
        public final List<RTSGroupInfo> rtsGroupInfos;

        @JsonCreator
        public HostSegmentInfo(@JsonProperty("rtsGroupInfos") List<RTSGroupInfo> rtsGroupInfos) {
            this.rtsGroupInfos = rtsGroupInfos;
        }
    }
}
