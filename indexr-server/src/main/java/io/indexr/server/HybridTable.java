package io.indexr.server;

import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;

import io.indexr.segment.SegmentFd;
import io.indexr.segment.SegmentLocality;
import io.indexr.segment.SegmentPool;
import io.indexr.server.rt.RealtimeSegmentPool;
import io.indexr.util.JsonUtil;
import io.indexr.util.Try;

/**
 * A table combines realtime segments and history segments.
 */
public class HybridTable implements SegmentPool, SegmentLocality {
    private static final Logger logger = LoggerFactory.getLogger(HybridTable.class);

    private String name;
    private TableSchema schema;
    private String zkTablePath;
    private CuratorFramework zkClient;
    private ZkWatcher schemaWatcher;

    private FileSegmentPool historySegmentPool;
    private RealtimeSegmentPool realtimeSegmentPool;

    public HybridTable(String hostName,
                       String tableName,
                       IndexRConfig indexRConfig,
                       ScheduledExecutorService notifyService,
                       ScheduledExecutorService rtHandleService) throws Exception {
        this.name = tableName;
        this.zkClient = indexRConfig.getZkClient();
        this.zkTablePath = IndexRConfig.zkTableDeclarePath(name);

        logger.info("Starting table. [table: {}]", name);

        refreshSchema();

        this.historySegmentPool = new FileSegmentPool(
                name,
                indexRConfig.getFileSystem(),
                IndexRConfig.segmentRootPath(indexRConfig.getDataRoot(), name, schema().location),
                indexRConfig.getLocalDataRoot(),
                notifyService);

        this.realtimeSegmentPool = new RealtimeSegmentPool(
                hostName,
                name,
                indexRConfig,
                historySegmentPool,
                zkClient,
                notifyService,
                rtHandleService);
        this.realtimeSegmentPool.updateSchema(schema());

        schemaWatcher = ZkWatcher.onData(zkClient, zkTablePath, null,
                () -> Try.on(this::refreshSchema, 3, logger, "Refresh table [" + name + "] schema failed"));
    }

    private void refreshSchema() throws Exception {
        logger.debug("refresh table schema. [table: {}]", name);
        byte[] bytes = zkClient.getData().forPath(zkTablePath);
        if (bytes == null) {
            throw new IllegalStateException(String.format("Table [%s] schema not found", name));
        }
        TableSchema newSchema = JsonUtil.fromJson(bytes, TableSchema.class);
        if (schema == null || !schema.equals(newSchema)) {
            schema = newSchema;
            if (realtimeSegmentPool != null) {
                realtimeSegmentPool.updateSchema(newSchema);
            }
        }
    }

    public void setRTIngest(boolean rtIngest) {
        realtimeSegmentPool.setRTIngest(rtIngest);
    }

    public boolean isSafeToExit() {
        return realtimeSegmentPool.isSafeToExit();
    }

    @Override
    public void close() {
        logger.info("Closing table [{}]", name);

        schemaWatcher.close();

        Try.on(realtimeSegmentPool::close, logger);
        Try.on(historySegmentPool::close, logger);
    }

    public String name() {
        return name;
    }

    public TableSchema schema() {
        return schema;
    }

    public SegmentPool segmentPool() {
        return this;
    }

    public SegmentLocality segmentLocality() {
        return this;
    }

    @Override
    public void refresh(boolean force) {
        historySegmentPool.refresh(force);
        realtimeSegmentPool.refresh(force);
    }

    @Override
    public SegmentFd get(String segmentName) {
        SegmentFd fd = historySegmentPool.get(segmentName);
        if (fd == null) {
            fd = realtimeSegmentPool.get(segmentName);
        }
        return fd;
    }

    @Override
    public List<SegmentFd> all() {
        List<SegmentFd> fds = new ArrayList<>(historySegmentPool.all());
        List<SegmentFd> realtimeFds = realtimeSegmentPool.all();
        for (SegmentFd fd : realtimeFds) {
            if (!historySegmentPool.exists(fd.name())) {
                fds.add(fd);
            }
        }
        return fds;
    }

    @Override
    public List<String> getHosts(String segmentName, boolean isRealtime) throws IOException {
        if (isRealtime) {
            return realtimeSegmentPool.getHosts(segmentName, true);
        } else {
            return historySegmentPool.getHosts(segmentName, false);
        }
    }

    @Override
    public List<String> realtimeHosts() {
        return realtimeSegmentPool.realtimeHosts();
    }

}
