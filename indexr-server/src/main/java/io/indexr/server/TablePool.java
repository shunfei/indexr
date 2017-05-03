package io.indexr.server;

import org.apache.curator.framework.CuratorFramework;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import io.indexr.util.Try;

public class TablePool extends ZkTableManager implements Closeable {
    private static final Logger logger = LoggerFactory.getLogger(TablePool.class);

    private String host;
    private FileSystem fileSystem;
    private String dataRoot;
    private Path localDataRoot;
    private Map<String, HybridTable> tables = new ConcurrentHashMap<>();
    private IndexRConfig indexRConfig;
    private volatile boolean closed = false;
    private volatile boolean stopRT = false;

    // Watch the addition/removal of tables.
    private ZkWatcher tableWatcher;
    // Watch the addition/removal of realtime tables hosted by this node.
    private ZkWatcher rtTableWatcher;
    private String zkRTHostPath;

    private final AtomicInteger id = new AtomicInteger(0);
    private final ScheduledExecutorService rtHandleService = Executors.newScheduledThreadPool(5, r -> {
        Thread t = new Thread(r);
        t.setName("IndexR-RT-Handle-" + id.getAndIncrement());
        t.setDaemon(true);
        if (t.getPriority() != Thread.NORM_PRIORITY)
            t.setPriority(Thread.NORM_PRIORITY);
        return t;
    });
    private final ScheduledExecutorService refreshNotify = Executors.newScheduledThreadPool(1, r -> {
        Thread t = new Thread(r);
        t.setName("IndexR-Refresh-Notify");
        t.setDaemon(true);
        return t;
    });

    public TablePool(String host, CuratorFramework zkClient, FileSystem fileSystem, IndexRConfig indexRConfig) throws Exception {
        super(zkClient);
        this.host = host;
        this.fileSystem = fileSystem;
        this.dataRoot = indexRConfig.getDataRoot();
        this.localDataRoot = indexRConfig.getLocalDataRoot();
        this.indexRConfig = indexRConfig;
        this.zkRTHostPath = IndexRConfig.zkRTHostPath(host);

        ZkHelper.createIfNotExist(zkClient, zkTableRoot);
        ZkHelper.createIfNotExist(zkClient, zkRTHostPath);

        refreshNotify.schedule(this::refreshTables, 1, TimeUnit.SECONDS);

        this.tableWatcher = ZkWatcher.onChildren(zkClient, zkTableRoot, this::refreshTables, this::refreshTables);
        this.rtTableWatcher = ZkWatcher.onChildren(zkClient, zkRTHostPath, this::refreshRTTables, this::refreshRTTables);
    }

    private synchronized void refreshTables() {
        if (closed) return;
        try {
            if (zkClient.checkExists().forPath(zkTableRoot) == null) {
                for (HybridTable table : tables.values()) {
                    Try.on(table::close, logger);
                }
                tables.clear();
                return;
            }
        } catch (Exception e) {
            logger.error("check zk table root failed", e);
            return;
        }
        List<String> names = Try.on(() -> zkClient.getChildren().forPath(zkTableRoot), 5, logger, "Read table names from zk failed");
        if (names == null) {
            logger.error("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<");
            logger.error("Refresh table failed, system in inconsistent state");
            logger.error(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
            return;
        }
        logger.debug("tables:{}", names);
        Set<String> nameSet = new HashSet<>(names);

        // Remove outdated tables.
        Iterator<Map.Entry<String, HybridTable>> it = tables.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, HybridTable> entry = it.next();
            if (!nameSet.contains(entry.getKey())) {
                logger.info("remove table [{}]", entry.getKey());
                HybridTable table = entry.getValue();
                it.remove();
                Try.on(table::close, logger);
            }
        }

        for (String name : names) {
            if (!tables.containsKey(name)) {
                HybridTable table = Try.on(() -> loadTable(name), 1, logger, "Load table [" + name + "] failed");
                if (table == null) {
                    logger.error("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<");
                    logger.error("Load table [{}] failed, system in inconsistent state", name);
                    logger.error(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
                    continue;
                }
                logger.info("add table [{}]", name);
                tables.put(name, table);
            }
        }

        logger.debug("refresh tables completed.");

        // also need to refresh realtime setting.
        refreshRTTables();
    }

    private synchronized void refreshRTTables() {
        if (closed) {
            return;
        }
        try {
            if (zkClient.checkExists().forPath(zkRTHostPath) == null) {
                tables.clear();
                return;
            }
        } catch (Exception e) {
            logger.error("check zk rt host root failed", e);
            return;
        }
        List<String> names = Try.on(() -> zkClient.getChildren().forPath(zkRTHostPath), 5, logger, "Read rt table names from zk failed");
        if (names == null) {
            return;
        }

        logger.debug("realtime tables:{}", names);
        Set<String> nameSet = new HashSet<>(names);

        for (HybridTable table : tables.values()) {
            boolean rtIngest = !stopRT && nameSet.contains(table.name());
            table.setRTIngest(rtIngest);
        }

        logger.debug("refresh realtime tables completed.");
    }

    private HybridTable loadTable(String name) throws Exception {
        return new HybridTable(host, name, indexRConfig, refreshNotify, rtHandleService);
    }

    @Override
    public Set<String> allTableNames() {
        return new HashSet<>(tables.keySet());
    }

    public HybridTable get(String name) {
        return tables.get(name);
    }

    @Override
    public TableSchema getTableSchema(String name) {
        HybridTable table = tables.get(name);
        return table == null ? null : table.schema();
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;

        // Stop the refresh thread pool and the zk watcher first.
        refreshNotify.shutdownNow();
        rtHandleService.shutdownNow();
        tableWatcher.close();

        // Stop tables.
        ExecutorService execSvr = Executors.newCachedThreadPool();
        for (HybridTable table : tables.values()) {
            execSvr.submit(() -> Try.on(table::close, logger));
        }
        execSvr.shutdown();
        try {
            execSvr.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            // Ignore.
        }
    }

    public void stopRealtime() {
        stopRT = true;
        for (HybridTable table : tables.values()) {
            table.setRTIngest(false);
        }
    }

    public void startRealtime() {
        stopRT = false;
        refreshRTTables();
    }

    public boolean isSafeToExit() {
        for (HybridTable table : tables.values()) {
            if (!table.isSafeToExit()) {
                return false;
            }
        }
        return true;
    }
}
