package io.indexr.server;

import com.google.common.base.Preconditions;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TimeZone;

import io.indexr.segment.cache.IndexExpiredMemCache;
import io.indexr.segment.cache.IndexMemCache;
import io.indexr.segment.cache.PackExpiredMemCache;
import io.indexr.segment.cache.PackMemCache;
import io.indexr.segment.rt.RTResources;
import io.indexr.util.MemoryUtil;

public class IndexRConfig implements Closeable {
    private static final Logger log = LoggerFactory.getLogger(IndexRConfig.class);

    public static final String ZK_ADDR = "indexr.zk.addr";
    public static final String ZK_ROOT = "indexr.zk.root";
    public static final String FILESYSTEM_CONNECTION = "indexr.fs.connection";
    public static final String DATA_ROOT = "indexr.fs.data.root";
    public static final String LOCAL_DATA_ROOT = "indexr.local.data.root";
    public static final String TIME_ZONE = "indexr.timezone";
    public static final String CONTROL_SERVER_ENABLE = "indexr.control.enable";
    public static final String CONTROL_POORT = "indexr.control.port";

    public static final String MEMCHACHE_EXPIRE_INDEX = "indexr.memory.indexcache.expire.minute";
    public static final String MEMCHACHE_MAX_INDEX = "indexr.memory.indexcache.limit.mb";
    public static final String MEMCHACHE_MAX_RATE_INDEX = "indexr.memory.indexcache.limit.rate";

    public static final String MEMCHACHE_PACK_ENABLE = "indexr.memory.packcache.enable";
    public static final String MEMCHACHE_EXPIRE_PACK = "indexr.memory.packcache.expire.minute";
    public static final String MEMCHACHE_MAX_PACK = "indexr.memory.packcache.limit.mb";
    public static final String MEMCHACHE_MAX_RATE_PACK = "indexr.memory.packcache.limit.rate";

    private static final String RT_ENABLE_MEMORY_LIMIT = "indexr.memory.rt.limit.enable";
    private static final String RT_MAX_MEMORY = "indexr.memory.rt.limit.mb";
    private static final String RT_MAX_MEMORY_RATE = "indexr.memory.rt.limit.rate";

    public static Path localRTPath(Path localDataRoot) {
        return localDataRoot.resolve("rt/");
    }

    public static Path localRTPath(Path localDataRoot, String tableName) {
        return localDataRoot.resolve("rt/" + tableName);
    }

    public static Path localCacheSegmentFdPath(Path localDataRoot, String tableName) {
        return localDataRoot.resolve(String.format("cache/%s.segfd.cache", tableName));
    }

    public static String segmentRootPath(String dataRoot, String tableName) {
        return String.format("%s/segment/%s", dataRoot, tableName);
    }

    public static String segmentUpdateFilePath(String dataRoot, String tableName) {
        return String.format("%s/segment/%s/__UPDATE__", dataRoot, tableName);
    }

    public static String tmpPath(String dataRoot) {
        return dataRoot + "/tmp";
    }

    public static String zkSegmentDeclarePath(String tableName) {
        return String.format("/segment/%s", tableName);
    }

    public static String zkTableRoot() {
        return "/table";
    }

    /**
     * The path where table schemas declared.
     */
    public static String zkTableDeclarePath(String tableName) {
        return "/table/" + tableName;
    }

    /**
     * The path where nodes declared themselves.
     */
    public static String zkHostRoot() {
        return "/host";
    }


    /**
     * The path where nodes declared themselves.
     */
    public static String zkHostDeclarePath(String host) {
        return "/host/" + host;
    }

    public static String zkRTHostRoot() {
        return "/rt/host";
    }

    /**
     * The path specify the realtime tables a node should host.
     * <pre>
     *     .../rt/host/host0/tblA ... tblZ
     *     .../rt/host/host1/tblX ... tblY
     * </pre>
     */
    public static String zkRTHostPath(String host) {
        return "/rt/host/" + host;
    }

    /**
     * The path where realtime segments declare.
     * <pre>
     *     .../rt/table/tableA/host0/seg0_0 ... segN_0
     *     .../rt/table/tableA/host1/seg0_1 ... segN_1
     * </pre>
     */
    public static String zkRTTablePath(String tableName) {
        return "/rt/table/" + tableName;
    }

    private CuratorFramework zkRootClient;
    private CuratorFramework zkClient;
    private FileSystem fileSystem;
    private IndexMemCache indexMemCache;
    private PackMemCache packMemCache;
    private RTResources rtResources;

    private String zkAddr;
    private String zkRoot;
    private int controlPort;
    private String fileSystemConnection;
    private String dataRoot;
    private Path localDataRoot;
    private TimeZone timeZone;

    private static Set<IndexRConfig> configInstances = Collections.synchronizedSet(new HashSet<>());

    public IndexRConfig() {
        // Try to load config from file.
        InputStream input = null;
        try {
            Properties config = new Properties();
            input = IndexRConfig.class.getClassLoader().getResourceAsStream("indexr.config.properties");
            if (input == null) {
                input = IndexRConfig.class.getClassLoader().getResourceAsStream("indexr.config.txt");
            }
            if (input != null) {
                config.load(input);
                System.getProperties().putAll(config);
            }
        } catch (IOException e) {
            log.error("", e);
        } finally {
            IOUtils.closeQuietly(input);
        }

        Properties systemProp = System.getProperties();
        for (Map.Entry<Object, Object> e : systemProp.entrySet()) {
            String key = e.getKey().toString();
            if (key.startsWith("indexr.")) {
                log.info("{} = {}", key, e.getValue());
            }
        }

        synchronized (IndexRConfig.class) {
            if (!configInstances.isEmpty()) {
                log.warn("Two config instance exists in one JVM!");
            }
            configInstances.add(this);
        }
    }

    private static String mustGetProperty(String key) {
        String value = System.getProperty(key);
        Preconditions.checkState(value != null, "please specify %s", key);
        return value;
    }

    public int getControlPort() {
        return Integer.parseInt(System.getProperty(CONTROL_POORT, "9235"));
    }

    public boolean getControlServerEnable() {
        return Boolean.parseBoolean(System.getProperty(CONTROL_SERVER_ENABLE, "false"));
    }

    public String getZkAddr() {
        if (zkAddr == null) {
            zkAddr = mustGetProperty(ZK_ADDR);
        }
        return zkAddr;
    }

    public String getZkRoot() {
        if (zkRoot == null) {
            zkRoot = System.getProperty(ZK_ROOT, "indexr");
            zkRoot = StringUtils.stripStart(StringUtils.stripEnd(zkRoot, "/"), "/");
        }
        return zkRoot;
    }

    public String getFileSystemConnection() {
        if (fileSystemConnection == null) {
            fileSystemConnection = mustGetProperty(FILESYSTEM_CONNECTION);
        }
        return fileSystemConnection;
    }

    public String getDataRoot() {
        if (dataRoot == null) {
            dataRoot = mustGetProperty(DATA_ROOT);
        }
        return dataRoot;
    }

    public Path getLocalDataRoot() {
        if (localDataRoot == null) {
            String localDataRootDirStr = mustGetProperty(LOCAL_DATA_ROOT);
            localDataRoot = Paths.get(localDataRootDirStr);
            if (!Files.exists(localDataRoot)) {
                try {
                    Files.createDirectories(localDataRoot);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        return localDataRoot;
    }

    public TimeZone getTimeZone() {
        if (timeZone == null) {
            String timeZoneStr = System.getProperty(TIME_ZONE);
            if (timeZoneStr == null) {
                timeZone = TimeZone.getDefault();
            } else {
                timeZone = TimeZone.getTimeZone(timeZoneStr);
            }
        }
        return timeZone;
    }

    public CuratorFramework getZkClient() {
        if (zkClient != null) {
            return zkClient;
        }
        zkRootClient = CuratorFrameworkFactory.newClient(getZkAddr(), new ExponentialBackoffRetry(1000, 10));
        zkRootClient.start();
        zkClient = zkRootClient.usingNamespace(getZkRoot());
        return zkClient;
    }

    public org.apache.hadoop.fs.Path segmentPath(String table, String segmentName) {
        return new org.apache.hadoop.fs.Path(segmentRootPath(getDataRoot(), table), segmentName);
    }

    public FileSystem getFileSystem() {
        if (fileSystem != null) {
            return fileSystem;
        }

        URI uri = URI.create(getFileSystemConnection());
        Configuration config = new Configuration();
        config.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
        try {
            fileSystem = FileSystem.get(uri, config);
        } catch (IOException e) {
            throw new RuntimeException("Initialize file system failed!", e);
        }
        return fileSystem;
    }

    public IndexMemCache getIndexMemCache() {
        if (indexMemCache != null) {
            return indexMemCache;
        }
        long[] values = getMemCacheConfig(MEMCHACHE_EXPIRE_INDEX, MEMCHACHE_MAX_INDEX, MEMCHACHE_MAX_RATE_INDEX, 30, 0.15f);
        indexMemCache = new IndexExpiredMemCache(values[0], values[1]);
        return indexMemCache;
    }

    public PackMemCache getPackMemCache() {
        if (packMemCache != null) {
            return packMemCache;
        }
        boolean enable = Boolean.parseBoolean(System.getProperty(MEMCHACHE_PACK_ENABLE, "false"));
        if (!enable) {
            return null;
        }
        long[] values = getMemCacheConfig(MEMCHACHE_EXPIRE_PACK, MEMCHACHE_MAX_PACK, MEMCHACHE_MAX_RATE_PACK, 10, 0.35f);
        packMemCache = new PackExpiredMemCache(values[0], values[1]);
        return packMemCache;
    }

    private long[] getMemCacheConfig(String expireKey, String maxCapKey, String maxCapRateKey, long defExpireMinute, float defRate) {
        long expireMinute = Long.parseLong(System.getProperty(expireKey, String.valueOf(defExpireMinute)));
        if (expireMinute < 5) {
            expireMinute = 5;
        }
        long expireMS = expireMinute * 60 * 1000;
        String maxCapStr = System.getProperty(maxCapKey);
        long maxCap;
        if (maxCapStr != null) {
            maxCap = Long.parseLong(maxCapStr) << 20; // 1 m = 1024 * 1024 b
        } else {
            float maxCacheRate = Float.parseFloat(System.getProperty(maxCapRateKey, String.valueOf(defRate)));
            maxCap = (long) (maxCacheRate * MemoryUtil.getTotalPhysicalMemorySize());
        }
        return new long[]{expireMS, maxCap};
    }

    public RTResources getRtResources() {
        if (rtResources != null) {
            return rtResources;
        }
        boolean enable = Boolean.parseBoolean(System.getProperty(RT_ENABLE_MEMORY_LIMIT, "true"));
        long maxMemory;
        String maxMemoryStr = System.getProperty(RT_MAX_MEMORY);
        if (maxMemoryStr != null) {
            maxMemory = Long.parseLong(maxMemoryStr) << 20;
        } else {
            double rate = Double.parseDouble(System.getProperty(RT_MAX_MEMORY_RATE, "0.35"));
            maxMemory = (long) (rate * MemoryUtil.getTotalPhysicalMemorySize());
        }
        rtResources = new RTResources(enable, maxMemory);
        return rtResources;
    }

    @Override
    public void close() throws IOException {
        synchronized (IndexRConfig.class) {
            configInstances.remove(this);
        }
        if (zkRootClient != null) {
            IOUtils.closeQuietly(zkRootClient);
            zkRootClient = null;
            zkClient = null;
        }
        if (fileSystem != null) {
            // Normally filesystem is cached, so don't really close it.
            //IOUtils.closeQuietly(fileSystem);
            fileSystem = null;
        }
        if (indexMemCache != null) {
            indexMemCache.close();
            indexMemCache = null;
        }
        if (packMemCache != null) {
            packMemCache.close();
            packMemCache = null;
        }
    }
}
