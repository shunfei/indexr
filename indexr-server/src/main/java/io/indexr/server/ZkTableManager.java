package io.indexr.server;

import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import io.indexr.util.JsonUtil;
import io.indexr.util.Try;

public class ZkTableManager {
    private static final Logger logger = LoggerFactory.getLogger(ZkTableManager.class);
    static final String zkTableRoot = IndexRConfig.zkTableRoot();
    CuratorFramework zkClient;

    public ZkTableManager(CuratorFramework zkClient) throws IOException {
        this.zkClient = zkClient;
    }

    public Set<String> allTableNames() throws Exception {
        if (zkClient.checkExists().forPath(zkTableRoot) == null) {
            return Collections.emptySet();
        }
        List<String> names = Try.on(() -> zkClient.getChildren().forPath(zkTableRoot), 5, logger, "Read table names from zk failed");
        return names == null ? null : new HashSet<>(names);
    }

    public void set(String name, TableSchema schema) throws Exception {
        String path = zkTableRoot + "/" + name;
        byte[] bytes = JsonUtil.toJsonBytes(schema);

        Stat stat = zkClient.checkExists().forPath(path);
        if (stat != null) {
            zkClient.setData().forPath(path, bytes);
            logger.debug("Updated table [{}]", name);
        } else {
            zkClient.create().creatingParentsIfNeeded().forPath(path, bytes);
            logger.debug("Added table [{}]", name);
        }
    }

    public void remove(String name) throws Exception {
        String path = zkTableRoot + "/" + name;
        if (zkClient.checkExists().forPath(path) == null) {
            return;
        }
        zkClient.delete().forPath(path);
        logger.debug("Removed table [{}]", name);
    }

    public TableSchema getTableSchema(String name) throws Exception {
        String path = zkTableRoot + "/" + name;
        byte[] bytes = zkClient.getData().forPath(path);
        if (bytes == null) {
            return null;
        }
        return JsonUtil.fromJson(bytes, TableSchema.class);
    }
}
