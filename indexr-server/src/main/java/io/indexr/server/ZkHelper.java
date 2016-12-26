package io.indexr.server;

import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class ZkHelper {

    public static void createIfNotExist(CuratorFramework zkClient, String path) throws Exception {
        createIfNotExist(zkClient, path, new byte[0], CreateMode.PERSISTENT);
    }

    public static void createIfNotExist(CuratorFramework zkClient, String path, CreateMode mode) throws Exception {
        createIfNotExist(zkClient, path, new byte[0], mode);
    }

    public static void createIfNotExist(CuratorFramework zkClient, String path, byte[] data, CreateMode mode) throws Exception {
        if (mode.isEphemeral()) {
            // We need to delete it first while exists in ephemeral mode.
            // Because in quick restart senario, the old node may still there and haven't been deleted by zookeeper.
            try {
                zkClient.delete().forPath(path);
            } catch (KeeperException.NoNodeException e) {
                // Ignore.
            }
        }
        if (zkClient.checkExists().forPath(path) == null) {
            try {
                zkClient.create().creatingParentsIfNeeded().withMode(mode).forPath(path);
            } catch (KeeperException.NodeExistsException e) {
                // Sombody else has done our job.
            }
        }
    }

    public static class ChildrenRefresher<T> {
        private final CuratorFramework zkClient;
        private final String path;
        private final NodeDataDeserializer<T> deserializer;

        private Map<String, Integer> localVersion = new HashMap<>();
        private Map<String, T> localCache = new HashMap<>();

        public ChildrenRefresher(CuratorFramework zkClient,
                                 String path,
                                 NodeDataDeserializer<T> deserializer) {
            this.zkClient = zkClient;
            this.path = path;
            this.deserializer = deserializer;
        }

        public Map<String, T> refresh() throws Exception {
            Map<String, T> nameToObj = new HashMap<>();

            List<String> children = zkClient.getChildren().forPath(path);
            for (String child : children) {
                String childPath = path + "/" + child;
                Stat stat = zkClient.checkExists().forPath(childPath);
                if (stat == null) {
                    continue;
                }
                T t = null;
                Integer cacheVersion = localVersion.get(child);
                if (cacheVersion != null && cacheVersion == stat.getVersion()) {
                    t = localCache.get(child);
                } else {
                    byte[] data = zkClient.getData().forPath(childPath);
                    t = data == null ? null : deserializer.deserialize(data);

                    localVersion.put(child, stat.getVersion());
                    localCache.put(child, t);
                }
                if (t != null) {
                    nameToObj.put(child, t);
                }
            }

            Iterator<String> itr = localVersion.keySet().iterator();
            while (itr.hasNext()) {
                String key = itr.next();
                if (!nameToObj.containsKey(key)) {
                    itr.remove();
                    localCache.remove(key);
                }
            }
            return nameToObj;
        }
    }

    public static interface NodeDataDeserializer<T> {
        T deserialize(byte[] data) throws Exception;
    }
}


