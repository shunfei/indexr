package io.indexr.server;

import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;

public abstract class ZkWatcher implements Watcher, Closeable {
    private static final Logger logger = LoggerFactory.getLogger(ZkWatcher.class);

    CuratorFramework zkClient;
    String path;
    Runnable onPathDelete;
    Runnable onChildrenUpdate;
    Runnable onDataUpdate;

    public ZkWatcher(CuratorFramework zkClient,
                     String path,
                     Runnable onPathDelete,
                     Runnable onChildrenUpdate,
                     Runnable onDataUpdate) {
        this.zkClient = zkClient;
        this.path = path;
        this.onPathDelete = onPathDelete;
        this.onChildrenUpdate = onChildrenUpdate;
        this.onDataUpdate = onDataUpdate;

        addWatcher();
    }

    @Override
    public void process(WatchedEvent event) {
        try {
            switch (event.getState()) {
                case ConnectedReadOnly:
                case Unknown:
                case Disconnected:
                case AuthFailed:
                case Expired:
                    logger.warn("zk connection in bad state: {}", event.getState());
            }
            switch (event.getState()) {
                case SyncConnected:
                case ConnectedReadOnly:
                case SaslAuthenticated:
                    switch (event.getType()) {
                        case None:
                            // what?
                            break;
                        case NodeCreated:
                            break;
                        case NodeDeleted:
                            if (onPathDelete != null) {
                                onPathDelete.run();
                            }
                            break;
                        case NodeDataChanged:
                            if (onDataUpdate != null) {
                                onDataUpdate.run();
                            }
                            break;
                        case NodeChildrenChanged:
                            if (onChildrenUpdate != null) {
                                onChildrenUpdate.run();
                            }
                            break;
                    }
                    break;
            }
        } finally {
            addWatcher();
        }
    }

    abstract void addWatcher();

    @Override
    public void close() {
        zkClient.clearWatcherReferences(this);
    }

    public static ZkWatcher onChildren(CuratorFramework zkClient, String path, Runnable onPathDelete, Runnable onChildrenUpdate) {
        return new ZkWatcher(zkClient, path, onPathDelete, onChildrenUpdate, null) {
            @Override
            void addWatcher() {
                try {
                    if (zkClient.checkExists().forPath(path) != null) {
                        zkClient.getChildren().usingWatcher(this).forPath(path);
                    }
                } catch (Exception e) {
                    logger.error("Failed to add children watcher to {}", path, e);
                }
            }
        };
    }

    public static ZkWatcher onData(CuratorFramework zkClient, String path, Runnable onPathDelete, Runnable onDataUpdate) {
        return new ZkWatcher(zkClient, path, onPathDelete, null, onDataUpdate) {
            @Override
            void addWatcher() {
                try {
                    if (zkClient.checkExists().forPath(path) != null) {
                        zkClient.getData().usingWatcher(this).forPath(path);
                    }
                } catch (Exception e) {
                    logger.error("Failed to add data watcher to {}", path, e);
                }
            }
        };
    }
}
