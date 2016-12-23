package io.indexr.server;

import org.apache.curator.framework.CuratorFramework;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ServerHelper {
    /**
     * Get current running nodes.
     */
    public static List<String> validHosts(CuratorFramework zkClient) throws Exception {
        return zkClient.getChildren().forPath(IndexRConfig.zkHostRoot());
    }

    /**
     * Get all realtime tables exists on the host.
     */
    public static List<String> getHostRTTables(CuratorFramework zkClient, String host) throws Exception {
        return zkClient.getChildren().forPath(IndexRConfig.zkRTHostPath(host));
    }

    /**
     * Get all nodes which hosting the realtime table.
     */
    public static List<String> getRTTableHosts(CuratorFramework zkClient, String table) throws Exception {
        if (zkClient.checkExists().forPath(IndexRConfig.zkRTHostRoot()) == null) {
            return Collections.emptyList();
        }
        List<String> hosts = zkClient.getChildren().forPath(IndexRConfig.zkRTHostRoot());
        List<String> validHosts = new ArrayList<>();
        for (String host : hosts) {
            List<String> tables = getHostRTTables(zkClient, host);
            if (tables.contains(table)) {
                validHosts.add(host);
            }
        }
        return validHosts;
    }

    /**
     * Add the realtime table to those hosts.
     */
    public static void addRTTableToHosts(CuratorFramework zkClient, String table, List<String> hosts) throws Exception {
        for (String host : hosts) {
            String path = IndexRConfig.zkRTHostPath(host) + "/" + table;
            if (zkClient.checkExists().forPath(path) != null) {
                zkClient.setData().forPath(path);
            } else {
                zkClient.create().creatingParentsIfNeeded().forPath(path);
            }
        }
    }

    /**
     * Remove the realtime table from those hosts.
     */
    public static void removeRTTableFromHosts(CuratorFramework zkClient, String table, List<String> hosts) throws Exception {
        for (String host : hosts) {
            String path = IndexRConfig.zkRTHostPath(host) + "/" + table;
            if (zkClient.checkExists().forPath(path) != null) {
                zkClient.delete().forPath(path);
            }
        }
    }
}
