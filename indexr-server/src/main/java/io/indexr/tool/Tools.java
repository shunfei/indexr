package io.indexr.tool;

import com.google.common.base.Preconditions;

import org.apache.commons.io.IOUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.HttpParams;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import io.indexr.io.ByteBufferReader;
import io.indexr.plugin.Plugins;
import io.indexr.segment.Column;
import io.indexr.segment.InfoSegment;
import io.indexr.segment.SegmentManager;
import io.indexr.segment.SegmentMode;
import io.indexr.segment.SegmentSchema;
import io.indexr.segment.pack.DataPackNode;
import io.indexr.segment.rt.RTSGroupInfo;
import io.indexr.segment.storage.StorageSegment;
import io.indexr.segment.storage.itg.IntegratedColumn;
import io.indexr.segment.storage.itg.IntegratedSegment;
import io.indexr.server.FileSegmentManager;
import io.indexr.server.IndexRConfig;
import io.indexr.server.SegmentHelper;
import io.indexr.server.ServerHelper;
import io.indexr.server.TableSchema;
import io.indexr.server.ZkTableManager;
import io.indexr.server.rt.RealtimeConfig;
import io.indexr.server.rt.RealtimeSegmentPool;
import io.indexr.server.rt2his.HiveHelper;
import io.indexr.util.GenericCompression;
import io.indexr.util.JsonUtil;
import io.indexr.util.RuntimeUtil;
import io.indexr.util.Strings;
import io.indexr.util.Try;

public class Tools {
    private static final Logger logger = LoggerFactory.getLogger(Tools.class);

    static {
        RealtimeConfig.loadSubtypes();
    }

    private static class MyOptions {
        @Option(name = "-h", usage = "print this help. \nUsage: -cmd <cmd> [-option ...]")
        boolean help;
        @Option(name = "-cmd", metaVar = "<cmd>", usage = "the command." +
                "\n=====================================" +
                "\nlisttb    - list all tables." +
                "\nsettb     - add or update table. [-t, -c]" +
                "\nrmtb      - remove table. [-t]" +
                "\ndctb      - describe table. [-t] " +
                "\nlisths    - list all historical segments. [-t]" +
                "\nlistrs    - list all realtime segments. [-t, -v]" +
                "\nrmseg     - remove segments. [-t, -s]" +
                "\ndchs      - describ historical segments. [-t, -s, -v]" +
                "\nstoprt    - stop realtime. [-host, -port]" +
                "\nstartrt   - start realtime. [-host, -port]" +
                "\nstopnode  - stop indexr node. [-host, -port]" +
                "\nlistnode  - list all running nodes." +
                "\nnodertt   - list all realtime tables on host. [-host]" +
                "\nrttnode   - list all hosts the realtime table exists. [-t]" +
                "\naddrtt    - add realtime table to hosts. [-t, -host]" +
                "\nrmrtt     - remove realtime table from hosts. [-t, -host]" +
                "\nnotifysu  - notify segment update. [-t]" +
                "\nhivesql   - get the hive table creation sql. Specify the partition column of hive table by -column <columnName>. [-t, -c, -column, -hivetb]" +
                "\nupmode    - update table segment mode. [-t, -mode]" +
                "\n=====================================")
        String cmd;

        @Option(name = "-v", usage = "verbose or not")
        boolean verbose = false;
        @Option(name = "-t", metaVar = "<tableName>", usage = "table name(s), splited by `,`")
        String table;
        @Option(name = "-s", metaVar = "<segmentName>", usage = "segment name(s), splited by `,`")
        String segment;
        @Option(name = "-c", metaVar = "<schemaPath>", usage = "the path of table schema")
        String schemapath;
        @Option(name = "-col", metaVar = "<columnName>", usage = "the column name, splited by `,`")
        String columnName;
        @Option(name = "-hivetb", metaVar = "<hiveTableName>", usage = "the hive table name")
        String hiveTable;

        @Option(name = "-host", metaVar = "<host>", usage = "host of node(s), splited by `,`")
        String host;
        @Option(name = "-port", metaVar = "<port>", usage = "control port of node")
        int port = 9235;

        @Option(name = "-mode", metaVar = "<mode>", usage = "segment mode")
        String mode = SegmentMode.DEFAULT.name();
    }

    public static void main(String[] args) throws Exception {
        MyOptions options = new MyOptions();
        CmdLineParser parser = RuntimeUtil.parseArgs(args, options);
        if (options.help) {
            parser.printUsage(System.out);
            return;
        }

        Plugins.loadPlugins();
        IndexRConfig config = new IndexRConfig();
        boolean ok = false;
        try {
            ok = runTool(options, config);
        } catch (Exception e) {
            logger.error("", e);
        } finally {
            IOUtils.closeQuietly(config);
        }
        if (ok) {
            System.exit(0);
        } else {
            System.exit(1);
        }
    }

    private static boolean runTool(MyOptions options, IndexRConfig config) throws Exception {
        if (Strings.isEmpty(options.cmd)) {
            System.out.println("Please specify -cmd");
            return false;
        }
        switch (options.cmd.toLowerCase()) {
            case "listtb":
                return listTable(options, config);
            case "settb":
                return setTable(options, config);
            case "rmtb":
                return removeTables(options, config);
            case "dctb":
                return describeTables(options, config);
            case "lisths":
                return listHisSegs(options, config);
            case "listrs":
                return listRTSegs(options, config);
            case "dchs":
                return describeHisSeg(options, config);
            case "rmseg":
                return removeSegments(options, config);
            case "stoprt":
                return stopRealtime(options, config);
            case "startrt":
                return startRealtime(options, config);
            case "stopnode":
                return stopNode(options, config);
            case "listnode":
                return listNode(options, config);
            case "nodertt":
                return nodeRTT(options, config);
            case "rttnode":
                return rttNode(options, config);
            case "addrtt":
                return addRTT(options, config);
            case "rmrtt":
                return removeRTT(options, config);
            case "notifysu":
                return notifySegmentUpdate(options, config);
            case "hivesql":
                return hiveCreateSql(options, config);
            case "upmode":
                return updateMode(options, config);
            default:
                System.out.println("Illegal cmd: " + options.cmd);
                return false;
        }
    }

    private static boolean listTable(MyOptions options, IndexRConfig config) throws Exception {
        ZkTableManager tm = new ZkTableManager(config.getZkClient());
        Set<String> tables = tm.allTableNames();
        if (tables == null) {
            return true;
        }
        List<String> list = new ArrayList<>(tables);
        list.sort(String::compareTo);
        list.forEach(System.out::println);
        return true;
    }

    private static boolean setTable(MyOptions options, IndexRConfig config) throws Exception {
        Preconditions.checkState(!Strings.isEmpty(options.table), "Please specify table name! -t <name>");
        Preconditions.checkState(!Strings.isEmpty(options.schemapath), "Please specify scheam path! -c <path>");
        String[] tableNames = options.table.split(",");
        Preconditions.checkState(tableNames.length == 1, "Can only add one table one time!");
        TableSchema schema = JsonUtil.loadConfig(Paths.get(options.schemapath), TableSchema.class);
        //RealtimeConfig rtConfig = schema.realtimeConfig;
        //if (rtConfig != null) {
        //    String error = RealtimeHelper.validateSetting(schema.schema.getColumns(), rtConfig.dims, rtConfig.metrics, rtConfig.grouping);
        //    if (error != null) {
        //        System.out.println(error);
        //        return false;
        //    }
        //}
        ZkTableManager tm = new ZkTableManager(config.getZkClient());
        tm.set(options.table, schema);
        System.out.println("OK");
        return true;
    }

    private static boolean removeTables(MyOptions options, IndexRConfig config) throws Exception {
        Preconditions.checkState(!Strings.isEmpty(options.table), "Please specify table name! -t <name>");
        String[] tableNames = options.table.split(",");
        ZkTableManager tm = new ZkTableManager(config.getZkClient());
        for (String name : tableNames) {
            tm.remove(name.trim());
        }
        System.out.println("OK");
        return true;
    }

    private static boolean describeTables(MyOptions options, IndexRConfig config) throws Exception {
        Preconditions.checkState(!Strings.isEmpty(options.table), "Please specify table name! -t <name>");
        String[] tableNames = options.table.split(",");
        ZkTableManager tm = new ZkTableManager(config.getZkClient());
        for (String name : tableNames) {
            String schema = JsonUtil.toJson(tm.getTableSchema(name));
            System.out.printf("%s:\n%s\n", name, schema);
        }
        return true;
    }

    private static boolean listHisSegs(MyOptions options, IndexRConfig config) throws Exception {
        Preconditions.checkState(!Strings.isEmpty(options.table), "Please specify table name! -t <name>");
        String[] tableNames = options.table.split(",");
        Preconditions.checkState(tableNames.length == 1, "Only one table name are supported!");
        ZkTableManager zkTableManager = new ZkTableManager(config.getZkClient());
        TableSchema schema = zkTableManager.getTableSchema(options.table);
        SegmentManager tm = new FileSegmentManager(
                options.table,
                config.getFileSystem(),
                IndexRConfig.segmentRootPath(config.getDataRoot(), options.table, schema.location));
        List<String> names = tm.allSegmentNames();
        if (names == null) {
            return true;
        }
        List<String> list = new ArrayList<String>(names);
        list.sort(String::compareTo);
        list.forEach(System.out::println);
        return true;
    }

    private static boolean listRTSegs(MyOptions options, IndexRConfig config) throws Exception {
        Preconditions.checkState(!Strings.isEmpty(options.table), "Please specify table name! -t <name>");
        String delcarePath = IndexRConfig.zkRTTablePath(options.table);
        CuratorFramework zkClient = config.getZkClient();
        List<String> hosts = zkClient.getChildren().forPath(delcarePath);
        for (String host : hosts) {
            byte[] bytes = Try.on(() -> zkClient.getData().forPath(delcarePath + "/" + host), 1, logger);
            if (bytes == null) {
                continue;
            }
            RealtimeSegmentPool.HostSegmentInfo hostInfo = JsonUtil.fromJson(GenericCompression.decomress(bytes), RealtimeSegmentPool.HostSegmentInfo.class);
            if (hostInfo == null) {
                continue;
            }
            List<RTSGroupInfo> rtsegs = new ArrayList<>(hostInfo.rtsGroupInfos);
            if (!rtsegs.isEmpty()) {
                rtsegs.sort((a, b) -> a.name().compareTo(b.name()));
                for (RTSGroupInfo info : rtsegs) {
                    System.out.println(info.name() + ":\n----------");
                    System.out.println("host: " + host);
                    System.out.println("rowCount: " + info.rowCount());
                    System.out.println("version: " + info.version());
                    System.out.println("mode: " + info.mode());
                    if (options.verbose) {
                        System.out.println("schema:\n" + JsonUtil.toJson(info.schema()));
                        System.out.println("columnNodes:\n" + JsonUtil.toJson(info.columnNodes));
                    }
                    System.out.println();
                }
            }
        }
        return true;
    }

    private static boolean describeHisSeg(MyOptions options, IndexRConfig config) throws Exception {
        Preconditions.checkState(!Strings.isEmpty(options.table), "Please specify table name! -t <name>");
        Preconditions.checkState(!Strings.isEmpty(options.segment), "Please specify segment name! -s <name>");

        ZkTableManager zkTableManager = new ZkTableManager(config.getZkClient());
        TableSchema tableSchema = zkTableManager.getTableSchema(options.table);
        Path segmentRootPath = IndexRConfig.segmentRootPath(config.getDataRoot(), options.table, tableSchema.location);

        String[] segNames = options.segment.split(",");
        for (String segName : segNames) {
            segName = segName.trim();
            Path path = new Path(segmentRootPath, segName);
            FileSystem fileSystem = config.getFileSystem();
            FileStatus fileStatus = fileSystem.getFileStatus(path);

            if (fileStatus == null) {
                System.out.printf("%s is not exists!\n", segName);
                continue;
            }
            int blockCount = fileSystem.getFileBlockLocations(fileStatus, 0, fileStatus.getLen()).length;
            ByteBufferReader.Opener readerOpener = ByteBufferReader.Opener.create(
                    fileSystem,
                    path,
                    fileStatus.getLen(),
                    blockCount);
            IntegratedSegment.Fd fd = IntegratedSegment.Fd.create(segName, readerOpener);
            if (fd == null) {
                System.out.printf("%s is not a legal segment!\n", segName);
                continue;
            }
            try (StorageSegment segment = fd.open()) {
                InfoSegment infoSegment = fd.info();
                SegmentSchema schema = infoSegment.schema();
                long rowCount = infoSegment.rowCount();
                System.out.println(segName + ":\n----------");
                System.out.println("rowCount: " + rowCount);
                System.out.println("size: " + fileStatus.getLen());
                System.out.println("version: " + segment.version());
                System.out.println("mode: " + segment.mode());
                if (options.verbose) {
                    System.out.println("schema:\n" + JsonUtil.toJson(schema));
                    System.out.println("sectionInfo:\n" + JsonUtil.toJson(fd.sectionInfo()));
                    System.out.println("columnInfo:");
                    for (int colId = 0; colId < schema.getColumns().size(); colId++) {
                        Column column = segment.column(colId);
                        long dpnSize = 0;
                        long indexSize = 0;
                        long extIndexSize = 0;
                        long outerIndexSize = ((IntegratedColumn) column).outerIndexSize();
                        long dataSize = 0;
                        for (int packId = 0; packId < column.packCount(); packId++) {
                            DataPackNode dpn = column.dpn(packId);
                            dpnSize += segment.mode().versionAdapter.dpnSize(segment.version(), segment.mode());
                            indexSize += dpn.indexSize();
                            extIndexSize += dpn.extIndexSize();
                            dataSize += dpn.packSize();
                        }
                        System.out.printf("  %s: dpn: %s, index: %s, extIndex: %s, outerIndex: %s, data: %s, dict(0th): %s\n", column.name(), dpnSize, indexSize, extIndexSize, outerIndexSize, dataSize, column.dpn(0).isDictEncoded());
                    }
                }
                System.out.println();
            }
        }
        return true;
    }

    private static boolean removeSegments(MyOptions options, IndexRConfig config) throws Exception {
        Preconditions.checkState(!Strings.isEmpty(options.table), "Please specify table name! -t <name>");
        String[] tableNames = options.table.split(",");
        Preconditions.checkState(tableNames.length == 1, "Only one table name are supported!");
        Preconditions.checkState(options.segment != null, "Please specify segment name! -s <name>");

        ZkTableManager zkTableManager = new ZkTableManager(config.getZkClient());
        TableSchema schema = zkTableManager.getTableSchema(options.table);
        SegmentManager tm = new FileSegmentManager(
                options.table,
                config.getFileSystem(),
                IndexRConfig.segmentRootPath(config.getDataRoot(), options.table, schema.location));
        String[] names = options.segment.split(",");
        for (String name : names) {
            tm.remove(name.trim());
        }
        System.out.println("OK");
        return true;
    }

    private static boolean stopRealtime(MyOptions options, IndexRConfig config) throws Exception {
        String host = options.host == null ? InetAddress.getLocalHost().getHostName() : options.host;
        int port = options.port == 0 ? config.getControlPort() : options.port;
        if (options.table == null) {
            sendCommand(host, port, "cmd=stoprt");
        } else {
            HttpParams params = new BasicHttpParams();
            sendCommand(host, port, "cmd=stoprt&table=" + options.table);
        }
        return true;
    }

    private static boolean startRealtime(MyOptions options, IndexRConfig config) throws Exception {
        String host = options.host == null ? InetAddress.getLocalHost().getHostName() : options.host;
        int port = options.port == 0 ? config.getControlPort() : options.port;
        if (options.table == null) {
            sendCommand(host, port, "cmd=startrt");
        } else {
            sendCommand(host, port, "cmd=startrt&table=" + options.table);
        }
        return true;
    }

    private static boolean stopNode(MyOptions options, IndexRConfig config) throws Exception {
        String host = options.host == null ? InetAddress.getLocalHost().getHostName() : options.host;
        int port = options.port == 0 ? config.getControlPort() : options.port;
        sendCommand(host, port, "cmd=stopnode");
        return true;
    }

    private static boolean listNode(MyOptions options, IndexRConfig config) throws Exception {
        List<String> hosts = ServerHelper.validHosts(config.getZkClient());
        hosts.sort(String::compareTo);
        hosts.forEach(System.out::println);
        return true;
    }

    private static boolean nodeRTT(MyOptions options, IndexRConfig config) throws Exception {
        CuratorFramework zkClient = config.getZkClient();
        List<String> hosts;
        if (Strings.isEmpty(options.host)) {
            hosts = ServerHelper.validHosts(zkClient);
            hosts.sort(String::compareTo);
        } else {
            hosts = Collections.singletonList(options.host);
        }
        for (String host : hosts) {
            System.out.println(host + ":\n-----------");
            List<String> tables = ServerHelper.getHostRTTables(zkClient, host);
            tables.sort(String::compareTo);
            tables.forEach(System.out::println);
            System.out.println();
        }
        return true;
    }

    private static boolean rttNode(MyOptions options, IndexRConfig config) throws Exception {
        CuratorFramework zkClient = config.getZkClient();
        List<String> tables;
        if (Strings.isEmpty(options.table)) {
            ZkTableManager tm = new ZkTableManager(zkClient);
            Set<String> tbs = tm.allTableNames();
            if (tbs == null) {
                return true;
            }
            tables = new ArrayList<>(tbs);
            tables.sort(String::compareTo);
        } else {
            tables = Collections.singletonList(options.table);
        }
        for (String table : tables) {
            System.out.println(table + ":\n-----------");
            List<String> hosts = ServerHelper.getRTTableHosts(zkClient, table);
            hosts.sort(String::compareTo);
            hosts.forEach(System.out::println);
            System.out.println();
        }
        return true;
    }

    private static boolean addRTT(MyOptions options, IndexRConfig config) throws Exception {
        Preconditions.checkState(!Strings.isEmpty(options.table), "Please specify table name! -t <name>");
        Preconditions.checkState(!Strings.isEmpty(options.host), "Please specify host! -host <host>");

        List<String> hosts = Arrays.asList(options.host.split(","));
        ServerHelper.addRTTableToHosts(config.getZkClient(), options.table, hosts);
        System.out.println("OK");
        return true;
    }

    private static boolean removeRTT(MyOptions options, IndexRConfig config) throws Exception {
        Preconditions.checkState(!Strings.isEmpty(options.table), "Please specify table name! -t <name>");
        Preconditions.checkState(!Strings.isEmpty(options.host), "Please specify host! -host <host>");

        List<String> hosts = Arrays.asList(options.host.split(","));
        ServerHelper.removeRTTableFromHosts(config.getZkClient(), options.table, hosts);
        System.out.println("OK");
        return true;
    }

    private static void sendCommand(String host, int port, String params) throws Exception {
        HttpClient httpclient = new DefaultHttpClient();
        try {
            String url = String.format("http://%s:%d/control?%s", host, port, params);
            HttpGet httpGet = new HttpGet(url);
            HttpResponse response = httpclient.execute(httpGet);
            if (response.getStatusLine().getStatusCode() == 200) {
                System.out.println("OK");
            } else {
                HttpEntity entity = response.getEntity();
                if (entity != null) {
                    InputStream instream = entity.getContent();
                    try {
                        System.out.println(IOUtils.toString(new InputStreamReader(instream)));
                    } catch (RuntimeException ex) {
                        instream.close();
                    }
                } else {
                    System.out.println("FAIL");
                }
            }
        } finally {
            httpclient.getConnectionManager().shutdown();
        }
    }

    private static boolean notifySegmentUpdate(MyOptions options, IndexRConfig config) throws Exception {
        Preconditions.checkState(!Strings.isEmpty(options.table), "Please specify table name! -t <name>");

        ZkTableManager tm = new ZkTableManager(config.getZkClient());
        Preconditions.checkState(tm.getTableSchema(options.table) != null, "Table [%s] not exists!", options.table);
        TableSchema schema = tm.getTableSchema(options.table);
        SegmentHelper.notifyUpdate(
                config.getFileSystem(),
                IndexRConfig.segmentRootPath(config.getDataRoot(), options.table, schema.location));
        System.out.println("OK");

        return true;
    }

    private static boolean hiveCreateSql(MyOptions options, IndexRConfig config) throws Exception {
        Preconditions.checkState(!Strings.isEmpty(options.table), "Please specify table name! -t <name>");
        Preconditions.checkState(!Strings.isEmpty(options.columnName), "Please specify hive table partition column by -col <columnName>");

        TableSchema schema;
        if (!Strings.isEmpty(options.schemapath)) {
            schema = JsonUtil.loadConfig(Paths.get(options.schemapath), TableSchema.class);
        } else {
            ZkTableManager tm = new ZkTableManager(config.getZkClient());
            schema = tm.getTableSchema(options.table);
        }
        if (schema == null) {
            System.out.println("Table schema not found. Neither specified by -c, nor found in system.");
            return false;
        }
        String createSql = HiveHelper.getHiveTableCreateSql(
                Strings.isEmpty(options.hiveTable) ? options.table : options.hiveTable,
                true,
                schema.schema,
                schema.mode,
                schema.aggSchema,
                IndexRConfig.segmentRootPath(config.getDataRoot(), options.table, schema.location).toString(),
                options.columnName
        );
        if (!createSql.trim().endsWith(";")) {
            createSql += ";";
        }
        System.out.println(createSql);
        return true;
    }

    private static boolean updateMode(MyOptions options, IndexRConfig config) throws Exception {
        Preconditions.checkState(!Strings.isEmpty(options.table), "Please specify table name! -t <name>");
        Preconditions.checkState(!Strings.isEmpty(options.mode), "Please specify segment mode by -mode <mode>");
        SegmentMode newMode = SegmentMode.fromName(options.mode);

        String[] tables = options.table.trim().split(",");
        for (String table : tables) {
            table = table.trim();
            ZkTableManager tm = new ZkTableManager(config.getZkClient());
            TableSchema schema = tm.getTableSchema(table);
            if (schema == null) {
                System.out.printf("Table [%s] schema not found in system.\n", table);
                return false;
            }

            schema.setMode(newMode);
            schema.realtimeConfig.setMode(newMode);

            tm.set(table, schema);
        }

        return true;
    }
}
