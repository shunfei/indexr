package io.indexr.server.rt2his;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import io.indexr.segment.ColumnSchema;
import io.indexr.segment.SQLType;
import io.indexr.segment.SegmentSchema;
import io.indexr.server.SegmentHelper;
import io.indexr.util.IOUtil;
import io.indexr.util.Try;

/**
 * A tool which move realtime data in rt folder into history partition.
 * Only support one partition column.
 * It only works on indexr tables which has a hive table with location pointed to the same path and with a table partition column.
 */
public class Rt2HisOnHive {
    private static final Logger logger = LoggerFactory.getLogger(Rt2HisOnHive.class);
    private static final DateTimeFormatter timeFormatter = DateTimeFormatter.ofPattern("yyyyMMddHHmm");
    private static final String TABLE_NAME = "{TABLE}";
    private static final String WHERE_PREDICATE = "{WHERE}";

    private final FileSystem fileSystem;
    private final String hiveConnect;
    private final String user;
    private final String pwd;
    private final String hiveTable;
    private final String segmentPartitionColumn;
    private final String selectSql;

    private final String time = LocalDateTime.now().format(timeFormatter);

    public Rt2HisOnHive(FileSystem fileSystem,
                        String hiveConnect,
                        String user,
                        String pwd,
                        String hiveTable,
                        String segmentPartitionColumn,
                        String selectSql) {
        if (selectSql != null) {
            Preconditions.checkState(selectSql.contains(TABLE_NAME), "select sql must contains {TABLE} name");
            Preconditions.checkState(selectSql.contains(WHERE_PREDICATE), "select sql must contains {WHERE} predicate");
        }

        this.fileSystem = fileSystem;
        this.hiveConnect = hiveConnect;
        this.user = user;
        this.pwd = pwd;
        this.hiveTable = hiveTable;
        this.segmentPartitionColumn = segmentPartitionColumn;
        this.selectSql = selectSql;
    }

    public boolean handle() {
        Connection connection = null;
        String tableLocation = null;
        String rtTableName = null;
        String segTmpTableName = null;
        try {
            Class.forName("org.apache.hive.jdbc.HiveDriver");
            connection = DriverManager.getConnection(hiveConnect, user, pwd);

            // Looks like hive could produce errors when running concurrent sql throught HiveServer2 in local mode.
            // We disable local mode here to avoid this issue.
            try (Statement stmt = connection.createStatement()) {
                stmt.execute("set hive.exec.mode.local.auto = false");
            }

            String createTable = getHiveTableCreateSql(connection, String.format("%s", hiveTable));
            logger.debug("createTable: {}", createTable);
            tableLocation = getHiveTableLocation(createTable);
            String tablePartitionColumn = getHiveTablePartitionColumn(createTable);
            SegmentSchema schema = getSchema(createTable);
            logger.debug("tableLocation: {}", tableLocation);
            logger.debug("tablePartitionColumn: {}", tablePartitionColumn);
            logger.debug("schema: {}", schema);

            rtTableName = String.format(
                    "indexr_rt2his_rt_%s_%s_%s",
                    hiveTable,
                    time,
                    RandomStringUtils.randomAlphabetic(16));
            segTmpTableName = String.format(
                    "indexr_rt2his_seg_%s_%s_%s",
                    hiveTable,
                    time,
                    RandomStringUtils.randomAlphabetic(16));

            return move(connection, schema, tableLocation, tablePartitionColumn, rtTableName, segTmpTableName);
        } catch (Exception e) {
            logger.error("", e);
            return false;
        } finally {
            if (tableLocation != null) {
                final String finalTableLocation = tableLocation;
                Try.on(() -> SegmentHelper.notifyUpdate(fileSystem, finalTableLocation),
                        1, logger,
                        "Notify segment update failed");
            }

            if (connection != null) {
                try (Statement statement = connection.createStatement()) {
                    String dropSegTmpSql = String.format("DROP TABLE %s", segTmpTableName);
                    logger.debug("dropSegTmpSql: {}", dropSegTmpSql);
                    Try.on(() -> statement.execute(dropSegTmpSql),
                            1, logger,
                            "Drop segment tmp table failed");

                    String dropRTTableSql = String.format("DROP TABLE %s", rtTableName);
                    logger.debug("dropRTTableSql: {}", dropRTTableSql);
                    Try.on(() -> {
                                statement.execute(dropRTTableSql);
                            }, 5, logger,
                            "Drop rt2his tmp table failed");
                } catch (Exception e) {
                    logger.warn("Drop tmp tables failed! You can safely remove them manually.", e);
                }
            }

            IOUtil.closeQuietly(connection);
        }
    }

    private boolean move(Connection connection,
                         SegmentSchema schema,
                         String tableLocation,
                         String tablePartitionColumn,
                         String rtTableName,
                         String segTmpTableName) throws Exception {
        String selectSql;
        if (this.selectSql == null) {
            String allColumnStr = Joiner.on(",").join(Lists.transform(
                    schema.getColumns(),
                    cs -> "`" + cs.getName() + "`"));
            selectSql = String.format("SELECT %s FROM %s WHERE %s", allColumnStr, TABLE_NAME, WHERE_PREDICATE);
        } else {
            selectSql = this.selectSql;
        }

        if (!hasChild(fileSystem, new Path(tableLocation, "rt"))
                && !hasChild(fileSystem, new Path(tableLocation, "rt2his"))) {
            logger.debug("Table {} has not realtime segments to move", hiveTable);
            return true;
        }

        String rt2hisPath = tableLocation + "/rt2his";
        createTable(connection, rtTableName, true, schema, rt2hisPath, null);
        createTable(connection, segTmpTableName, false, schema, null, tablePartitionColumn);
        String segTmpTableLocation = getHiveTableLocation(getHiveTableCreateSql(connection, segTmpTableName));

        // Move segments in /rt into /rt2his folder.
        if (hasChild(fileSystem, new Path(tableLocation, "rt"))) {
            String importRtTableSql = String.format("LOAD DATA INPATH '%s' INTO TABLE %s", tableLocation + "/rt", rtTableName);
            try (Statement statement = connection.createStatement()) {
                statement.execute(importRtTableSql);
            }
        }

        // Notify indexr to update segments.
        SegmentHelper.notifyUpdate(fileSystem, tableLocation);

        String selectPartitionSql = String.format("SELECT DISTINCT %s FROM %s", segmentPartitionColumn, rtTableName);
        logger.debug("selectPartitionSql: {}", selectPartitionSql);
        List<String> partitions = new ArrayList<>();

        try (Statement statement = connection.createStatement()) {
            ResultSet rs = statement.executeQuery(selectPartitionSql);
            while (rs.next()) {
                partitions.add(rs.getString(1));
            }
            if (partitions.size() == 0) {
                return true;
            }
            rs.close();
        }
        logger.debug("partitions: {}", partitions);

        // Group rows in rt folder into partitions.
        for (String partition : partitions) {
            String selectFromSql = selectSql.replace(TABLE_NAME, rtTableName);
            selectFromSql = selectFromSql.replace(
                    WHERE_PREDICATE,
                    String.format("%s = '%s'", segmentPartitionColumn, partition));

            String insertPartitionSql = String.format(
                    "INSERT INTO TABLE %s PARTITION (`%s` = '%s') %s",
                    segTmpTableName,
                    tablePartitionColumn,
                    partition,
                    selectFromSql);
            logger.debug("insertPartitionSql: {}", insertPartitionSql);

            try (Statement myStatement = connection.createStatement()) {
                myStatement.execute(insertPartitionSql);
            }
        }

        // Move segment files from segTmpTable back into hiveTable.
        List<Path> movedSegmentPaths = new ArrayList<>();
        try {
            String fromPrefix = segTmpTableLocation + "/";
            for (String partition : partitions) {
                String fromPartitionPath = String.format("%s/%s=%s", segTmpTableLocation, tablePartitionColumn, partition);
                String toPartitionPath = String.format("%s/%s=%s", tableLocation, tablePartitionColumn, partition);
                if (!fileSystem.exists(new Path(toPartitionPath))) {
                    fileSystem.mkdirs(new Path(toPartitionPath));
                }
                FileStatus[] files = fileSystem.listStatus(new Path(fromPartitionPath));
                for (FileStatus f : files) {
                    Path from = f.getPath();
                    if (!SegmentHelper.checkSegmentByPath(from)) {
                        continue;
                    }
                    String toFileName = String.format("rt2his.%s.%s.seg", time, UUID.randomUUID().toString());
                    Path to = new Path(toPartitionPath, toFileName);
                    fileSystem.rename(from, to);

                    movedSegmentPaths.add(to);
                }

                // Add partitions manually.
                String addPartition = String.format("ALTER TABLE %s ADD IF NOT EXISTS PARTITION (%s='%s')",
                        hiveTable, tablePartitionColumn, partition);
                try (Statement myStatement = connection.createStatement()) {
                    myStatement.execute(addPartition);
                }
            }
        } catch (Exception e) {
            logger.error("move segment file from segTmpTable failed", e);
            // Try the best to roll back.
            List<Path> remainPaths = new LinkedList<>(movedSegmentPaths);
            Boolean ok = Try.on(() -> {
                        Iterator<Path> it = remainPaths.iterator();
                        while (it.hasNext()) {
                            Path p = it.next();
                            if (fileSystem.exists(p)) {
                                if (fileSystem.delete(p)) {
                                    it.remove();
                                }
                            }
                        }
                        return remainPaths.isEmpty();
                    },
                    5, logger);
            if (ok == null || !ok) {
                logger.error(String.format("Some generated segments failed to move into destination path, data in table [%s] may duplicated. " +
                        "You can try remove those successful segments (%s) manually to fix it. " +
                        "You may need to re-run this task after fix the issue.", hiveTable, remainPaths));
            }

            return false;
        }

        Boolean ok = Try.on(() -> fileSystem.delete(new Path(rt2hisPath), true),
                5, logger,
                String.format("Remove rt2his folder failed. " +
                                "Some old segments failed to deleted, data in table [%s] duplicated. " +
                                "You should delete them manually in [%s] to fix it.",
                        hiveTable, rt2hisPath));
        return ok != null && ok;
    }

    private static String getHiveTableCreateSql(Connection connection, String tableName) throws Exception {
        try (Statement statement = connection.createStatement()) {
            ResultSet resultSet = statement.executeQuery(String.format("SHOW CREATE TABLE %s", tableName));
            String createTable = "";
            while (resultSet.next()) {
                createTable += resultSet.getString(1) + "\n";
            }
            resultSet.close();
            return createTable;
        }
    }

    private static String getHiveTableLocation(String createSql) {
        String locationSql = fetchByRegx(createSql, "LOCATION\\s+'.+'");
        return StringUtils.strip(StringUtils.removeStart(locationSql, "LOCATION").trim(), "'");
    }

    private static String getHiveTablePartitionColumn(String createSql) {
        String partitionSql = fetchByRegx(createSql, "PARTITIONED BY\\s?\\([^)]+\\)");
        String colDef = StringUtils.removeStart(partitionSql, "PARTITIONED BY").trim();
        colDef = StringUtils.stripStart(colDef, "(");
        colDef = StringUtils.stripEnd(colDef, ")");
        if (colDef.split(",").length > 1) {
            throw new IllegalStateException(String.format("%s contains more than one partition column", createSql));
        }
        String colName = fetchByRegx(colDef, "`.+`");
        return StringUtils.strip(colName, "`");
    }

    private static SegmentSchema getSchema(String createSql) {
        String regx = "`\\w+` \\w+ COMMENT '.*'";
        Pattern pattern = Pattern.compile(regx);
        Matcher matcher = pattern.matcher(createSql);
        List<ColumnSchema> columnSchemas = new ArrayList<>();
        while (matcher.find()) {
            String colStr = matcher.group().trim();
            colStr = colStr.substring(0, colStr.indexOf("COMMENT"));
            String[] strs = colStr.trim().split(" ", 2);
            String colName = StringUtils.strip(strs[0], "`");
            String colType = strs[1].trim();
            SQLType indexrType;
            switch (colType.toUpperCase()) {
                case "INT":
                    indexrType = SQLType.INT;
                    break;
                case "BIGINT":
                    indexrType = SQLType.BIGINT;
                    break;
                case "FLOAT":
                    indexrType = SQLType.FLOAT;
                    break;
                case "DOUBLE":
                    indexrType = SQLType.DOUBLE;
                    break;
                case "STRING":
                    indexrType = SQLType.VARCHAR;
                    break;
                case "DATE":
                    indexrType = SQLType.DATE;
                    break;
                case "TIMESTAMP":
                    indexrType = SQLType.DATETIME;
                    break;
                default:
                    throw new IllegalStateException("unsupported hive type " + colType);
            }
            columnSchemas.add(new ColumnSchema(colName, indexrType));
        }
        return new SegmentSchema(columnSchemas);
    }

    private static String fetchByRegx(String source, String regx) {
        Pattern pattern = Pattern.compile(regx);
        Matcher matcher = pattern.matcher(source);
        if (matcher.find()) {
            return matcher.group();
        }
        throw new IllegalStateException(String.format("cannot find regx %s from %s", regx, source));
    }

    private static void createTable(Connection connection,
                                    String tableName,
                                    boolean external,
                                    SegmentSchema schema,
                                    String location,
                                    String partitionColumn) throws Exception {
        try (Statement statement = connection.createStatement()) {
            String createTableSql = HiveHelper.getHiveTableCreateSql(tableName, external, schema, location, partitionColumn);
            logger.debug("create table:\n{}", createTableSql);
            statement.execute(createTableSql);
        }
    }

    private static boolean hasChild(FileSystem fileSystem, Path dir) throws IOException {
        if (!fileSystem.exists(dir)) {
            return false;
        }
        RemoteIterator<LocatedFileStatus> iterator = fileSystem.listFiles(dir, true);
        return iterator.hasNext();
    }
}

