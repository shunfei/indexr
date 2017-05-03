package io.indexr.server;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import io.indexr.data.BytePiece;
import io.indexr.segment.ColumnSchema;
import io.indexr.segment.Row;
import io.indexr.segment.Segment;
import io.indexr.segment.SegmentFd;
import io.indexr.segment.SegmentSchema;
import io.indexr.segment.cache.IndexMemCache;
import io.indexr.segment.cache.PackMemCache;
import io.indexr.util.Try;

public class IndexRNodeTest {
    private static final Logger logger = LoggerFactory.getLogger(IndexRNodeTest.class);
    private static ScheduledExecutorService queryExecSvr = Executors.newScheduledThreadPool(10);

    private static String tableName;

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            System.out.println("Please specify one table name by program arguments");
            System.exit(1);
        }
        tableName = args[0];
        IndexRNode node = new IndexRNode("localhost");
        query(node);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                IOUtils.closeQuietly(node);
            }
        });
        //System.in.read();
    }

    public static void query(IndexRNode node) throws Exception {
        queryExecSvr.scheduleWithFixedDelay(
                () -> Try.on(() -> literalRows(node), logger),
                200, 1000, TimeUnit.MILLISECONDS);

    }

    private static void literalRows(IndexRNode node) throws Exception {
        long lastTime = System.currentTimeMillis();
        long count = 0;
        TableSchema tableSchema = node.getTablePool().getTableSchema(tableName);
        if (tableSchema == null) {
            logger.warn("Table {} not exists.", tableName);
            return;
        }
        IndexMemCache indexMemCache = node.getConfig().getIndexMemCache();
        PackMemCache packMemCache = node.getConfig().getPackMemCache();
        BytePiece bytePiece = new BytePiece();
        List<SegmentFd> segmentFds = node.getTablePool().get(tableName).segmentPool().all();
        for (SegmentFd segmentFd : segmentFds) {
            try (Segment segment = segmentFd.open(indexMemCache, null, packMemCache)) {
                Iterator<Row> rows = segment.rowTraversal().iterator();
                while (rows.hasNext() && !Thread.interrupted()) {
                    literalRow(tableSchema.schema, rows.next(), bytePiece);
                    count++;
                }
            }
        }
        long currentTime = System.currentTimeMillis();
        logger.info("{}: {} rows, query time: {} ms", currentTime, count, currentTime - lastTime);
    }

    private static void literalRow(SegmentSchema schema, Row row, BytePiece bytePiece) throws Exception {
        if (true) {
            return;
        }
        int colId = 0;
        for (ColumnSchema cs : schema.columns) {
            switch (cs.getSqlType()) {
                case INT:
                case TIME:
                    row.getInt(colId);
                    break;
                case BIGINT:
                case DATE:
                case DATETIME:
                    row.getLong(colId);
                    break;
                case FLOAT:
                    row.getFloat(colId);
                    break;
                case DOUBLE:
                    row.getDouble(colId);
                    break;
                case VARCHAR:
                    row.getRaw(colId, bytePiece);
                    break;
                default:
            }
            colId++;
        }
    }

    private static void printRow(Row row) throws Exception {
        System.out.println(row);
    }
}
