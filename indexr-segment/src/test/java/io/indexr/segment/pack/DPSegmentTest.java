package io.indexr.segment.pack;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;

import org.apache.commons.io.FileUtils;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import io.indexr.segment.ColumnSchema;
import io.indexr.segment.Row;
import io.indexr.segment.SQLType;
import io.indexr.segment.SegmentSchema;
import io.indexr.segment.helper.SimpleRow;
import io.indexr.util.DateTimeUtil;

public class DPSegmentTest {
    private static final Logger log = LoggerFactory.getLogger(DPSegmentTest.class);
    private static Path workDir;

    @BeforeClass
    public static void init() throws IOException {
        workDir = Files.createTempDirectory("segment_test_");
        log.debug("workDir: {}", workDir.toString());
    }

    @AfterClass
    public static void cleanUp() throws IOException {
        FileUtils.deleteDirectory(workDir.toFile());
    }

    static List<ColumnSchema> columnSchemas = Arrays.asList(
            new ColumnSchema("c0", SQLType.INT),
            new ColumnSchema("c1", SQLType.BIGINT),
            new ColumnSchema("c2", SQLType.FLOAT),
            new ColumnSchema("c3", SQLType.DOUBLE),
            new ColumnSchema("c4", SQLType.VARCHAR),
            new ColumnSchema("c5", SQLType.DATE),
            new ColumnSchema("c6", SQLType.TIME),
            new ColumnSchema("c7", SQLType.DATETIME)
    );
    static SegmentSchema segmentSchema = new SegmentSchema(columnSchemas);
    private static String[][] rawRows = new String[][]{
            {"89", "222222", "4.5", "9.1", "windows", "2014-12-09", "00:00:00", "2014-12-09T00:00:00"},
            {"3", String.valueOf(Long.MAX_VALUE), "4.5", "9.199", "mac", "1901-03-24", "11:43:56", "1901-03-24T11:43:56"},
            {"14121", "99", "2.5", "11.1", "linux", "9999-01-01", "12:59:59", "9999-01-01T12:59:59"},
            {String.valueOf(Integer.MAX_VALUE), "11", String.valueOf(Float.MIN_VALUE), "1.51", "android", "2741-1-3", "1:1:1", "2741-1-3T1:1:1"},
    };

    static List<Row> sample_rows = new ArrayList<>(rawRows.length);
    static final int rowCount = DataPack.MAX_COUNT * 3 + 99;

    static {
        SimpleRow.Builder builder = SimpleRow.Builder.createByColumnSchemas(columnSchemas);
        for (int rowId = 0; rowId < rawRows.length; rowId++) {
            builder.appendStringFormVals(Arrays.asList(rawRows[rowId]));
            sample_rows.add(builder.buildAndReset());
        }
    }

    static Iterator<Row> genRows(final int theRowCount) {
        return new Iterator<Row>() {
            int curIndex;

            @Override
            public boolean hasNext() {
                return curIndex < theRowCount;
            }

            @Override
            public Row next() {
                return sample_rows.get((curIndex++) % sample_rows.size());
            }
        };
    }

    static void rowsCmp(Iterator<Row> expected, Iterator<Row> actual) {
        while (expected.hasNext()) {
            Assert.assertEquals(true, actual.hasNext());
            rowCmp(expected.next(), actual.next());
        }
        Assert.assertEquals(false, actual.hasNext());
    }

    static void rowCmp(Row expected, Row actual) {
        for (int colId = 0; colId < columnSchemas.size(); colId++) {
            ColumnSchema cs = columnSchemas.get(colId);
            switch (cs.getSqlType()) {
                case INT:
                    Assert.assertEquals(expected.getInt(colId), actual.getInt(colId));
                    break;
                case BIGINT:
                    Assert.assertEquals(expected.getLong(colId), actual.getLong(colId));
                    break;
                case FLOAT:
                    Assert.assertEquals(0, Float.compare(expected.getFloat(colId), actual.getFloat(colId)));
                    break;
                case DOUBLE:
                    Assert.assertEquals(0, Double.compare(expected.getDouble(colId), actual.getDouble(colId)));
                    break;
                case VARCHAR:
                    UTF8String actualStr = actual.getString(colId);
                    UTF8String expectedStr = expected.getString(colId);
                    Assert.assertEquals(expectedStr, actualStr);
                    break;
                case DATE:
                    Assert.assertEquals(
                            DateTimeUtil.getLocalDate(expected.getLong(colId)),
                            DateTimeUtil.getLocalDate(actual.getLong(colId)));
                    break;
                case TIME:
                    Assert.assertEquals(
                            DateTimeUtil.getLocalTime(expected.getInt(colId)),
                            DateTimeUtil.getLocalTime(actual.getInt(colId)));
                    break;
                case DATETIME:
                    Assert.assertEquals(
                            DateTimeUtil.getLocalDateTime(expected.getLong(colId)),
                            DateTimeUtil.getLocalDateTime(actual.getLong(colId)));
                    break;
                default:
                    throw new IllegalStateException();
            }
        }
    }

    static void addRows(DPSegment segment, Iterator<Row> rows) throws IOException {
        while (rows.hasNext()) {
            segment.add(rows.next());
        }
    }

    private void test_generate(int version, String name, String path, boolean compress) throws IOException {
        DPSegment segment = DPSegment.open(
                version,
                Paths.get(path),
                name,
                segmentSchema,
                OpenOption.Overwrite).setCompress(compress).update();
        addRows(segment, genRows(rowCount));
        segment.seal();
        rowsCmp(genRows(rowCount), segment.rowTraversal().iterator());
    }

    private void test_append(int version, String name, String path) throws IOException {
        DPSegment segment = DPSegment.open(version, Paths.get(path), name, null);

        rowsCmp(genRows(rowCount), segment.rowTraversal().iterator());
        segment.update();
        segment.seal();
        segment.update();
        DPSegment _segment = segment;
        addRows(segment, genRows(rowCount));
        segment.seal();

        Iterator<Row> toCompare = Iterators.<Row>concat(genRows(rowCount), genRows(rowCount));
        rowsCmp(toCompare, segment.rowTraversal().iterator());

        segment = DPSegment.open(version, Paths.get(path), name, segmentSchema);
        toCompare = Iterators.<Row>concat(genRows(rowCount), genRows(rowCount));
        rowsCmp(toCompare, segment.rowTraversal().iterator());
    }

    private void test_merge(int version, String name, String path) throws IOException {
        DPSegment segment = DPSegment.open(
                version, Paths.get(path), name, segmentSchema, OpenOption.Overwrite).update();
        addRows(segment, genRows(rowCount));

        DPSegment segment2 = DPSegment.open(
                version,
                Paths.get(path + UUID.randomUUID().toString()),
                name,
                segmentSchema,
                OpenOption.Overwrite).update();
        addRows(segment2, genRows(rowCount));

        DPSegment segment3 = DPSegment.open(
                version,
                Paths.get(path + UUID.randomUUID().toString()),
                name,
                segmentSchema,
                OpenOption.Overwrite).update();
        addRows(segment3, genRows(rowCount));

        segment.merge(Lists.newArrayList(segment2, segment3));
        segment.seal();

        Iterator<Row> toCompare = Iterators.<Row>concat(
                genRows(DataPack.MAX_COUNT * 3),
                genRows(DataPack.MAX_COUNT * 3),
                genRows(DataPack.MAX_COUNT * 3),
                genRows(99),
                genRows(99),
                genRows(99));
        rowsCmp(toCompare, segment.rowTraversal().iterator());

        segment.update();
        addRows(segment, genRows(rowCount));
        segment.seal();

        toCompare = Iterators.<Row>concat(
                genRows(DataPack.MAX_COUNT * 3),
                genRows(DataPack.MAX_COUNT * 3),
                genRows(DataPack.MAX_COUNT * 3),
                genRows(99),
                genRows(99),
                genRows(99),
                genRows(rowCount));
        rowsCmp(toCompare, segment.rowTraversal().iterator());


        segment = DPSegment.open(
                version,
                Paths.get(path + UUID.randomUUID().toString()),
                name,
                segmentSchema,
                OpenOption.Overwrite).update();
        segment.merge(Lists.newArrayList(segment2, segment3));
        toCompare = Iterators.<Row>concat(
                genRows(DataPack.MAX_COUNT * 3),
                genRows(DataPack.MAX_COUNT * 3),
                genRows(99),
                genRows(99));
        rowsCmp(toCompare, segment.rowTraversal().iterator());
    }

    @Test
    public void test_compress() throws IOException {
        String segmentName = "test_segment" + 0;
        String segmentPath = workDir.toString();
        for (Version version : Version.values()) {
            test_generate(version.id, segmentName, segmentPath, true);
            test_append(version.id, segmentName, segmentPath);
            test_merge(version.id, segmentName, segmentPath);
        }
    }

    @Test
    public void test_notcompress() throws IOException {
        String segmentName = "test_segment" + 1;
        String segmentPath = workDir.toString();
        for (Version version : Version.values()) {
            test_generate(version.id, segmentName, segmentPath, false);
            test_append(version.id, segmentName, segmentPath);
            test_merge(version.id, segmentName, segmentPath);
        }
    }
}
