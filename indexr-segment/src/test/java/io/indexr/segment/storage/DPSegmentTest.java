package io.indexr.segment.storage;

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
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import io.indexr.segment.ColumnSchema;
import io.indexr.segment.Row;
import io.indexr.segment.SegmentMode;
import io.indexr.segment.SegmentSchema;
import io.indexr.segment.index.RSIndexTest;
import io.indexr.segment.pack.DataPack;
import io.indexr.util.DateTimeUtil;

public class DPSegmentTest {
    private static final Logger log = LoggerFactory.getLogger(DPSegmentTest.class);
    private static Path workDir;
    private static SegmentSchema segmentSchema = TestRows.segmentSchema;
    private static List<ColumnSchema> columnSchemas = TestRows.columnSchemas;
    private static int rowCount = DataPack.MAX_COUNT * 3 + 99;

    @BeforeClass
    public static void init() throws IOException {
        workDir = Files.createTempDirectory("segment_test_");
        log.debug("workDir: {}", workDir.toString());
    }

    @AfterClass
    public static void cleanUp() throws IOException {
        FileUtils.deleteDirectory(workDir.toFile());
    }

    public static Iterator<Row> genRows(final int theRowCount) {
        return TestRows.genRows(theRowCount);
    }

    public static void rowsCmp(Iterator<? extends Row> expected, Iterator<? extends Row> actual) {
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

    public static void addRows(DPSegment segment, Iterator<Row> rows) throws IOException {
        while (rows.hasNext()) {
            segment.add(rows.next());
        }
    }

    private void test_generate(int version, SegmentMode mode, String name, String path) throws IOException {
        DPSegment segment = DPSegment.open(
                version,
                mode,
                Paths.get(path),
                name,
                segmentSchema,
                OpenOption.Overwrite).update();
        addRows(segment, genRows(rowCount));
        segment.seal();
        rowsCmp(genRows(rowCount), segment.rowTraversal().iterator());
    }

    private void test_generate2(int version, SegmentMode mode, String name, String path) throws IOException {
        DPSegment segment = DPSegment.open(
                version,
                mode,
                Paths.get(path),
                name,
                segmentSchema,
                OpenOption.Overwrite).update();
        addRows(segment, genRows(rowCount));
        segment.seal();

        RSIndexTest.checkIndex(segment);
        segment.close();
    }

    private void test_append(String path) throws IOException {
        DPSegment segment = DPSegment.open(Paths.get(path));

        rowsCmp(genRows(rowCount), segment.rowTraversal().iterator());
        segment.update();
        segment.seal();
        segment.update();
        addRows(segment, genRows(rowCount));
        segment.seal();

        Iterator<Row> toCompare = Iterators.<Row>concat(genRows(rowCount), genRows(rowCount));
        rowsCmp(toCompare, segment.rowTraversal().iterator());

        segment = DPSegment.open(Paths.get(path));
        toCompare = Iterators.<Row>concat(genRows(rowCount), genRows(rowCount));
        rowsCmp(toCompare, segment.rowTraversal().iterator());

        RSIndexTest.checkIndex(segment);

        segment.close();
    }

    private void test_merge(int version, SegmentMode mode, String name, String path) throws IOException {
        DPSegment segment = DPSegment.open(
                version,
                mode,
                Paths.get(path),
                name,
                segmentSchema,
                OpenOption.Overwrite).update();
        addRows(segment, genRows(rowCount));

        DPSegment segment2 = DPSegment.open(
                version,
                mode,
                Paths.get(path + UUID.randomUUID().toString()),
                name,
                segmentSchema,
                OpenOption.Overwrite).update();
        addRows(segment2, genRows(rowCount));

        DPSegment segment3 = DPSegment.open(
                version,
                mode,
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
                mode,
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

        RSIndexTest.checkIndex(segment);

        segment.close();
    }

    @Test
    public void test() throws IOException {
        String segmentName = "test_segment" + 1;
        String segmentPath = workDir.toString();
        for (Version version : Version.values()) {
            for (SegmentMode mode : SegmentMode.values()) {
                log.info("{}, {}", version, mode);
                long now = System.currentTimeMillis();
                test_generate(version.id, mode, segmentName, segmentPath);
                test_generate2(version.id, mode, segmentName, segmentPath + "_2");
                test_append(segmentPath);
                test_merge(version.id, mode, segmentName, segmentPath);
                log.info("{}", System.currentTimeMillis() - now);
            }
        }
    }
}
