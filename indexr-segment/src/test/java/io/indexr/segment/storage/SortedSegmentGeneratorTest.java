package io.indexr.segment.storage;

import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.TreeMap;

import io.indexr.segment.Row;
import io.indexr.segment.SegmentMode;
import io.indexr.segment.rt.EventIgnoreStrategy;
import io.indexr.segment.rt.UTF8Row;

public class SortedSegmentGeneratorTest {
    private static final Logger log = LoggerFactory.getLogger(SortedSegmentGeneratorTest.class);
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

    @Test
    public void test() throws IOException {
        for (Version version : Version.values()) {
            for (SegmentMode mode : SegmentMode.values()) {
                log.info("{}, {}", version, mode);
                long time = System.currentTimeMillis();
                _test(version.id, mode);
                log.info("time: {}", System.currentTimeMillis() - time);
            }
        }
    }

    public void _test(int version, SegmentMode mode) throws IOException {
        int rowCount = 1000000;
        List<String> sortCols = Arrays.asList(TestRows.columnSchemas.get(0).getName(), TestRows.columnSchemas.get(1).getName());
        SortedSegmentGenerator generator = new SortedSegmentGenerator(
                version,
                mode,
                workDir,
                "test",
                TestRows.segmentSchema,
                false,
                sortCols,
                null,
                100000);
        Iterator<Row> rows = TestRows.genRows(rowCount);
        while (rows.hasNext()) {
            generator.add(rows.next());
        }
        DPSegment segment = generator.seal();
        Assert.assertEquals(rowCount, segment.rowCount());

        TreeMap<UTF8Row, UTF8Row> sortMap = new TreeMap<>(UTF8Row.dimBytesComparator());
        UTF8Row.Creator creator = new UTF8Row.Creator(
                false,
                TestRows.columnSchemas,
                sortCols,
                null,
                null,
                null,
                EventIgnoreStrategy.NO_IGNORE);
        rows = TestRows.genRows(rowCount);
        while (rows.hasNext()) {
            UTF8Row utf8Row = UTF8Row.from(creator, rows.next());
            sortMap.put(utf8Row, utf8Row);
        }
        DPSegmentTest.rowsCmp(sortMap.values().iterator(), segment.rowTraversal().iterator());
    }
}
