package io.indexr.segment.storage.itg;

import com.google.common.collect.Lists;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.RandomStringUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import io.indexr.io.ByteBufferReader;
import io.indexr.io.ByteBufferWriter;
import io.indexr.segment.Row;
import io.indexr.segment.SQLType;
import io.indexr.segment.Segment;
import io.indexr.segment.SegmentFd;
import io.indexr.segment.SegmentMode;
import io.indexr.segment.cache.IndexExpiredMemCache;
import io.indexr.segment.cache.IndexMemCache;
import io.indexr.segment.cache.PackExpiredMemCache;
import io.indexr.segment.cache.PackMemCache;
import io.indexr.segment.index.RSIndexTest;
import io.indexr.segment.pack.DataPack;
import io.indexr.segment.query.SegmentSelectHelper;
import io.indexr.segment.storage.DPSegment;
import io.indexr.segment.storage.DPSegmentTest;
import io.indexr.segment.storage.OpenOption;
import io.indexr.segment.storage.StorageSegment;
import io.indexr.segment.storage.TestRows;
import io.indexr.segment.storage.UpdateColSchema;
import io.indexr.segment.storage.UpdateColSegment;
import io.indexr.segment.storage.Version;

public class IntegratedTest {
    private static final Logger log = LoggerFactory.getLogger(IntegratedTest.class);
    private static Path workDir;

    @BeforeClass
    public static void init() throws IOException {
        workDir = Files.createTempDirectory("integrate_test_");
        log.debug("workDir: {}\n", workDir.toString());
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
                testCache(version.id, mode, TestRows.genRandomRows(TestRows.ROW_COUNT));
                log.info("time: {}", System.currentTimeMillis() - time);
            }
        }
    }

    public void testCache(int version, SegmentMode mode, Iterator<Row> rows) throws IOException {
        IndexMemCache indexMemCache = new IndexExpiredMemCache(TimeUnit.MINUTES.toMillis(10), 100 * 1024 * 1024);
        PackMemCache packMemCache = new PackExpiredMemCache(TimeUnit.MINUTES.toMillis(10), 100 * 1024 * 1024);

        DPSegment segment = DPSegment.open(
                version,
                mode,
                workDir.resolve("cache" + version),
                "test_segment_integrated",
                TestRows.segmentSchema,
                OpenOption.Overwrite).update();
        DPSegmentTest.addRows(segment, rows);
        segment.seal();

        StorageSegment segment1 = testIntegrated(segment, indexMemCache, packMemCache);
        segment.close();

        StorageSegment segment2 = testIntegrated(segment1, indexMemCache, packMemCache);
        segment2.close();
        segment1.close();

        indexMemCache.close();
        packMemCache.close();
    }

    private StorageSegment testIntegrated(StorageSegment segment, IndexMemCache indexMemCache, PackMemCache packMemCache) throws IOException {
        Path segmentPath = workDir.resolve(".segment." + RandomStringUtils.randomAlphabetic(8));

        FileChannel fileChannel = FileChannel.open(segmentPath,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.READ,
                StandardOpenOption.TRUNCATE_EXISTING);
        ByteBufferWriter writer = ByteBufferWriter.of(fileChannel, null);
        writer.setName(segmentPath.toString());
        ByteBufferReader reader = ByteBufferReader.of(fileChannel, 0, null);
        reader.setName(segmentPath.toString());

        SegmentMeta sectionInfo = Integrate.INSTANCE.write(segment, size -> writer);
        SegmentMeta readSectionInfo = Integrate.INSTANCE.read(reader);

        Assert.assertEquals(sectionInfo, readSectionInfo);

        IntegratedSegment newSegment = IntegratedSegment.Fd.create(segment.name(), ByteBufferReader.Opener.create(fileChannel)).open(indexMemCache, null, packMemCache);

        DPSegmentTest.rowsCmp(
                segment.rowTraversal().iterator(),
                newSegment.rowTraversal().iterator());

        List<SegmentFd> segmentFds = new ArrayList<>();
        segmentFds.add(IntegratedSegment.Fd.create(segment.name() + "1", sectionInfo, ByteBufferReader.Opener.create(fileChannel)));
        segmentFds.add(IntegratedSegment.Fd.create(segment.name() + "2", sectionInfo, ByteBufferReader.Opener.create(fileChannel)));

        Path cachePath = workDir.resolve("section.cache");
        IntegratedSegment.Fd.saveToLocalCache(cachePath, segmentFds);

        List<SegmentFd> newSegmentFds = IntegratedSegment.Fd.loadFromLocalCache(cachePath, n -> ByteBufferReader.Opener.create(fileChannel));

        for (int i = 0; i < segmentFds.size(); i++) {
            Segment s1 = segmentFds.get(i).open(indexMemCache, null, packMemCache);
            Segment s2 = newSegmentFds.get(i).open(indexMemCache, null, packMemCache);
            DPSegmentTest.rowsCmp(
                    s1.rowTraversal().iterator(),
                    s2.rowTraversal().iterator());
            s1.close();
            s2.close();
        }


        IntegratedSegment segmentNotCache = IntegratedSegment.Fd.create(segment.name(), ByteBufferReader.Opener.create(fileChannel)).open();
        RSIndexTest.checkIndex(segmentNotCache);
        segmentNotCache.close();

        return newSegment;
    }

    @Test
    public void testUpdateColumn() throws IOException {
        for (Version version : Version.values()) {
            for (SegmentMode mode : SegmentMode.values()) {
                log.info("{}, {}", version, mode);
                long time = System.currentTimeMillis();
                testUpdateColumn(version.id, mode);
                log.info("time: {}", System.currentTimeMillis() - time);
            }
        }
    }

    public void testUpdateColumn(int version, SegmentMode mode) throws IOException {
        DPSegment segment = DPSegment.open(
                version,
                mode,
                workDir.resolve("updatecolumn" + version),
                "test_segment_altercolumn",
                TestRows.segmentSchema,
                OpenOption.Overwrite).update();
        DPSegmentTest.addRows(segment, DPSegmentTest.genRows(DataPack.MAX_COUNT * 3 + 99));
        segment.seal();

        Path segmentPath = workDir.resolve(".segment." + RandomStringUtils.randomAlphabetic(8));

        FileChannel fileChannel = FileChannel.open(segmentPath,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.READ,
                StandardOpenOption.TRUNCATE_EXISTING);
        ByteBufferWriter writer = ByteBufferWriter.of(fileChannel, null);
        writer.setName(segmentPath.toString());
        ByteBufferReader reader = ByteBufferReader.of(fileChannel, 0, null);
        reader.setName(segmentPath.toString());

        SegmentMeta sectionInfo = Integrate.INSTANCE.write(segment, size -> writer);
        SegmentMeta readSectionInfo = Integrate.INSTANCE.read(reader);

        if (!sectionInfo.equals(readSectionInfo))
            Assert.assertEquals(sectionInfo, readSectionInfo);

        IntegratedSegment baseSegment = IntegratedSegment.Fd.create(segment.name(), ByteBufferReader.Opener.create(fileChannel)).open();

        System.out.println("baseSegment=============");
        SegmentSelectHelper.selectSegment(baseSegment, "select * from A limit 10", itr -> {
            SegmentSelectHelper.printRows(itr, baseSegment.schema().getColumns());
        });


        // add
        System.out.println("addColumnSegment=============");
        List<UpdateColSchema> addColumns = Arrays.asList(
                new UpdateColSchema("_c0", SQLType.INT, true, "c0"),
                new UpdateColSchema("_c1", SQLType.BIGINT, false, "c1"),
                new UpdateColSchema("_c2", SQLType.FLOAT, true, "c2"),
                new UpdateColSchema("_c3", SQLType.DOUBLE, false, "c3"),
                new UpdateColSchema("_c4", SQLType.VARCHAR, true, "c4"),

                new UpdateColSchema("_cstr", SQLType.VARCHAR, false, "cast(c0, string)"),
                new UpdateColSchema("_ccast", SQLType.VARCHAR, true, "cast(c0 + c3, string)"),
                new UpdateColSchema("_cplus", SQLType.FLOAT, false, "c0 + c1 + c2"),
                new UpdateColSchema("_cif", SQLType.INT, true, "if((c0 > 100), 100, 10)"),

                new UpdateColSchema("_date", SQLType.DATE, false, "'2015-09-12'"),
                new UpdateColSchema("_time", SQLType.TIME, true, "'23:59:59'"),
                new UpdateColSchema("_datetime", SQLType.DATETIME, false, "'2015-09-12T23:59:59'")
        );

        Path addStorePath = workDir.resolve(".segment_add." + RandomStringUtils.randomAlphabetic(8));
        Files.createDirectory(addStorePath);
        StorageSegment addSegment = UpdateColSegment.addColumn("add_segment", baseSegment, addColumns, addStorePath);

        SegmentSelectHelper.selectSegment(addSegment, "select * from A limit 10", itr -> {
            SegmentSelectHelper.printRows(itr, addSegment.schema().getColumns());
        });


        // Delete
        System.out.println("deleteColumnSegment=============");

        List<String> deleteColumns = Lists.transform(addColumns, c -> c.name);
        StorageSegment deleteSegment = UpdateColSegment.deleteColumn("delete_segment", addSegment, deleteColumns);

        SegmentSelectHelper.selectSegment(deleteSegment, "select * from A limit 10", itr -> {
            SegmentSelectHelper.printRows(itr, deleteSegment.schema().getColumns());
        });

        DPSegmentTest.rowsCmp(baseSegment.rowTraversal().iterator(), deleteSegment.rowTraversal().iterator());

        // Alter
        System.out.println("alterColumnSegment=============");

        List<UpdateColSchema> alterColumns = Arrays.asList(
                new UpdateColSchema("c0", SQLType.VARCHAR, true, "cast(c0, string)"),
                new UpdateColSchema("c1", SQLType.VARCHAR, false, "cast(c0 + c3, string)"),
                new UpdateColSchema("c2", SQLType.FLOAT, true, "c0 + c1 + c2"),
                new UpdateColSchema("c3", SQLType.INT, false, "if((c0 > 100), 100, 10)"),
                new UpdateColSchema("c4", SQLType.INT, true, "c0"),
                new UpdateColSchema("_date", SQLType.DATE, false, "'2015-09-12'")
        );

        Path alterStorePath = workDir.resolve(".segment_alter." + RandomStringUtils.randomAlphabetic(8));
        Files.createDirectory(alterStorePath);

        StorageSegment alterSegment = UpdateColSegment.alterColumn("alter_segment", deleteSegment, alterColumns, alterStorePath);
        SegmentSelectHelper.selectSegment(alterSegment, "select * from A limit 10", itr -> {
            SegmentSelectHelper.printRows(itr, alterSegment.schema().getColumns());
        });
    }
}
