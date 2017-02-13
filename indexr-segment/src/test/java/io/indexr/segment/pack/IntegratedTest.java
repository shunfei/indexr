package io.indexr.segment.pack;

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
import java.util.List;
import java.util.concurrent.TimeUnit;

import io.indexr.io.ByteBufferReader;
import io.indexr.io.ByteBufferWriter;
import io.indexr.segment.SQLType;
import io.indexr.segment.Segment;
import io.indexr.segment.SegmentFd;
import io.indexr.segment.pack.Integrated.SectionInfo;
import io.indexr.segment.query.SegmentSelectHelper;

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
    public void testNotCache() throws IOException {
        for (Version version : Version.values()) {
            testNotCache(version.id);
        }
    }

    private void testNotCache(int version) throws IOException {
        DPSegment segment = DPSegment.open(
                version,
                workDir.resolve("notcache" + version),
                "test_segment_integrated",
                DPSegmentTest.segmentSchema,
                OpenOption.Overwrite).update();
        DPSegmentTest.addRows(segment, DPSegmentTest.genRows(DataPack.MAX_COUNT * 3 + 99));
        segment.seal();

        StorageSegment segment1 = testIntegrated(segment, null, null);
        segment.close();

        StorageSegment segment2 = testIntegrated(segment1, null, null);
        segment2.close();
        segment1.close();
    }

    @Test
    public void testCache() throws IOException {
        for (Version version : Version.values()) {
            testCache(version.id);
        }
    }

    private void testCache(int version) throws IOException {
        IndexMemCache indexMemCache = new IndexExpiredMemCache(TimeUnit.MINUTES.toMillis(10), 100 * 1024 * 1024);
        PackMemCache packMemCache = new PackExpiredMemCache(TimeUnit.MINUTES.toMillis(10), 100 * 1024 * 1024);

        DPSegment segment = DPSegment.open(
                version,
                workDir.resolve("cache" + version),
                "test_segment_integrated",
                DPSegmentTest.segmentSchema,
                OpenOption.Overwrite).update();
        DPSegmentTest.addRows(segment, DPSegmentTest.genRows(DataPack.MAX_COUNT * 3 + 99));
        segment.seal();

        StorageSegment segment1 = testIntegrated(segment, indexMemCache, packMemCache);
        segment.close();

        StorageSegment segment2 = testIntegrated(segment1, indexMemCache, packMemCache);
        segment2.close();
        segment1.close();

        indexMemCache.close();
        packMemCache.capacity();
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

        SectionInfo sectionInfo = Integrated.write(segment, size -> writer);
        SectionInfo readSectionInfo = Integrated.read(reader);

        Assert.assertEquals(sectionInfo, readSectionInfo);

        IntegratedSegment newSegment = IntegratedSegment.Fd.create(segment.name, ByteBufferReader.Opener.create(fileChannel)).open(indexMemCache, packMemCache);

        DPSegmentTest.rowsCmp(
                segment.rowTraversal().iterator(),
                newSegment.rowTraversal().iterator());

        List<SegmentFd> segmentFds = new ArrayList<>();
        segmentFds.add(IntegratedSegment.Fd.create(segment.name + "1", sectionInfo, ByteBufferReader.Opener.create(fileChannel)));
        segmentFds.add(IntegratedSegment.Fd.create(segment.name + "2", sectionInfo, ByteBufferReader.Opener.create(fileChannel)));

        Path cachePath = workDir.resolve("section.cache");
        IntegratedSegment.Fd.saveToLocalCache(cachePath, segmentFds);

        List<SegmentFd> newSegmentFds = IntegratedSegment.Fd.loadFromLocalCache(cachePath, n -> ByteBufferReader.Opener.create(fileChannel));

        for (int i = 0; i < segmentFds.size(); i++) {
            Segment s1 = segmentFds.get(i).open(indexMemCache, packMemCache);
            Segment s2 = newSegmentFds.get(i).open(indexMemCache, packMemCache);
            DPSegmentTest.rowsCmp(
                    s1.rowTraversal().iterator(),
                    s2.rowTraversal().iterator());
            s1.close();
            s2.close();
        }
        return newSegment;
    }

    @Test
    public void testUpdateColumn() throws IOException {
        for (Version version : Version.values()) {
            testUpdateColumn(version.id);
        }
    }

    private void testUpdateColumn(int version) throws IOException {
        DPSegment segment = DPSegment.open(
                version,
                workDir.resolve("updatecolumn" + version),
                "test_segment_altercolumn",
                DPSegmentTest.segmentSchema,
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

        SectionInfo sectionInfo = Integrated.write(segment, size -> writer);
        SectionInfo readSectionInfo = Integrated.read(reader);

        Assert.assertEquals(sectionInfo, readSectionInfo);

        IntegratedSegment baseSegment = IntegratedSegment.Fd.create(segment.name, ByteBufferReader.Opener.create(fileChannel)).open(null, null);

        System.out.println("baseSegment=============");
        SegmentSelectHelper.selectSegment(baseSegment, "select * from A limit 10", itr -> {
            SegmentSelectHelper.printRows(itr, baseSegment.schema().getColumns());
        });


        // add
        System.out.println("addColumnSegment=============");
        List<UpdateColSchema> addColumns = Arrays.asList(
                new UpdateColSchema("_c0", SQLType.INT, "c0"),
                new UpdateColSchema("_c1", SQLType.BIGINT, "c1"),
                new UpdateColSchema("_c2", SQLType.FLOAT, "c2"),
                new UpdateColSchema("_c3", SQLType.DOUBLE, "c3"),
                new UpdateColSchema("_c4", SQLType.VARCHAR, "c4"),

                new UpdateColSchema("_cstr", SQLType.VARCHAR, "cast(c0, string)"),
                new UpdateColSchema("_ccast", SQLType.VARCHAR, "cast(c0 + c3, string)"),
                new UpdateColSchema("_cplus", SQLType.FLOAT, "c0 + c1 + c2"),
                new UpdateColSchema("_cif", SQLType.INT, "if((c0 > 100), 100, 10)"),

                new UpdateColSchema("_date", SQLType.DATE, "'2015-09-12'"),
                new UpdateColSchema("_time", SQLType.TIME, "'23:59:59'"),
                new UpdateColSchema("_datetime", SQLType.DATETIME, "'2015-09-12T23:59:59'")
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
                new UpdateColSchema("c0", SQLType.VARCHAR, "cast(c0, string)"),
                new UpdateColSchema("c1", SQLType.VARCHAR, "cast(c0 + c3, string)"),
                new UpdateColSchema("c2", SQLType.FLOAT, "c0 + c1 + c2"),
                new UpdateColSchema("c3", SQLType.INT, "if((c0 > 100), 100, 10)"),
                new UpdateColSchema("c4", SQLType.INT, "c0")
        );

        Path alterStorePath = workDir.resolve(".segment_alter." + RandomStringUtils.randomAlphabetic(8));
        Files.createDirectory(alterStorePath);

        StorageSegment alterSegment = UpdateColSegment.alterColumn("alter_segment", deleteSegment, alterColumns, alterStorePath);
        SegmentSelectHelper.selectSegment(alterSegment, "select * from A limit 10", itr -> {
            SegmentSelectHelper.printRows(itr, alterSegment.schema().getColumns());
        });
    }
}
