package io.indexr.segment.pack;

import org.apache.commons.io.FileUtils;
import org.apache.spark.unsafe.types.UTF8String;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Stream;

import io.indexr.data.BytePiece;
import io.indexr.io.ByteSlice;
import io.indexr.segment.Column;
import io.indexr.segment.ColumnSchema;
import io.indexr.segment.ColumnType;
import io.indexr.segment.DPValues;
import io.indexr.segment.Row;
import io.indexr.segment.RowTraversal;
import io.indexr.segment.SQLType;
import io.indexr.segment.Segment;
import io.indexr.segment.SegmentSchema;
import io.indexr.util.MemoryUtil;

public class SegmentBenchmark {

    private static List<ColumnSchema> columnSchemas = Arrays.asList(
            new ColumnSchema("c0", SQLType.INT),
            new ColumnSchema("c1", SQLType.BIGINT),
            new ColumnSchema("c2", SQLType.FLOAT),
            new ColumnSchema("c3", SQLType.DOUBLE),
            new ColumnSchema("c4", SQLType.VARCHAR),
            new ColumnSchema("c5", SQLType.DATE),
            new ColumnSchema("c6", SQLType.TIME),
            new ColumnSchema("c5", SQLType.DATETIME)
    );
    private static SegmentSchema segmentSchema = new SegmentSchema(columnSchemas);

    private static class MyRow implements Row {
        private Random random = new Random();

        @Override
        public long getUniformValue(int colId, byte type) {
            return random.nextLong();
        }

        @Override
        public int getInt(int colId) {
            return random.nextInt();
        }

        @Override
        public double getDouble(int colId) {
            return random.nextDouble();
        }

        @Override
        public float getFloat(int colId) {
            return random.nextFloat();
        }

        @Override
        public long getLong(int colId) {
            return random.nextLong();
        }

        @Override
        public UTF8String getString(int colId) {
            return UTF8String.fromString(String.valueOf(random.nextLong()));
        }
    }

    private static Iterator<Row> genRows(final int rowCount) {
        return new Iterator<Row>() {
            int curIndex;
            MyRow row = new MyRow();

            @Override
            public boolean hasNext() {
                return curIndex < rowCount;
            }

            @Override
            public Row next() {
                curIndex++;
                return row;
            }
        };
    }

    static void addRows(DPSegment segment, Iterator<Row> rows) throws IOException {
        while (rows.hasNext()) {
            segment.add(rows.next());
        }
    }

    private static class RowTraveler implements Consumer<Row> {
        final List<ColumnSchema> colSchemas;
        long count = 0;


        RowTraveler(List<ColumnSchema> colSchemas) {
            this.colSchemas = colSchemas;
        }

        @Override
        public void accept(Row row) {
            for (int colId = 0; colId < colSchemas.size(); colId++) {
                ColumnSchema cs = colSchemas.get(colId);
                switch (cs.getDataType()) {
                    case ColumnType.INT:
                        row.getInt(colId);
                        break;
                    case ColumnType.LONG:
                        row.getLong(colId);
                        break;
                    case ColumnType.FLOAT:
                        row.getFloat(colId);
                        break;
                    case ColumnType.DOUBLE:
                        row.getDouble(colId);
                        break;
                    case ColumnType.STRING:
                        row.getString(colId);
                        break;
                    default:
                }
            }
            count++;
        }
    }

    @State(Scope.Thread)
    @Fork(2)
    @Measurement(iterations = 5)
    @Warmup(iterations = 1)
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public static class Insert {
        //@Param({"1024", "1048576", "1073741824"})
        //@Param({"1024", "1048576"})
        @Param({"6553500"})
        public int rowCount;
        @Param({"false", "true"})
        public boolean compress;

        private Path workDir;
        private DPSegment segment;

        @Setup(Level.Invocation)
        public void setup() throws IOException {
            workDir = Files.createTempDirectory("dpsegment_bm_");
            String segmentId = "test_segment";
            segment = DPSegment.open(workDir.toString(), segmentId, segmentSchema, OpenOption.Overwrite).setCompress(compress).update();
        }

        @TearDown(Level.Invocation)
        public void cleanup() throws IOException {
            System.gc();
            FileUtils.deleteDirectory(workDir.toFile());
        }

        @Benchmark
        public void insert() throws IOException {
            addRows(segment, genRows(rowCount));
            segment.seal();
        }

    }

    @State(Scope.Thread)
    @Fork(1)
    @Measurement(iterations = 5)
    @Warmup(iterations = 1)
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public static class TravelNotCached {
        @Param({"6553500"})
        public int rowCount;
        @Param({"false", "true"})
        public boolean compress;
        @Param({"0", "1"})
        public int version;

        private Path workDir;

        @Setup(Level.Trial)
        public void setup() throws IOException {
            workDir = Files.createTempDirectory("dpsegment_bm_");
            System.out.println(workDir);
            String segmentId = "test_segment";

            DPSegment insertSegment = DPSegment.open(version, workDir, segmentId, segmentSchema, OpenOption.Overwrite).setCompress(compress).update();
            addRows(insertSegment, genRows(rowCount));
            insertSegment.seal();

            IntegratedSegment.Fd.create(
                    insertSegment,
                    workDir.resolve("integreated"),
                    false);
        }

        @TearDown(Level.Trial)
        public void cleanup() throws IOException {
            System.gc();
            FileUtils.deleteDirectory(workDir.toFile());
        }

        @Benchmark
        public void travel_stream_forEach() throws IOException {
            IntegratedSegment.Fd fd = IntegratedSegment.Fd.create("aa", workDir.resolve("integreated"));
            Segment segment = fd.open();
            RowTraversal traversal = segment.rowTraversal();
            Stream<Row> stream = traversal.stream();

            stream.forEach(new RowTraveler(columnSchemas));
        }

        @Benchmark
        public void travel_stream_forEachOrdered() throws IOException {
            IntegratedSegment.Fd fd = IntegratedSegment.Fd.create("aa", workDir.resolve("integreated"));
            Segment segment = fd.open();
            RowTraversal traversal = segment.rowTraversal();
            Stream<Row> stream = traversal.stream();

            stream.forEachOrdered(new RowTraveler(columnSchemas));
        }

        @Benchmark
        public void travel_iterator() throws IOException {
            IntegratedSegment.Fd fd = IntegratedSegment.Fd.create("aa", workDir.resolve("integreated"));
            Segment segment = fd.open();
            RowTraversal traversal = segment.rowTraversal();
            Iterator<Row> iterator = traversal.iterator();
            RowTraveler traveler = new RowTraveler(columnSchemas);
            while (iterator.hasNext()) {
                traveler.accept(iterator.next());
            }
        }

        @Benchmark
        public void travel_by_pack() throws IOException {
            IntegratedSegment.Fd fd = IntegratedSegment.Fd.create("aa", workDir.resolve("integreated"));
            IntegratedSegment segment = fd.open();
            SegmentBenchmark.travel_by_pack(segment);
        }
    }

    @State(Scope.Thread)
    @Fork(1)
    @Measurement(iterations = 5)
    @Warmup(iterations = 1)
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public static class TravelOnCached {
        @Param({"6553500"})
        public int rowCount;
        @Param({"0", "1"})
        public int version;

        private Path workDir;
        private StorageSegment segment;
        private Column[] columns;

        private IndexMemCache indexMemCache;
        private PackMemCache packMemCache;

        DPSegment getSegment(String path) throws IOException {
            return DPSegment.open(path, null, null);
        }

        @Setup(Level.Trial)
        public void setup() throws IOException {
            indexMemCache = new IndexExpiredMemCache(TimeUnit.MINUTES.toMillis(10), 1000 * 1024 * 1024);
            packMemCache = new PackExpiredMemCache(TimeUnit.MINUTES.toMillis(10), 1000 * 1024 * 1024);

            workDir = Files.createTempDirectory("dpsegment_bm_");
            String segmentId = "test_segment";

            DPSegment insertSegment = DPSegment.open(version, workDir, segmentId, segmentSchema, OpenOption.Overwrite).update();
            addRows(insertSegment, genRows(rowCount));
            insertSegment.seal();

            IntegratedSegment.Fd.create(
                    insertSegment,
                    workDir.resolve("integreated"),
                    false);

            IntegratedSegment.Fd fd = IntegratedSegment.Fd.create("aa", workDir.resolve("integreated"));
            segment = fd.open(indexMemCache, packMemCache);

            // warm up.
            travel_stream_forEach();
            travel_by_pack();
        }

        @TearDown(Level.Trial)
        public void cleanup() throws IOException {
            indexMemCache.close();
            packMemCache.close();
            System.gc();
            FileUtils.deleteDirectory(workDir.toFile());
        }

        @Benchmark
        public void travel_stream_forEach() {
            segment.rowTraversal().stream().forEach(new RowTraveler(columnSchemas));
        }

        @Benchmark
        public void travel_by_pack() throws IOException {
            SegmentBenchmark.travel_by_pack(segment);
        }
    }

    private static class NumWrapper {
        long l;
        double d;
    }

    private static void travel_by_pack(StorageSegment segment) throws IOException {
        long rowCount = segment.rowCount();
        BytePiece bytePiece = new BytePiece();

        Column[] columns = new Column[segmentSchema.columns.size()];
        for (int i = 0; i < segmentSchema.columns.size(); i++) {
            columns[i] = segment.column(i);
        }
        int packCount = columns[0].packCount();

        int version = segment.version();
        ByteSlice buffer = ByteSlice.allocateDirect(DataPack.MAX_COUNT * 8);

        NumWrapper num = new NumWrapper();
        for (int colId = 0; colId < columnSchemas.size(); colId++) {
            Column column = columns[colId];
            ColumnSchema cs = columnSchemas.get(colId);
            switch (cs.getDataType()) {
                case ColumnType.INT:
                    for (int packId = 0; packId < packCount; packId++) {
                        DataPack pack = (DataPack) column.pack(packId);
                        if (version == Version.VERSION_0_ID) {
                            int objCount = pack.count();
                            pack.foreach(0, objCount, (int i, int v) -> {num.l += v;});
                        } else {
                            MemoryUtil.copyMemory(pack.dataAddr, buffer.address(), (int) pack.size());
                        }
                    }
                    break;
                case ColumnType.LONG:
                    for (int packId = 0; packId < packCount; packId++) {
                        DataPack pack = (DataPack) column.pack(packId);
                        if (version == Version.VERSION_0_ID) {
                            int objCount = pack.count();
                            pack.foreach(0, objCount, (int i, long v) -> {num.l += v;});
                        } else {
                            MemoryUtil.copyMemory(pack.dataAddr, buffer.address(), (int) pack.size());
                        }
                    }
                    break;
                case ColumnType.FLOAT:
                    for (int packId = 0; packId < packCount; packId++) {
                        DataPack pack = (DataPack) column.pack(packId);
                        if (version == Version.VERSION_0_ID) {
                            int objCount = pack.count();
                            pack.foreach(0, objCount, (int i, float v) -> {num.d += v;});
                        } else {
                            MemoryUtil.copyMemory(pack.dataAddr, buffer.address(), (int) pack.size());
                        }
                    }
                    break;
                case ColumnType.DOUBLE:
                    for (int packId = 0; packId < packCount; packId++) {
                        DataPack pack = (DataPack) column.pack(packId);
                        if (version == Version.VERSION_0_ID) {
                            int objCount = pack.count();
                            pack.foreach(0, objCount, (int i, double v) -> {num.d += v;});
                        } else {
                            MemoryUtil.copyMemory(pack.dataAddr, buffer.address(), (int) pack.size());
                        }
                    }
                    break;
                case ColumnType.STRING:
                    for (int packId = 0; packId < packCount; packId++) {
                        DPValues pack = column.pack(packId);
                        int objCount = pack.count();
                        pack.foreach(0, objCount, (int i, BytePiece bytes) -> {});
                    }
                    break;
                default:
            }
        }
    }

    private static boolean stop;

    public static void main(String[] args) throws Exception {
        final TravelOnCached travel = new TravelOnCached();
        travel.rowCount = 10485760;
        travel.setup();

        System.out.println("begin!");

        while (!stop) {
            travel.travel_by_pack();
        }

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                stop = true;
                try {
                    travel.cleanup();
                } catch (IOException e) {

                }
            }
        });
    }
}
