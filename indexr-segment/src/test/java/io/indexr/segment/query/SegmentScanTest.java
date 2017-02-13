package io.indexr.segment.query;

import org.apache.commons.io.FileUtils;
import org.apache.spark.memory.MemoryManager;
import org.apache.spark.memory.TaskMemoryManager;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import io.indexr.query.BasicPlanner;
import io.indexr.query.Catalog;
import io.indexr.query.QueryContext;
import io.indexr.query.QueryExecution;
import io.indexr.query.QueryPlanner;
import io.indexr.query.TaskContext;
import io.indexr.query.TaskContextImpl;
import io.indexr.query.plan.physical.PhysicalPlan;
import io.indexr.query.row.InternalRow;
import io.indexr.segment.ColumnSchema;
import io.indexr.segment.Row;
import io.indexr.segment.SQLType;
import io.indexr.segment.SegmentSchema;
import io.indexr.segment.helper.SimpleRow;
import io.indexr.segment.pack.DPSegment;

public class SegmentScanTest {
    static List<ColumnSchema> columnSchemas = Arrays.asList(
            new ColumnSchema("c0", SQLType.INT),
            new ColumnSchema("c1", SQLType.BIGINT),
            new ColumnSchema("c2", SQLType.FLOAT),
            new ColumnSchema("c3", SQLType.DOUBLE),
            new ColumnSchema("c4", SQLType.VARCHAR),
            new ColumnSchema("c5", SQLType.DATE),
            new ColumnSchema("c6", SQLType.TIME),
            new ColumnSchema("c5", SQLType.DATETIME)
    );
    static SegmentSchema segmentSchema = new SegmentSchema(columnSchemas);
    private static String[][] rawRows = new String[][]{
            {"89", "222222", "4.5", "9.1", "windows", "2014-12-09", "00:00:00", "2014-12-09T00:00:00"},
            {"3", String.valueOf(Long.MAX_VALUE), "4.5", "9.199", "mac", "1901-03-24", "11:43:56", "1901-03-24T11:43:56"},
            {"14121", "99", "2.5", "11.1", "linux", "9999-01-01", "12:59:59", "9999-01-01T12:59:59"},
            {String.valueOf(Integer.MAX_VALUE), "11", String.valueOf(Float.MIN_VALUE), "1.51", "android", "2741-1-3", "1:1:1", "2741-1-3T1:1:1"},
    };
    static List<Row> sample_rows = new ArrayList<>(rawRows.length);
    static int rowCount = 10; // DataPack.MAX_COUNT * 3 + 99;
    static DPSegment segment;
    static Path workDir;
    static MemoryManager memoryManager = new MemoryManager();
    static long id = 1;

    static {
        SimpleRow.Builder builder = SimpleRow.Builder.createByColumnSchemas(columnSchemas);
        for (int rowId = 0; rowId < rawRows.length; rowId++) {
            builder.appendStringFormVals(Arrays.asList(rawRows[rowId]));
            sample_rows.add(builder.buildAndReset());
        }

        try {
            workDir = Files.createTempDirectory("indexr_test_segment");
            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    try {
                        FileUtils.deleteDirectory(workDir.toFile());
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        try {
            String testSegmentDir = System.getenv().get("SEGMENTDIR");
            if (testSegmentDir == null) {
                System.out.println("SEGMENTDIR env not found!");
                segment = createTmpSegment(rowCount);
            } else {
                System.out.println("SEGMENTDIR: " + testSegmentDir);
                //segment = DPSegment.fromPath(testSegmentDir, false);
                segment = DPSegment.open(testSegmentDir);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static DPSegment createTmpSegment(int rowCount) throws Exception {
        String segmentId = "test_segment";
        DPSegment segment = null;
        try {
            String path = workDir.toString();
            System.out.println("Segment paht:" + path);
            segment = DPSegment.open(path, segmentId, segmentSchema).update();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        Iterator<Row> it = genRows(rowCount);
        while (it.hasNext()) {
            segment.add(it.next());
        }
        segment.seal();
        return segment;
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

    public static void main(String[] args) {
        //STRING sql = "SELECT user_id, sum(impressions) as sum_c1, count(*) as count from A where user_id > 1000 group by user_id having count >= 2  ";
        //STRING sql 0= "select c0, sum(c1), avg(c3) from A group by c0 limit 10";
        execute("select c0, c1, if(((c0 > 100) & (c1 != 0)), 'a', c4), cast((10 + 20), string), c3, cast(c3, string) from A limit 10");
        //execute("select c0, avg(c1), sum((c1 - ((c2 / c3) * c3)) % 10), min(one(c3) + 100) as _cc from A group by c0 having  _cc > 10 limit 10");
        //execute(sql);
        //execute(sql);
    }


    private static void execute(String sql) {
        long taskId = id++;
        TaskContextImpl taskContext = new TaskContextImpl(taskId, new TaskMemoryManager(memoryManager, taskId));
        TaskContext.setTaskContext(taskContext);

        Catalog catalog = n -> {
            return new SegmentRelation(
                    SegmentSelectHelper.fromSegmentSchema(segment.schema()).toAttributes(),
                    segment);
        };

        QueryPlanner<PhysicalPlan> planner = new BasicPlanner(
                plan -> {
                    if (plan instanceof SegmentRelation) {
                        SegmentRelation relation = (SegmentRelation) plan;
                        return Collections.singletonList(new SegmentScan(relation.output(), relation.segments));
                    } else {
                        return Collections.emptyList();
                    }
                }
        );

        QueryContext context = new QueryContext(catalog, planner);
        QueryExecution execution = context.executeSql(sql);
        System.out.println("logicalPlan: =========\n" + execution.logicalPlan());
        System.out.println("analyzedPlan: =========\n" + execution.analyzedPlan());
        System.out.println("optimizedPlan: =========\n" + execution.optimizedPlan());
        System.out.println("optimizedPlan.resolved: =========\n" + execution.optimizedPlan().resolved());
        System.out.println("physicalPlan: =========\n" + execution.physicalPlan());
        System.out.println("physicalPlan: =========\n");

        Iterator<InternalRow> res = execution.result();
        SegmentSelectHelper.printRows(res, columnSchemas);

        taskContext.markTaskCompleted();
        long freedMemory = taskContext.taskMemoryManager().cleanUpAllAllocatedMemory();
        if (freedMemory > 0) {
            String errMsg = String.format("Managed memory leak detected; size = %s bytes, TID = %s", freedMemory, taskId);
            System.out.println(errMsg);
        }
    }
}
