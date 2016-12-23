package io.indexr.segment.query;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.memory.MemoryManager;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import io.indexr.query.BasicPlanner;
import io.indexr.query.Catalog;
import io.indexr.query.QueryContext;
import io.indexr.query.QueryExecution;
import io.indexr.query.QueryPlanner;
import io.indexr.query.TaskContext;
import io.indexr.query.TaskContextImpl;
import io.indexr.query.expr.attr.Attribute;
import io.indexr.query.expr.attr.AttributeReference;
import io.indexr.query.plan.physical.PhysicalPlan;
import io.indexr.query.row.InternalRow;
import io.indexr.query.types.DataType;
import io.indexr.segment.ColumnSchema;
import io.indexr.segment.ColumnType;
import io.indexr.segment.Segment;
import io.indexr.segment.SegmentSchema;

public class SegmentSelectHelper {
    public static StructType fromSegmentSchema(SegmentSchema schema) {
        return new StructType(
                schema.columns.stream().map(
                        cs -> new StructField(cs.name, storageTypeToQueryType(cs.dataType))
                ).collect(Collectors.toList())
        );
    }

    public static DataType storageTypeToQueryType(byte segColType) {
        switch (segColType) {
            case ColumnType.INT:
                return DataType.IntegerType;
            case ColumnType.LONG:
                return DataType.LongType;
            case ColumnType.FLOAT:
                return DataType.FloatType;
            case ColumnType.DOUBLE:
                return DataType.DoubleType;
            case ColumnType.STRING:
                return DataType.StringType;
            default:
                throw new IllegalArgumentException();
        }
    }

    public static byte queryTypeToStorageType(DataType type) {
        switch (type) {
            case IntegerType:
                return ColumnType.INT;
            case LongType:
                return ColumnType.LONG;
            case FloatType:
                return ColumnType.FLOAT;
            case DoubleType:
                return ColumnType.DOUBLE;
            case StringType:
                return ColumnType.STRING;
            default:
                throw new IllegalArgumentException();
        }
    }

    public static Attribute columnSchemaToAttribute(ColumnSchema schema) {
        return new AttributeReference(schema.name, storageTypeToQueryType(schema.dataType));
    }

    private static MemoryManager memoryManager = new MemoryManager();

    public static void selectSegment(Segment segment, String sql, RowsConsumer consumer) throws IOException {
        TaskContextImpl taskContext = new TaskContextImpl(0, new TaskMemoryManager(memoryManager, 0));
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

        consumer.accept(execution.result());

        taskContext.markTaskCompleted();
        taskContext.taskMemoryManager().cleanUpAllAllocatedMemory();
    }

    public static interface RowsConsumer {
        void accept(Iterator<InternalRow> it) throws IOException;
    }

    public static void printRows(Iterator<InternalRow> rows, SegmentSchema schema) {
        List<DataType> dataTypes = schema.getColumns().stream().map(
                c -> SegmentSelectHelper.storageTypeToQueryType(c.getDataType())).collect(Collectors.toList());
        for (ColumnSchema cs : schema.getColumns()) {
            System.out.print(equalWidthText(cs.getName()));
        }
        System.out.println();
        while (rows.hasNext()) {
            System.out.println(rowText(rows.next(), dataTypes));
        }
    }

    private static String rowText(InternalRow row, List<DataType> dataTypes) {
        String s = "";
        int i = 0;
        for (DataType c : dataTypes) {
            switch (c) {
                case IntegerType:
                    s += equalWidthText(row.getInt(i));
                    break;
                case LongType:
                    s += equalWidthText(row.getLong(i));
                    break;
                case FloatType:
                    s += equalWidthText(row.getFloat(i));
                    break;
                case DoubleType:
                    s += equalWidthText(row.getDouble(i));
                    break;
                case StringType:
                    s += equalWidthText(row.getString(i));
                    break;
            }
            i++;
        }
        return s;
    }

    private static String equalWidthText(Object o) {
        int len = 10;
        String txt = o.toString();
        if (txt.length() >= len) {
            return txt.substring(0, len - 4) + StringUtils.repeat(".", 2) + txt.substring(txt.length() - 2, txt.length()) + " ";
        } else {
            return txt + StringUtils.repeat(" ", len - txt.length()) + " ";
        }
    }
}
