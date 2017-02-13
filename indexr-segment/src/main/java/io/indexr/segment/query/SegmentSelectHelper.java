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
import io.indexr.util.DateTimeUtil;

public class SegmentSelectHelper {
    public static StructType fromSegmentSchema(SegmentSchema schema) {
        return new StructType(
                schema.columns.stream().map(
                        cs -> new StructField(cs.name, storageTypeToQueryType(cs.getSqlType().dataType))
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
        return new AttributeReference(schema.name, storageTypeToQueryType(schema.getSqlType().dataType));
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

    public static void printRows(Iterator<InternalRow> rows, List<ColumnSchema> schemas) {
        for (ColumnSchema cs : schemas) {
            System.out.print(equalWidthText(cs.getName()));
        }
        System.out.println();
        while (rows.hasNext()) {
            System.out.println(rowText(rows.next(), schemas));
        }
    }

    public static String rowText(InternalRow row, List<ColumnSchema> schemas) {
        String s = "";
        int i = 0;
        for (ColumnSchema c : schemas) {
            switch (c.getSqlType()) {
                case INT:
                    s += equalWidthText(row.getInt(i));
                    break;
                case BIGINT:
                    s += equalWidthText(row.getLong(i));
                    break;
                case FLOAT:
                    s += equalWidthText(row.getFloat(i));
                    break;
                case DOUBLE:
                    s += equalWidthText(row.getDouble(i));
                    break;
                case VARCHAR:
                    s += equalWidthText(row.getString(i));
                    break;
                case DATE:
                    s += equalWidthText(DateTimeUtil.getLocalDate(row.getLong(i)));
                    break;
                case TIME:
                    s += equalWidthText(DateTimeUtil.getLocalTime(row.getInt(i)));
                    break;
                case DATETIME:
                    s += equalWidthText(DateTimeUtil.getLocalDateTime(row.getLong(i)));
                    break;
                default:
                    throw new RuntimeException("Unsupported type: " + c.getSqlType());
            }
            i++;
        }
        return s;
    }

    private static String equalWidthText(Object o) {
        int len = 20;
        String txt = o.toString();
        if (txt.length() >= len) {
            return txt.substring(0, len - 4) + StringUtils.repeat(".", 2) + txt.substring(txt.length() - 2, txt.length()) + " ";
        } else {
            return txt + StringUtils.repeat(" ", len - txt.length()) + " ";
        }
    }
}
