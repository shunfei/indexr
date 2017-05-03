package io.indexr.segment.storage;

import org.apache.commons.lang.RandomStringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import io.indexr.segment.ColumnSchema;
import io.indexr.segment.ColumnType;
import io.indexr.segment.Row;
import io.indexr.segment.SQLType;
import io.indexr.segment.SegmentSchema;
import io.indexr.segment.helper.SimpleRow;

public class TestRows {
    public static List<ColumnSchema> columnSchemas = Arrays.asList(
            new ColumnSchema("c0", SQLType.INT, false),
            new ColumnSchema("idx_c0", SQLType.INT, true),
            new ColumnSchema("c1", SQLType.BIGINT, false),
            new ColumnSchema("idx_c1", SQLType.BIGINT, true),
            new ColumnSchema("c2", SQLType.FLOAT, false),
            new ColumnSchema("idx_c2", SQLType.FLOAT, true),
            new ColumnSchema("c3", SQLType.DOUBLE, false),
            new ColumnSchema("idx_c3", SQLType.DOUBLE, true),
            new ColumnSchema("c4", SQLType.VARCHAR, false),
            new ColumnSchema("idx_c4", SQLType.VARCHAR, true),
            new ColumnSchema("c5", SQLType.DATE, false),
            new ColumnSchema("idx_c5", SQLType.DATE, true),
            new ColumnSchema("c6", SQLType.TIME, false),
            new ColumnSchema("idx_c6", SQLType.TIME, true),
            new ColumnSchema("c7", SQLType.DATETIME, false),
            new ColumnSchema("idx_c7", SQLType.DATETIME, true)
    );
    public static SegmentSchema segmentSchema = new SegmentSchema(columnSchemas);

    public static final int ROW_COUNT = 70000;
    public static final SegmentSchema schema = segmentSchema;
    public static final List<Row> sampleRows;

    static {
        sampleRows = genRows(ROW_COUNT);
    }

    public static List<Row> genRows(int count) {
        ArrayList<Row> rows = new ArrayList<Row>(count);
        Random r = new Random();
        SimpleRow.Builder builder = SimpleRow.Builder.createByColumnSchemas(schema.getColumns());
        for (int i = 0; i < count; i++) {
            for (ColumnSchema cs : schema.getColumns()) {
                switch (cs.getDataType()) {
                    case ColumnType.INT:
                        builder.appendInt(r.nextInt());
                        break;
                    case ColumnType.LONG:
                        builder.appendLong(r.nextLong());
                        break;
                    case ColumnType.FLOAT:
                        builder.appendFloat((float) r.nextDouble());
                        break;
                    case ColumnType.DOUBLE:
                        builder.appendDouble(r.nextDouble());
                        break;
                    case ColumnType.STRING:
                        builder.appendString(RandomStringUtils.randomAlphabetic(r.nextInt(10)));
                        break;
                    default:
                        throw new RuntimeException();
                }
            }
            rows.add(builder.buildAndReset());
        }
        return rows;
    }
}
