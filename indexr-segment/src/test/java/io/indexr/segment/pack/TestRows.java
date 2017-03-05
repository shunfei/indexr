package io.indexr.segment.pack;

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
            new ColumnSchema("c0", SQLType.INT),
            new ColumnSchema("c1", SQLType.BIGINT),
            new ColumnSchema("c2", SQLType.FLOAT),
            new ColumnSchema("c3", SQLType.DOUBLE),
            new ColumnSchema("c4", SQLType.VARCHAR),
            new ColumnSchema("c5", SQLType.DATE),
            new ColumnSchema("c6", SQLType.TIME),
            new ColumnSchema("c7", SQLType.DATETIME)
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
