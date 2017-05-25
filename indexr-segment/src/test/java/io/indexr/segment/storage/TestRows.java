package io.indexr.segment.storage;

import org.apache.commons.lang.RandomStringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
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

    private static String[][] rawRows = new String[][]{
            {"89", "89", "222222", "222222", "4.5", "4.5", "9.1", "9.1", "windows", "windows", "2014-12-09", "2014-12-09", "00:00:00", "00:00:00", "2014-12-09T00:00:00", "2014-12-09T00:00:00"},
            {"3", "3", String.valueOf(Long.MAX_VALUE), String.valueOf(Long.MAX_VALUE), "4.5", "4.5", "9.199", "9.199", "mac", "mac", "1901-03-24", "1901-03-24", "11:43:56", "11:43:56", "1901-03-24T11:43:56", "1901-03-24T11:43:56"},
            {"14121", "14121", "99", "99", "2.5", "2.5", "11.1", "11.1", "linux", "linux", "9999-01-01", "9999-01-01", "12:59:59", "12:59:59", "9999-01-01T12:59:59", "9999-01-01T12:59:59"},
            {String.valueOf(Integer.MAX_VALUE), String.valueOf(Integer.MAX_VALUE), "11", "11", String.valueOf(Float.MIN_VALUE), String.valueOf(Float.MIN_VALUE), "1.51", "1.51", "android", "android", "2741-01-03", "2741-01-03", "01:01:01", "01:01:01", "2741-01-03T01:01:01", "2741-01-03T01:01:01"},
    };

    static List<Row> sample_rows = new ArrayList<>(rawRows.length);

    static {
        SimpleRow.Builder builder = SimpleRow.Builder.createByColumnSchemas(columnSchemas);
        for (int rowId = 0; rowId < rawRows.length; rowId++) {
            builder.appendStringFormVals(Arrays.asList(rawRows[rowId]));
            sample_rows.add(builder.buildAndReset());
        }
    }

    public static final int ROW_COUNT = 70000;

    public static Iterator<Row> genRows(final int theRowCount) {
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

    public static Iterator<Row> genRandomRows(final int theRowCount) {
        return genRandomRows(theRowCount, columnSchemas);
    }

    public static Iterator<Row> genRandomRows(final int theRowCount, List<ColumnSchema> schemas) {
        return new Iterator<Row>() {
            Random r = new Random();
            int curIndex;

            @Override
            public boolean hasNext() {
                return curIndex < theRowCount;
            }

            @Override
            public Row next() {
                SimpleRow.Builder builder = SimpleRow.Builder.createByColumnSchemas(schemas);
                for (ColumnSchema cs : schemas) {
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
                curIndex++;
                return builder.buildAndReset();
            }
        };
    }
}
