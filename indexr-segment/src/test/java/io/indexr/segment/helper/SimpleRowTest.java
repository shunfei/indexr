package io.indexr.segment.helper;

import org.junit.Assert;
import org.junit.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import io.indexr.segment.ColumnSchema;
import io.indexr.segment.SQLType;
import io.indexr.util.DateTimeUtil;

public class SimpleRowTest {

    private static List<ColumnSchema> schemas = Arrays.asList(
            new ColumnSchema("c0", SQLType.INT, false),
            new ColumnSchema("_c0", SQLType.INT, true),
            new ColumnSchema("c1", SQLType.BIGINT, false),
            new ColumnSchema("_c1", SQLType.BIGINT, true),
            new ColumnSchema("c2", SQLType.FLOAT, false),
            new ColumnSchema("_c2", SQLType.FLOAT, true),
            new ColumnSchema("c3", SQLType.DOUBLE, false),
            new ColumnSchema("_c3", SQLType.DOUBLE, true),
            new ColumnSchema("c4", SQLType.VARCHAR, false),
            new ColumnSchema("_c4", SQLType.VARCHAR, true),
            new ColumnSchema("c5", SQLType.DATE, false),
            new ColumnSchema("_c5", SQLType.DATE, true),
            new ColumnSchema("c6", SQLType.TIME, false),
            new ColumnSchema("_c6", SQLType.TIME, true),
            new ColumnSchema("c7", SQLType.DATETIME, false),
            new ColumnSchema("_c7", SQLType.DATETIME, true)
    );
    private static String[][] rowVals = new String[][]{
            {"89", "89", "222222", "222222", "4.5", "4.5", "9.1", "9.1", "windows", "windows", "2014-12-09", "2014-12-09", "00:00:00", "00:00:00", "2014-12-09T00:00:00", "2014-12-09T00:00:00"},
            {"3", "3", String.valueOf(Long.MAX_VALUE), String.valueOf(Long.MAX_VALUE), "4.5", "4.5", "9.199", "9.199", "mac", "mac", "1901-03-24", "1901-03-24", "11:43:56", "11:43:56", "1901-03-24T11:43:56", "1901-03-24T11:43:56"},
            {"14121", "14121", "99", "99", "2.5", "2.5", "11.1", "11.1", "linux", "linux", "9999-01-01", "9999-01-01", "12:59:59", "12:59:59", "9999-01-01T12:59:59", "9999-01-01T12:59:59"},
            {String.valueOf(Integer.MAX_VALUE), String.valueOf(Integer.MAX_VALUE), "11", "11", String.valueOf(Float.MIN_VALUE), String.valueOf(Float.MIN_VALUE), "1.51", "1.51", "android", "android", "2741-01-03", "2741-01-03", "01:01:01", "01:01:01", "2741-01-03T01:01:01", "2741-01-03T01:01:01"},
    };

    @Test
    public void test() {
        SimpleRow.Builder builder = SimpleRow.Builder.createByColumnSchemas(schemas);

        List<SimpleRow> rows = new ArrayList<>();

        for (String[] rawRow : rowVals) {
            builder.appendStringFormVals(Arrays.asList(rawRow));
            rows.add(builder.buildAndReset());
        }

        for (int i = 0; i < rowVals.length; i++) {
            String[] rawRow = rowVals[i];
            SimpleRow row = rows.get(i);
            for (int colId = 0; colId < schemas.size(); colId++) {
                ColumnSchema sc = schemas.get(colId);
                switch (sc.getSqlType()) {
                    case INT:
                        Assert.assertEquals(Integer.parseInt(rawRow[colId]), row.getInt(colId));
                        break;
                    case BIGINT:
                        Assert.assertEquals(Long.parseLong(rawRow[colId]), row.getLong(colId));
                        break;
                    case FLOAT:
                        Assert.assertEquals(0, Float.compare(Float.parseFloat(rawRow[colId]), row.getFloat(colId)));
                        break;
                    case DOUBLE:
                        Assert.assertEquals(0, Double.compare(Double.parseDouble(rawRow[colId]), row.getDouble(colId)));
                        break;
                    case VARCHAR:
                        Assert.assertEquals(rawRow[colId], row.getString(colId).toString());
                        break;
                    case DATE:
                        Assert.assertEquals(LocalDate.parse(rawRow[colId]), DateTimeUtil.getLocalDate(row.getLong(colId)));
                        break;
                    case TIME:
                        Assert.assertEquals(LocalTime.parse(rawRow[colId]), DateTimeUtil.getLocalTime(row.getInt(colId)));
                        break;
                    case DATETIME:
                        Assert.assertEquals(LocalDateTime.parse(rawRow[colId]), DateTimeUtil.getLocalDateTime(row.getLong(colId)));
                        break;
                    default:
                        throw new IllegalStateException();
                }
            }
        }
    }
}
