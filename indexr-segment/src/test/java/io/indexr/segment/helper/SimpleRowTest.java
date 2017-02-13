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
            new ColumnSchema("c0", SQLType.INT),
            new ColumnSchema("c1", SQLType.BIGINT),
            new ColumnSchema("c2", SQLType.FLOAT),
            new ColumnSchema("c3", SQLType.DOUBLE),
            new ColumnSchema("c4", SQLType.VARCHAR),
            new ColumnSchema("c5", SQLType.DATE),
            new ColumnSchema("c6", SQLType.TIME),
            new ColumnSchema("c7", SQLType.DATETIME)
    );
    private static String[][] rowVals = new String[][]{
            {"89", "222222", "4.5", "9.1", "windows", "2014-12-09", "00:00:00", "2014-12-09T00:00:00"},
            {"3", String.valueOf(Long.MAX_VALUE), "4.5", "9.199", "mac", "1901-03-24", "11:43:56", "1901-03-24T11:43:56"},
            {"14121", "99", "2.5", "11.1", "linux", "9999-01-01", "12:59:59", "9999-01-01T12:59:59"},
            {String.valueOf(Integer.MAX_VALUE), "11", String.valueOf(Float.MIN_VALUE), "1.51", "android", "2741-01-03", "01:01:01", "2741-01-03T01:01:01"},
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
