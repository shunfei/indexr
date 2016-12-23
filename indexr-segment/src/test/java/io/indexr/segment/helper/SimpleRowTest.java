package io.indexr.segment.helper;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import io.indexr.segment.ColumnSchema;
import io.indexr.segment.ColumnType;

public class SimpleRowTest {

    private static List<ColumnSchema> schemas = Arrays.asList(
            new ColumnSchema("c0", ColumnType.INT),
            new ColumnSchema("c1", ColumnType.LONG),
            new ColumnSchema("c2", ColumnType.FLOAT),
            new ColumnSchema("c3", ColumnType.DOUBLE),
            new ColumnSchema("c4", ColumnType.STRING),
            new ColumnSchema("c6", ColumnType.INT)
    );
    private static String[][] rowVals = new String[][]{
            {"89", "222222", "4.5", "9.1", "windows", "12"},
            {"3", String.valueOf(Long.MAX_VALUE), "4.5", "9.199", "mac", "33"},
            {"14121", "99", "2.5", "11.1", "linux", "87"},
            {"39", "11", String.valueOf(Float.MIN_VALUE), "1.51", "android", String.valueOf(Integer.MAX_VALUE)},
    };


    @Test
    public void test() {
        SimpleRow.Builder builder = new SimpleRow.Builder(schemas.stream().map(schema -> schema.dataType).collect(Collectors.toList()));

        List<SimpleRow> rows = new ArrayList<>();

        for (String[] rawRow : rowVals) {
            builder.appendRawVals(Arrays.asList(rawRow));
            rows.add(builder.buildAndReset());
        }

        for (int i = 0; i < rowVals.length; i++) {
            String[] rawRow = rowVals[i];
            SimpleRow row = rows.get(i);
            for (int colId = 0; colId < schemas.size(); colId++) {
                ColumnSchema sc = schemas.get(colId);
                switch (sc.dataType) {
                    case ColumnType.INT:
                        Assert.assertEquals(Integer.parseInt(rawRow[colId]), row.getInt(colId));
                        break;
                    case ColumnType.LONG:
                        Assert.assertEquals(Long.parseLong(rawRow[colId]), row.getLong(colId));
                        break;
                    case ColumnType.FLOAT:
                        Assert.assertEquals(0, Float.compare(Float.parseFloat(rawRow[colId]), row.getFloat(colId)));
                        break;
                    case ColumnType.DOUBLE:
                        Assert.assertEquals(0, Double.compare(Double.parseDouble(rawRow[colId]), row.getDouble(colId)));
                        break;
                    case ColumnType.STRING:
                        Assert.assertEquals(true, rawRow[colId].equals(row.getString(colId).toString()));
                        break;
                    default:
                        throw new IllegalStateException();
                }
            }
        }
    }
}
