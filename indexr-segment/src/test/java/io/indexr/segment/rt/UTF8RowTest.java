package io.indexr.segment.rt;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.indexr.segment.ColumnSchema;
import io.indexr.segment.Row;
import io.indexr.segment.SQLType;
import io.indexr.util.DateTimeUtil;
import io.indexr.util.UTF8Util;

public class UTF8RowTest {
    static List<ColumnSchema> columnSchemas = Arrays.asList(
            new ColumnSchema("c0", SQLType.INT),
            new ColumnSchema("c1", SQLType.BIGINT, "456"),
            new ColumnSchema("c2", SQLType.FLOAT, "3.14"),
            new ColumnSchema("c3", SQLType.DOUBLE),
            new ColumnSchema("c4", SQLType.VARCHAR, "c4_default"),
            new ColumnSchema("c5", SQLType.INT),
            new ColumnSchema("c6", SQLType.DATE),
            new ColumnSchema("c7", SQLType.TIME),
            new ColumnSchema("c8", SQLType.DATETIME)
    );
    static List<String> dims = Arrays.asList("c1", "c4", "c6", "c7");
    static List<Metric> metrics = Arrays.asList(
            new Metric("c0", "sum"),
            new Metric("c2", "last"),
            new Metric("c3", "min"),
            new Metric("c5", "max"),
            new Metric("c8", "first")
    );
    static Map<String, String> nameToAlias = new HashMap<>();
    static Comparator<UTF8Row> comparator = UTF8Row.dimBytesComparator();

    static {
        nameToAlias.put("c2", "c2_alias");
        nameToAlias.put("c4", "c4_alias");
    }

    static byte[][] jsons = new byte[][]{
            UTF8Util.toUtf8("{\"c0\": 100, \"c2\": 0.11, \"c3\": 0.22, \"c4\": \"col4\", \"c5\": 300}, {\"c0\": 2100, \"c1\": 2200, \"c2\": 2.11, \"c3\": 2.22, \"c4\": \"col4_2\", \"c5\": 300}"),
            UTF8Util.toUtf8("{\"c0\": 100, \"notUsed\": 333,\"c3\": 0.22, \"c4\": \"col4\", \"c5\": 300}, {\"c0\": 100, \"c3\": 0.22, \"c5\": 300, \"c6\": \"2014-12-09\", \"c8\": \"1901-03-24T11:43:56\", \"c7\": \"3:12:56\"}"),
            UTF8Util.toUtf8("{\"c0\": 2100, \"c1\": 2200, \"c2\": 2.11, \"c3\": 2.22, \"c4_alias\": \"col4_2_alias\"}"),
    };
    static UTF8JsonRowCreator creator = new UTF8JsonRowCreator().setRowCreator("test", new UTF8Row.Creator(true, columnSchemas, dims, metrics, nameToAlias, null, EventIgnoreStrategy.NO_IGNORE));

    @Test
    public void testBuild() {
        System.out.println("testBuild");
        for (byte[] bytes : jsons) {
            row(bytes);
        }
    }

    @Test
    public void testMerge() {
        System.out.println("testMerge");
        UTF8Row r1 = creator.create(UTF8Util.toUtf8("{\"c0\": 100, \"c1\": 88, \"c2\": 4.7, \"c3\": 0.22, \"c4\": \"col4\", \"c5\": 300}")).get(0);
        UTF8Row r2 = creator.create(UTF8Util.toUtf8("{\"c0\": 100, \"c2\": 4.71,  \"c3\": 0.22, \"c4\": \"col4\", \"c5\": 300}")).get(0);
        UTF8Row r3 = creator.create(UTF8Util.toUtf8("{\"c0\": 11, \"c1\": 88,   \"c2_alias\" : 8.2, \"c3\": 0.1, \"c4\": \"col4\", \"c5\": 200}")).get(0);
        printRow(r1);
        System.out.println();
        printRow(r2);
        System.out.println();
        printRow(r3);

        Assert.assertNotEquals(0, comparator.compare(r1, r2));
        Assert.assertEquals(0, comparator.compare(r1, r3));

        r1.merge(r3);
        System.out.println();
        System.out.println("merge:");
        printRow(r1);
    }

    private void row(byte[] json) {
        List<UTF8Row> rows = creator.create(json);
        for (UTF8Row row : rows) {
            printRow(row);
            System.out.print(", ");
        }
        System.out.println();
    }

    private static void printRow(Row row) {
        int colId = 0;
        System.out.print("{");
        for (ColumnSchema cs : columnSchemas) {
            String v = null;
            switch (cs.getSqlType()) {
                case INT:
                    v = String.valueOf(row.getInt(colId));
                    break;
                case BIGINT:
                    v = String.valueOf(row.getLong(colId));
                    break;
                case FLOAT:
                    v = String.valueOf(row.getFloat(colId));
                    break;
                case DOUBLE:
                    v = String.valueOf(row.getDouble(colId));
                    break;
                case VARCHAR:
                    v = "\"" + row.getString(colId).toString() + "\"";
                    break;
                case DATE:
                    v = "\"" + DateTimeUtil.getLocalDate(row.getLong(colId)).format(DateTimeUtil.DATE_FORMATTER) + "\"";
                    break;
                case TIME:
                    v = "\"" + DateTimeUtil.getLocalTime(row.getLong(colId)).format(DateTimeUtil.TIME_FORMATTER) + "\"";
                    break;
                case DATETIME:
                    v = "\"" + DateTimeUtil.getLocalDateTime(row.getLong(colId)).format(DateTimeUtil.DATETIME_FORMATTER) + "\"";
                    break;
                default:

            }
            System.out.printf("\"%s\": %s, ", cs.getName(), v);
            colId++;
        }
        System.out.print("}");
    }
}
