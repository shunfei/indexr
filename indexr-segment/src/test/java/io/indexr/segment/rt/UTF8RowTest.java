package io.indexr.segment.rt;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.indexr.segment.ColumnSchema;
import io.indexr.segment.ColumnType;
import io.indexr.segment.Row;
import io.indexr.util.UTF8Util;

public class UTF8RowTest {
    static List<ColumnSchema> columnSchemas = Arrays.asList(
            new ColumnSchema("c0", ColumnType.INT),
            new ColumnSchema("c1", ColumnType.LONG, "456"),
            new ColumnSchema("c2", ColumnType.FLOAT, "3.14"),
            new ColumnSchema("c3", ColumnType.DOUBLE),
            new ColumnSchema("c4", ColumnType.STRING, "c4_default"),
            new ColumnSchema("c5", ColumnType.INT)
    );
    static List<String> dims = Arrays.asList("c1", "c4");
    static List<Metric> metrics = Arrays.asList(
            new Metric("c0", "sum"),
            new Metric("c2", "last"),
            new Metric("c3", "min"),
            new Metric("c5", "max")
    );
    static Map<String, String> nameToAlias = new HashMap<>();
    static Comparator<UTF8Row> comparator = UTF8Row.dimBytesComparator();

    static {
        nameToAlias.put("c2", "c2_alias");
        nameToAlias.put("c4", "c4_alias");
    }

    static byte[][] jsons = new byte[][]{
            UTF8Util.toUtf8("{\"c0\": 100, \"c2\": 0.11, \"c3\": 0.22, \"c4\": \"col4\", \"c5\": 300}, {\"c0\": 2100, \"c1\": 2200, \"c2\": 2.11, \"c3\": 2.22, \"c4\": \"col4_2\", \"c5\": 300}"),
            UTF8Util.toUtf8("{\"c0\": 100, \"notUsed\": 333,\"c3\": 0.22, \"c4\": \"col4\", \"c5\": 300}, {\"c0\": 100, \"c3\": 0.22, \"c5\": 300}"),
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
            switch (cs.getDataType()) {
                case ColumnType.INT:
                    v = String.valueOf(row.getInt(colId));
                    break;
                case ColumnType.LONG:
                    v = String.valueOf(row.getLong(colId));
                    break;
                case ColumnType.FLOAT:
                    v = String.valueOf(row.getFloat(colId));
                    break;
                case ColumnType.DOUBLE:
                    v = String.valueOf(row.getDouble(colId));
                    break;
                case ColumnType.STRING:
                    v = "\"" + row.getString(colId).toString() + "\"";
                    break;
                default:

            }
            System.out.printf("\"%s\": %s, ", cs.getName(), v);
            colId++;
        }
        System.out.print("}");
    }
}
