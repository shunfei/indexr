package io.indexr.query.util;

import org.apache.spark.sql.execution.UnsafeExternalRowSorter;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.collection.unsafe.sort.PrefixComparator;
import org.apache.spark.util.collection.unsafe.sort.PrefixComparators;

import java.util.List;

import io.indexr.query.expr.SortOrder;
import io.indexr.query.expr.attr.BoundReference;
import io.indexr.query.types.DataType;

import static io.indexr.util.Trick.mapToList;

public class SortPrefixUtils {
    public static PrefixComparator getPrefixComparator(SortOrder sortOrder) {
        switch (sortOrder.dataType()) {
            case BooleanType:
            case IntegerType:
            case LongType:
                return sortOrder.isAscending() ? PrefixComparators.LONG : PrefixComparators.LONG_DESC;
            case FloatType:
            case DoubleType:
                return sortOrder.isAscending() ? PrefixComparators.DOUBLE : PrefixComparators.DOUBLE_DESC;
            default:
                return (a, b) -> 0;
        }
    }

    /**
     * Creates the prefix comparator for the first field in the given schema, in ascending order.
     */
    public static PrefixComparator getPrefixComparator(StructType schema) {
        if (schema.size() != 0) {
            return getPrefixComparator(
                    new SortOrder(new BoundReference(0, schema.get(0).dataType),
                            SortOrder.SortDirection.Ascending));
        } else {
            return (a, b) -> 0;
        }
    }

    /**
     * Creates the prefix computer for the first field in the given schema, in ascending order.
     */
    public static UnsafeExternalRowSorter.PrefixComputer createPrefixGenerator(StructType schema) {
        return createPrefixGenerator(mapToList(schema.fields(), f -> f.dataType));
    }

    public static UnsafeExternalRowSorter.PrefixComputer createPrefixGenerator(List<DataType> schema) {
        if (schema.size() != 0) {
            return row -> row.getUniformVal(0);
        } else {
            return row -> 0;
        }
    }
}
