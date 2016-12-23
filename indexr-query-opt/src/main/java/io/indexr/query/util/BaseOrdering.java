package io.indexr.query.util;

import org.apache.spark.sql.types.StructType;

import java.util.Comparator;

import io.indexr.query.row.InternalRow;
import io.indexr.query.types.DataType;

@FunctionalInterface
public interface BaseOrdering extends Comparator<InternalRow> {

    @Override
    public int compare(InternalRow a, InternalRow b);

    public static BaseOrdering create(StructType schema) {
        final DataType[] dataTypes = new DataType[schema.size()];
        for (int i = 0; i < schema.size(); i++) {
            dataTypes[i] = schema.get(i).dataType;
        }
        return new BaseOrdering() {
            @Override
            public int compare(InternalRow a, InternalRow b) {
                assert a.numFields() == b.numFields() && a.numFields() == schema.size();
                int res;
                for (int i = 0; i < dataTypes.length; i++) {
                    if ((res = dataTypes[i].comparator.compare(a.getUniformVal(i), b.getUniformVal(i))) != 0) {
                        return res;
                    }
                }
                return 0;
            }
        };
    }
}
