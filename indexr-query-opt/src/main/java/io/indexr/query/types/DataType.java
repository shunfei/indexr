package io.indexr.query.types;

import org.apache.commons.lang.StringUtils;

import io.indexr.util.JsonUtil;

public enum DataType implements AbstractDataType {
    // @formatter:off
    BooleanType("bool",1, 0, 1, (v1, v2) -> Boolean.compare(v1 != 0, v2 != 0)),
    IntegerType("int", 4, Integer.MIN_VALUE, Integer.MAX_VALUE ,(v1, v2) -> Integer.compare((int) v1, (int) v2)),
    LongType("bigint", 8, Long.MIN_VALUE, Long.MAX_VALUE, Long::compare),
    FloatType("float", 4, Double.doubleToRawLongBits(Float.MIN_VALUE), Double.doubleToRawLongBits(Float.MAX_VALUE), (v1, v2) -> Float.compare((float) Double.longBitsToDouble(v1), (float) Double.longBitsToDouble(v2))),
    DoubleType("double", 8,Double.doubleToRawLongBits(Double.MIN_VALUE), Double.doubleToRawLongBits(Double.MAX_VALUE), (v1, v2) -> Double.compare(Double.longBitsToDouble(v1), Double.longBitsToDouble(v2))),
    StringType("vchar", 1024, 0, 0, null),
    ;
    // @formatter:on

    public final String aliasName;
    public final int defaultSize;
    public final long min, max; // In uniform.
    public final UniformComparator comparator;

    DataType(String aliasName, int defaultSize, long min, long max, UniformComparator comparator) {
        this.aliasName = aliasName;
        this.defaultSize = defaultSize;
        this.min = min;
        this.max = max;
        this.comparator = comparator;
    }

    public int defaultSize() {return defaultSize;}

    public String typeName() {return StringUtils.stripEnd(this.name(), "Type").toLowerCase();}

    public String simpleString() {return typeName();}

    public String json() {return JsonUtil.toJson(typeName());}

    public boolean sameType(DataType other) {return this == other;}

    public static DataType fromName(String name) {
        for (DataType dt : DataType.values()) {
            if (dt.typeName().equalsIgnoreCase(name) || dt.aliasName.equalsIgnoreCase(name)) {
                return dt;
            }
        }
        return null;
    }
}
