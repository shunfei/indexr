package io.indexr.segment.rt;

import java.util.HashMap;
import java.util.Map;

import io.indexr.segment.ColumnType;

public class AggType {
    // Don't change their value.
    public static final int SUM = 1;
    public static final int FIRST = 2;
    public static final int LAST = 3;
    public static final int MIN = 4;
    public static final int MAX = 5;

    private static HashMap<String, Integer> nameToType = new HashMap<>();
    private static HashMap<Integer, String> typeToName = new HashMap<>();

    static {
        nameToType.put("sum", SUM);
        nameToType.put("last", LAST);
        nameToType.put("first", FIRST);
        nameToType.put("min", MIN);
        nameToType.put("max", MAX);

        for (Map.Entry<String, Integer> e : nameToType.entrySet()) {
            typeToName.put(e.getValue(), e.getKey());
        }
    }

    public static int forName(String name) {
        Integer t = nameToType.get(name.toLowerCase());
        if (t == null) {
            throw new IllegalStateException("Illegal type: " + name);
        }
        return t;
    }

    public static String getName(int t) {
        return typeToName.get(t);
    }

    public static long agg(int aggType, byte dataType, long value1, long value2) {
        switch (aggType) {
            case SUM:
                switch (dataType) {
                    case ColumnType.INT:
                    case ColumnType.LONG:
                        return value1 + value2;
                    case ColumnType.FLOAT:
                    case ColumnType.DOUBLE:
                        return Double.doubleToRawLongBits(Double.longBitsToDouble(value1) + Double.longBitsToDouble(value2));
                    default:
                        throw new IllegalStateException("Illegal data type:" + dataType);
                }
            case FIRST:
                return value1;
            case LAST:
                return value2;
            case MIN:
                switch (dataType) {
                    case ColumnType.INT:
                    case ColumnType.LONG:
                        return Math.min(value1, value2);
                    case ColumnType.FLOAT:
                    case ColumnType.DOUBLE:
                        return Double.doubleToRawLongBits(Math.min(Double.longBitsToDouble(value1), Double.longBitsToDouble(value2)));
                    default:
                        throw new IllegalStateException("Illegal data type:" + dataType);
                }
            case MAX:
                switch (dataType) {
                    case ColumnType.INT:
                    case ColumnType.LONG:
                        return Math.max(value1, value2);
                    case ColumnType.FLOAT:
                    case ColumnType.DOUBLE:
                        return Double.doubleToRawLongBits(Math.max(Double.longBitsToDouble(value1), Double.longBitsToDouble(value2)));
                    default:
                        throw new IllegalStateException("Illegal data type:" + dataType);
                }
            default:
                throw new IllegalStateException("Illegal agg type:" + aggType);
        }
    }
}
