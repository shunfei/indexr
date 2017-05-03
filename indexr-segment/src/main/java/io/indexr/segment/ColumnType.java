package io.indexr.segment;

import io.indexr.data.DataType;

public class ColumnType {
    public static final byte INT = DataType.INT;
    public static final byte LONG = DataType.LONG;
    public static final byte FLOAT = DataType.FLOAT;
    public static final byte DOUBLE = DataType.DOUBLE;
    public static final byte STRING = DataType.STRING;

    public static final int MAX_STRING_UTF8_SIZE = DataType.MAX_STRING_UTF8_SIZE;
    public static final int MAX_STRING_UTF8_SIZE_MASK = DataType.MAX_STRING_UTF8_SIZE_MASK;

    public static boolean isFloatPoint(byte type) {
        return DataType.isFloatPoint(type);
    }

    public static boolean isIntegral(byte type) {
        return DataType.isIntegral(type);
    }

    public static boolean isNumber(byte type) {
        return DataType.isNumber(type);
    }

    public static long minNumValue(byte type) {
        return DataType.minNumValue(type);
    }

    public static long maxNumValue(byte type) {
        return DataType.maxNumValue(type);
    }

    public static int bufferSize(byte type) {
        return DataType.bufferSize(type);
    }

    public static int numTypeShift(byte numType) {
        return DataType.numTypeShift(numType);
    }
}
