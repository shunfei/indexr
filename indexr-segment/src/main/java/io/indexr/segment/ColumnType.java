package io.indexr.segment;

public class ColumnType {
    public static final byte INT = 1;
    public static final byte LONG = 2;
    public static final byte FLOAT = 3;
    public static final byte DOUBLE = 4;
    public static final byte STRING = 5;

    public static final int MAX_STRING_UTF8_SIZE = 65535; // In utf-8 bytes. Max of unsign short.
    public static final int MAX_STRING_UTF8_SIZE_MASK = 0xFFFF;

    public static boolean isFloatPoint(byte type) {
        return type == FLOAT || type == DOUBLE;
    }

    public static boolean isIntegral(byte type) {
        return type == INT || type == LONG;
    }

    public static boolean isNumber(byte type) {
        return type != STRING;
    }

    public static long minNumValue(byte type) {
        switch (type) {
            case INT:
                return Integer.MIN_VALUE;
            case LONG:
                return Long.MIN_VALUE;
            case FLOAT:
                return Double.doubleToRawLongBits(Float.MIN_VALUE);
            case DOUBLE:
                return Double.doubleToRawLongBits(Double.MIN_VALUE);
            default:
                throw new IllegalStateException("Unsupported number type " + type);
        }
    }

    public static long maxNumValue(byte type) {
        switch (type) {
            case INT:
                return Integer.MAX_VALUE;
            case LONG:
                return Long.MAX_VALUE;
            case FLOAT:
                return Double.doubleToRawLongBits(Float.MAX_VALUE);
            case DOUBLE:
                return Double.doubleToRawLongBits(Double.MAX_VALUE);
            default:
                throw new IllegalStateException("Unsupported number type " + type);
        }
    }

    public static int bufferSize(byte type) {
        switch (type) {
            case INT:
                return 4;
            case LONG:
                return 8;
            case FLOAT:
                return 4;
            case DOUBLE:
                return 8;
            case STRING:
                return MAX_STRING_UTF8_SIZE;
            default:
                throw new IllegalStateException("column type " + type + " is illegal");
        }
    }

    public static int numTypeShift(byte numType) {
        switch (numType) {
            case INT:
            case FLOAT:
                return 2;
            case LONG:
            case DOUBLE:
                return 3;
            default:
                throw new IllegalStateException("column type " + numType + " is illegal");
        }
    }
}
