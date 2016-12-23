package io.indexr.segment;

public class ColumnType {
    public static final byte INT = 1;
    public static final byte LONG = 2;
    public static final byte FLOAT = 3;
    public static final byte DOUBLE = 4;
    public static final byte STRING = 5;
    //public static final byte MInt = 6;
    //public static final byte MLong = 7;
    //public static final byte MFloat = 8;
    //public static final byte MDouble = 9;
    //public static final byte MString = 10;

    public static final int MAX_STRING_UTF8_SIZE = 65535; // In utf-8 bytes. Max of unsign short.
    public static final int MAX_STRING_UTF8_SIZE_MASK = 0xFFFF;

    public static byte fromName(String name) {
        switch (name.toLowerCase()) {
            case "int":
                return INT;
            case "bigint":
            case "long":
                return LONG;
            case "float":
                return FLOAT;
            case "double":
                return DOUBLE;
            case "string":
                return STRING;
            default:
                throw new IllegalStateException("column type " + name + " is illegal");
        }
    }

    public static String toName(byte type) {
        switch (type) {
            case INT:
                return "int";
            case LONG:
                return "long";
            case FLOAT:
                return "float";
            case DOUBLE:
                return "double";
            case STRING:
                return "string";
            default:
                throw new IllegalStateException("column type " + type + " is illegal");
        }
    }

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

    public static long castStringToNumber(String strVal, byte numType) {
        switch (numType) {
            case INT:
                return Integer.parseInt(strVal);
            case LONG:
                return Long.parseLong(strVal);
            case FLOAT:
                return Double.doubleToLongBits(Float.parseFloat(strVal));
            case DOUBLE:
                return Double.doubleToLongBits(Double.parseDouble(strVal));
            default:
                throw new IllegalStateException("column type " + numType + " is illegal");
        }
    }

    public static String castNumberToString(long numVal, byte numType) {
        switch (numType) {
            case INT:
                return String.valueOf((int) numVal);
            case LONG:
                return String.valueOf(numVal);
            case FLOAT:
                return String.valueOf((float) Double.longBitsToDouble(numVal));
            case DOUBLE:
                return String.valueOf(Double.longBitsToDouble(numVal));
            default:
                throw new IllegalStateException("column type " + numType + " is illegal");
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
