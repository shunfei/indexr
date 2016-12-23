package io.indexr.segment.pack;

import com.google.common.base.Preconditions;

import io.indexr.segment.ColumnType;

public class NumType {
    // The value here should never be changed. Please append new numType to the end.
    public static final byte NZero = 1;
    public static final byte NByte = 2;
    public static final byte NShort = 3;
    public static final byte NInt = 4;
    public static final byte NLong = 5;

    public static long maxValue(byte numType) {
        switch (numType) {
            case NZero:
                return 0;
            case NByte:
                return Byte.MAX_VALUE;
            case NShort:
                return Short.MAX_VALUE;
            case NInt:
                return Integer.MAX_VALUE;
            case NLong:
                return Long.MAX_VALUE;
            default:
                throw new IllegalArgumentException("illegal numType: " + numType);
        }
    }

    public static int bitWidth(byte numType) {
        switch (numType) {
            case NZero:
                return 0;
            case NByte:
                return 1;
            case NShort:
                return 2;
            case NInt:
                return 4;
            case NLong:
                return 8;
            default:
                throw new IllegalArgumentException("illegal numType: " + numType);
        }
    }

    public static byte fromByteWidth(int byteWidth) {
        switch (byteWidth) {
            case 0:
                return NZero;
            case 1:
                return NByte;
            case 2:
                return NShort;
            case 4:
                return NInt;
            case 8:
                return NLong;
            default:
                throw new IllegalArgumentException("illegal number numType width: " + byteWidth);
        }
    }

    public static byte fromMaxValue(long max) {
        Preconditions.checkArgument(max >= 0);

        if (max == 0) {
            return NZero;
        } else if (max <= java.lang.Byte.MAX_VALUE) {
            return NByte;
        } else if (max <= java.lang.Short.MAX_VALUE) {
            return NShort;
        } else if (max <= Integer.MAX_VALUE) {
            return NInt;
        } else {
            return NLong;
        }
    }

    public static boolean isNumType(byte dataType) {
        return dataType == ColumnType.INT ||
                dataType == ColumnType.LONG ||
                dataType == ColumnType.FLOAT ||
                dataType == ColumnType.DOUBLE;
    }

    // ------------------------------------------------------------
    // Convert utils
    // ------------------------------------------------------------

    public static long floatToLong(float v) {

        // Maybe it is a better idea using
        // (long)Float.floatToRawIntBits(v)

        return Double.doubleToRawLongBits(v);
    }

    public static float longToFloat(long v) {
        return (float) Double.longBitsToDouble(v);
    }

    public static long doubleToLong(double v) {
        return Double.doubleToRawLongBits(v);
    }

    public static double longToDouble(long v) {
        return Double.longBitsToDouble(v);
    }
}

