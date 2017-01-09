package org.apache.spark.util;

public class Utils {
    /**
     * NaN-safe version of [[java.lang.DOUBLE.compare()]] which allows NaN values to be compared
     * according to semantics where NaN == NaN and NaN bigger than any non-NaN double.
     */
    public static int nanSafeCompareDoubles(double x, double y) {
        boolean xIsNan = java.lang.Double.isNaN(x);
        boolean yIsNan = java.lang.Double.isNaN(y);
        if ((xIsNan && yIsNan) || (x == y)) return 0;
        else if (xIsNan) return 1;
        else if (yIsNan) return -1;
        else if (x > y) return 1;
        else return -1;
    }

    /**
     * Convert a quantity in bytes to a human-readable string such as "4.0 MB".
     */
    public static String bytesToString(long size) {
        long TB = 1L << 40;
        long GB = 1L << 30;
        long MB = 1L << 20;
        long KB = 1L << 10;

        double value;
        String unit;
        if (size >= 2 * TB) {
            value = (double) size / TB;
            unit = "TB";
        } else if (size >= 2 * GB) {
            value = (double) size / GB;
            unit = "GB";
        } else if (size >= 2 * MB) {
            value = (double) size / MB;
            unit = "MB";
        } else if (size >= 2 * KB) {
            value = (double) size / KB;
            unit = "KB";
        } else {
            value = (double) size;
            unit = "B";
        }
        return String.format("%.1f %s", value, unit);
    }
}
