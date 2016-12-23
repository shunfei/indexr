package io.indexr.segment.rc;

import java.io.IOException;

import io.indexr.segment.Column;
import io.indexr.segment.ColumnType;
import io.indexr.segment.RSIndexNum;
import io.indexr.segment.RSValue;
import io.indexr.segment.pack.ColumnNode;
import io.indexr.segment.pack.DataPackNode;

import static io.indexr.segment.RSValue.All;
import static io.indexr.segment.RSValue.None;
import static io.indexr.segment.RSValue.Some;
import static io.indexr.segment.RSValue.Unknown;

public class RoughCheck_N {
    private static final int IN_CHECK_FOREACH_LIMIT = 1024;

    private static boolean contains(long[] values, long toCheck) {
        for (long v : values) {
            if (v == toCheck) {
                return true;
            }
        }
        return false;
    }

    private static byte inCheck(long[] inValues, long inMin, long inMax, long indexMin, long indexMax, byte dataType) {
        byte res = Some;
        if (inMin > indexMax || inMax < indexMin) {
            res = None;
        } else if (indexMin == indexMax) {
            res = contains(inValues, indexMin) ? All : None;
        } else if (indexMin + 1 == indexMax) { // It is common for sorted dimensions.
            boolean v1 = contains(inValues, indexMin);
            boolean v2 = contains(inValues, indexMax);
            if (v1 && v2) {
                res = All;
            } else if (v1 || v2) {
                res = Some;
            } else {
                res = None;
            }
        } else {
            res = Unknown;
        }
        return res;
    }

    /**
     * Rough check the in condition.
     */
    public static byte inCheckOnPack(Column column, int packId, long[] inValues, long inMin, long inMax) throws IOException {
        // Only support intergral number for now.
        if (!ColumnType.isIntegral(column.dataType())) {
            return Some;
        }
        if (inValues.length == 0) {
            return None;
        }

        DataPackNode dpn = column.dpn(packId);
        long packMin = dpn.minValue();
        long packMax = dpn.maxValue();
        byte res = inCheck(inValues, inMin, inMax, packMin, packMax, column.dataType());
        if (res == Unknown) {
            RSIndexNum index = column.rsIndex();
            res = index.isValue(packId, inMin, inMax, packMin, packMax);
            // inMin, inMax are just a boundary, not continuous interval.
            res = res == All ? Some : res;
        }

        if (res == Some && inValues.length < IN_CHECK_FOREACH_LIMIT) {
            RSIndexNum index = column.rsIndex();
            boolean none = true;
            for (long v : inValues) {
                byte vRes = index.isValue(packId, v, v, packMin, packMax);
                if (vRes == All) {
                    return All;
                } else if (vRes == Some) {
                    none = false;
                    // There are very little chances be All after Some, so jump out fast.
                    break;
                }
            }
            if (none) {
                return None;
            } else {
                return Some;
            }
        }
        return res;
    }

    public static byte inCheckOnColumn(ColumnNode columnNode, byte valueType, long[] inValues, long inMin, long inMax) {
        byte res = inCheck(inValues, inMin, inMax, columnNode.getMinNumValue(), columnNode.getMaxNumValue(), valueType);
        if (res == RSValue.Unknown) {
            res = RSValue.Some;
        }
        return res;
    }

    private static byte betweenCheck(long minValue, long maxValue, long indexMin, long indexMax, byte dataType) {
        boolean isFloat = ColumnType.isFloatPoint(dataType);
        if ((isFloat && Double.longBitsToDouble(minValue) > Double.longBitsToDouble(maxValue)) ||
                !isFloat && minValue > maxValue) {
            return None;
        } else if (isFloat &&
                (Double.longBitsToDouble(minValue) > Double.longBitsToDouble(indexMax) ||
                        Double.longBitsToDouble(maxValue) < Double.longBitsToDouble(indexMin))) {
            return None;
        } else if (isFloat &&
                (Double.longBitsToDouble(minValue) <= Double.longBitsToDouble(indexMin) &&
                        Double.longBitsToDouble(maxValue) >= Double.longBitsToDouble(indexMax))) {
            return All;
        } else if (!isFloat && (minValue > indexMax || maxValue < indexMin)) {
            return None;
        } else if (!isFloat && (minValue <= indexMin && maxValue >= indexMax)) {
            return All;
        } else {
            return Unknown;
        }
    }

    /**
     * Rough check between condition.
     */
    public static byte betweenCheckOnPack(Column column, int packId, long minValue, long maxValue) throws IOException {
        DataPackNode dpn = column.dpn(packId);
        long packMin = dpn.minValue();
        long packMax = dpn.maxValue();
        byte res = betweenCheck(minValue, maxValue, packMin, packMax, column.dataType());
        if (res == Unknown) {
            RSIndexNum index = column.rsIndex();
            res = index.isValue(packId, minValue, maxValue, packMin, packMax);
        }
        return res;
    }

    public static byte betweenCheckOnColumn(ColumnNode columnNode, byte valueType, long minValue, long maxValue) {
        long colMin = columnNode.getMinNumValue();
        long colMax = columnNode.getMaxNumValue();
        byte res = betweenCheck(minValue, maxValue, colMin, colMax, valueType);
        if (res == RSValue.Unknown) {
            res = RSValue.Some;
        }
        return res;
    }

    public static byte equalCheckOnPack(Column column, int packId, long value) throws IOException {
        return betweenCheckOnPack(column, packId, value, value);
    }

    public static byte equalCheckOnColumn(ColumnNode columnNode, byte valueType, long value) {
        return betweenCheckOnColumn(columnNode, valueType, value, value);
    }

    private static byte greaterCheck(long value, long indexMin, long indexMax, byte dataType) {
        byte res = Some;
        boolean isFloat = ColumnType.isFloatPoint(dataType);
        if (isFloat && Double.longBitsToDouble(value) >= Double.longBitsToDouble(indexMax)) {
            res = None;
        } else if (isFloat && Double.longBitsToDouble(value) < Double.longBitsToDouble(indexMin)) {
            res = All;
        } else if (!isFloat && value >= indexMax) {
            res = None;
        } else if (!isFloat && value < indexMin) {
            res = All;
        }
        return res;
    }

    public static byte greaterCheckOnPack(Column column, int packId, long value) throws IOException {
        DataPackNode dpn = column.dpn(packId);
        return greaterCheck(value, dpn.minValue(), dpn.maxValue(), column.dataType());
    }

    public static byte greaterCheckOnColumn(ColumnNode columnNode, byte valueType, long value) {
        return greaterCheck(value, columnNode.getMinNumValue(), columnNode.getMaxNumValue(), valueType);
    }

    private static byte greaterEqualCheck(long value, long indexMin, long indexMax, byte dataType) {
        byte res = Some;
        boolean isFloat = ColumnType.isFloatPoint(dataType);
        if (isFloat && Double.longBitsToDouble(value) > Double.longBitsToDouble(indexMax)) {
            res = None;
        } else if (isFloat && Double.longBitsToDouble(value) <= Double.longBitsToDouble(indexMin)) {
            res = All;
        } else if (!isFloat && value > indexMax) {
            res = None;
        } else if (!isFloat && value <= indexMin) {
            res = All;
        }
        return res;
    }

    public static byte greaterEqualCheckOnPack(Column column, int packId, long value) throws IOException {
        DataPackNode dpn = column.dpn(packId);
        return greaterEqualCheck(value, dpn.minValue(), dpn.maxValue(), column.dataType());
    }

    public static byte greaterEqualCheckOnColumn(ColumnNode columnNode, byte valueType, long value) {
        return greaterEqualCheck(value, columnNode.getMinNumValue(), columnNode.getMaxNumValue(), valueType);
    }
    //
    //private static byte lessCheck(long value, long indexMin, long indexMax, byte dataType) {
    //    byte res = Some;
    //    boolean isFloat = ColumnType.isFloatPoint(dataType);
    //    if (isFloat && Double.longBitsToDouble(value) <= Double.longBitsToDouble(indexMin)) {
    //        res = None;
    //    } else if (isFloat && Double.longBitsToDouble(value) > Double.longBitsToDouble(indexMax)) {
    //        res = All;
    //    } else if (!isFloat && value <= indexMin) {
    //        res = None;
    //    } else if (!isFloat && value > indexMax) {
    //        res = All;
    //    }
    //    return res;
    //}
    //
    //public static byte lessCheckOnPack(Column column, int packId, long value) throws IOException {
    //    DataPackNode dpn = column.dpn(packId);
    //    return lessCheck(value, dpn.minValue(), dpn.maxValue(), column.dataType());
    //}
    //
    //public static byte lessCheckOnColumn(ColumnNode columnNode, byte valueType, long value) {
    //    return lessCheck(value, columnNode.getMinNumValue(), columnNode.getMaxNumValue(), valueType);
    //}
    //
    //private static byte lessEqualCheck(long value, long indexMin, long indexMax, byte dataType) {
    //    byte res = Some;
    //    boolean isFloat = ColumnType.isFloatPoint(dataType);
    //    if (isFloat && Double.longBitsToDouble(value) < Double.longBitsToDouble(indexMin)) {
    //        res = None;
    //    } else if (isFloat && Double.longBitsToDouble(value) >= Double.longBitsToDouble(indexMax)) {
    //        res = All;
    //    } else if (!isFloat && value < indexMin) {
    //        res = None;
    //    } else if (!isFloat && value >= indexMax) {
    //        res = All;
    //    }
    //    return res;
    //}
    //
    //public static byte lessEqualCheckOnPack(Column column, int packId, long value) throws IOException {
    //    DataPackNode dpn = column.dpn(packId);
    //    return lessEqualCheck(value, dpn.minValue(), dpn.maxValue(), column.dataType());
    //}
    //
    //public static byte lessEqualCheckOnColumn(ColumnNode columnNode, byte valueType, long value) {
    //    return lessEqualCheck(value, columnNode.getMinNumValue(), columnNode.getMaxNumValue(), valueType);
    //}
}
