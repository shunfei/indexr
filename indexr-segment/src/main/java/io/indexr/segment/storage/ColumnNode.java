package io.indexr.segment.storage;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

import io.indexr.segment.ColumnType;
import io.indexr.segment.pack.DataPackNode;
import io.indexr.segment.pack.NumType;

public class ColumnNode {
    @JsonProperty("minNumValue")
    public long minNumValue;
    @JsonProperty("maxNumValue")
    public long maxNumValue;

    @JsonCreator
    public ColumnNode(@JsonProperty("minNumValue") long minNumValue,
                      @JsonProperty("maxNumValue") long maxNumValue) {
        this.minNumValue = minNumValue;
        this.maxNumValue = maxNumValue;
    }

    public long getMinNumValue() {
        return minNumValue;
    }

    public long getMaxNumValue() {
        return maxNumValue;
    }

    public static ColumnNode from(DataPackNode[] dpns, byte dataType) {
        if (dpns.length == 0) {
            return none(dataType);
        }
        long minLong = dpns[0].minValue();
        long maxLong = dpns[0].maxValue();
        double minDouble = NumType.longToDouble(minLong);
        double maxDouble = NumType.longToDouble(maxLong);
        for (int i = 1; i < dpns.length; i++) {
            DataPackNode n = dpns[i];
            switch (dataType) {
                case ColumnType.INT:
                case ColumnType.LONG:
                    minLong = Math.min(minLong, n.minValue());
                    maxLong = Math.max(maxLong, n.maxValue());
                    break;
                case ColumnType.FLOAT:
                case ColumnType.DOUBLE:
                    minDouble = Math.min(minDouble, NumType.longToDouble(n.minValue()));
                    maxDouble = Math.max(maxDouble, NumType.longToDouble(n.maxValue()));
                    break;
                default:
                    minLong = 0;
                    maxLong = 0;
                    minDouble = 0.0;
                    maxDouble = 0.0;
            }
        }
        if (dataType == ColumnType.FLOAT || dataType == ColumnType.DOUBLE) {
            return new ColumnNode(
                    NumType.doubleToLong(minDouble),
                    NumType.doubleToLong(maxDouble));
        } else {
            return new ColumnNode(minLong, maxLong);
        }
    }

    public static ColumnNode merge(List<ColumnNode> nodes, byte dataType) {
        if (nodes.size() == 0) {
            return none(dataType);
        }
        long minLong = nodes.get(0).minNumValue;
        long maxLong = nodes.get(0).maxNumValue;
        double minDouble = NumType.longToDouble(minLong);
        double maxDouble = NumType.longToDouble(maxLong);
        for (ColumnNode n : nodes) {
            switch (dataType) {
                case ColumnType.INT:
                case ColumnType.LONG:
                    minLong = Math.min(minLong, n.minNumValue);
                    maxLong = Math.max(maxLong, n.maxNumValue);
                    break;
                case ColumnType.FLOAT:
                case ColumnType.DOUBLE:
                    minDouble = Math.min(minDouble, NumType.longToDouble(n.minNumValue));
                    maxDouble = Math.max(maxDouble, NumType.longToDouble(n.maxNumValue));
                    break;
                default:
                    minLong = 0;
                    maxLong = 0;
                    minDouble = 0.0;
                    maxDouble = 0.0;
            }
        }
        if (dataType == ColumnType.FLOAT || dataType == ColumnType.DOUBLE) {
            return new ColumnNode(
                    NumType.doubleToLong(minDouble),
                    NumType.doubleToLong(maxDouble));
        } else {
            return new ColumnNode(minLong, maxLong);
        }
    }

    public static ColumnNode none(byte dataType) {
        switch (dataType) {
            case ColumnType.INT:
            case ColumnType.LONG:
                return new ColumnNode(Long.MAX_VALUE, Long.MIN_VALUE);
            case ColumnType.FLOAT:
            case ColumnType.DOUBLE:
                return new ColumnNode(
                        Double.doubleToRawLongBits(Double.MAX_VALUE),
                        Double.doubleToRawLongBits(Double.MIN_VALUE));
            default:
                return new ColumnNode(0, 0);
        }
    }

    public static long minNum(byte dataType, long v1, long v2) {
        switch (dataType) {
            case ColumnType.INT:
            case ColumnType.LONG:
                return Math.min(v1, v2);
            case ColumnType.FLOAT:
            case ColumnType.DOUBLE:
                return Double.doubleToRawLongBits(
                        Math.min(Double.longBitsToDouble(v1),
                                Double.longBitsToDouble(v2)));
            default:
                return 0;
        }
    }

    public static long maxNum(byte dataType, long v1, long v2) {
        switch (dataType) {
            case ColumnType.INT:
            case ColumnType.LONG:
                return Math.max(v1, v2);
            case ColumnType.FLOAT:
            case ColumnType.DOUBLE:
                return Double.doubleToRawLongBits(
                        Math.max(Double.longBitsToDouble(v1),
                                Double.longBitsToDouble(v2)));
            default:
                return 0;
        }
    }
}
