package io.indexr.segment.rc;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.spark.unsafe.types.UTF8String;

import java.io.IOException;
import java.util.BitSet;

import io.indexr.segment.Column;
import io.indexr.segment.ColumnType;
import io.indexr.segment.InfoSegment;
import io.indexr.segment.RSValue;
import io.indexr.segment.Segment;
import io.indexr.segment.pack.ColumnNode;
import io.indexr.segment.pack.DataPack;

public class Greater extends ColCmpVal {
    @JsonCreator
    public Greater(@JsonProperty("attr") Attr attr,
                   @JsonProperty("numValue") long numValue,
                   @JsonProperty("strValue") String strValue) {
        super(attr, numValue, strValue);
    }

    public Greater(Attr attr,
                   long numValue,
                   UTF8String strValue) {
        super(attr, numValue, strValue);
    }

    @Override
    public String getType() {
        return "greater";
    }

    @Override
    public RCOperator applyNot() {
        return new LessEqual(attr, numValue, strValue);
    }

    @Override
    public RCOperator switchDirection() {
        return new Less(attr, numValue, strValue);
    }

    @Override
    public byte roughCheckOnPack(Segment segment, int packId) throws IOException {
        assert attr.checkCurrent(segment.schema().columns);

        int colId = attr.columnId();
        Column column = segment.column(colId);
        byte type = column.dataType();
        if (ColumnType.isNumber(type)) {
            return RoughCheck_N.greaterCheckOnPack(column, packId, numValue);
        } else {
            return RSValue.Some;
        }
    }

    @Override
    public byte roughCheckOnColumn(InfoSegment segment) throws IOException {
        assert attr.checkCurrent(segment.schema().columns);

        int colId = attr.columnId();
        ColumnNode columnNode = segment.columnNode(colId);
        byte type = attr.dataType();
        if (ColumnType.isNumber(type)) {
            return RoughCheck_N.greaterCheckOnColumn(columnNode, type, numValue);
        } else {
            return RSValue.Some;
        }
    }

    @Override
    public byte roughCheckOnRow(Segment segment, int packId) throws IOException {
        Column column = segment.column(attr.columnId());
        DataPack pack = column.pack(packId);
        byte type = attr.dataType();
        int rowCount = pack.objCount();
        int hitCount = 0;
        switch (type) {
            case ColumnType.INT: {
                int value = (int) numValue;
                for (int rowId = 0; rowId < rowCount; rowId++) {
                    int v = pack.intValueAt(rowId);
                    if (v > value) {
                        hitCount++;
                        break;
                    }
                }
                break;
            }
            case ColumnType.LONG: {
                long value = numValue;
                for (int rowId = 0; rowId < rowCount; rowId++) {
                    long v = pack.longValueAt(rowId);
                    if (v > value) {
                        hitCount++;
                        break;
                    }
                }
                break;
            }
            case ColumnType.FLOAT: {
                float value = (float) Double.longBitsToDouble(numValue);
                for (int rowId = 0; rowId < rowCount; rowId++) {
                    float v = pack.floatValueAt(rowId);
                    if (v > value) {
                        hitCount++;
                        break;
                    }
                }
                break;
            }
            case ColumnType.DOUBLE: {
                double value = Double.longBitsToDouble(numValue);
                for (int rowId = 0; rowId < rowCount; rowId++) {
                    double v = pack.doubleValueAt(rowId);
                    if (v > value) {
                        hitCount++;
                        break;
                    }
                }
                break;
            }
            default:
                throw new IllegalStateException("column type " + attr.dataType() + " is illegal in " + getType().toUpperCase());
        }
        if (hitCount == rowCount) {
            return RSValue.All;
        } else if (hitCount > 0) {
            return RSValue.Some;
        } else {
            return RSValue.None;
        }
    }

    @Override
    public BitSet exactCheckOnRow(Segment segment, int packId) throws IOException {
        Column column = segment.column(attr.columnId());
        DataPack pack = column.pack(packId);
        int rowCount = pack.objCount();
        BitSet colRes = new BitSet(rowCount);
        switch (attr.dataType()) {
            case ColumnType.INT: {
                int value = (int) numValue;
                for (int rowId = 0; rowId < rowCount; rowId++) {
                    int v = pack.intValueAt(rowId);
                    colRes.set(rowId, v > value);
                }
                break;
            }
            case ColumnType.LONG: {
                long value = numValue;
                for (int rowId = 0; rowId < rowCount; rowId++) {
                    long v = pack.longValueAt(rowId);
                    colRes.set(rowId, v > value);
                }
                break;
            }
            case ColumnType.FLOAT: {
                float value = (float) Double.longBitsToDouble(numValue);
                for (int rowId = 0; rowId < rowCount; rowId++) {
                    float v = pack.floatValueAt(rowId);
                    colRes.set(rowId, v > value);
                }
                break;
            }
            case ColumnType.DOUBLE: {
                double value = Double.longBitsToDouble(numValue);
                for (int rowId = 0; rowId < rowCount; rowId++) {
                    double v = pack.doubleValueAt(rowId);
                    colRes.set(rowId, v > value);
                }
                break;
            }
            default:
                throw new IllegalStateException("column type " + attr.dataType() + " is illegal in " + getType().toUpperCase());
        }
        return colRes;
    }
}
