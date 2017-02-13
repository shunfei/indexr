package io.indexr.segment.rc;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.spark.unsafe.types.UTF8String;

import java.io.IOException;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;

import io.indexr.segment.Column;
import io.indexr.segment.ColumnType;
import io.indexr.segment.InfoSegment;
import io.indexr.segment.RSValue;
import io.indexr.segment.Segment;
import io.indexr.segment.pack.ColumnNode;
import io.indexr.segment.pack.DataPack;

public class Between implements CmpOperator {
    @JsonProperty("attr")
    public final Attr attr;
    @JsonProperty("numValue1")
    public final long numValue1;
    @JsonProperty("numValue2")
    public final long numValue2;
    @JsonIgnore
    public final UTF8String strValue1;
    @JsonIgnore
    public final UTF8String strValue2;

    @JsonProperty("strValue1")
    public String getStrValue1() {return strValue1 == null ? null : strValue1.toString();}

    @JsonProperty("strValue2")
    public String getStrValue2() {return strValue2 == null ? null : strValue2.toString();}

    @JsonCreator
    public Between(@JsonProperty("attr") Attr attr,
                   @JsonProperty("numValue1") long numValue1,
                   @JsonProperty("numValue2") long numValue2,
                   @JsonProperty("strValue1") String strValue1,
                   @JsonProperty("strValue2") String strValue2) {
        this(attr, numValue1, numValue2,
                strValue1 == null ? null : UTF8String.fromString(strValue1),
                strValue2 == null ? null : UTF8String.fromString(strValue2));
    }

    public Between(Attr attr,
                   long numValue1,
                   long numValue2,
                   UTF8String strValue1,
                   UTF8String strValue2) {
        this.attr = attr;
        this.numValue1 = numValue1;
        this.numValue2 = numValue2;
        this.strValue1 = strValue1;
        this.strValue2 = strValue2;
    }

    @Override
    public String getType() {return "between";}

    @Override
    public Collection<Attr> attr() {
        return Collections.singleton(attr);
    }

    @Override
    public RCOperator applyNot() {
        return new NotBetween(attr, numValue1, numValue2, strValue1, strValue2);
    }

    @Override
    public byte roughCheckOnPack(Segment segment, int packId) throws IOException {
        assert attr.checkCurrent(segment.schema().columns);

        int colId = attr.columnId();
        Column column = segment.column(colId);
        byte type = column.dataType();
        if (ColumnType.isNumber(type)) {
            return RoughCheck_N.betweenCheckOnPack(column, packId, numValue1, numValue2);
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
            return RoughCheck_N.betweenCheckOnColumn(columnNode, type, numValue1, numValue2);
        } else {
            return RSValue.Some;
        }
    }

    @Override
    public byte roughCheckOnRow(DataPack[] rowPacks) {
        DataPack pack = rowPacks[attr.columnId()];
        byte type = attr.dataType();
        int rowCount = pack.objCount();
        int hitCount = 0;
        switch (type) {
            case ColumnType.INT: {
                int min = (int) numValue1;
                int max = (int) numValue2;
                for (int rowId = 0; rowId < rowCount; rowId++) {
                    int v = pack.intValueAt(rowId);
                    if (v >= min && v <= max) {
                        hitCount++;
                    }
                }
                break;
            }
            case ColumnType.LONG: {
                long min = numValue1;
                long max = numValue2;
                for (int rowId = 0; rowId < rowCount; rowId++) {
                    long v = pack.longValueAt(rowId);
                    if (v >= min && v <= max) {
                        hitCount++;
                    }
                }
                break;
            }
            case ColumnType.FLOAT: {
                float min = (float) numValue1;
                float max = (float) numValue2;
                for (int rowId = 0; rowId < rowCount; rowId++) {
                    float v = pack.floatValueAt(rowId);
                    if (v >= min && v <= max) {
                        hitCount++;
                    }
                }
                break;
            }
            case ColumnType.DOUBLE: {
                double min = (double) numValue1;
                double max = (double) numValue2;
                for (int rowId = 0; rowId < rowCount; rowId++) {
                    double v = pack.doubleValueAt(rowId);
                    if (v >= min && v <= max) {
                        hitCount++;
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
    public BitSet exactCheckOnRow(DataPack[] rowPacks) {
        DataPack pack = rowPacks[attr.columnId()];
        int rowCount = pack.objCount();
        BitSet colRes = new BitSet(pack.objCount());
        switch (attr.dataType()) {
            case ColumnType.INT: {
                int min = (int) numValue1;
                int max = (int) numValue2;
                for (int rowId = 0; rowId < rowCount; rowId++) {
                    int v = pack.intValueAt(rowId);
                    colRes.set(rowId, v >= min && v <= max);
                }
                break;
            }
            case ColumnType.LONG: {
                long min = numValue1;
                long max = numValue2;
                for (int rowId = 0; rowId < rowCount; rowId++) {
                    long v = pack.longValueAt(rowId);
                    colRes.set(rowId, v >= min && v <= max);
                }
                break;
            }
            case ColumnType.FLOAT: {
                float min = (float) numValue1;
                float max = (float) numValue2;
                for (int rowId = 0; rowId < rowCount; rowId++) {
                    float v = pack.floatValueAt(rowId);
                    colRes.set(rowId, v >= min && v <= max);
                }
                break;
            }
            case ColumnType.DOUBLE: {
                double min = (double) numValue1;
                double max = (double) numValue2;
                for (int rowId = 0; rowId < rowCount; rowId++) {
                    double v = pack.doubleValueAt(rowId);
                    colRes.set(rowId, v >= min && v <= max);
                }
                break;
            }
            default:
                throw new IllegalStateException("column type " + attr.dataType() + " is illegal in " + getType().toUpperCase());
        }
        return colRes;
    }

    @Override
    public String toString() {
        if (strValue1 == null && strValue2 == null) {
            return String.format("%s($%s: %s, %s)", this.getClass().getSimpleName(), attr, numValue1, numValue2);
        } else {
            return String.format("%s($%s: %s, %s)", this.getClass().getSimpleName(), attr, strValue2, strValue2);
        }
    }
}
