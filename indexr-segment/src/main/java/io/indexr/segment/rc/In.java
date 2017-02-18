package io.indexr.segment.rc;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.spark.unsafe.array.ByteArrayMethods;
import org.apache.spark.unsafe.types.UTF8String;

import java.io.IOException;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.indexr.data.BytePiece;
import io.indexr.segment.Column;
import io.indexr.segment.ColumnType;
import io.indexr.segment.InfoSegment;
import io.indexr.segment.PackExtIndexStr;
import io.indexr.segment.RSValue;
import io.indexr.segment.Segment;
import io.indexr.segment.pack.ColumnNode;
import io.indexr.segment.pack.DataPack;

public class In implements CmpOperator {
    @JsonProperty("attr")
    public final Attr attr;
    @JsonProperty("numValues")
    public final long[] numValues;
    @JsonIgnore
    public final UTF8String[] strValues;

    @JsonProperty("strValues")
    public String[] getStrValues() {
        if (strValues == null) {
            return null;
        }
        String[] vs = new String[strValues.length];
        for (int i = 0; i < strValues.length; i++) {
            vs[i] = strValues[i] == null ? null : strValues[i].toString();
        }
        return vs;
    }

    boolean calced = false;
    long numMin, numMax;

    @JsonCreator
    public In(@JsonProperty("attr") Attr attr,
              @JsonProperty("numValues") long[] numValues,
              @JsonProperty("strValues") String[] strValues) {
        this(attr, numValues, toUTF8Arr(strValues));
    }

    public In(Attr attr,
              long[] numValues,
              UTF8String[] strValues) {
        this.attr = attr;
        this.numValues = numValues;
        this.strValues = strValues;
    }

    static UTF8String[] toUTF8Arr(String[] strValues) {
        if (strValues == null) {
            return null;
        }
        UTF8String[] vs = new UTF8String[strValues.length];
        for (int i = 0; i < strValues.length; i++) {
            vs[i] = strValues[i] == null ? null : UTF8String.fromString(strValues[i]);
        }
        return vs;
    }

    @Override
    public String getType() {
        return "in";
    }

    @Override
    public Collection<Attr> attr() {
        return Collections.singleton(attr);
    }

    @Override
    public RCOperator applyNot() {
        return new NotIn(attr, numValues, strValues);
    }

    private void calMinMax(byte type) {
        long longMin = numValues[0];
        long longMax = numValues[0];
        double doubleMin = Double.longBitsToDouble(numValues[0]);
        double doubleMax = Double.longBitsToDouble(numValues[0]);
        for (long v : numValues) {
            if (ColumnType.isIntegral(type)) {
                longMin = Math.min(longMin, v);
                longMax = Math.max(longMax, v);
            } else {
                doubleMin = Math.min(doubleMin, Double.longBitsToDouble(v));
                doubleMax = Math.max(doubleMax, Double.longBitsToDouble(v));
            }
        }
        if (ColumnType.isIntegral(type)) {
            numMin = longMin;
            numMax = longMax;
        } else {
            numMin = Double.doubleToRawLongBits(doubleMin);
            numMax = Double.doubleToRawLongBits(doubleMax);
        }
    }

    @Override
    public byte roughCheckOnPack(Segment segment, int packId) throws IOException {
        assert attr.checkCurrent(segment.schema().columns);

        int colId = attr.columnId();
        Column column = segment.column(colId);
        byte type = column.dataType();
        if (ColumnType.isNumber(type)) {
            if (!calced) {
                calMinMax(type);
                calced = true;
            }
            return RoughCheck_N.inCheckOnPack(column, packId, numValues, numMin, numMax);
        } else {
            return RoughCheck_R.inCheckOnPack(column, packId, strValues);
        }
    }

    @Override
    public byte roughCheckOnColumn(InfoSegment segment) throws IOException {
        assert attr.checkCurrent(segment.schema().columns);

        int colId = attr.columnId();
        ColumnNode columnNode = segment.columnNode(colId);
        byte type = attr.dataType();
        if (ColumnType.isNumber(type)) {
            if (!calced) {
                calMinMax(type);
                calced = true;
            }
            return RoughCheck_N.inCheckOnColumn(columnNode, type, numValues, numMin, numMax);
        } else {
            return RSValue.Some;
        }
    }

    @Override
    public byte roughCheckOnRow(Segment segment, int packId) throws IOException {
        Column column = segment.column(attr.columnId());
        byte type = attr.dataType();
        int rowCount = column.dpn(packId).objCount();
        int hitCount = 0;
        switch (type) {
            case ColumnType.STRING: {
                PackExtIndexStr extIndex = column.extIndex(packId);
                byte res = RSValue.None;
                _:
                for (UTF8String value : strValues) {
                    for (int rowId = 0; rowId < rowCount; rowId++) {
                        res = extIndex.isValue(rowId, value);
                        if (res != RSValue.None) {
                            break _;
                        }
                    }
                }
                if (res == RSValue.None) {
                    return RSValue.None;
                }

                DataPack pack = column.pack(packId);
                BytePiece bp = new BytePiece();
                for (int rowId = 0; rowId < rowCount; rowId++) {
                    pack.rawValueAt(rowId, bp);
                    for (UTF8String value : strValues) {
                        if (bp.len == value.numBytes()
                                && ByteArrayMethods.arrayEquals(value.getBaseObject(), value.getBaseOffset(), bp.base, bp.addr, bp.len)) {
                            hitCount++;
                            break;
                        }
                    }
                }
                break;
            }
            default: {
                DataPack pack = column.pack(packId);
                for (int rowId = 0; rowId < rowCount; rowId++) {
                    long v = pack.uniformValAt(rowId, type);
                    for (long value : numValues) {
                        if (v == value) {
                            hitCount++;
                            break;
                        }
                    }
                }
                break;
            }
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
        int rowCount = column.dpn(packId).objCount();
        BitSet colRes = new BitSet(rowCount);
        byte type = attr.dataType();
        switch (type) {
            case ColumnType.STRING: {
                DataPack pack = column.pack(packId);
                BytePiece bp = new BytePiece();
                for (int rowId = 0; rowId < rowCount; rowId++) {
                    pack.rawValueAt(rowId, bp);
                    boolean ok = false;
                    for (UTF8String value : strValues) {
                        if (bp.len == value.numBytes()
                                && ByteArrayMethods.arrayEquals(value.getBaseObject(), value.getBaseOffset(), bp.base, bp.addr, bp.len)) {
                            ok = true;
                        }
                    }
                    colRes.set(rowId, ok);
                }
                break;
            }
            default: {
                DataPack pack = column.pack(packId);
                for (int rowId = 0; rowId < rowCount; rowId++) {
                    long v = pack.uniformValAt(rowId, type);
                    boolean ok = false;
                    for (long value : numValues) {
                        if (v == value) {
                            ok = true;
                        }
                    }
                    colRes.set(rowId, ok);
                }
                break;
            }
        }
        return colRes;
    }

    @Override
    public String toString() {
        String valuesStr;
        Stream<? extends Object> valuesStream;
        int valueCount;
        if (numValues != null) {
            valuesStream = Arrays.stream(numValues).boxed();
            valueCount = numValues.length;
        } else {
            valuesStream = Arrays.stream(strValues);
            valueCount = strValues.length;
        }
        String listStr = valuesStream.map(String::valueOf).limit(5).collect(Collectors.joining(","));
        if (valueCount > 5) {
            valuesStr = String.format("(%d)[%s ...]", valueCount, listStr);
        } else {
            valuesStr = String.format("(%d)[%s]", valueCount, listStr);
        }
        return String.format("%s($%s: %s)", this.getClass().getSimpleName(), attr, valuesStr);
    }

}
