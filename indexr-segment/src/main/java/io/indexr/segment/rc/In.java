package io.indexr.segment.rc;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.spark.unsafe.types.UTF8String;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.indexr.segment.Column;
import io.indexr.segment.ColumnType;
import io.indexr.segment.InfoSegment;
import io.indexr.segment.OuterIndex;
import io.indexr.segment.PackExtIndex;
import io.indexr.segment.RSValue;
import io.indexr.segment.Segment;
import io.indexr.segment.storage.ColumnNode;
import io.indexr.util.BitMap;

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
    public BitMap exactCheckOnPack(Segment segment) throws IOException {
        assert attr.checkCurrent(segment.schema().columns);

        Column column = segment.column(attr.columnId());
        try (OuterIndex outerIndex = column.outerIndex()) {
            return outerIndex.in(column, numValues, strValues, false);
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
    public BitMap exactCheckOnRow(Segment segment, int packId) throws IOException {
        Column column = segment.column(attr.columnId());
        PackExtIndex extIndex = column.extIndex(packId);
        return extIndex.in(column, packId, numValues, strValues);
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
