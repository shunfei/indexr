package io.indexr.segment.rc;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.spark.unsafe.types.UTF8String;

import java.io.IOException;

import io.indexr.segment.Column;
import io.indexr.segment.ColumnType;
import io.indexr.segment.InfoSegment;
import io.indexr.segment.PackExtIndex;
import io.indexr.segment.RSValue;
import io.indexr.segment.Segment;
import io.indexr.segment.storage.ColumnNode;
import io.indexr.util.BitMap;

public class Equal extends ColCmpVal {
    @JsonCreator
    public Equal(@JsonProperty("attr") Attr attr,
                 @JsonProperty("numValue") long numValue,
                 @JsonProperty("strValue") String strValue) {
        super(attr, numValue, strValue);
    }

    public Equal(Attr attr,
                 long numValue,
                 UTF8String strValue) {
        super(attr, numValue, strValue);
    }

    @Override
    public String getType() {
        return "equal";
    }

    @Override
    public RCOperator applyNot() {
        return new NotEqual(attr, numValue, strValue);
    }

    @Override
    public byte roughCheckOnPack(Segment segment, int packId) throws IOException {
        assert attr.checkCurrent(segment.schema().columns);

        int colId = attr.columnId();
        Column column = segment.column(colId);
        byte type = column.dataType();
        if (ColumnType.isNumber(type)) {
            return RoughCheck_N.equalCheckOnPack(column, packId, numValue);
        } else {
            return RoughCheck_R.equalCheckOnPack(column, packId, strValue);
        }
    }

    @Override
    public byte roughCheckOnColumn(InfoSegment segment) throws IOException {
        assert attr.checkCurrent(segment.schema().columns);

        int colId = attr.columnId();
        ColumnNode columnNode = segment.columnNode(colId);
        byte type = attr.dataType();
        if (ColumnType.isNumber(type)) {
            return RoughCheck_N.equalCheckOnColumn(columnNode, type, numValue);
        } else {
            return RSValue.Some;
        }
    }

    @Override
    public BitMap exactCheckOnRow(Segment segment, int packId) throws IOException {
        Column column = segment.column(attr.columnId());
        PackExtIndex extIndex = column.extIndex(packId);
        return extIndex.equal(column, packId, numValue, strValue);
    }
}
