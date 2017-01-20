package io.indexr.segment.rc;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.spark.unsafe.types.UTF8String;

import java.io.IOException;
import java.util.BitSet;

import io.indexr.data.LikePattern;
import io.indexr.segment.Column;
import io.indexr.segment.ColumnType;
import io.indexr.segment.InfoSegment;
import io.indexr.segment.RSValue;
import io.indexr.segment.Segment;
import io.indexr.segment.pack.ColumnNode;
import io.indexr.segment.pack.DataPack;
import io.indexr.util.SQLLike;

public class Like extends ColCmpVal {
    private LikePattern pattern;

    @JsonCreator
    public Like(@JsonProperty("attr") Attr attr,
                @JsonProperty("numValue") long numValue,
                @JsonProperty("strValue") String strValue) {
        super(attr, numValue, strValue);
        this.pattern = new LikePattern(super.strValue);
    }

    public Like(Attr attr,
                long numValue,
                UTF8String strValue) {
        super(attr, numValue, strValue);
        this.pattern = new LikePattern(strValue);
    }

    @Override
    public String getType() {
        return "like";
    }

    @Override
    public RCOperator applyNot() {
        return new NotLike(attr, numValue, strValue);
    }

    @Override
    public byte roughCheckOnPack(Segment segment, int packId) throws IOException {
        assert attr.checkCurrent(segment.schema().columns);

        int colId = attr.columnId();
        Column column = segment.column(colId);
        byte type = column.dataType();
        if (ColumnType.isNumber(type)) {
            return RSValue.Some;
        } else {
            return RoughCheck_R.likeCheckOnPack(column, packId, pattern);
        }
    }

    @Override
    public byte roughCheckOnColumn(InfoSegment segment) throws IOException {
        assert attr.checkCurrent(segment.schema().columns);

        int colId = attr.columnId();
        ColumnNode columnNode = segment.columnNode(colId);
        byte type = attr.columType();
        if (ColumnType.isNumber(type)) {
            return RSValue.Some;
        } else {
            return RSValue.Some;
        }
    }

    @Override
    public byte roughCheckOnRow(DataPack[] rowPacks) {
        DataPack pack = rowPacks[attr.columnId()];
        byte type = attr.columType();
        int rowCount = pack.objCount();
        int hitCount = 0;
        switch (type) {
            case ColumnType.STRING: {
                for (int rowId = 0; rowId < rowCount; rowId++) {
                    if (SQLLike.match(pack.stringValueAt(rowId), strValue)) {
                        hitCount++;
                    }
                }
                break;
            }
            default:
                throw new IllegalStateException("column type " + attr.columType() + " is illegal in " + getType().toUpperCase());
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
        byte type = attr.columType();
        switch (type) {
            case ColumnType.STRING: {
                for (int rowId = 0; rowId < rowCount; rowId++) {
                    colRes.set(rowId, SQLLike.match(pack.stringValueAt(rowId), strValue));
                }
                break;
            }
            default:
                throw new IllegalStateException("column type " + attr.columType() + " is illegal in " + getType().toUpperCase());
        }
        return colRes;
    }
}
