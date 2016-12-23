package io.indexr.segment.rc;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.spark.unsafe.array.ByteArrayMethods;
import org.apache.spark.unsafe.types.UTF8String;

import java.io.IOException;
import java.util.BitSet;

import io.indexr.data.BytePiece;
import io.indexr.segment.Column;
import io.indexr.segment.ColumnType;
import io.indexr.segment.InfoSegment;
import io.indexr.segment.RSValue;
import io.indexr.segment.Segment;
import io.indexr.segment.pack.ColumnNode;
import io.indexr.segment.pack.DataPack;

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
        byte type = attr.columType();
        if (ColumnType.isNumber(type)) {
            return RoughCheck_N.equalCheckOnColumn(columnNode, type, numValue);
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
                BytePiece bp = new BytePiece();
                Object valBase = strValue.getBaseObject();
                long valOffset = strValue.getBaseOffset();
                int valLen = strValue.numBytes();
                for (int rowId = 0; rowId < rowCount; rowId++) {
                    pack.rawValueAt(rowId, bp);
                    if (bp.len == valLen && ByteArrayMethods.arrayEquals(valBase, valOffset, bp.base, bp.addr, valLen)) {
                        hitCount++;
                    }
                }
                break;
            }
            default: {
                for (int rowId = 0; rowId < pack.objCount(); rowId++) {
                    if (pack.uniformValAt(rowId, type) == numValue) {
                        hitCount++;
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
    public BitSet exactCheckOnRow(DataPack[] rowPacks) {
        DataPack pack = rowPacks[attr.columnId()];
        int rowCount = pack.objCount();
        BitSet colRes = new BitSet(rowCount);
        byte type = attr.columType();
        switch (type) {
            case ColumnType.STRING: {
                BytePiece bp = new BytePiece();
                Object valBase = strValue.getBaseObject();
                long valOffset = strValue.getBaseOffset();
                int valLen = strValue.numBytes();
                for (int rowId = 0; rowId < rowCount; rowId++) {
                    pack.rawValueAt(rowId, bp);
                    colRes.set(rowId, bp.len == valLen && ByteArrayMethods.arrayEquals(valBase, valOffset, bp.base, bp.addr, valLen));
                }
                break;
            }
            default: {
                for (int rowId = 0; rowId < rowCount; rowId++) {
                    colRes.set(rowId, pack.uniformValAt(rowId, type) == numValue);
                }
                break;
            }
        }
        return colRes;
    }
}
