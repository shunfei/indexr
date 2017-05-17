package io.indexr.segment.index;

import org.apache.spark.unsafe.array.ByteArrayMethods;
import org.apache.spark.unsafe.types.UTF8String;

import java.io.IOException;

import io.indexr.data.BytePiece;
import io.indexr.segment.Column;
import io.indexr.segment.ColumnType;
import io.indexr.segment.PackExtIndex;
import io.indexr.segment.pack.DataPack;
import io.indexr.segment.pack.DataPackNode;
import io.indexr.util.BitMap;
import io.indexr.util.BytesUtil;
import io.indexr.util.SQLLike;

public class ExtIndex_SimpleBits implements PackExtIndex {
    protected byte dataType;

    public ExtIndex_SimpleBits(byte dataType) {
        this.dataType = dataType;
    }

    @Override
    public BitMap equal(Column column, int packId, long numValue, UTF8String strValue) throws IOException {
        DataPack pack = column.pack(packId);
        DataPackNode dpn = column.dpn(packId);
        int rowCount = pack.valueCount();
        BitMap res = new BitMap();
        switch (dataType) {
            case ColumnType.INT: {
                int value = (int) numValue;
                for (int rowId = 0; rowId < rowCount; rowId++) {
                    int v = pack.intValueAt(rowId);
                    if (v == value) {
                        res.set(rowId);
                    }
                }
                break;
            }
            case ColumnType.LONG: {
                long value = numValue;
                for (int rowId = 0; rowId < rowCount; rowId++) {
                    long v = pack.longValueAt(rowId);
                    if (v == value) {
                        res.set(rowId);
                    }
                }
                break;
            }
            case ColumnType.FLOAT: {
                float value = (float) Double.longBitsToDouble(numValue);
                for (int rowId = 0; rowId < rowCount; rowId++) {
                    float v = pack.floatValueAt(rowId);
                    if (v == value) {
                        res.set(rowId);
                    }
                }
                break;
            }
            case ColumnType.DOUBLE: {
                double value = Double.longBitsToDouble(numValue);
                for (int rowId = 0; rowId < rowCount; rowId++) {
                    double v = pack.doubleValueAt(rowId);
                    if (v == value) {
                        res.set(rowId);
                    }
                }
                break;
            }
            case ColumnType.STRING: {
                BytePiece bp = new BytePiece();
                Object valBase = strValue.getBaseObject();
                long valOffset = strValue.getBaseOffset();
                int valLen = strValue.numBytes();
                for (int rowId = 0; rowId < rowCount; rowId++) {
                    pack.rawValueAt(rowId, bp);
                    if (bp.len == valLen && ByteArrayMethods.arrayEquals(valBase, valOffset, bp.base, bp.addr, valLen)) {
                        res.set(rowId);
                    }
                }
                break;
            }
            default:
                throw new IllegalStateException("Illegal dataType: " + dataType);
        }
        return fixBitmap(res, rowCount);
    }

    @Override
    public BitMap in(Column column, int packId, long[] numValues, UTF8String[] strValues) throws IOException {
        DataPack pack = column.pack(packId);
        DataPackNode dpn = column.dpn(packId);
        int rowCount = pack.valueCount();
        BitMap res = new BitMap();
        switch (dataType) {
            case ColumnType.INT: {
                for (int rowId = 0; rowId < rowCount; rowId++) {
                    int v = pack.intValueAt(rowId);
                    for (long value : numValues) {
                        if (v == (int) value) {
                            res.set(rowId);
                            break;
                        }
                    }
                }
                break;
            }
            case ColumnType.LONG: {
                for (int rowId = 0; rowId < rowCount; rowId++) {
                    long v = pack.longValueAt(rowId);
                    for (long value : numValues) {
                        if (v == value) {
                            res.set(rowId);
                            break;
                        }
                    }
                }
                break;
            }
            case ColumnType.FLOAT: {
                for (int rowId = 0; rowId < rowCount; rowId++) {
                    float v = pack.floatValueAt(rowId);
                    for (long value : numValues) {
                        if (v == (float) Double.longBitsToDouble(value)) {
                            res.set(rowId);
                            break;
                        }
                    }
                }
                break;
            }
            case ColumnType.DOUBLE: {
                for (int rowId = 0; rowId < rowCount; rowId++) {
                    double v = pack.doubleValueAt(rowId);
                    for (long value : numValues) {
                        if (v == Double.longBitsToDouble(value)) {
                            res.set(rowId);
                            break;
                        }
                    }
                }
                break;
            }
            case ColumnType.STRING: {
                BytePiece bp = new BytePiece();
                for (int rowId = 0; rowId < rowCount; rowId++) {
                    pack.rawValueAt(rowId, bp);
                    for (UTF8String value : strValues) {
                        if (bp.len == value.numBytes()
                                && ByteArrayMethods.arrayEquals(
                                value.getBaseObject(), value.getBaseOffset(),
                                bp.base, bp.addr, bp.len)) {
                            res.set(rowId);
                            break;
                        }
                    }
                }
                break;
            }
            default:
                throw new IllegalStateException("Illegal dataType: " + dataType);
        }
        return fixBitmap(res, rowCount);
    }

    @Override
    public BitMap greater(Column column, int packId, long numValue, UTF8String strValue, boolean acceptEqual) throws IOException {
        DataPack pack = column.pack(packId);
        DataPackNode dpn = column.dpn(packId);
        int rowCount = pack.valueCount();
        BitMap res = new BitMap();
        switch (dataType) {
            case ColumnType.INT: {
                int value = (int) numValue;
                for (int rowId = 0; rowId < rowCount; rowId++) {
                    int v = pack.intValueAt(rowId);
                    if (v > value || (acceptEqual && v == value)) {
                        res.set(rowId);
                    }
                }
                break;
            }
            case ColumnType.LONG: {
                long value = numValue;
                for (int rowId = 0; rowId < rowCount; rowId++) {
                    long v = pack.longValueAt(rowId);
                    if (v > value || (acceptEqual && v == value)) {
                        res.set(rowId);
                    }
                }
                break;
            }
            case ColumnType.FLOAT: {
                float value = (float) Double.longBitsToDouble(numValue);
                for (int rowId = 0; rowId < rowCount; rowId++) {
                    float v = pack.floatValueAt(rowId);
                    if (v > value || (acceptEqual && v == value)) {
                        res.set(rowId);
                    }
                }
                break;
            }
            case ColumnType.DOUBLE: {
                double value = Double.longBitsToDouble(numValue);
                for (int rowId = 0; rowId < rowCount; rowId++) {
                    double v = pack.doubleValueAt(rowId);
                    if (v > value || (acceptEqual && v == value)) {
                        res.set(rowId);
                    }
                }
                break;
            }
            case ColumnType.STRING: {
                Object valBase = strValue.getBaseObject();
                long valOffset = strValue.getBaseOffset();
                int valLen = strValue.numBytes();

                BytePiece bp = new BytePiece();
                for (int rowId = 0; rowId < rowCount; rowId++) {
                    pack.rawValueAt(rowId, bp);
                    int cmp = BytesUtil.compareBytes(bp.base, bp.addr, bp.len, valBase, valOffset, valLen);
                    if (cmp > 0 || (acceptEqual && cmp == 0)) {
                        res.set(rowId);
                    }
                }
                break;
            }
            default:
                throw new IllegalStateException("Illegal dataType: " + dataType);
        }
        return fixBitmap(res, rowCount);
    }

    @Override
    public BitMap between(Column column, int packId, long numValue1, long numValue2, UTF8String strValue1, UTF8String strValue2) throws IOException {
        DataPack pack = column.pack(packId);
        DataPackNode dpn = column.dpn(packId);
        int rowCount = pack.valueCount();
        BitMap res = new BitMap();
        switch (dataType) {
            case ColumnType.INT: {
                int value1 = (int) numValue1;
                int value2 = (int) numValue2;
                for (int rowId = 0; rowId < rowCount; rowId++) {
                    int v = pack.intValueAt(rowId);
                    if (v >= value1 && v <= value2) {
                        res.set(rowId);
                    }
                }
                break;
            }
            case ColumnType.LONG: {
                long value1 = numValue1;
                long value2 = numValue2;
                for (int rowId = 0; rowId < rowCount; rowId++) {
                    long v = pack.longValueAt(rowId);
                    if (v >= value1 && v <= value2) {
                        res.set(rowId);
                    }
                }
                break;
            }
            case ColumnType.FLOAT: {
                float value1 = (float) Double.longBitsToDouble(numValue1);
                float value2 = (float) Double.longBitsToDouble(numValue2);
                for (int rowId = 0; rowId < rowCount; rowId++) {
                    float v = pack.floatValueAt(rowId);
                    if (v >= value1 && v <= value2) {
                        res.set(rowId);
                    }
                }
                break;
            }
            case ColumnType.DOUBLE: {
                double value1 = Double.longBitsToDouble(numValue1);
                double value2 = Double.longBitsToDouble(numValue2);
                for (int rowId = 0; rowId < rowCount; rowId++) {
                    double v = pack.doubleValueAt(rowId);
                    if (v >= value1 && v <= value2) {
                        res.set(rowId);
                    }
                }
                break;
            }
            case ColumnType.STRING: {
                Object valBase1 = strValue1.getBaseObject();
                long valOffset1 = strValue1.getBaseOffset();
                int valLen1 = strValue1.numBytes();

                Object valBase2 = strValue2.getBaseObject();
                long valOffset2 = strValue2.getBaseOffset();
                int valLen2 = strValue2.numBytes();

                BytePiece bp = new BytePiece();
                for (int rowId = 0; rowId < rowCount; rowId++) {
                    pack.rawValueAt(rowId, bp);
                    int cmp1 = BytesUtil.compareBytes(bp.base, bp.addr, bp.len, valBase1, valOffset1, valLen1);
                    int cmp2 = BytesUtil.compareBytes(bp.base, bp.addr, bp.len, valBase2, valOffset2, valLen2);
                    if (cmp1 >= 0 && cmp2 <= 0) {
                        res.set(rowId);
                    }
                }
                break;
            }
            default:
                throw new IllegalStateException("Illegal dataType: " + dataType);
        }
        return fixBitmap(res, rowCount);
    }

    @Override
    public BitMap like(Column column, int packId, long numValue, UTF8String strValue) throws IOException {
        DataPack pack = column.pack(packId);
        int rowCount = pack.valueCount();
        BitMap res = new BitMap();
        switch (dataType) {
            case ColumnType.STRING: {
                for (int rowId = 0; rowId < rowCount; rowId++) {
                    if (SQLLike.match(pack.stringValueAt(rowId), strValue)) {
                        res.set(rowId);
                    }
                }
                break;
            }
            default:
                throw new IllegalStateException("column type " + dataType + " is illegal in LIKE");
        }
        return fixBitmap(res, rowCount);
    }
}
