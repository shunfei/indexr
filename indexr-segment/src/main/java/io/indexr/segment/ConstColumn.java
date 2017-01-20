package io.indexr.segment;

import org.apache.spark.unsafe.types.UTF8String;

import java.io.IOException;

import io.indexr.data.LikePattern;
import io.indexr.segment.pack.DataPack;
import io.indexr.segment.pack.DataPackNode;

public class ConstColumn implements Column {
    private String name;
    private byte dataType;
    private int packCount;
    private long rowCount;
    private long numValue;
    private UTF8String strValue;
    private DataPackNode dpn;
    private RSIndex index;

    public ConstColumn(int version, String name, byte dataType, long rowCount, long numValue, UTF8String strValue) {
        this.name = name;
        this.dataType = dataType;
        this.rowCount = rowCount;
        this.packCount = DataPack.rowCountToPackCount(rowCount);
        this.numValue = numValue;
        this.strValue = strValue;

        this.dpn = new DataPackNode(version);
        dpn.setMinValue(numValue);
        dpn.setMaxValue(numValue);

        if (ColumnType.isNumber(dataType)) {
            this.index = new RSIndexNum() {
                boolean isFloat = ColumnType.isFloatPoint(dataType);

                @Override
                public byte isValue(int packId, long minVal, long maxVal, long packMin, long packMax) {
                    return RSIndexNum.minMaxCheck(minVal, maxVal, packMin, packMax, isFloat);
                }
            };
        } else {
            this.index = new RSIndexStr() {
                @Override
                public byte isValue(int packId, UTF8String value) {
                    if (value == null ? strValue == null : value.equals(strValue)) {
                        return RSValue.All;
                    } else {
                        return RSValue.None;
                    }
                }

                @Override
                public byte isLike(int packId, LikePattern value) {
                    // TODO implementation.
                    return RSValue.Some;
                }
            };
        }
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public byte dataType() {
        return dataType;
    }

    @Override
    public int packCount() {
        return packCount;
    }

    @Override
    public long rowCount() throws IOException {
        return rowCount;
    }

    @Override
    public DataPackNode dpn(int packId) throws IOException {
        return dpn;
    }

    @Override
    public DPValues pack(int packId) throws IOException {
        return new DPValues() {
            @Override
            public int count() {
                return DataPack.packRowCount(rowCount, packId);
            }

            @Override
            public long uniformValAt(int index, byte type) {
                return numValue;
            }

            @Override
            public int intValueAt(int index) {
                return (int) numValue;
            }

            @Override
            public long longValueAt(int index) {
                return numValue;
            }

            @Override
            public float floatValueAt(int index) {
                return (float) Double.longBitsToDouble(numValue);
            }

            @Override
            public double doubleValueAt(int index) {
                return Double.longBitsToDouble(numValue);
            }

            @Override
            public UTF8String stringValueAt(int index) {
                return strValue;
            }
        };
    }

    @Override
    public <T extends RSIndex> T rsIndex() throws IOException {
        return (T) index;
    }
}
