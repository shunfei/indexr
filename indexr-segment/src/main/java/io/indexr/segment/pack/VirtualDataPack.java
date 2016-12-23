package io.indexr.segment.pack;

import org.apache.spark.unsafe.types.UTF8String;

import java.util.Arrays;

import io.indexr.segment.ColumnType;
import io.indexr.segment.DPValues;
import io.indexr.segment.PackRSIndex;
import io.indexr.segment.PackRSIndexStr;
import io.indexr.util.Pair;

class VirtualDataPack implements DPValues {
    private final byte dataType;

    private Object values;
    private int size;

    VirtualDataPack(byte dataType, Object values, int size) {
        this.dataType = dataType;
        this.values = values;
        this.size = size;
    }

    VirtualDataPack(byte dataType, DataPack pack) {
        this.dataType = dataType;
        this.size = pack == null ? 0 : pack.objCount();
        this.values = allocateValues(dataType, DataPack.MAX_COUNT);
        if (pack != null) {
            switch (dataType) {
                case ColumnType.INT: {
                    int[] buffer = (int[]) values;
                    if (size != 0) {
                        for (int i = 0; i < size; i++) {
                            buffer[i] = pack.intValueAt(i);
                        }
                    }
                }
                break;
                case ColumnType.LONG: {
                    long[] buffer = (long[]) values;
                    if (size != 0) {
                        for (int i = 0; i < size; i++) {
                            buffer[i] = pack.longValueAt(i);
                        }
                    }
                }
                break;
                case ColumnType.FLOAT: {
                    float[] buffer = (float[]) values;
                    if (size != 0) {
                        for (int i = 0; i < size; i++) {
                            buffer[i] = pack.floatValueAt(i);
                        }
                    }
                }
                break;
                case ColumnType.DOUBLE: {
                    double[] buffer = (double[]) values;
                    if (size != 0) {
                        for (int i = 0; i < size; i++) {
                            buffer[i] = pack.doubleValueAt(i);
                        }
                    }
                }
                break;
                case ColumnType.STRING: {
                    UTF8String[] buffer = (UTF8String[]) values;
                    if (size != 0) {
                        for (int i = 0; i < size; i++) {
                            buffer[i] = pack.stringValueAt(i).clone();
                        }
                    }
                }
                break;
                default:
                    throw new IllegalArgumentException(String.format("Not support data type of %s", dataType));
            }
        }
    }

    private static Object allocateValues(byte dataType, int cap) {
        switch (dataType) {
            case ColumnType.INT:
                return new int[cap];
            case ColumnType.LONG:
                return new long[cap];
            case ColumnType.FLOAT:
                return new float[cap];
            case ColumnType.DOUBLE:
                return new double[cap];
            case ColumnType.STRING:
                return new UTF8String[cap];
            default:
                throw new IllegalArgumentException(String.format("Not support data type of %s", dataType));
        }
    }

    Pair<DataPack, DataPackNode> asPack(int version, PackRSIndex index) {
        switch (dataType) {
            case ColumnType.INT:
                return DataPack_N.from(version, (int[]) values, 0, size, (RSIndex_Histogram.HistPackIndex) index);
            case ColumnType.LONG:
                return DataPack_N.from(version, (long[]) values, 0, size, (RSIndex_Histogram.HistPackIndex) index);
            case ColumnType.FLOAT:
                return DataPack_N.from(version, (float[]) values, 0, size, (RSIndex_Histogram.HistPackIndex) index);
            case ColumnType.DOUBLE:
                return DataPack_N.from(version, (double[]) values, 0, size, (RSIndex_Histogram.HistPackIndex) index);
            case ColumnType.STRING:
                return DataPack_R.from(version, Arrays.asList((UTF8String[]) values).subList(0, size), (PackRSIndexStr) index);
            default:
                throw new IllegalArgumentException(String.format("Not support data type of %s", dataType));
        }
    }

    VirtualDataPack duplicate() {
        int curSize = size;
        Object tmpVals = allocateValues(dataType, curSize);
        System.arraycopy(values, 0, tmpVals, 0, curSize);
        return new VirtualDataPack(dataType, tmpVals, curSize);
    }

    void clear() {
        size = 0;
    }

    boolean isFull() {
        return size >= DataPack.MAX_COUNT;
    }

    public boolean add(int val) {
        ((int[]) values)[size++] = val;
        return isFull();
    }

    public boolean add(long val) {
        ((long[]) values)[size++] = val;
        return isFull();
    }

    public boolean add(float val) {
        ((float[]) values)[size++] = val;
        return isFull();
    }

    public boolean add(double val) {
        ((double[]) values)[size++] = val;
        return isFull();
    }

    public boolean add(UTF8String val) {
        ((UTF8String[]) values)[size++] = val;
        return isFull();
    }

    @Override
    public int count() {
        return size;
    }

    @Override
    public long uniformValAt(int index, byte type) {
        assert type == dataType;

        switch (dataType) {
            case ColumnType.INT:
                return intValueAt(index);
            case ColumnType.LONG:
                return longValueAt(index);
            case ColumnType.FLOAT:
                return Double.doubleToRawLongBits((double) floatValueAt(index));
            case ColumnType.DOUBLE:
                return Double.doubleToRawLongBits(doubleValueAt(index));
            default:
                throw new UnsupportedOperationException();
        }
    }

    @Override
    public int intValueAt(int index) {
        assert dataType == ColumnType.INT;
        return ((int[]) values)[(int) index];
    }

    @Override
    public long longValueAt(int index) {
        assert dataType == ColumnType.LONG;
        return ((long[]) values)[(int) index];
    }

    @Override
    public float floatValueAt(int index) {
        assert dataType == ColumnType.FLOAT;
        return ((float[]) values)[(int) index];
    }

    @Override
    public double doubleValueAt(int index) {
        assert dataType == ColumnType.DOUBLE;
        return ((double[]) values)[(int) index];
    }

    @Override
    public UTF8String stringValueAt(int index) {
        assert dataType == ColumnType.STRING;
        return ((UTF8String[]) values)[(int) index];
    }

    public void free() {
        values = null;
        size = 0;
    }
}
