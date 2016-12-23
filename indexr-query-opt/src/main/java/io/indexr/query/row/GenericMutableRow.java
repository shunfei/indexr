package io.indexr.query.row;

import org.apache.spark.unsafe.types.UTF8String;

import java.util.ArrayList;
import java.util.List;

import io.indexr.query.types.DataType;
import io.indexr.query.types.TypeConverters;

public class GenericMutableRow extends MutableRow {
    private long[] values;
    private List<UTF8String> stringValues;

    public GenericMutableRow(long[] values, List<UTF8String> stringValues) {
        this.values = values;
        this.stringValues = stringValues;
    }

    public GenericMutableRow(int numFields) {
        this.values = new long[numFields];
    }

    @Override
    public void setUniformVal(int ordinal, long value) {
        values[ordinal] = value;
    }

    @Override
    public long getUniformVal(int ordinal) {
        return values[ordinal];
    }

    @Override
    public int numFields() {
        return values.length;
    }

    @Override
    public GenericMutableRow copy() {
        return new GenericMutableRow(values.clone(),
                stringValues != null ? new ArrayList<>(stringValues) : null);
    }

    @Override
    public Object get(int ordinal, DataType dataType) {
        if (dataType == DataType.StringType) {
            return stringValues.get(ordinal);
        } else {
            return TypeConverters.cast(values[ordinal], dataType);
        }
    }

    // @formatter:off
    @Override public void setBoolean(int ordinal, boolean value) {values[ordinal] = value ? 1 : 0;}
    @Override public void setByte(int ordinal, byte value) {values[ordinal] = value;}
    @Override public void setShort(int ordinal, short value) {values[ordinal] = value;}
    @Override public void setInt(int ordinal, int value) {values[ordinal] = value;}
    @Override public void setLong(int ordinal, long value) {values[ordinal]= value;}
    @Override public void setFloat(int ordinal, float value) {values[ordinal] = Double.doubleToRawLongBits(value);}
    @Override public void setDouble(int ordinal, double value) {values[ordinal] = Double.doubleToRawLongBits(value);}
    @Override public void setString(int ordinal, UTF8String value) {
        if(stringValues == null){
            stringValues = new ArrayList<>();
        }
        int index = stringValues.size();
        stringValues.add(value);
        setLong(ordinal, index);
    }

    @Override public boolean getBoolean(int ordinal) {return values[ordinal] != 0;}
    @Override public int getInt(int ordinal) {return (int)values[ordinal];}
    @Override public long getLong(int ordinal) {return values[ordinal];}
    @Override public float getFloat(int ordinal) {return (float)Double.longBitsToDouble(values[ordinal]);}
    @Override public double getDouble(int ordinal) {return Double.longBitsToDouble(values[ordinal]);}
    @Override public UTF8String getString(int ordinal) {
        int index = (int)getLong(ordinal);
        return stringValues.get(index);
    }

    // @formatter:on
}
