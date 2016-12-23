package io.indexr.query.row;


import org.apache.spark.unsafe.types.UTF8String;

import io.indexr.query.types.DataType;

public interface SpecializedGetters {

    boolean getBoolean(int ordinal);

    //byte getByte(int ordinal);

    //short getShort(int ordinal);

    int getInt(int ordinal);

    long getLong(int ordinal);

    float getFloat(int ordinal);

    double getDouble(int ordinal);

    long getUniformVal(int ordinal);

    UTF8String getString(int ordinal);

    //Decimal getDecimal(int ordinal, int precision, int scale);

    //byte[] getBinary(int ordinal);

    //CalendarInterval getInterval(int ordinal);

    //InternalRow getStruct(int ordinal, int numFields);

    //ArrayData getArray(int ordinal);

    //MapData getMap(int ordinal);

    Object get(int ordinal, DataType dataType);
}
