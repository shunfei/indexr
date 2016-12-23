package io.indexr.query.row;

import org.apache.spark.unsafe.types.UTF8String;

public abstract class MutableRow extends InternalRow {

    public abstract void setUniformVal(int ordinal, long value);

    public abstract void setBoolean(int ordinal, boolean value);

    public abstract void setByte(int ordinal, byte value);

    public abstract void setShort(int ordinal, short value);

    public abstract void setInt(int ordinal, int value);

    public abstract void setLong(int ordinal, long value);

    public abstract void setFloat(int ordinal, float value);

    public abstract void setDouble(int ordinal, double value);

    public void setString(int ordinal, UTF8String value) {
        throw new UnsupportedOperationException();
    }
}
