package io.indexr.query.row;

import org.apache.spark.unsafe.types.UTF8String;

import io.indexr.query.types.DataType;

public class JoinedRow extends InternalRow {
    private InternalRow row1, row2;
    private int numField1, numField2;

    public JoinedRow() {}

    public JoinedRow(InternalRow left, InternalRow right) {
        apply(left, right);
    }

    public JoinedRow apply(InternalRow left, InternalRow right) {
        this.row1 = left;
        this.row2 = right;
        this.numField1 = left.numFields();
        this.numField2 = right.numFields();
        return this;
    }

    public JoinedRow withLeft(InternalRow left) {
        apply(left, row2);
        return this;
    }

    public JoinedRow withRight(InternalRow right) {
        apply(row1, right);
        return this;
    }

    @Override
    public int numFields() {
        return numField1 + numField2;
    }

    @Override
    public InternalRow copy() {
        return new JoinedRow(row1.copy(), row2.copy());
    }

    // @formatter:off
    @Override public boolean getBoolean(int ordinal) {return ordinal< numField1? row1.getBoolean(ordinal) : row2.getBoolean(ordinal - numField1);}
    @Override public int getInt(int ordinal) {return ordinal< numField1? row1.getInt(ordinal) : row2.getInt(ordinal - numField1);}
    @Override public long getLong(int ordinal) {return ordinal< numField1? row1.getLong(ordinal) : row2.getLong(ordinal - numField1);}
    @Override public float getFloat(int ordinal) {return ordinal< numField1? row1.getFloat(ordinal) : row2.getFloat(ordinal - numField1);}
    @Override public double getDouble(int ordinal) {return ordinal< numField1? row1.getDouble(ordinal) : row2.getDouble(ordinal - numField1);}
    @Override public long getUniformVal(int ordinal) {return ordinal< numField1? row1.getUniformVal(ordinal) : row2.getUniformVal(ordinal - numField1);}
    @Override public UTF8String getString(int ordinal) {return ordinal< numField1? row1.getString(ordinal) : row2.getString(ordinal - numField1);}
    @Override public Object get(int ordinal, DataType dataType) {return ordinal< numField1? row1.get(ordinal, dataType) : row2.get(ordinal - numField1, dataType);}
    // @formatter:on
}
