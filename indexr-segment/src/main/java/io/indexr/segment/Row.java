package io.indexr.segment;

import org.apache.spark.unsafe.types.UTF8String;

import io.indexr.data.BytePiece;

public interface Row {

    default String getDisplayString(int colId, int type) {
        switch (type) {
            case ColumnType.INT:
                return String.valueOf(getInt(colId));
            case ColumnType.LONG:
                return String.valueOf(getLong(colId));
            case ColumnType.FLOAT:
                return String.valueOf(getFloat(colId));
            case ColumnType.DOUBLE:
                return String.valueOf(getDouble(colId));
            case ColumnType.STRING:
                return getString(colId).toString();
            default:
                throw new IllegalStateException("illegal type: " + type);
        }
    }

    default long getUniformValue(int colId, byte type) {
        switch (type) {
            case ColumnType.INT:
                return getInt(colId);
            case ColumnType.LONG:
                return getLong(colId);
            case ColumnType.FLOAT:
                return Double.doubleToRawLongBits(getFloat(colId));
            case ColumnType.DOUBLE:
                return Double.doubleToRawLongBits(getDouble(colId));
            default:
                throw new IllegalStateException("illegal type: " + type);
        }
    }

    default int getInt(int colId) {
        throw new UnsupportedOperationException();
    }

    default long getLong(int colId) {
        throw new UnsupportedOperationException();
    }

    default float getFloat(int colId) {
        throw new UnsupportedOperationException();
    }

    default double getDouble(int colId) {
        throw new UnsupportedOperationException();
    }

    /**
     * The user should <b>NOT</b> cache the returned {@link UTF8String} instance,
     * as it just a pointer pointed to some memory which could be freed or changed at any time.
     * The right way to hold those value is using {@link UTF8String#clone()} to create a new one.
     */
    default UTF8String getString(int colId) {
        throw new UnsupportedOperationException();
    }

    /**
     * The memory <code>bytes</code> pointed to should considered being freed or changed at any time.
     */
    default void getRaw(int colId, BytePiece bytes) {
        throw new UnsupportedOperationException();
    }

    default public byte[] getRaw(int colId) {
        throw new UnsupportedOperationException();
    }
}
