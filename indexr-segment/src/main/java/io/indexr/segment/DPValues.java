package io.indexr.segment;

import org.apache.spark.unsafe.types.UTF8String;

import io.indexr.data.BytePiece;
import io.indexr.data.BytePieceSetter;
import io.indexr.data.DoubleSetter;
import io.indexr.data.FloatSetter;
import io.indexr.data.IntSetter;
import io.indexr.data.LongSetter;

/**
 * A data pack.
 */
public interface DPValues {

    int valueCount();

    default long uniformValAt(int index, byte type) {
        switch (type) {
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

    default int intValueAt(int index) {
        throw new UnsupportedOperationException();
    }

    default long longValueAt(int index) {
        throw new UnsupportedOperationException();
    }

    default float floatValueAt(int index) {
        throw new UnsupportedOperationException();
    }

    default double doubleValueAt(int index) {
        throw new UnsupportedOperationException();
    }

    /**
     * The user should <b>NOT</b> cache the returned {@link UTF8String} instance,
     * normally it just a pointer pointed to some memory which could be freed or changed at any time.
     * The right way to hold those value is using {@link UTF8String#clone()} to create a new one.
     */
    default UTF8String stringValueAt(int index) {
        throw new UnsupportedOperationException();
    }

    /**
     * The memory <code>bytes</code> pointed to should considered being freed or changed at any time.
     */
    default void rawValueAt(int index, BytePiece bytes) {
        UTF8String string = stringValueAt(index);
        bytes.base = string.getBaseObject();
        bytes.addr = string.getBaseOffset();
        bytes.len = string.numBytes();
    }

    default byte[] rawValueAt(int index) {
        return stringValueAt(index).getBytes();
    }

    default void foreach(int start, int count, IntSetter setter) {
        int end = start + count;
        for (int index = start; index < end; index++) {
            setter.set(index, intValueAt(index));
        }
    }

    default void foreach(int start, int count, LongSetter setter) {
        int end = start + count;
        for (int index = start; index < end; index++) {
            setter.set(index, longValueAt(index));
        }
    }

    default void foreach(int start, int count, FloatSetter setter) {
        int end = start + count;
        for (int index = start; index < end; index++) {
            setter.set(index, floatValueAt(index));
        }
    }

    default void foreach(int start, int count, DoubleSetter setter) {
        int end = start + count;
        for (int index = start; index < end; index++) {
            setter.set(index, doubleValueAt(index));
        }
    }

    default void foreach(int start, int count, BytePieceSetter setter) {
        int end = start + count;
        BytePiece bytes = new BytePiece();
        for (int index = start; index < end; index++) {
            rawValueAt(index, bytes);
            setter.set(index, bytes);
        }
    }
}
