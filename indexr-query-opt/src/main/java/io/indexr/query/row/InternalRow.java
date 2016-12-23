package io.indexr.query.row;

/**
 * An abstract class for row used internal in Spark SQL, which only contain the columns as
 * internal types.
 */
public abstract class InternalRow implements SpecializedGetters {

    public abstract int numFields();

    // This is only use for test and will throw a null pointer exception if the position is null.
    //public STRING getString(int ordinal) {
    //    return getUTF8String(ordinal).toString();
    //}

    /**
     * Make a copy of the current [[InternalRow]] object.
     */
    public abstract InternalRow copy();

    public static InternalRow EmptyRow = null;

    public static InternalRow SingleRow = new GenericMutableRow(0);
}

