package io.indexr.query.row;


import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.array.ByteArrayMethods;
import org.apache.spark.unsafe.hash.Murmur3_x86_32;
import org.apache.spark.unsafe.types.UTF8String;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import io.indexr.query.types.DataType;

/**
 * An Unsafe implementation of Row which is backed by raw memory instead of Java objects.
 * 
 * Each tuple has three parts: [null bit set] [values] [variable length portion]
 * 
 * The bit set is used for null tracking and is aligned to 8-byte word boundaries.  It stores
 * one bit per field.
 * 
 * In the `values` region, we store one 8-byte word per field. For fields that hold fixed-length
 * primitive types, such as long, double, or int, we store the value directly in the word. For
 * fields with non-primitive or variable-length values, we store a relative offset (w.r.t. the
 * base address of the row) that points to the beginning of the variable-length field, and length
 * (they are combined into a long).
 * 
 * Instances of `UnsafeRow` act as pointers to row data stored in this format.
 * 
 * FIXME This class currently brokes aggregation logic, for string type value will not be compared.
 */
public final class UnsafeRow extends MutableRow implements Externalizable {

    //////////////////////////////////////////////////////////////////////////////
    // Static methods
    //////////////////////////////////////////////////////////////////////////////

    /**
     * Field types that can be updated in place in UnsafeRows (e.g. we support set() for these types)
     */
    public static final Set<DataType> mutableFieldTypes;

    // DecimalType is also mutable
    static {
        mutableFieldTypes = Collections.unmodifiableSet(
                new HashSet<>(
                        Arrays.asList(new DataType[]{
                                //NullType,
                                DataType.BooleanType,
                                //ByteType,
                                //ShortType,
                                DataType.IntegerType,
                                DataType.LongType,
                                DataType.FloatType,
                                DataType.DoubleType,
                                //DateType,
                                //TimestampType
                        })));
    }

    public static boolean isFixedLength(DataType dt) {
        return mutableFieldTypes.contains(dt);
    }

    public static boolean isMutable(DataType dt) {
        return mutableFieldTypes.contains(dt);
    }

    //////////////////////////////////////////////////////////////////////////////
    // Private fields and methods
    //////////////////////////////////////////////////////////////////////////////

    private Object baseObject;
    private long baseOffset;

    /** The number of fields in this row, used for calculating the bitset width (and in assertions) */
    private int numFields;

    /** The size of this row's backing data, in bytes) */
    private int sizeInBytes;

    // Ugly hack for string values.
    private ArrayList<UTF8String> stringValues;

    public boolean hasString() {
        return stringValues != null && !stringValues.isEmpty();
    }

    private long getFieldOffset(int ordinal) {
        return baseOffset + ordinal * 8L;
    }

    private void assertIndexIsValid(int index) {
        assert index >= 0 : "index (" + index + ") should >= 0";
        assert index < numFields : "index (" + index + ") should < " + numFields;
    }

    //////////////////////////////////////////////////////////////////////////////
    // Public methods
    //////////////////////////////////////////////////////////////////////////////

    /**
     * Construct a new UnsafeRow. The resulting row won't be usable until `pointTo()` has been called,
     * since the value returned by this constructor is equivalent to a null pointer.
     *
     * @param numFields the number of fields in this row
     */
    public UnsafeRow(int numFields) {
        this.numFields = numFields;
    }

    // for serializer
    public UnsafeRow() {
    }

    public Object getBaseObject() {
        return baseObject;
    }

    public long getBaseOffset() {
        return baseOffset;
    }

    public int getSizeInBytes() {
        return sizeInBytes;
    }

    @Override
    public int numFields() {
        return numFields;
    }

    /**
     * Update this UnsafeRow to point to different backing data.
     *
     * @param baseObject  the base object
     * @param baseOffset  the offset within the base object
     * @param sizeInBytes the size of this row's backing data, in bytes
     */
    public void pointTo(Object baseObject, long baseOffset, int sizeInBytes) {
        assert numFields >= 0 : "numFields (" + numFields + ") should >= 0";
        this.baseObject = baseObject;
        this.baseOffset = baseOffset;
        this.sizeInBytes = sizeInBytes;
    }

    /**
     * Update this UnsafeRow to point to the underlying byte array.
     *
     * @param buf         byte array to point to
     * @param sizeInBytes the number of bytes valid in the byte array
     */
    public void pointTo(byte[] buf, int sizeInBytes) {
        pointTo(buf, Platform.BYTE_ARRAY_OFFSET, sizeInBytes);
    }

    public void setTotalSize(int sizeInBytes) {
        this.sizeInBytes = sizeInBytes;
    }

    @Override
    public void setUniformVal(int ordinal, long value) {
        assertIndexIsValid(ordinal);
        Platform.putLong(baseObject, getFieldOffset(ordinal), value);
    }

    @Override
    public void setInt(int ordinal, int value) {
        assertIndexIsValid(ordinal);
        Platform.putInt(baseObject, getFieldOffset(ordinal), value);
    }

    @Override
    public void setLong(int ordinal, long value) {
        assertIndexIsValid(ordinal);
        Platform.putLong(baseObject, getFieldOffset(ordinal), value);
    }

    @Override
    public void setDouble(int ordinal, double value) {
        assertIndexIsValid(ordinal);
        if (Double.isNaN(value)) {
            value = Double.NaN;
        }
        Platform.putDouble(baseObject, getFieldOffset(ordinal), value);
    }

    @Override
    public void setBoolean(int ordinal, boolean value) {
        assertIndexIsValid(ordinal);
        Platform.putBoolean(baseObject, getFieldOffset(ordinal), value);
    }

    @Override
    public void setShort(int ordinal, short value) {
        assertIndexIsValid(ordinal);
        Platform.putShort(baseObject, getFieldOffset(ordinal), value);
    }

    @Override
    public void setByte(int ordinal, byte value) {
        assertIndexIsValid(ordinal);
        Platform.putByte(baseObject, getFieldOffset(ordinal), value);
    }

    @Override
    public void setFloat(int ordinal, float value) {
        assertIndexIsValid(ordinal);
        if (Float.isNaN(value)) {
            value = Float.NaN;
        }
        Platform.putDouble(baseObject, getFieldOffset(ordinal), value);
    }

    @Override
    public void setString(int ordinal, UTF8String value) {
        if (stringValues == null) {
            stringValues = new ArrayList<>();
        }
        int index = stringValues.size();
        stringValues.add(value);
        setLong(ordinal, index);
    }

    @Override
    public Object get(int ordinal, DataType dataType) {
        switch (dataType) {
            case BooleanType:
                return getBoolean(ordinal);
            case IntegerType:
                return getInt(ordinal);
            case LongType:
                return getLong(ordinal);
            case FloatType:
                return getFloat(ordinal);
            case DoubleType:
                return getDouble(ordinal);
            case StringType:
                return getString(ordinal);
            //return getStruct(ordinal, ((StructType) dataType).size());
            default:
                throw new UnsupportedOperationException("Unsupported data type " + dataType.typeName());
        }
    }

    @Override
    public boolean getBoolean(int ordinal) {
        assertIndexIsValid(ordinal);
        return Platform.getBoolean(baseObject, getFieldOffset(ordinal));
    }

    //@Override
    //public byte getByte(int ordinal) {
    //    assertIndexIsValid(ordinal);
    //    return Platform.getByte(baseObject, getFieldOffset(ordinal));
    //}
    //
    //@Override
    //public short getShort(int ordinal) {
    //    assertIndexIsValid(ordinal);
    //    return Platform.getShort(baseObject, getFieldOffset(ordinal));
    //}

    @Override
    public int getInt(int ordinal) {
        assertIndexIsValid(ordinal);
        return Platform.getInt(baseObject, getFieldOffset(ordinal));
    }

    @Override
    public long getLong(int ordinal) {
        assertIndexIsValid(ordinal);
        return Platform.getLong(baseObject, getFieldOffset(ordinal));
    }

    @Override
    public float getFloat(int ordinal) {
        assertIndexIsValid(ordinal);
        //return Platform.getFloat(baseObject, getFieldOffset(ordinal));
        return (float) Platform.getDouble(baseObject, getFieldOffset(ordinal));
    }

    @Override
    public double getDouble(int ordinal) {
        assertIndexIsValid(ordinal);
        return Platform.getDouble(baseObject, getFieldOffset(ordinal));
    }

    @Override
    public long getUniformVal(int ordinal) {
        assertIndexIsValid(ordinal);
        return Platform.getLong(baseObject, getFieldOffset(ordinal));
    }

    //@Override
    //public UTF8String getString(int ordinal) {
    //    final long offsetAndSize = getLong(ordinal);
    //    final int offset = (int) (offsetAndSize >> 32);
    //    final int size = (int) offsetAndSize;
    //    return UTF8String.fromAddress(baseObject, baseOffset + offset, size);
    //}

    @Override
    public UTF8String getString(int ordinal) {
        int index = (int) getLong(ordinal);
        return stringValues.get(index);
    }

    //@Override
    //public UnsafeRow getStruct(int ordinal, int numFields) {
    //    final long offsetAndSize = getLong(ordinal);
    //    final int offset = (int) (offsetAndSize >> 32);
    //    final int size = (int) offsetAndSize;
    //    final UnsafeRow row = new UnsafeRow(numFields);
    //    row.pointTo(baseObject, baseOffset + offset, size);
    //    return row;
    //}

    /**
     * Copies this row, returning a self-contained UnsafeRow that stores its data in an internal
     * byte array rather than referencing data stored in a data page.
     */
    @Override
    public UnsafeRow copy() {
        UnsafeRow rowCopy = new UnsafeRow(numFields);
        final byte[] rowDataCopy = new byte[sizeInBytes];
        Platform.copyMemory(
                baseObject,
                baseOffset,
                rowDataCopy,
                Platform.BYTE_ARRAY_OFFSET,
                sizeInBytes
        );
        rowCopy.pointTo(rowDataCopy, Platform.BYTE_ARRAY_OFFSET, sizeInBytes);
        if (stringValues != null) {
            rowCopy.stringValues = new ArrayList<>(stringValues);
        }
        return rowCopy;
    }

    /**
     * Creates an empty UnsafeRow from a byte array with specified numBytes and numFields.
     * The returned row is invalid until we call copyFrom on it.
     */
    public static UnsafeRow createFromByteArray(int numBytes, int numFields) {
        final UnsafeRow row = new UnsafeRow(numFields);
        row.pointTo(new byte[numBytes], numBytes);
        return row;
    }

    /**
     * Copies the input UnsafeRow to this UnsafeRow, and resize the underlying byte[] when the
     * input row is larger than this row.
     */
    public void copyFrom(UnsafeRow row) {
        // copyFrom is only available for UnsafeRow created from byte array.
        assert (baseObject instanceof byte[]) && baseOffset == Platform.BYTE_ARRAY_OFFSET;
        if (row.sizeInBytes > this.sizeInBytes) {
            // resize the underlying byte[] if it's not large enough.
            this.baseObject = new byte[row.sizeInBytes];
        }
        Platform.copyMemory(
                row.baseObject, row.baseOffset, this.baseObject, this.baseOffset, row.sizeInBytes);
        // update the sizeInBytes.
        this.sizeInBytes = row.sizeInBytes;
    }

    /**
     * Write this UnsafeRow's underlying bytes to the given OutputStream.
     *
     * @param out         the stream to write to.
     * @param writeBuffer a byte array for buffering chunks of off-heap data while writing to the
     *                    output stream. If this row is backed by an on-heap byte array, then this
     *                    buffer will not be used and may be null.
     */
    public void writeToStream(OutputStream out, byte[] writeBuffer) throws IOException {
        if (baseObject instanceof byte[]) {
            int offsetInByteArray = (int) (Platform.BYTE_ARRAY_OFFSET - baseOffset);
            out.write((byte[]) baseObject, offsetInByteArray, sizeInBytes);
        } else {
            int dataRemaining = sizeInBytes;
            long rowReadPosition = baseOffset;
            while (dataRemaining > 0) {
                int toTransfer = Math.min(writeBuffer.length, dataRemaining);
                Platform.copyMemory(
                        baseObject, rowReadPosition, writeBuffer, Platform.BYTE_ARRAY_OFFSET, toTransfer);
                out.write(writeBuffer, 0, toTransfer);
                rowReadPosition += toTransfer;
                dataRemaining -= toTransfer;
            }
        }
    }

    @Override
    public int hashCode() {
        return Murmur3_x86_32.hashUnsafeWords(baseObject, baseOffset, sizeInBytes, 42);
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof UnsafeRow) {
            UnsafeRow o = (UnsafeRow) other;
            return (sizeInBytes == o.sizeInBytes) &&
                    ByteArrayMethods.arrayEquals(baseObject, baseOffset, o.baseObject, o.baseOffset,
                            sizeInBytes);
        }
        return false;
    }

    /**
     * Returns the underlying bytes for this UnsafeRow.
     */
    public byte[] getBytes() {
        if (baseObject instanceof byte[] && baseOffset == Platform.BYTE_ARRAY_OFFSET
                && (((byte[]) baseObject).length == sizeInBytes)) {
            return (byte[]) baseObject;
        } else {
            byte[] bytes = new byte[sizeInBytes];
            Platform.copyMemory(baseObject, baseOffset, bytes, Platform.BYTE_ARRAY_OFFSET, sizeInBytes);
            return bytes;
        }
    }

    // This is for debugging
    @Override
    public String toString() {
        StringBuilder build = new StringBuilder("[");
        for (int i = 0; i < sizeInBytes; i += 8) {
            build.append(java.lang.Long.toHexString(Platform.getLong(baseObject, baseOffset + i)));
            build.append(',');
        }
        build.deleteCharAt(build.length() - 1);
        build.append(']');
        return build.toString();
    }

    /**
     * Writes the content of this row into a memory address, identified by an object and an offset.
     * The target memory address must already been allocated, and have enough space to hold all the
     * bytes in this string.
     */
    public void writeToMemory(Object target, long targetOffset) {
        Platform.copyMemory(baseObject, baseOffset, target, targetOffset, sizeInBytes);
    }

    public void writeTo(ByteBuffer buffer) {
        assert (buffer.hasArray());
        byte[] target = buffer.array();
        int offset = buffer.arrayOffset();
        int pos = buffer.position();
        writeToMemory(target, Platform.BYTE_ARRAY_OFFSET + offset + pos);
        buffer.position(pos + sizeInBytes);
    }

    /**
     * Write the bytes of var-length field into ByteBuffer
     * 
     * Note: only work with HeapByteBuffer
     */
    public void writeFieldTo(int ordinal, ByteBuffer buffer) {
        final long offsetAndSize = getLong(ordinal);
        final int offset = (int) (offsetAndSize >> 32);
        final int size = (int) offsetAndSize;

        buffer.putInt(size);
        int pos = buffer.position();
        buffer.position(pos + size);
        Platform.copyMemory(
                baseObject,
                baseOffset + offset,
                buffer.array(),
                Platform.BYTE_ARRAY_OFFSET + buffer.arrayOffset() + pos,
                size);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        byte[] bytes = getBytes();
        out.writeInt(bytes.length);
        out.writeInt(this.numFields);
        out.write(bytes);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.baseOffset = Platform.BYTE_ARRAY_OFFSET;
        this.sizeInBytes = in.readInt();
        this.numFields = in.readInt();
        this.baseObject = new byte[sizeInBytes];
        in.readFully((byte[]) baseObject);
    }

    // Temporary method, for now we only support fixed-length primitive type.
    public static UnsafeRow create8BytesFieldsRow(int numFields) {
        byte[] data = new byte[numFields * 8];
        UnsafeRow row = new UnsafeRow(numFields);
        row.pointTo(data, data.length);
        return row;
    }
}

