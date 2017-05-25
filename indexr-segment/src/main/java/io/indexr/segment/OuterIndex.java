package io.indexr.segment;

import org.apache.spark.unsafe.types.UTF8String;

import java.io.Closeable;
import java.io.IOException;

import io.indexr.io.ByteBufferWriter;
import io.indexr.util.BitMap;

/**
 * Pack row index.
 */
public interface OuterIndex extends Closeable {

    BitMap equal(Column column, long numValue, UTF8String strValue, boolean isNot) throws IOException;

    BitMap in(Column column, long[] numValues, UTF8String[] strValues, boolean isNot) throws IOException;

    BitMap greater(Column column, long numValue, UTF8String strValue, boolean acceptEqual, boolean isNot) throws IOException;

    BitMap between(Column column, long numValue1, long numValue2, UTF8String strValue1, UTF8String strValue2, boolean isNot) throws IOException;

    BitMap like(Column column, long numValue, UTF8String strValue, boolean isNot) throws IOException;

    public static interface Cache extends Closeable {

        long size() throws IOException;

        int write(ByteBufferWriter writer) throws IOException;

    }
}
