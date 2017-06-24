package io.indexr.segment;

import org.apache.spark.unsafe.types.UTF8String;

import java.io.IOException;

import io.indexr.data.Freeable;
import io.indexr.io.ByteBufferWriter;
import io.indexr.util.BitMap;

/**
 * Extend index for one data pack.
 */
public interface PackExtIndex extends Freeable {
    default int serializedSize() {return 0;}

    default int write(ByteBufferWriter writer) throws IOException {return 0;}

    @Override
    default void free() {}

    BitMap equal(Column column, int packId, long numValue, UTF8String strValue) throws IOException;

    BitMap in(Column column, int packId, long[] numValues, UTF8String[] strValues) throws IOException;

    BitMap greater(Column column, int packId, long numValue, UTF8String strValue, boolean acceptEqual) throws IOException;

    BitMap between(Column column, int packId, long numValue1, long numValue2, UTF8String strValue1, UTF8String strValue2) throws IOException;

    BitMap like(Column column, int packId, long numValue, UTF8String strValue) throws IOException;

    default BitMap fixBitmapInPack(BitMap bitMap, int rowCount) {
        if (bitMap.isEmpty()) {
            bitMap.free();
            return BitMap.NONE;
        } else {
            return bitMap;
        }
    }
}
