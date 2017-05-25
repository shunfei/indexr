package io.indexr.segment.index;

import org.apache.spark.unsafe.types.UTF8String;

import java.io.IOException;

import io.indexr.io.ByteBufferWriter;
import io.indexr.segment.Column;
import io.indexr.segment.OuterIndex;
import io.indexr.util.BitMap;

public class OuterIndex_Invalid implements OuterIndex {
    @Override
    public BitMap equal(Column column, long numValue, UTF8String strValue, boolean isNot) throws IOException {
        return BitMap.ALL;
    }

    @Override
    public BitMap in(Column column, long[] numValues, UTF8String[] strValues, boolean isNot) throws IOException {
        return BitMap.ALL;
    }

    @Override
    public BitMap greater(Column column, long numValue, UTF8String strValue, boolean acceptEqual, boolean isNot) throws IOException {
        return BitMap.ALL;
    }

    @Override
    public BitMap between(Column column, long numValue1, long numValue2, UTF8String strValue1, UTF8String strValue2, boolean isNot) throws IOException {
        return BitMap.ALL;
    }

    @Override
    public BitMap like(Column column, long numValue, UTF8String strValue, boolean isNot) throws IOException {
        return BitMap.ALL;
    }

    @Override
    public void close() throws IOException {}

    public static class Cache implements OuterIndex.Cache {
        @Override
        public long size() throws IOException {
            return 0;
        }

        @Override
        public int write(ByteBufferWriter writer) throws IOException {
            return 0;
        }

        @Override
        public void close() throws IOException {}
    }
}
