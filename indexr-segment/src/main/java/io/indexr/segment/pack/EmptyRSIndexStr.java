package io.indexr.segment.pack;

import org.apache.spark.unsafe.types.UTF8String;

import java.io.IOException;

import io.indexr.data.LikePattern;
import io.indexr.io.ByteBufferWriter;
import io.indexr.segment.PackRSIndex;
import io.indexr.segment.PackRSIndexStr;
import io.indexr.segment.RSIndexStr;
import io.indexr.segment.RSValue;

public class EmptyRSIndexStr implements RSIndexStr {
    @Override
    public byte isValue(int packId, UTF8String value) {
        return RSValue.Some;
    }

    @Override
    public byte isLike(int packId, LikePattern value) {
        return RSValue.Some;
    }

    @Override
    public PackRSIndex packIndex(int packId) {
        return new EmptyPackIndex();
    }

    @Override
    public void write(ByteBufferWriter writer) throws IOException {}

    @Override
    public void free() {}

    @Override
    public long size() {
        return 0;
    }

    public static class EmptyPackIndex implements PackRSIndexStr {
        @Override
        public byte isValue(UTF8String value) {
            return RSValue.Some;
        }

        @Override
        public byte isLike(LikePattern pattern) {
            return RSValue.Some;
        }

        @Override
        public void putValue(UTF8String value) {}

        @Override
        public int serializedSize() {
            return 0;
        }

        @Override
        public void write(ByteBufferWriter writer) throws IOException {}

        @Override
        public void clear() {}

        @Override
        public void free() {}
    }
}
