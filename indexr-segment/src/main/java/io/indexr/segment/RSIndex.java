package io.indexr.segment;

import java.io.IOException;

import io.indexr.data.Freeable;
import io.indexr.data.Sizable;
import io.indexr.io.ByteBufferWriter;
import io.indexr.io.ByteSlice;

/**
 * Rough set index.
 */
public interface RSIndex extends Freeable, Sizable {

    default PackRSIndex packIndex(int packId) {
        throw new UnsupportedOperationException();
    }

    default int write(ByteBufferWriter writer) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    default void free() {}

    @Override
    default long size() {return 0;}

    public static interface Factory {

        PackRSIndex createPack(byte dataType);

        RSIndex create(byte dataType, ByteSlice buffer, int packCount);

        public static Factory of(Factory stringTypeFactory, Factory numTypeFactory) {
            return new Factory() {
                @Override
                public PackRSIndex createPack(byte dataType) {
                    if (dataType == ColumnType.STRING) {
                        return stringTypeFactory.createPack(dataType);
                    } else {
                        return numTypeFactory.createPack(dataType);
                    }
                }

                @Override
                public RSIndex create(byte dataType, ByteSlice buffer, int packCount) {
                    if (dataType == ColumnType.STRING) {
                        return stringTypeFactory.create(dataType, buffer, packCount);
                    } else {
                        return numTypeFactory.create(dataType, buffer, packCount);
                    }
                }
            };
        }
    }
}
