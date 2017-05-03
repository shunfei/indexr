package io.indexr.segment.storage.itg;

import java.nio.ByteBuffer;

public class ColumnNodeMeta {
    public final long minNumValue;
    public final long maxNumValue;

    public ColumnNodeMeta(long minNumValue, long maxNumValue) {
        this.minNumValue = minNumValue;
        this.maxNumValue = maxNumValue;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ColumnNodeMeta that = (ColumnNodeMeta) o;
        return minNumValue == that.minNumValue && maxNumValue == that.maxNumValue;
    }

    int infoSize() {
        return 8 + 8;
    }

    static void write(ColumnNodeMeta cni, ByteBuffer buffer) {
        buffer.putLong(cni.minNumValue);
        buffer.putLong(cni.maxNumValue);
    }

    static ColumnNodeMeta read(ByteBuffer buffer) {
        return new ColumnNodeMeta(buffer.getLong(), buffer.getLong());
    }
}
