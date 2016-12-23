package io.indexr.segment.io;

import io.indexr.io.ByteSlice;

public interface FixedWidthNumbers {

    long max();

    int width();

    int gap();

    long get(int index);

    void set(int index, long value);


    public static FixedWidthNumbers wrap(ByteSlice source, int width) {
        return new FixedByteWidthNumbers(source, width);
    }

    public static int calWidth(long max) {
        return FixedByteWidthNumbers.calWidth(max);
    }

    public static int calByteSize(int itemSize, int width) {
        return FixedByteWidthNumbers.calByteSize(itemSize, width);
    }
}
