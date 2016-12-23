package io.indexr.segment.io;

import com.google.common.base.Preconditions;

import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.LongBuffer;
import java.nio.ShortBuffer;

import io.indexr.io.ByteSlice;
import io.indexr.segment.pack.NumType;

public class FixedByteWidthNumbers implements FixedWidthNumbers {
    private final ByteBuffer source;
    private NumberValue numValue;

    private final int width;
    private final long max;

    public FixedByteWidthNumbers(ByteSlice source, int width) {
        Preconditions.checkArgument(source != null);
        byte numberType = NumType.fromByteWidth(width);

        this.source = source.toByteBuffer();
        this.width = width;
        this.max = NumType.maxValue(numberType);

        if (numberType == NumType.NZero) {
            numValue = new ZeroValue();
        } else {
            switch (numberType) {
                case NumType.NByte:
                    numValue = new ByteValue();
                    break;
                case NumType.NShort:
                    numValue = new ShortValue();
                    break;
                case NumType.NInt:
                    numValue = new IntValue();
                    break;
                case NumType.NLong:
                    numValue = new LongValue();
                    break;
                default:
                    throw new IllegalArgumentException();
            }
        }
    }

    public static int calWidth(long max) {
        Preconditions.checkArgument(max >= 0);
        return NumType.bitWidth(NumType.fromMaxValue(max));
    }

    public static int calByteSize(int itemSize, int width) {
        Preconditions.checkArgument(itemSize >= 0 && width >= 0);
        return itemSize * width;
    }

    @Override
    public long max() {
        return max;
    }

    @Override
    public int width() {
        return width;
    }

    @Override
    public int gap() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long get(int index) {
        Preconditions.checkArgument(index >= 0);
        return numValue.get(index);
    }

    @Override
    public void set(int index, long value) {
        Preconditions.checkArgument(index >= 0);
        Preconditions.checkArgument(value >= 0 && value <= max);
        numValue.set(index, value);
    }

    private static interface NumberValue {

        long get(int index);

        void set(int index, long value);

    }

    private static class ZeroValue implements NumberValue {
        @Override
        public long get(int index) {
            return 0;
        }

        @Override
        public void set(int index, long value) {
        }
    }

    private class ByteValue implements NumberValue {

        @Override
        public long get(int index) {
            return source.get(index);
        }

        @Override
        public void set(int index, long value) {
            source.put(index, (byte) value);
        }
    }

    private class ShortValue implements NumberValue {
        private final ShortBuffer buffer;

        ShortValue() {
            buffer = FixedByteWidthNumbers.this.source.asShortBuffer();
        }

        @Override
        public long get(int index) {
            return buffer.get(index);
        }

        @Override
        public void set(int index, long value) {
            buffer.put(index, (short) value);
        }
    }

    private class IntValue implements NumberValue {
        private final IntBuffer buffer;

        IntValue() {
            buffer = FixedByteWidthNumbers.this.source.asIntBuffer();
        }

        @Override
        public long get(int index) {
            return buffer.get(index);
        }

        @Override
        public void set(int index, long value) {
            buffer.put(index, (int) value);
        }
    }

    private class LongValue implements NumberValue {
        private final LongBuffer buffer;

        LongValue() {
            buffer = FixedByteWidthNumbers.this.source.asLongBuffer();
        }

        @Override
        public long get(int index) {
            return buffer.get(index);
        }

        @Override
        public void set(int index, long value) {
            buffer.put(index, value);
        }
    }
}
