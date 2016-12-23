package io.indexr.segment.helper;

import com.google.common.base.Preconditions;

import org.apache.spark.unsafe.types.UTF8String;

import java.nio.ByteBuffer;
import java.util.List;

import io.indexr.segment.ColumnSchema;
import io.indexr.segment.ColumnType;
import io.indexr.segment.Row;
import io.indexr.util.Trick;
import io.indexr.util.UTF8Util;

/**
 * A simple row can only store simple values like int, double, string. But cannot store multi-value column.
 */
public class SimpleRow implements Row {
    private ByteBuffer data;
    private int[] sums;

    public SimpleRow(ByteBuffer data, int[] sums) {
        this.data = data;
        this.sums = sums;
    }

    public static class Builder {
        private final byte[] columnTypes;

        private ByteBuffer buffer;
        private int[] curSums;
        private int curColIndex;

        public Builder(List<Byte> columnTypes) {
            this(toByteArr(columnTypes));
        }

        public static Builder createByColumnSchemas(List<ColumnSchema> columnSchemas) {
            return new Builder(Trick.mapToList(columnSchemas, cs -> cs.dataType));
        }

        private static byte[] toByteArr(List<Byte> list) {
            byte[] arr = new byte[list.size()];
            for (int i = 0; i < arr.length; i++) {
                arr[i] = list.get(i);
            }
            return arr;
        }

        public Builder(byte[] columnTypes) {
            this.columnTypes = columnTypes;

            int bufferSize = 0;
            for (byte type : columnTypes) {
                switch (type) {
                    case ColumnType.INT:
                        bufferSize += 4;
                        break;
                    case ColumnType.LONG:
                        bufferSize += 8;
                        break;
                    case ColumnType.FLOAT:
                        bufferSize += 4;
                        break;
                    case ColumnType.DOUBLE:
                        bufferSize += 8;
                        break;
                    case ColumnType.STRING:
                        bufferSize += ColumnType.MAX_STRING_UTF8_SIZE;
                        break;
                    default:
                        throw new IllegalArgumentException("Unsupported data type: " + type);
                }
            }

            buffer = ByteBuffer.allocate(bufferSize);
            curSums = new int[columnTypes.length];
            curColIndex = 0;
        }

        public SimpleRow buildAndReset() {
            Preconditions.checkState(curColIndex == curSums.length,
                    String.format("We have [%s] columns, by now inserted [%s]", curSums.length, curColIndex));

            int rowSize = curSums[curSums.length - 1];
            ByteBuffer data = ByteBuffer.allocate(rowSize);
            data.put(buffer.array(), 0, rowSize);
            SimpleRow r = new SimpleRow(data, curSums);

            buffer.clear();
            curSums = new int[columnTypes.length];
            curColIndex = 0;

            return r;
        }

        public void appendInt(int val) {
            Preconditions.checkState(columnTypes[curColIndex] == ColumnType.INT);

            buffer.putInt(val);
            curSums[curColIndex] = buffer.position();
            curColIndex++;
        }

        public void appendLong(long val) {
            Preconditions.checkState(columnTypes[curColIndex] == ColumnType.LONG);

            buffer.putLong(val);
            curSums[curColIndex] = buffer.position();
            curColIndex++;
        }

        public void appendFloat(float val) {
            Preconditions.checkState(columnTypes[curColIndex] == ColumnType.FLOAT);

            buffer.putFloat(val);
            curSums[curColIndex] = buffer.position();
            curColIndex++;
        }

        public void appendDouble(double val) {
            Preconditions.checkState(columnTypes[curColIndex] == ColumnType.DOUBLE);

            buffer.putDouble(val);
            curSums[curColIndex] = buffer.position();
            curColIndex++;
        }

        public void appendString(CharSequence val) {
            Preconditions.checkState(columnTypes[curColIndex] == ColumnType.STRING);

            byte[] bytes = UTF8Util.toUtf8(val);
            buffer.put(bytes);
            curSums[curColIndex] = buffer.position();
            curColIndex++;
        }

        public void appendRawVals(List<String> rawVals) {
            for (String rawVal : rawVals) {
                if (curColIndex >= columnTypes.length) {
                    return;
                }
                byte type = columnTypes[curColIndex];
                switch (type) {
                    case ColumnType.INT:
                        appendInt(Integer.parseInt(rawVal));
                        break;
                    case ColumnType.LONG:
                        appendLong(Long.parseLong(rawVal));
                        break;
                    case ColumnType.FLOAT:
                        appendFloat(Float.parseFloat(rawVal));
                        break;
                    case ColumnType.DOUBLE:
                        appendDouble(Double.parseDouble(rawVal));
                        break;
                    case ColumnType.STRING:
                        appendString(rawVal);
                        break;
                    default:
                        throw new IllegalStateException();
                }
            }
        }
    }

    private int offset(int colId) {
        return colId == 0 ? 0 : sums[colId - 1];
    }

    @Override
    public int getInt(int colId) {
        return data.getInt(offset(colId));
    }

    @Override
    public long getLong(int colId) {
        return data.getLong(offset(colId));
    }

    @Override
    public float getFloat(int colId) {
        return data.getFloat(offset(colId));
    }

    @Override
    public double getDouble(int colId) {
        return data.getDouble(offset(colId));
    }

    @Override
    public UTF8String getString(int colId) {
        int offset = colId == 0 ? 0 : sums[colId - 1];
        int to = sums[colId];
        return UTF8String.fromBytes(data.array(), offset, to - offset);
    }
}
