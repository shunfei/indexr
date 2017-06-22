package io.indexr.segment.helper;

import com.google.common.base.Preconditions;

import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.types.UTF8String;

import java.nio.ByteBuffer;
import java.util.List;

import io.indexr.data.BytePiece;
import io.indexr.segment.ColumnSchema;
import io.indexr.segment.ColumnType;
import io.indexr.segment.Row;
import io.indexr.segment.SQLType;
import io.indexr.util.DateTimeUtil;
import io.indexr.util.Strings;
import io.indexr.util.Trick;
import io.indexr.util.UTF8Util;

import static org.apache.spark.unsafe.Platform.BYTE_ARRAY_OFFSET;

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
        private final SQLType[] columnTypes;

        private ByteBuffer buffer;
        private int[] curSums;
        private int curColIndex;

        public static Builder createByColumnSchemas(List<ColumnSchema> columnSchemas) {
            return new Builder(Trick.mapToList(columnSchemas, ColumnSchema::getSqlType));
        }

        public Builder(List<SQLType> columnTypes) {
            this.columnTypes = columnTypes.toArray(new SQLType[columnTypes.size()]);

            int bufferSize = 0;
            for (SQLType type : columnTypes) {
                switch (type) {
                    case INT:
                    case TIME:
                        bufferSize += 4;
                        break;
                    case BIGINT:
                    case DATE:
                    case DATETIME:
                        bufferSize += 8;
                        break;
                    case FLOAT:
                        bufferSize += 4;
                        break;
                    case DOUBLE:
                        bufferSize += 8;
                        break;
                    case VARCHAR:
                        bufferSize += ColumnType.MAX_STRING_UTF8_SIZE;
                        break;
                    default:
                        throw new IllegalArgumentException("Unsupported data type: " + type);
                }
            }

            buffer = ByteBuffer.allocate(bufferSize);
            curSums = new int[columnTypes.size()];
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
            assert columnTypes[curColIndex] == SQLType.INT
                    || columnTypes[curColIndex] == SQLType.TIME;

            buffer.putInt(val);
            curSums[curColIndex] = buffer.position();
            curColIndex++;
        }

        public void appendLong(long val) {
            assert columnTypes[curColIndex] == SQLType.BIGINT
                    || columnTypes[curColIndex] == SQLType.DATE
                    || columnTypes[curColIndex] == SQLType.DATETIME;

            buffer.putLong(val);
            curSums[curColIndex] = buffer.position();
            curColIndex++;
        }

        public void appendFloat(float val) {
            assert columnTypes[curColIndex] == SQLType.FLOAT;

            buffer.putFloat(val);
            curSums[curColIndex] = buffer.position();
            curColIndex++;
        }

        public void appendDouble(double val) {
            assert columnTypes[curColIndex] == SQLType.DOUBLE;

            buffer.putDouble(val);
            curSums[curColIndex] = buffer.position();
            curColIndex++;
        }

        public void appendString(CharSequence val) {
            assert columnTypes[curColIndex] == SQLType.VARCHAR;

            byte[] bytes = UTF8Util.toUtf8(val);
            appendUTF8String(bytes);
        }

        public void appendUTF8String(byte[] bytes) {
            appendUTF8String(bytes, 0, bytes.length);
        }

        public void appendUTF8String(byte[] bytes, int offset, int len) {
            assert columnTypes[curColIndex] == SQLType.VARCHAR;

            buffer.put(bytes, offset, len);
            curSums[curColIndex] = buffer.position();
            curColIndex++;
        }

        public void appendUTF8String(UTF8String val) {
            assert columnTypes[curColIndex] == SQLType.VARCHAR && val.numBytes() <= buffer.remaining();

            Platform.copyMemory(
                    val.getBaseObject(), val.getBaseOffset(),
                    buffer.array(), Platform.BYTE_ARRAY_OFFSET + buffer.arrayOffset() + buffer.position(), val.numBytes());
            buffer.position(buffer.position() + val.numBytes());
            curSums[curColIndex] = buffer.position();
            curColIndex++;
        }

        public void appendStringFormVal(String val) {
            SQLType type = columnTypes[curColIndex];
            switch (type) {
                case INT:
                    appendInt(Strings.isEmpty(val) ? 0 : Integer.parseInt(val));
                    break;
                case BIGINT:
                    appendLong(Strings.isEmpty(val) ? 0 : Long.parseLong(val));
                    break;
                case FLOAT:
                    appendFloat(Strings.isEmpty(val) ? 0 : Float.parseFloat(val));
                    break;
                case DOUBLE:
                    appendDouble(Strings.isEmpty(val) ? 0 : Double.parseDouble(val));
                    break;
                case VARCHAR:
                    appendString(Strings.isEmpty(val) ? "" : val);
                    break;
                case DATE:
                    appendLong(Strings.isEmpty(val) ? 0 : DateTimeUtil.parseDate(UTF8Util.toUtf8(val)));
                    break;
                case TIME:
                    appendInt(Strings.isEmpty(val) ? 0 : DateTimeUtil.parseTime(UTF8Util.toUtf8(val)));
                    break;
                case DATETIME:
                    appendLong(Strings.isEmpty(val) ? 0 : DateTimeUtil.parseDateTime(UTF8Util.toUtf8(val)));
                    break;
                default:
                    throw new IllegalStateException();
            }
        }

        public void appendStringFormVals(List<String> vals) {
            for (String val : vals) {
                if (curColIndex >= columnTypes.length) {
                    return;
                }
                appendStringFormVal(val);
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

    @Override
    public void getRaw(int colId, BytePiece bytes) {
        int offset = colId == 0 ? 0 : sums[colId - 1];
        int to = sums[colId];
        bytes.base = data.array();
        bytes.addr = BYTE_ARRAY_OFFSET + offset;
        bytes.len = to - offset;
    }

    @Override
    public byte[] getRaw(int colId) {
        int offset = colId == 0 ? 0 : sums[colId - 1];
        int to = sums[colId];
        int len = to - offset;
        byte[] bytes = new byte[len];
        Platform.copyMemory(data.array(), BYTE_ARRAY_OFFSET + offset, bytes, BYTE_ARRAY_OFFSET, len);
        return bytes;
    }
}
