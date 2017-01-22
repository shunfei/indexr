package io.indexr.segment.rt;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import org.apache.directory.api.util.Strings;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.array.ByteArrayMethods;
import org.apache.spark.unsafe.types.UTF8String;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.indexr.data.BytePiece;
import io.indexr.segment.ColumnSchema;
import io.indexr.segment.ColumnType;
import io.indexr.segment.Row;
import io.indexr.util.ByteArrayWrapper;
import io.indexr.util.MemoryUtil;
import io.indexr.util.Serializable;
import io.indexr.util.Trick;
import io.indexr.util.UTF8JsonDeserializer;
import io.indexr.util.UTF8Util;

import static org.apache.spark.unsafe.Platform.BYTE_ARRAY_OFFSET;
import static org.apache.spark.unsafe.Platform.LONG_ARRAY_OFFSET;

/**
 * A row with strings stored in UTF-8 format.
 *
 * This class is <b>NOT</b> multi-thread safe.
 *
 * Data structure:
 * If grouping:
 * <pre>
 *      |........................|.................|.....................|
 *         dim values               dim raw values        metric values
 *                                                       (metric values are all numbers)
 * </pre>
 *
 * no grouping:
 * <pre>
 *      |........................|.................|
 *           values                   raw values
 * </pre>
 *
 * The value, if number type, represents the uniform value;
 * if string type, higher 32 bits represents the offset of raw value, lower 32 bits represents the len.
 *
 * The rows is sorted by dims if exists. And if grouping is true, those rows with the same dims will be
 * merged into one.
 *
 * Node: An UTF8Row should call {@link #free()} to free it memory after done with, otherwise will lead to memory leak.
 */
public class UTF8Row implements Row, Serializable {
    private static final Logger logger = LoggerFactory.getLogger(UTF8Row.class);

    public static class Creator {
        // The orginal schema.
        private final List<ColumnSchema> originalSchema;
        // The schema we finally used to arrange our data.
        // The only difference is the ordering.
        // Use colIdToIndex and indexToColId to map between.
        private final List<ColumnSchema> finalSchema;

        private final byte[] tagField;
        private final byte[][] acceptTags;
        private final boolean acceptNoneTag;

        private final int ignoreStrategy;

        // Default values.
        private final long[] numDefaultValues;
        private final byte[][] strDefaultValues;

        // "fn" -> "fieldName"
        private final Map<ByteArrayWrapper, byte[]> aliasToNames;
        // columnName -> index
        private final Map<ByteArrayWrapper, Integer> nameToIndex;
        private final int columnCount;

        // colId -> type
        private final byte[] columnTypes;
        // index -> type
        private final byte[] indexTypes;
        // metric agg types.
        private final int[] indexAggTypes;

        private final boolean grouping;
        private final boolean hasDims;
        private final int dimCount;
        // map colId to real value index.
        private final int[] colIdToIndex;
        private final int[] indexToColId;

        // byte array holder, used to do map look up.
        private final ByteArrayWrapper cmpWrapper = new ByteArrayWrapper();

        // Buffers of current row. Store real value if it is a number, otherwise an offset|len to real value.
        private final long[] valuesBuffer;
        // Faster fill zeros to valuesBuffer.
        private final long[] zeroValues;
        // Store those raw values, like the real content of string.
        private final byte[] rawValuesBuffer;

        // buffers for current parsing row.
        private boolean curRowHasTagField;
        private boolean curRowIsTagField;
        private boolean curRowIgnore;

        private int curRowIndex;
        private int curRowRawValueBytes;
        // Used to mark which column has been set.
        private boolean[] curRowSetMark;

        private long nextRowId = Long.MIN_VALUE;

        public Creator(boolean grouping,
                       List<ColumnSchema> columnSchemas,
                       List<String> dims,
                       List<Metric> metrics,
                       Map<String, String> nameToAlias,
                       TagSetting tagSetting,
                       int ignoreStrategy) {
            Trick.notRepeated(columnSchemas, ColumnSchema::getName);
            Trick.notRepeated(dims, d -> d);
            Trick.notRepeated(metrics, m -> m.name);
            if (dims != null && metrics != null) {
                Trick.notRepeated(Trick.concatToList(dims, Lists.transform(metrics, m -> m.name)), s -> s);
            }

            this.ignoreStrategy = ignoreStrategy;

            this.originalSchema = columnSchemas;
            this.columnCount = columnSchemas.size();

            this.nameToIndex = new HashMap<>(columnCount);
            this.columnTypes = new byte[columnCount];
            this.indexTypes = new byte[columnCount];
            this.numDefaultValues = new long[columnCount];
            this.strDefaultValues = new byte[columnCount][];
            this.hasDims = dims != null && dims.size() != 0;
            this.dimCount = dims == null ? 0 : dims.size();
            this.grouping = grouping;

            if (grouping) {
                Preconditions.checkState(hasDims, "We need dims if grouping is enable!");
            } else {
                Preconditions.checkState(metrics == null || metrics.size() == 0, "Metrics are useless when not grouping.");
            }

            for (int i = 0; i < columnCount; i++) {
                ColumnSchema cs = columnSchemas.get(i);
                columnTypes[i] = cs.getDataType();
            }

            if (nameToAlias != null && !nameToAlias.isEmpty()) {
                this.aliasToNames = new HashMap<>(nameToAlias.size());
                for (Map.Entry<String, String> a : nameToAlias.entrySet()) {
                    byte[] name = UTF8Util.toUtf8(a.getKey());
                    byte[] alias = UTF8Util.toUtf8(a.getValue());
                    this.aliasToNames.put(new ByteArrayWrapper(alias), name);
                }
            } else {
                this.aliasToNames = null;
            }

            if (tagSetting != null) {
                this.tagField = UTF8Util.toUtf8(tagSetting.tagField);
                this.acceptTags = new byte[tagSetting.acceptTags.size()][];
                this.acceptNoneTag = tagSetting.acceptNone;
                for (int i = 0; i < tagSetting.acceptTags.size(); i++) {
                    this.acceptTags[i] = UTF8Util.toUtf8(tagSetting.acceptTags.get(i).trim());
                }
            } else {
                this.tagField = null;
                this.acceptTags = null;
                this.acceptNoneTag = false;
            }

            if (hasDims) {
                // If dims exists, we reorder the column, put dimensions before all metrics.

                Map<String, ColumnSchema> schemaMap = new HashMap<>(columnCount);
                for (ColumnSchema cs : columnSchemas) {
                    schemaMap.put(cs.getName(), cs);
                }
                List<ColumnSchema> reorderingColumnSchemas = new ArrayList<>(columnCount);
                // Put dims first.
                for (String dim : dims) {
                    ColumnSchema columnSchema = schemaMap.remove(dim);
                    Preconditions.checkState(columnSchema != null, "Dim not found : " + dim);
                    reorderingColumnSchemas.add(columnSchema);
                }
                // Now put metrics.
                if (grouping) {
                    if (metrics != null) {
                        for (Metric metric : metrics) {
                            ColumnSchema columnSchema = schemaMap.remove(metric.name);
                            Preconditions.checkState(columnSchema != null, "Metric not found : " + metric.name);
                            reorderingColumnSchemas.add(columnSchema);
                            Preconditions.checkState(ColumnType.isNumber(columnSchema.getDataType()),
                                    "metric field should be a number: " + metric.name);
                        }
                    }
                } else {
                    for (ColumnSchema columnSchema : schemaMap.values()) {
                        reorderingColumnSchemas.add(columnSchema);
                    }
                }

                if (reorderingColumnSchemas.size() != columnCount) {
                    for (ColumnSchema cs : columnSchemas) {
                        String name = cs.getName();
                        if (Trick.indexWhere(reorderingColumnSchemas, c -> Strings.equals(c.getName(), name)) < 0) {
                            throw new IllegalStateException("Field must either be a dim or metric: " + name);
                        }
                    }
                    throw new IllegalStateException(String.format("Illegal setting. grouping: %s, columns: %s, dims: %s, metrics: %s",
                            grouping, columnSchemas, dims, metrics));
                }

                this.colIdToIndex = new int[columnCount];
                this.indexToColId = new int[columnCount];
                if (grouping) {
                    this.indexAggTypes = new int[columnCount];
                } else {
                    this.indexAggTypes = null;
                }
                for (int colId = 0; colId < columnCount; colId++) {
                    String cn = columnSchemas.get(colId).getName();
                    int index = Trick.indexWhere(reorderingColumnSchemas, sc -> Strings.equals(sc.getName(), cn));
                    Preconditions.checkState(index >= 0);
                    colIdToIndex[colId] = index;
                    indexToColId[index] = colId;
                    // Dimension don't have agg method.
                    if (grouping) {
                        indexAggTypes[index] = index < dimCount ? 0 : metrics.get(index - dimCount).aggType;
                    }
                }

                // Use new ordering.
                this.finalSchema = reorderingColumnSchemas;
            } else {
                int[] itself = new int[columnCount];
                for (int i = 0; i < columnCount; i++) {
                    itself[i] = i;
                }
                this.colIdToIndex = itself;
                this.indexToColId = itself;

                this.indexAggTypes = null;
                this.finalSchema = columnSchemas;
            }

            int bufferSize = 0;
            for (int i = 0; i < columnCount; i++) {
                ColumnSchema cs = finalSchema.get(i);
                byte type = cs.getDataType();
                indexTypes[i] = type;
                numDefaultValues[i] = cs.getDefaultNumberValue();
                strDefaultValues[i] = UTF8Util.toUtf8(cs.getDefaultStringValue());

                bufferSize += ColumnType.bufferSize(type);
                byte[] utf8Name = UTF8Util.toUtf8(cs.getName());
                nameToIndex.put(new ByteArrayWrapper(utf8Name), i);
            }

            this.valuesBuffer = new long[columnCount];
            this.zeroValues = new long[columnCount];
            this.rawValuesBuffer = new byte[bufferSize];
        }

        private UTF8Row buildRow() {
            if (curRowIgnore) {
                return null;
            }
            if (tagField != null && !acceptNoneTag && !curRowHasTagField) {
                // if accept tags exists but tag field in this event not found,
                // then this event should be ignored.
                return null;
            }
            switch (ignoreStrategy) {
                case EventIgnoreStrategy.IGNORE_EMPTY:
                    if (isEmpty()) {
                        return null;
                    }
                    break;
                default:
                    // Do nothing.
                    break;
            }
            // Set default value.
            for (int index = 0; index < columnCount; index++) {
                if (curRowSetMark[index]) {
                    continue;
                }
                onColumnIndex(index);
                switch (indexTypes[index]) {
                    case ColumnType.STRING:
                        onStringValue(strDefaultValues[index]);
                        break;
                    default:
                        onNumberValue(numDefaultValues[index]);
                }
            }

            // Now move values from buffer into curRow.
            int totalRowSize = (columnCount << 3) + curRowRawValueBytes;

            // Use off-heap memory to store row data.
            // We don't want those rows stored in JVM headp as they can put too much pressure on GC.
            // Besides those rows' lifecycle can be easily managed.
            long rowDataAddr = MemoryUtil.allocate(totalRowSize);
            if (hasDims) {
                // Put dims values and raw values together, convient for equal check.
                int dimValueSize = dimCount << 3;
                Platform.copyMemory(valuesBuffer, LONG_ARRAY_OFFSET, null, rowDataAddr, dimValueSize);

                int curRawDataOffset = dimValueSize;
                for (int dimId = 0; dimId < dimCount; dimId++) {
                    byte type = indexTypes[dimId];
                    if (type == ColumnType.STRING) {
                        long offsetAndLen = MemoryUtil.getLong(rowDataAddr + (dimId << 3));
                        int offset = (int) (offsetAndLen >>> 32);
                        int len = (int) offsetAndLen;
                        int newOffset = curRawDataOffset;
                        curRawDataOffset += len;

                        // Copy raw data into row data.
                        Platform.copyMemory(rawValuesBuffer, BYTE_ARRAY_OFFSET + offset, null, rowDataAddr + newOffset, len);
                        // The raw data is moved, we need to specify the new offset and len.
                        MemoryUtil.setLong(rowDataAddr + (dimId << 3), (((long) newOffset) << 32) | (long) len);
                    } else {
                        // Number value doesn't have raw value.
                    }
                }
                int dimDataSize = curRawDataOffset;

                // Now handle metric values.
                int metricCount = (columnCount - dimCount);
                int metricValueSize = metricCount << 3;
                Platform.copyMemory(valuesBuffer, LONG_ARRAY_OFFSET + dimValueSize, null, rowDataAddr + dimDataSize, metricValueSize);

                curRawDataOffset += metricValueSize;
                for (int metricId = 0; metricId < metricCount; metricId++) {
                    byte type = indexTypes[dimCount + metricId];
                    if (type == ColumnType.STRING) {
                        long offsetAndLen = MemoryUtil.getLong(rowDataAddr + dimDataSize + (metricId << 3));
                        int offset = (int) (offsetAndLen >>> 32);
                        int len = (int) offsetAndLen;
                        int newOffset = curRawDataOffset;
                        curRawDataOffset += len;

                        // Copy raw data into row data.
                        Platform.copyMemory(rawValuesBuffer, BYTE_ARRAY_OFFSET + offset, null, rowDataAddr + newOffset, len);
                        // The raw data is moved, we need to specify the new offset and len.
                        MemoryUtil.setLong(rowDataAddr + dimDataSize + (metricId << 3), (((long) newOffset) << 32) | (long) len);
                    } else {
                        // Number value doesn't have raw value.
                    }
                }

                long code = grouping ? 0 : nextRowId++;
                return new UTF8Row(code, this, rowDataAddr, totalRowSize, dimDataSize);
            } else {
                int valueSize = columnCount << 3;
                Platform.copyMemory(valuesBuffer, LONG_ARRAY_OFFSET, null, rowDataAddr, valueSize);

                int curRawDataOffset = valueSize;
                for (int index = 0; index < columnCount; index++) {
                    byte type = indexTypes[index];
                    if (type == ColumnType.STRING) {
                        long offsetAndLen = MemoryUtil.getLong(rowDataAddr + (index << 3));
                        int offset = (int) (offsetAndLen >>> 32);
                        int len = (int) offsetAndLen;
                        int newOffset = curRawDataOffset;
                        curRawDataOffset += len;

                        // Copy raw data into row data.
                        Platform.copyMemory(rawValuesBuffer, BYTE_ARRAY_OFFSET + offset, null, rowDataAddr + newOffset, len);
                        // The raw data is moved, we need to specify the new offset and len.
                        MemoryUtil.setLong(rowDataAddr + (index << 3), (((long) newOffset) << 32) | (long) len);
                    } else {
                        // Number value doesn't have raw value.
                    }
                }

                return new UTF8Row(nextRowId++, this, rowDataAddr, totalRowSize, 0);
            }
        }

        private boolean isEmpty() {
            boolean setDim = false;
            boolean setMetric = false;

            if (hasDims) {
                for (int i = 0; i < dimCount; i++) {
                    if (curRowSetMark[i]) {
                        setDim = true;
                        break;
                    }
                }
                if (!setDim) {
                    return true;
                }
            }
            if (grouping) {
                for (int i = dimCount; i < columnCount; i++) {
                    if (curRowSetMark[i]) {
                        setMetric = true;
                        break;
                    }
                }
                if (!setMetric) {
                    return true;
                }
            }
            if (setDim | setMetric) {
                return false;
            }
            for (boolean b : curRowSetMark) {
                if (b) {
                    return false;
                }
            }
            return true;
        }

        public void startRow() {
            // Clear buffers.
            if (columnCount < 12) {
                Arrays.fill(valuesBuffer, 0);
            } else {
                System.arraycopy(zeroValues, 0, valuesBuffer, 0, columnCount);
            }

            curRowHasTagField = false;
            curRowIsTagField = false;
            curRowIgnore = false;

            curRowRawValueBytes = 0;
            curRowIndex = -1;
            curRowSetMark = new boolean[columnCount];
        }

        public UTF8Row endRow() {
            return buildRow();
        }

        public byte onColumnUTF8Name(ByteBuffer key, int size) {
            assert key.hasArray() && key.arrayOffset() == 0;

            if (curRowIgnore) {
                return -1;
            }

            byte[] arr = key.array();
            int offset = key.position();

            if (!curRowHasTagField
                    && tagField != null
                    && size == tagField.length
                    && ByteArrayMethods.arrayEquals(arr, BYTE_ARRAY_OFFSET + offset, tagField, BYTE_ARRAY_OFFSET, size)) {
                // The tag field always be string.
                curRowIsTagField = true;
                curRowHasTagField = true;
                return UTF8JsonDeserializer.STRING;
            }

            cmpWrapper.set(arr, offset, size);
            Integer index = nameToIndex.get(cmpWrapper);
            if (index == null && aliasToNames != null) {
                // The name in json could be alias.
                byte[] realName = aliasToNames.get(cmpWrapper);
                if (realName != null) {
                    cmpWrapper.set(realName);
                    index = nameToIndex.get(cmpWrapper);
                }
            }
            onColumnIndex(index == null ? -1 : index);
            if (index == null) {
                return -1;
            } else {
                return indexTypes[index];
            }
        }

        public void onColumnIndex(int index) {
            curRowIsTagField = false;
            curRowIndex = index;
        }

        public void onColumnId(int id) {
            onColumnIndex(colIdToIndex[id]);
        }

        private void putValue(int index, long value) {
            assert index >= 0;
            valuesBuffer[index] = value;
            curRowSetMark[index] = true;
        }

        /**
         * Take size of bytes from buffer and move forward pos.
         */
        public boolean onStringValue(ByteBuffer buffer, int size) {
            if (!buffer.isDirect()) {
                return onStringValue(buffer.array(), buffer.position(), size);
            }
            if (size > ColumnType.MAX_STRING_UTF8_SIZE) {
                logger.warn("string size overflow, expected less than {}, got {}", ColumnType.MAX_STRING_UTF8_SIZE, size);
                return false;
            }
            long addr = MemoryUtil.getAddress(buffer);
            int pos = buffer.position();
            boolean ok = onStringValue(addr + pos, size);
            buffer.position(pos + size);
            return ok;
        }

        public boolean onStringValue(long addr, int size) {
            if (curRowIsTagField) {
                boolean ok = false;
                for (byte[] tag : acceptTags) {
                    if (UTF8Util.containsCommaSep(null, addr, size, tag)) {
                        ok = true;
                        break;
                    }
                }
                if (!ok) {
                    curRowIgnore = true;
                }
                return true;
            }

            int rawValueOffset = curRowRawValueBytes;
            putValue(curRowIndex, (((long) rawValueOffset) << 32) | (long) size);

            Platform.copyMemory(
                    null, addr,
                    rawValuesBuffer, BYTE_ARRAY_OFFSET + rawValueOffset,
                    size);

            curRowRawValueBytes += size;
            return true;
        }

        public boolean onStringValue(byte[] value) {
            return onStringValue(value, 0, value.length);
        }

        public boolean onStringValue(byte[] value, int offset, int size) {
            if (size > ColumnType.MAX_STRING_UTF8_SIZE) {
                logger.warn("string size overflow, expected less than {}, got {}", ColumnType.MAX_STRING_UTF8_SIZE, size);
                return false;
            }

            if (curRowIsTagField) {
                boolean ok = false;
                for (byte[] tag : acceptTags) {
                    if (UTF8Util.containsCommaSep(value, offset, size, tag)) {
                        ok = true;
                        break;
                    }
                }
                if (!ok) {
                    curRowIgnore = true;
                }
                return true;
            }

            int rawValueOffset = curRowRawValueBytes;
            putValue(curRowIndex, (((long) rawValueOffset) << 32) | (long) size);
            Platform.copyMemory(
                    value, BYTE_ARRAY_OFFSET + offset,
                    rawValuesBuffer, BYTE_ARRAY_OFFSET + rawValueOffset, size);
            curRowRawValueBytes += size;
            return true;
        }

        public boolean onNumberValue(long value) {
            putValue(curRowIndex, value);
            return true;
        }

        public boolean onIntValue(int value) {
            putValue(curRowIndex, value);
            return true;
        }

        public boolean onLongValue(long value) {
            putValue(curRowIndex, value);
            return true;
        }

        public boolean onFloatValue(float value) {
            putValue(curRowIndex, Double.doubleToRawLongBits((double) value));
            return true;
        }

        public boolean onDoubleValue(double value) {
            putValue(curRowIndex, Double.doubleToRawLongBits(value));
            return true;
        }

        public UTF8Row deserialize(ByteBuffer byteBuffer, int size) {
            int pos = byteBuffer.position();
            startRow();
            while (byteBuffer.position() < pos + size) {
                int colId = byteBuffer.getShort();
                byte type = columnTypes[colId];
                onColumnIndex(colIdToIndex[colId]);
                switch (type) {
                    case ColumnType.INT:
                        onIntValue(byteBuffer.getInt());
                        break;
                    case ColumnType.LONG:
                        onLongValue(byteBuffer.getLong());
                        break;
                    case ColumnType.FLOAT:
                        onFloatValue(byteBuffer.getFloat());
                        break;
                    case ColumnType.DOUBLE:
                        onDoubleValue(byteBuffer.getDouble());
                        break;
                    case ColumnType.STRING:
                        int valueSize = byteBuffer.getShort() & 0xFFFF;
                        onStringValue(byteBuffer, valueSize);
                        break;
                    default:
                        throw new IllegalStateException("column type " + type + " is illegal");
                }
            }
            return endRow();
        }
    }

    private final Creator creator;
    private long rowDataAddr;
    private final int rowDataSize;
    private final int dimDataSize;

    // A unique number.
    // This is used to stop comparator return 0 if grouping is disable.
    private final long code;

    private UTF8Row(long code, Creator creator, long rowDataAddr, int rowDataSize, int dimDataSize) {
        this.code = code;
        this.creator = creator;
        this.rowDataAddr = rowDataAddr;
        this.rowDataSize = rowDataSize;
        this.dimDataSize = dimDataSize;
    }

    public void free() {
        if (rowDataAddr != 0) {
            MemoryUtil.free(rowDataAddr);
            rowDataAddr = 0;
        }
    }

    /**
     * The aproximately memeory usage of this object. Currently only return the raw row data size.
     */
    public int memoryUsage() {
        return rowDataSize;
    }

    @Override
    public int hashCode() {
        throw new IllegalStateException("Should not call this method!");
    }

    @Override
    public boolean equals(Object obj) {
        throw new IllegalStateException("Should not call this method!");
    }

    /*
     * This compare method does not actually sort the rows by the real values, but by raw bytes.
     * It only guarrantee the consistent of comparation. i.e. a >= b, b >= c -> a >= c.
     */
    public static Comparator<UTF8Row> dimBytesComparator() {
        return (r1, r2) -> {
            if ((r1 == null || r2 == null)
                    || (r1.rowDataAddr == 0 || r2.rowDataAddr == 0)) {
                throw new IllegalStateException("illegal row compare");
            }
            assert r1.creator.dimCount == r2.creator.dimCount;

            int len = Math.min(r1.dimDataSize, r2.dimDataSize);
            if (len == 0) {
                return Long.compare(r1.code, r2.code);
            }

            int dimCount = r1.creator.dimCount;
            long rowDataAddr1 = r1.rowDataAddr;
            long rowDataAddr2 = r2.rowDataAddr;

            int res;
            long word1, word2;
            for (int i = 0; i < dimCount; i++) {
                word1 = MemoryUtil.getLong(rowDataAddr1 + (i << 3));
                word2 = MemoryUtil.getLong(rowDataAddr2 + (i << 3));
                if (r1.creator.indexTypes[i] == ColumnType.STRING) {
                    int offset1 = (int) (word1 >>> 32);
                    int len1 = (int) word1 & ColumnType.MAX_STRING_UTF8_SIZE_MASK;

                    int offset2 = (int) (word2 >>> 32);
                    int len2 = (int) word2 & ColumnType.MAX_STRING_UTF8_SIZE_MASK;

                    res = compareBytes(rowDataAddr1 + offset1, len1, rowDataAddr2 + offset2, len2);
                } else {
                    res = Long.compare(word1, word2);
                }
                if (res != 0) {
                    return res;
                }
            }

            // If we don't do grouping we should never let it return zero.
            return Long.compare(r1.code, r2.code);
        };
    }

    private static int compareBytes(long addr1, int len1, long addr2, int len2) {
        int len = Math.min(len1, len2);
        int res;

        long word1, word2;
        int wordLen = len & 0xFFFF_FFF8;
        for (int i = 0; i < wordLen; i += 8) {
            word1 = MemoryUtil.getLong(addr1 + i);
            word2 = MemoryUtil.getLong(addr2 + i);
            res = Long.compare(word1, word2);
            if (res != 0) {
                return res;
            }
        }

        if ((len & 0x07) != 0) {
            long tail1 = 0;
            long tail2 = 0;
            for (int i = wordLen; i < len; i++) {
                tail1 = (tail1 << 8) | (MemoryUtil.getByte(addr1 + i) & 0xFF);
                tail2 = (tail2 << 8) | (MemoryUtil.getByte(addr2 + i) & 0xFF);
            }
            res = Long.compare(tail1, tail2);
            if (res != 0) {
                return res;
            }
        }

        return len1 - len2;
    }

    @Override
    public String toString() {
        int colId = 0;
        StringBuilder sb = new StringBuilder();
        sb.append('{');
        for (ColumnSchema cs : creator.originalSchema) {
            sb.append('\"').append(cs.getName()).append("\": ");
            if (cs.getDataType() == ColumnType.STRING) {
                sb.append('\"').append(getDisplayString(colId, cs.getDataType())).append('\"');
            } else {
                sb.append(getDisplayString(colId, cs.getDataType()));
            }
            colId++;
            if (colId < creator.columnCount) {
                sb.append(',');
            }
        }
        sb.append('}');
        return sb.toString();
    }

    @Override
    public boolean serialize(ByteBuffer byteBuffer) {
        if (byteBuffer.capacity() < rowDataSize + 4 + (creator.columnCount << 1)) {
            // Fast way to detect whether the remaining cap can hold the serialize data or not.
            // We don't need to be exactualy acurrate.
            return false;
        }

        int sizePos = byteBuffer.position();
        // put size place holder.
        byteBuffer.putInt(0);

        int size = 0;
        for (int colId = 0; colId < creator.columnCount; colId++) {
            byteBuffer.putShort((short) colId);
            size += 2;
            // Store in real type instead of long to reduce size.
            byte type = creator.columnTypes[colId];
            switch (type) {
                case ColumnType.INT:
                    byteBuffer.putInt(getInt(colId));
                    size += 4;
                    break;
                case ColumnType.LONG:
                    byteBuffer.putLong(getLong(colId));
                    size += 8;
                    break;
                case ColumnType.FLOAT:
                    byteBuffer.putFloat(getFloat(colId));
                    size += 4;
                    break;
                case ColumnType.DOUBLE:
                    byteBuffer.putDouble(getDouble(colId));
                    size += 8;
                    break;
                case ColumnType.STRING:
                    byte[] bytes = getRaw(colId);
                    byteBuffer.putShort((short) bytes.length);
                    byteBuffer.put(bytes);
                    size += 2 + bytes.length;
                    break;
                default:
                    throw new IllegalStateException("column type " + type + " is illegal");
            }
        }
        // Put the size info before.
        byteBuffer.putInt(sizePos, size);
        return true;
    }

    private long numValue(int colId) {
        if (rowDataAddr == 0) {
            throw new IllegalStateException("illegal row");
        }
        int offset = 0;
        if (creator.hasDims) {
            int realIndex = creator.colIdToIndex[colId];
            if (realIndex < creator.dimCount) {
                // A dim.
                offset = realIndex << 3;
            } else {
                // A metric.
                int metricIndex = realIndex - creator.dimCount;
                offset = dimDataSize + (metricIndex << 3);
            }
        } else {
            offset = colId << 3;
        }
        return MemoryUtil.getLong(rowDataAddr + offset);
    }

    private long strOffsetLen(int colId) {
        return numValue(colId);
    }

    public void merge(UTF8Row other) {
        assert creator.hasDims;
        assert creator.indexAggTypes != null;
        assert dimDataSize == other.dimDataSize;
        if (rowDataAddr == 0) {
            throw new IllegalStateException("illegal row");
        }

        for (int index = creator.dimCount; index < creator.columnCount; index++) {
            byte type = creator.indexTypes[index];
            int aggType = creator.indexAggTypes[index];
            int metricIndex = index - creator.dimCount;
            long offset = dimDataSize + (metricIndex << 3);
            long value1 = MemoryUtil.getLong(rowDataAddr + offset);
            long value2 = MemoryUtil.getLong(other.rowDataAddr + offset);
            long newValue = AggType.agg(aggType, type, value1, value2);
            MemoryUtil.setLong(rowDataAddr + offset, newValue);
        }
    }

    // ===================================
    // implement interfaces
    // ===================================

    @Override
    public long getUniformValue(int colId, byte type) {
        return numValue(colId);
    }

    @Override
    public int getInt(int colId) {
        return (int) numValue(colId);
    }

    @Override
    public long getLong(int colId) {
        return numValue(colId);
    }

    @Override
    public float getFloat(int colId) {
        return (float) Double.longBitsToDouble(numValue(colId));
    }

    @Override
    public double getDouble(int colId) {
        return Double.longBitsToDouble(numValue(colId));
    }

    @Override
    public UTF8String getString(int colId) {
        if (rowDataAddr == 0) {
            throw new IllegalStateException("illegal row");
        }
        long offsetAndLen = strOffsetLen(colId);
        int offset = (int) (offsetAndLen >>> 32);
        int len = (int) offsetAndLen & ColumnType.MAX_STRING_UTF8_SIZE_MASK;
        assert len >= 0;
        if (len == 0) {
            return UTF8String.EMPTY_UTF8;
        } else {
            return UTF8String.fromAddress(null, rowDataAddr + offset, len);
        }
    }

    @Override
    public void getRaw(int colId, BytePiece bytes) {
        if (rowDataAddr == 0) {
            throw new IllegalStateException("illegal row");
        }
        long offsetAndLen = strOffsetLen(colId);
        int offset = (int) (offsetAndLen >>> 32);
        int len = (int) offsetAndLen & ColumnType.MAX_STRING_UTF8_SIZE_MASK;
        bytes.base = null;
        bytes.addr = rowDataAddr + offset;
        bytes.len = len;
    }

    @Override
    public byte[] getRaw(int colId) {
        if (rowDataAddr == 0) {
            throw new IllegalStateException("illegal row");
        }
        long offsetAndLen = strOffsetLen(colId);
        int offset = (int) (offsetAndLen >>> 32);
        int len = (int) offsetAndLen & ColumnType.MAX_STRING_UTF8_SIZE_MASK;
        byte[] bytes = new byte[len];
        Platform.copyMemory(null, rowDataAddr + offset, bytes, BYTE_ARRAY_OFFSET, len);
        return bytes;
    }
}
