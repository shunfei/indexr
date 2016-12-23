package io.indexr.segment.pack;

import com.google.common.base.Preconditions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import io.indexr.segment.ColumnSchema;
import io.indexr.segment.Row;
import io.indexr.segment.RowTraversal;
import io.indexr.segment.Segment;
import io.indexr.segment.SegmentSchema;

public class StorageSegment<SC extends StorageColumn> implements Segment {
    // The max value an unsign short can count.
    public static final int MAX_PACK_COUNT = 0x01 << 16;
    public static final int PACK_ID_MASK = 0xFFFF;
    public static final int MAX_COLUMN_COUNT = 0x01 << 16;
    public static final int COLUMN_ID_MASK = 0xFFFF;
    public static final long MAX_ROW_COUNT = (long) DataPack.MAX_COUNT << 16;

    private final int version;
    final String name;
    final SegmentSchema segmentSchema;

    ColumnNode[] columnNodes;
    List<SC> columns;

    long rowCount;

    StorageSegment(int version, String name, SegmentSchema schema, long rowCount, StorageColumnCreator<SC> scCreator) throws IOException {
        Preconditions.checkArgument(schema.columns.size() <= MAX_COLUMN_COUNT,
                "Too many columns, limit is %s, current %s",
                MAX_COLUMN_COUNT, schema.columns.size());
        this.version = version;
        this.name = name;
        this.rowCount = rowCount;
        this.segmentSchema = schema;

        int colCount = this.segmentSchema.columns.size();
        this.columnNodes = new ColumnNode[colCount];
        this.columns = new ArrayList<>(colCount);

        int columnId = 0;
        for (ColumnSchema cs : this.segmentSchema.columns) {
            SC column = scCreator.create(columnId, cs, rowCount);
            columns.add(column);
            columnId++;
        }
    }

    @FunctionalInterface
    public static interface StorageColumnCreator<SC extends StorageColumn> {
        SC create(int columnId, ColumnSchema schema, long rowCount) throws IOException;
    }

    public int version() {
        return version;
    }

    List<SC> columns() {
        return columns;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public SegmentSchema schema() {
        return segmentSchema;
    }

    @Override
    public boolean isColumned() {
        return true;
    }

    @Override
    public long rowCount() {
        return rowCount;
    }

    @Override
    public int packCount() {
        return DataPack.rowCountToPackCount(rowCount);
    }

    @Override
    public SC column(int colId) {
        return columns.get(colId);
    }

    @Override
    public ColumnNode columnNode(int colId) throws IOException {
        ColumnNode cn = columnNodes[colId];
        if (cn != null) {
            return cn;
        }
        StorageColumn sc = columns.get(colId);
        return cn = columnNodes[colId] = ColumnNode.from(sc.getDPNs(), sc.dataType());
    }

    @Override
    public RowTraversal rowTraversal(long offset, long count) {
        DPRowSpliterator.DPLoader[] loaders = new DPRowSpliterator.DPLoader[columns.size()];
        for (int colId = 0; colId < columns.size(); colId++) {
            loaders[colId] = columns.get(colId).getDPLoader();
        }
        return new RowTraversal() {
            @Override
            public Stream<Row> stream() {
                return StreamSupport.stream(new DPRowSpliterator(offset, offset + count, loaders), false);
            }

            @Override
            public DPRowSpliterator iterator() {
                return new DPRowSpliterator(offset, offset + count, loaders);
            }
        };
    }

    @Override
    public void close() throws IOException {
        if (columns != null) {
            columns.forEach(StorageColumn::free);
            columns = null;
            columnNodes = null;
        }
    }

    @Override
    public String toString() {
        return String.format("%s{name=%s, rowCount=%d}", this.getClass().getSimpleName(), name, rowCount);
    }
}
