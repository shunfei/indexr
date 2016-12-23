package io.indexr.segment.pack;

import io.indexr.segment.InfoSegment;
import io.indexr.segment.SegmentSchema;

/**
 * Simple implementation of InfoSegment.
 */
public class StorageInfoSegment implements InfoSegment {
    final String name;
    final long rowCount;
    final SegmentSchema schema;
    final ColumnNode[] columnNodes;

    public StorageInfoSegment(String name, long rowCount, SegmentSchema schema, ColumnNode[] columnNodes) {
        this.name = name;
        this.rowCount = rowCount;
        this.schema = schema;
        this.columnNodes = columnNodes;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public SegmentSchema schema() {
        return schema;
    }

    @Override
    public long rowCount() {
        return rowCount;
    }

    @Override
    public ColumnNode columnNode(int colId) {
        return columnNodes[colId];
    }

    @Override
    public boolean isColumned() {
        return true;
    }

    @Override
    public int packCount() {
        return DataPack.rowCountToPackCount(rowCount);
    }
}
