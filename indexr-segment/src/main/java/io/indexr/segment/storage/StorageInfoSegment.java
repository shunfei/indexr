package io.indexr.segment.storage;

import io.indexr.segment.InfoSegment;
import io.indexr.segment.SegmentMode;
import io.indexr.segment.SegmentSchema;
import io.indexr.segment.pack.DataPack;

/**
 * Simple implementation of InfoSegment.
 */
public class StorageInfoSegment implements InfoSegment {
    final int version;
    final SegmentMode mode;
    final String name;
    final long rowCount;
    final SegmentSchema schema;
    final ColumnNode[] columnNodes;

    public StorageInfoSegment(int version, SegmentMode mode, String name, long rowCount, SegmentSchema schema, ColumnNode[] columnNodes) {
        this.version = version;
        this.mode = mode;
        this.name = name;
        this.rowCount = rowCount;
        this.schema = schema;
        this.columnNodes = columnNodes;
    }

    @Override
    public int version() {
        return version;
    }

    @Override
    public SegmentMode mode() {
        return mode;
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

    public ColumnNode[] columnNodes() {
        return columnNodes;
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
