package io.indexr.segment;

import java.io.IOException;

import io.indexr.segment.pack.ColumnNode;
import io.indexr.segment.pack.DataPackNode;

public class CachedSegment implements Segment {
    private Segment segment;
    private CachedColumn[] columns;

    public CachedSegment(Segment segment) {
        this.segment = segment;
        this.columns = new CachedColumn[segment.schema().getColumns().size()];
    }

    @Override
    public String name() {
        return segment.name();
    }

    @Override
    public SegmentSchema schema() {
        return segment.schema();
    }

    @Override
    public long rowCount() {
        return segment.rowCount();
    }

    @Override
    public int packCount() {
        return segment.packCount();
    }

    @Override
    public boolean isColumned() {
        return segment.isColumned();
    }

    @Override
    public ColumnNode columnNode(int colId) throws IOException {
        return segment.columnNode(colId);
    }

    @Override
    public boolean isRealtime() {
        return segment.isRealtime();
    }

    @Override
    public Column column(int colId) {
        if (columns[colId] == null) {
            columns[colId] = new CachedColumn(segment.column(colId));
        }
        return columns[colId];
    }

    @Override
    public void close() throws IOException {
        segment.close();
    }

    @Override
    public RowTraversal rowTraversal(long offset, long count) {
        return segment.rowTraversal(offset, count);
    }

    @Override
    public RowTraversal rowTraversal() {
        return segment.rowTraversal();
    }

    private static class CachedColumn implements Column {
        private Column column;
        private DPValues[] dpValues;
        private RSIndex rsIndex;
        private PackExtIndex[] extIndices;

        public CachedColumn(Column column) {
            this.column = column;
            this.dpValues = new DPValues[column.packCount()];
            this.extIndices = new PackExtIndex[column.packCount()];
        }

        @Override
        public String name() {
            return column.name();
        }

        @Override
        public SQLType sqlType() {
            return column.sqlType();
        }

        @Override
        public int packCount() {
            return column.packCount();
        }

        @Override
        public long rowCount() throws IOException {
            return column.rowCount();
        }

        @Override
        public DataPackNode dpn(int packId) throws IOException {
            return column.dpn(packId);
        }

        @Override
        public DPValues pack(int packId) throws IOException {
            if (dpValues[packId] == null) {
                dpValues[packId] = column.pack(packId);
            }
            return dpValues[packId];
        }

        @Override
        public RSIndex rsIndex() throws IOException {
            if (rsIndex == null) {
                rsIndex = column.rsIndex();
            }
            return rsIndex;
        }

        @Override
        public PackExtIndex extIndex(int packId) throws IOException {
            if (extIndices[packId] == null) {
                extIndices[packId] = column.extIndex(packId);
            }
            return extIndices[packId];
        }
    }
}
