package io.indexr.segment.storage;

import java.io.IOException;

import io.indexr.data.Freeable;
import io.indexr.segment.Column;
import io.indexr.segment.PackExtIndex;
import io.indexr.segment.RSIndex;
import io.indexr.segment.RowTraversal;
import io.indexr.segment.SQLType;
import io.indexr.segment.Segment;
import io.indexr.segment.SegmentMode;
import io.indexr.segment.SegmentSchema;
import io.indexr.segment.pack.DataPack;
import io.indexr.segment.pack.DataPackNode;

public class CachedSegment implements Segment, Freeable {
    private Segment segment;
    private CachedColumn[] columns;

    public CachedSegment(Segment segment) {
        this.segment = segment;
        this.columns = new CachedColumn[segment.schema().getColumns().size()];
    }

    @Override
    public int version() {
        return segment.version();
    }

    @Override
    public SegmentMode mode() {
        return segment.mode();
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
        free();
        if (segment != null) {
            segment.close();
            segment = null;
        }
    }

    /**
     * Free the cache content before die.
     */
    @Override
    public void free() {
        // Hack!
        // We don't free index for now.
        if (columns != null) {
            for (CachedColumn c : columns) {
                if (c == null) {
                    continue;
                }
                for (DataPack pack : c.packs) {
                    if (pack != null) pack.free();
                }
                for (PackExtIndex extIndex : c.extIndices) {
                    if (extIndex != null) extIndex.free();
                }
            }
            columns = null;
        }
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
        private DataPack[] packs;
        private RSIndex rsIndex;
        private PackExtIndex[] extIndices;

        public CachedColumn(Column column) {
            this.column = column;
            this.packs = new DataPack[column.packCount()];
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
        public boolean isIndexed() {
            return column.isIndexed();
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
        public DataPack pack(int packId) throws IOException {
            if (packs[packId] == null) {
                packs[packId] = column.pack(packId);
            }
            return packs[packId];
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
