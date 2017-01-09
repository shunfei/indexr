package io.indexr.segment;

import java.io.Closeable;
import java.io.IOException;

/**
 * A segment is a part of a table which contains many rows.
 * 
 * It is very much like a file in normal file system, though it could be consisted by many files.
 * A segment should be closed after done processing with it by calling {@link #close()}.
 */
public interface Segment extends InfoSegment, Closeable {
    /**
     * Get the column by colId, i.e. ordinal. Return null if it dosen't support full column based operation.
     * 
     * Normally you should not cache the Column for a long time.
     */
    default Column column(int colId) {
        return null;
    }

    /**
     * @deprecated Don't use it!
     */
    default RowTraversal rowTraversal(long offset, long count) {
        throw new UnsupportedOperationException();
    }

    default RowTraversal rowTraversal() {
        return rowTraversal(0, rowCount());
    }

    @Override
    default void close() throws IOException {}
}
