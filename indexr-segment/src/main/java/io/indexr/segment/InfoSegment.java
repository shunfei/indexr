package io.indexr.segment;

import java.io.IOException;

import io.indexr.segment.storage.ColumnNode;

/**
 * An info segment contains basic info of a real segment.
 */
public interface InfoSegment {

    int version();

    SegmentMode mode();

    /**
     * Name of the segment. Unique in the whole system.
     */
    String name();

    /**
     * Schema of the segment.
     */
    SegmentSchema schema();

    /**
     * Row count.
     */
    long rowCount();

    /**
     * Get the pack count of this segment, return 0 if {@link #isColumned()} is false.
     */
    default int packCount() {
        return 0;
    }

    /**
     * Indicate whether this segment is column based or not.
     */
    default boolean isColumned() {
        return false;
    }

    /**
     * Get the basic predicate info of one column.
     */
    default ColumnNode columnNode(int colId) throws IOException {
        return null;
    }

    /**
     * It is a realtime segment or not.
     */
    default boolean isRealtime() {
        return false;
    }
}
